"""OSRS Wiki real-time prices API v2 client (ADR-0005).

Etiquette, enforced here rather than left to callers:
- a descriptive User-Agent with the repo URL and a contact address; default library agents
  are refused before any request is sent;
- `Accept-Encoding: gzip` on every request;
- bucket endpoints only with explicit timestamps, and only for buckets that have closed;
- at most one request per second from this process.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ge_intel import __version__

BASE_URL = "https://prices.runescape.wiki/api/v2/osrs"
REPO_URL = "https://github.com/sKoih-pond/osrs-ge-market-intel"
CONTACT = "sylvester@kohstack.au"
USER_AGENT = f"osrs-ge-market-intel/{__version__} (+{REPO_URL}; {CONTACT})"

BUCKET_SECONDS = {"5m": 300, "1h": 3600}
# Buckets appear in the CDN 10 to 20 s after they close (discovery report 2.2).
PUBLICATION_DELAY = timedelta(seconds=60)
MIN_INTERVAL_S = 1.0

# Agents the Wiki blocks or that identify a library rather than a project.
_DEFAULT_AGENTS = re.compile(
    r"^(python-urllib|python-requests|python-httpx|aiohttp|curl|wget|go-http-client|java|"
    r"okhttp|axios|node-fetch|undici|libwww-perl|apache-httpclient|mozilla/5\.0)",
    re.IGNORECASE,
)


class ContractError(ValueError):
    """A response did not match the shape this pipeline relies on."""


def check_user_agent(user_agent: str | None) -> str:
    """Return the agent if it is descriptive; raise otherwise (never send a default agent)."""
    ua = (user_agent or "").strip()
    if not ua:
        raise ValueError("a descriptive User-Agent is required by the Wiki's API terms")
    if _DEFAULT_AGENTS.match(ua):
        raise ValueError(f"refusing default or browser User-Agent: {ua!r}")
    if "@" not in ua or "http" not in ua:
        raise ValueError("User-Agent must name the project URL and a contact address")
    return ua


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    body: bytes  # decompressed
    fetched_at: datetime

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()

    def json(self):
        return json.loads(self.body)


# (url, headers) -> (status, response headers, raw body)
Transport = Callable[[str, dict[str, str]], tuple[int, dict[str, str], bytes]]


def urllib_transport(url: str, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()


def bucket_is_closed(start: datetime, step: str, now: datetime) -> bool:
    return start + timedelta(seconds=BUCKET_SECONDS[step]) + PUBLICATION_DELAY <= now


class WikiClient:
    def __init__(
        self,
        user_agent: str = USER_AGENT,
        base_url: str = BASE_URL,
        transport: Transport = urllib_transport,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        min_interval_s: float = MIN_INTERVAL_S,
        sleep: Callable[[float], None] = time.sleep,
        retries: int = 3,
    ):
        self.user_agent = check_user_agent(user_agent)
        self.base_url = base_url.rstrip("/")
        self.transport = transport
        self.clock = clock
        self.min_interval_s = min_interval_s
        self.sleep = sleep
        self.retries = retries
        self.requests_sent = 0
        self._last_sent: float | None = None

    def _get(self, path: str) -> Response:
        url = f"{self.base_url}{path}"
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip"}
        for attempt in range(self.retries + 1):
            if self._last_sent is not None:
                wait = self.min_interval_s - (time.monotonic() - self._last_sent)
                if wait > 0:
                    self.sleep(wait)
            self._last_sent = time.monotonic()
            self.requests_sent += 1
            try:
                status, resp_headers, raw = self.transport(url, headers)
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == self.retries:
                    raise
                self.sleep(2 ** (attempt + 1))
                continue
            if status >= 500 or status == 429:
                if attempt == self.retries:
                    raise RuntimeError(f"GET {url} failed with HTTP {status}")
                self.sleep(2 ** (attempt + 1))
                continue
            if status != 200:
                raise RuntimeError(f"GET {url} failed with HTTP {status}")
            body = gzip.decompress(raw) if resp_headers.get("content-encoding") == "gzip" else raw
            return Response(url, status, body, self.clock())
        raise AssertionError("unreachable")  # pragma: no cover

    def bucket(self, step: str, start: datetime) -> Response:
        """One closed `/5m` or `/1h` bucket, requested by its start time."""
        if start.tzinfo is None:
            raise ValueError("bucket start must be timezone-aware (UTC)")
        ts = int(start.timestamp())
        if ts % BUCKET_SECONDS[step]:
            raise ValueError(f"{start.isoformat()} is not aligned to a {step} bucket")
        if not bucket_is_closed(start, step, self.clock()):
            raise ValueError(f"{step} bucket {start.isoformat()} has not closed and settled")
        return self._get(f"/{step}?timestamp={ts}")

    def latest(self) -> Response:
        return self._get("/latest")

    def mapping(self) -> Response:
        return self._get("/mapping")


# Parsing: strict about shape, so a source change fails loudly instead of corrupting tables.


def _int_or_none(value, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractError(f"{field} must be a non-negative integer or null, got {value!r}")
    return value


def _price_or_none(value, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ContractError(f"{field} must be a non-negative number or null, got {value!r}")
    return float(value)


def parse_bucket(payload: dict, expected_start: datetime) -> list[dict]:
    """Rows of a `/5m` or `/1h` payload; refuses a payload for a different bucket."""
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise ContractError("bucket payload must be an object with a 'data' object")
    if payload.get("timestamp") != int(expected_start.timestamp()):
        raise ContractError(
            f"asked for bucket {int(expected_start.timestamp())}, got {payload.get('timestamp')}"
        )
    rows = []
    for key, rec in payload["data"].items():
        if not key.isdigit() or not isinstance(rec, dict):
            raise ContractError(f"unexpected item entry {key!r}")
        row = {
            "item_id": int(key),
            "avg_high_price": _price_or_none(rec.get("avgHighPrice"), "avgHighPrice"),
            "high_volume": _int_or_none(rec.get("highPriceVolume", 0), "highPriceVolume") or 0,
            "avg_low_price": _price_or_none(rec.get("avgLowPrice"), "avgLowPrice"),
            "low_volume": _int_or_none(rec.get("lowPriceVolume", 0), "lowPriceVolume") or 0,
        }
        rows.append(row)
    return rows


def parse_latest(payload: dict) -> list[dict]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise ContractError("latest payload must be an object with a 'data' object")
    rows = []
    for key, rec in payload["data"].items():
        if not key.isdigit() or not isinstance(rec, dict):
            raise ContractError(f"unexpected item entry {key!r}")
        rows.append(
            {
                "item_id": int(key),
                "high": _int_or_none(rec.get("high"), "high"),
                "high_time": _int_or_none(rec.get("highTime"), "highTime"),
                "low": _int_or_none(rec.get("low"), "low"),
                "low_time": _int_or_none(rec.get("lowTime"), "lowTime"),
            }
        )
    return rows


MAPPING_FIELDS = ("name", "examine", "members", "lowalch", "highalch", "value", "limit", "icon")


def parse_mapping(payload: list) -> list[dict]:
    if not isinstance(payload, list) or not payload:
        raise ContractError("mapping payload must be a non-empty list")
    rows = []
    seen: set[int] = set()
    for rec in payload:
        if not isinstance(rec, dict) or not isinstance(rec.get("id"), int):
            raise ContractError(f"mapping entry without an integer id: {rec!r}")
        if not isinstance(rec.get("name"), str) or not rec["name"]:
            raise ContractError(f"mapping entry {rec['id']} has no name")
        if rec["id"] in seen:
            raise ContractError(f"duplicate item id {rec['id']} in mapping")
        seen.add(rec["id"])
        rows.append(
            {
                "item_id": rec["id"],
                "name": rec["name"],
                "examine": rec.get("examine"),
                "members": rec.get("members"),
                "lowalch": _int_or_none(rec.get("lowalch"), "lowalch"),
                "highalch": _int_or_none(rec.get("highalch"), "highalch"),
                "value": _int_or_none(rec.get("value"), "value"),
                "buy_limit": _int_or_none(rec.get("limit"), "limit"),
                "icon": rec.get("icon"),
            }
        )
    return rows
