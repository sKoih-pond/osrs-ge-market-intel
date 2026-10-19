"""Raw object writes for bronze (ADR-0005, ADR-0015).

`R2Store` signs its own S3 requests (SigV4, standard library only) so that every request the
project's code sends to R2 passes through `RunBudget.charge` first, as ADR-0015 layer 4
requires. Writes are write-if-absent (`If-None-Match: *`), so a re-run never rewrites an
object and needs no HEAD request to find out. No code sets a storage class: every object is
Standard, the only class with a free tier.
"""

from __future__ import annotations

import hashlib
import hmac
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from ge_intel.cost_guard import RunBudget


class ObjectStore(Protocol):
    def put_if_absent(
        self, key: str, body: bytes, content_type: str, metadata: Mapping[str, str]
    ) -> bool:
        """Write `body` at `key` unless an object is already there. True if written."""


class LocalStore:
    """Filesystem stand-in for tests and local runs."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def put_if_absent(self, key, body, content_type, metadata) -> bool:
        path = self.root / key
        if path.exists():
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(body)
        tmp.replace(path)
        return True


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def sign_v4(
    method: str,
    url: str,
    headers: Mapping[str, str],
    payload_sha256: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    now: datetime,
    service: str = "s3",
) -> str:
    """Authorization header value for an AWS Signature Version 4 request.

    `headers` must already include `host`, `x-amz-date` and `x-amz-content-sha256`; every
    header passed is signed.
    """
    parts = urllib.parse.urlsplit(url)
    canonical_uri = urllib.parse.quote(parts.path or "/", safe="/~")
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    canonical_query = "&".join(
        f"{urllib.parse.quote(k, safe='~')}={urllib.parse.quote(v, safe='~')}"
        for k, v in sorted(query)
    )
    lowered = {k.lower(): " ".join(str(v).strip().split()) for k, v in headers.items()}
    signed = ";".join(sorted(lowered))
    canonical_headers = "".join(f"{k}:{lowered[k]}\n" for k in sorted(lowered))
    canonical_request = "\n".join(
        [method, canonical_uri, canonical_query, canonical_headers, signed, payload_sha256]
    )
    day = now.strftime("%Y%m%d")
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            now.strftime("%Y%m%dT%H%M%SZ"),
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    key = _hmac(("AWS4" + secret_access_key).encode(), day)
    for part in (region, service, "aws4_request"):
        key = _hmac(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    return (
        f"AWS4-HMAC-SHA256 Credential={access_key_id}/{scope},"
        f"SignedHeaders={signed},Signature={signature}"
    )


# (method, url, headers, body) -> HTTP status
HttpSend = Callable[[str, str, dict[str, str], bytes], int]


def _urllib_send(method: str, url: str, headers: dict[str, str], body: bytes) -> int:
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


class R2Store:
    def __init__(
        self,
        account_id: str,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        budget: RunBudget,
        send: HttpSend = _urllib_send,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        retries: int = 2,
    ):
        self.endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
        self.bucket = bucket
        self._kid = access_key_id
        self._secret = secret_access_key
        self.budget = budget
        self.send = send
        self.clock = clock
        self.retries = retries

    def put_if_absent(self, key, body, content_type, metadata) -> bool:
        url = f"{self.endpoint}/{self.bucket}/{urllib.parse.quote(key, safe='/~')}"
        payload_hash = hashlib.sha256(body).hexdigest()
        for attempt in range(self.retries + 1):
            now = self.clock()
            headers = {
                "host": urllib.parse.urlsplit(self.endpoint).netloc,
                "x-amz-date": now.strftime("%Y%m%dT%H%M%SZ"),
                "x-amz-content-sha256": payload_hash,
                "content-type": content_type,
                "if-none-match": "*",
                **{f"x-amz-meta-{k}": v for k, v in metadata.items()},
            }
            headers["authorization"] = sign_v4(
                "PUT", url, headers, payload_hash, self._kid, self._secret, "auto", now
            )
            self.budget.charge("PutObject", bytes_written=len(body))  # raises before sending
            status = self.send("PUT", url, headers, body)
            if status == 200:
                return True
            if status == 412:  # precondition failed: the object already exists
                return False
            if status < 500 or attempt == self.retries:
                raise RuntimeError(f"PUT {key} failed with HTTP {status}")
            time.sleep(2 ** (attempt + 1))
        raise AssertionError("unreachable")  # pragma: no cover
