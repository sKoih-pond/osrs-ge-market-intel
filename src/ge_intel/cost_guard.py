"""Keep R2 inside Cloudflare's free tier (ADR-0015).

Two controls:
- `evaluate` decides, before a run, whether month-to-date account usage leaves room to run.
  It fails closed: if usage cannot be read, the run does not start.
- `RunBudget` counts the R2 requests our own code makes during a run and refuses the
  request that would cross the run's cap, before it is sent.

Engine traffic (delta-rs, DuckDB) never passes through `RunBudget`, so the same analytics
query also reads the previous run's own window: if that run used more than one run's caps, the
next run is blocked until someone reviews it and records the run id as acknowledged. Analytics
settle about 5 minutes after the fact (spike S1), which is why the check happens at the start
of the next run rather than at the end of the run itself.

Run `python -m ge_intel.cost_guard` with CLOUDFLARE_ACCOUNT_ID and an Account Analytics
Read token in CLOUDFLARE_ANALYTICS_TOKEN (month-to-date check only; `ge-intel guard` adds the
previous-run check). Exit codes: 0 allowed, 2 usage unreadable, 3 halted.
"""

from __future__ import annotations

import calendar
import json
import os
import sys
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# Free allowance per month (R2 pricing page, read 2026-10-20). Standard storage class only.
FREE_CLASS_A = 1_000_000
FREE_CLASS_B = 10_000_000
FREE_STORAGE_BYTES = 10 * 10**9

HALT_FRACTION = 0.5
STORAGE_HALT_BYTES = 6 * 10**9
WARN_PROJECTED_FRACTION = 0.5
PROJECTION_MIN_HOURS = 72

CLASS_A_OPS = frozenset(
    {
        "ListBuckets",
        "PutBucket",
        "ListObjects",
        "ListObjectsV2",
        "PutObject",
        "CopyObject",
        "CompleteMultipartUpload",
        "CreateMultipartUpload",
        "LifecycleStorageTierTransition",
        "ListMultipartUploads",
        "UploadPart",
        "UploadPartCopy",
        "ListParts",
        "PutBucketEncryption",
        "PutBucketCors",
        "PutBucketLifecycleConfiguration",
    }
)
CLASS_B_OPS = frozenset(
    {
        "HeadBucket",
        "HeadObject",
        "GetObject",
        "UsageSummary",
        "GetBucketEncryption",
        "GetBucketLocation",
        "GetBucketCors",
        "GetBucketLifecycleConfiguration",
    }
)
FREE_OPS = frozenset({"DeleteObject", "DeleteBucket", "AbortMultipartUpload"})


def op_class(operation: str) -> str:
    """'A', 'B' or 'free'. Unknown operations count as Class A, the scarcer allowance."""
    if operation in CLASS_B_OPS:
        return "B"
    if operation in FREE_OPS:
        return "free"
    return "A"


@dataclass(frozen=True)
class Usage:
    class_a: int
    class_b: int
    storage_bytes: int
    as_of: datetime
    # Class A and B operations on the project bucket during the previous run's window.
    previous_window: tuple[int, int] | None = None


@dataclass(frozen=True)
class PreviousRun:
    run_id: str
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True)
class Verdict:
    allowed: bool
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def month_start(moment: datetime) -> datetime:
    return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def evaluate(usage: Usage) -> Verdict:
    """Halt at 50% of either operation allowance or at 6 GB stored; warn on projections."""
    if usage.as_of.tzinfo is None:
        raise ValueError("usage.as_of must be timezone-aware (UTC)")
    reasons: list[str] = []
    warnings: list[str] = []
    for name, used, free in (
        ("Class A", usage.class_a, FREE_CLASS_A),
        ("Class B", usage.class_b, FREE_CLASS_B),
    ):
        if used >= free * HALT_FRACTION:
            reasons.append(f"{name} at {used:,} of {free:,} free ({used / free:.0%})")
    if usage.storage_bytes >= STORAGE_HALT_BYTES:
        reasons.append(f"storage at {usage.storage_bytes / 1e9:.2f} GB (halt at 6 GB)")

    start = month_start(usage.as_of)
    elapsed_h = (usage.as_of - start) / timedelta(hours=1)
    if elapsed_h >= PROJECTION_MIN_HOURS:
        month_h = calendar.monthrange(start.year, start.month)[1] * 24
        for name, used, free in (
            ("Class A", usage.class_a, FREE_CLASS_A),
            ("Class B", usage.class_b, FREE_CLASS_B),
        ):
            projected = used * month_h / elapsed_h
            if projected >= free * WARN_PROJECTED_FRACTION:
                warnings.append(f"{name} projected at {projected:,.0f} by month end")
    return Verdict(not reasons, tuple(reasons), tuple(warnings))


class BudgetExceeded(RuntimeError):
    """Raised before a request that would take the run past its cap."""


@dataclass
class RunBudget:
    """Per-run caps for requests made by the project's own R2 client (ADR-0015, layer 4)."""

    max_class_a: int = 500
    max_class_b: int = 5_000
    max_bytes_written: int = 50 * 10**6
    class_a: int = 0
    class_b: int = 0
    bytes_written: int = 0

    def charge(self, operation: str, bytes_written: int = 0) -> None:
        """Call immediately before sending `operation`; raises instead of overspending."""
        if bytes_written < 0:
            raise ValueError("bytes_written must not be negative")
        cls = op_class(operation)
        a = self.class_a + (cls == "A")
        b = self.class_b + (cls == "B")
        written = self.bytes_written + bytes_written
        if a > self.max_class_a or b > self.max_class_b or written > self.max_bytes_written:
            raise BudgetExceeded(
                f"{operation} refused: run would reach A={a}/{self.max_class_a}, "
                f"B={b}/{self.max_class_b}, bytes={written}/{self.max_bytes_written}"
            )
        self.class_a, self.class_b, self.bytes_written = a, b, written


SETTLE = timedelta(minutes=6)  # analytics complete about 5 minutes after the fact


def check_previous_run(
    usage: Usage,
    previous: PreviousRun | None,
    acknowledged_run_id: str | None = None,
    caps: RunBudget | None = None,
) -> Verdict:
    """Block if the previous run used more than one run's caps, unless it was acknowledged."""
    if previous is None or usage.previous_window is None:
        return Verdict(True, (), ())
    caps = caps or RunBudget()
    a, b = usage.previous_window
    warnings: list[str] = []
    if usage.as_of - previous.finished_at < SETTLE:
        warnings.append(
            f"previous run {previous.run_id} ended under 6 minutes ago; counts may be low"
        )
    over = []
    if a > caps.max_class_a:
        over.append(f"Class A {a:,} > {caps.max_class_a:,}")
    if b > caps.max_class_b:
        over.append(f"Class B {b:,} > {caps.max_class_b:,}")
    if not over:
        return Verdict(True, (), tuple(warnings))
    if acknowledged_run_id == previous.run_id:
        warnings.append(
            f"previous run {previous.run_id} over budget ({'; '.join(over)}), acknowledged"
        )
        return Verdict(True, (), tuple(warnings))
    reason = (
        f"previous run {previous.run_id} used {'; '.join(over)} per run; review it, then set "
        f"GE_GUARD_ACK_RUN_ID={previous.run_id} to resume"
    )
    return Verdict(False, (reason,), tuple(warnings))


def _window_ops(rows: list[dict], bucket: str) -> tuple[int, int]:
    a = b = 0
    for row in rows:
        if row["dimensions"].get("bucketName") != bucket:
            continue
        cls = op_class(row["dimensions"]["actionType"])
        if cls == "A":
            a += row["sum"]["requests"]
        elif cls == "B":
            b += row["sum"]["requests"]
    return a, b


GRAPHQL_URL = "https://api.cloudflare.com/client/v4/graphql"
QUERY = """query($acct: String!, $start: Time!, $end: Time!, $storageStart: Time!,
    $prevStart: Time!, $prevEnd: Time!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    ops: r2OperationsAdaptiveGroups(limit: 100,
        filter: {datetime_geq: $start, datetime_leq: $end}) {
      sum { requests } dimensions { actionType bucketName }
    }
    prev: r2OperationsAdaptiveGroups(limit: 100,
        filter: {datetime_geq: $prevStart, datetime_leq: $prevEnd}) {
      sum { requests } dimensions { actionType bucketName }
    }
    storage: r2StorageAdaptiveGroups(limit: 100,
        filter: {datetime_geq: $storageStart, datetime_leq: $end},
        orderBy: [datetime_DESC]) {
      max { payloadSize metadataSize objectCount uploadCount } dimensions { bucketName datetime }
    }
  } }
}"""


def parse_usage(payload: Mapping, as_of: datetime, bucket: str | None = None) -> Usage:
    """Turn a GraphQL response into month-to-date usage. Raises if the response has errors."""
    if payload.get("errors"):
        raise RuntimeError(f"analytics query failed: {payload['errors']}")
    accounts = payload["data"]["viewer"]["accounts"]
    if len(accounts) != 1:
        raise RuntimeError(f"expected one account in analytics response, got {len(accounts)}")
    acct = accounts[0]
    a = b = 0
    for row in acct["ops"]:
        cls = op_class(row["dimensions"]["actionType"])
        if cls == "A":
            a += row["sum"]["requests"]
        elif cls == "B":
            b += row["sum"]["requests"]
    # Rows are newest first; take each bucket's latest reading.
    latest: dict[str, int] = {}
    for row in acct["storage"]:
        bucket = row["dimensions"]["bucketName"]
        if bucket not in latest:
            latest[bucket] = row["max"]["payloadSize"] + row["max"]["metadataSize"]
    previous = (
        _window_ops(acct["prev"], bucket) if bucket and acct.get("prev") is not None else None
    )
    return Usage(
        class_a=a,
        class_b=b,
        storage_bytes=sum(latest.values()),
        as_of=as_of,
        previous_window=previous,
    )


def _post_json(url: str, body: dict, headers: dict) -> dict:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_usage(
    account_id: str,
    token: str,
    now: datetime | None = None,
    post_json: Callable[[str, dict, dict], dict] = _post_json,
    previous: PreviousRun | None = None,
    bucket: str = "ge-intel-lake",
) -> Usage:
    """Month-to-date usage plus the previous run's window, in one query (one per run)."""
    now = now or datetime.now(UTC)
    if previous is not None:  # whole minutes either side, the analytics resolution
        prev_start = previous.started_at.replace(second=0, microsecond=0) - timedelta(minutes=1)
        prev_end = previous.finished_at.replace(second=0, microsecond=0) + timedelta(minutes=2)
    else:
        prev_start = prev_end = now
    variables = {
        "acct": account_id,
        "start": month_start(now).isoformat(),
        "end": now.isoformat(),
        "storageStart": (now - timedelta(days=2)).isoformat(),
        "prevStart": prev_start.isoformat(),
        "prevEnd": prev_end.isoformat(),
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = post_json(GRAPHQL_URL, {"query": QUERY, "variables": variables}, headers)
    return parse_usage(payload, as_of=now, bucket=bucket if previous is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    token = os.environ.get("CLOUDFLARE_ANALYTICS_TOKEN")
    if not account_id or not token:
        print("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_ANALYTICS_TOKEN must be set", file=sys.stderr)
        return 2
    try:
        usage = fetch_usage(account_id, token)
    except Exception as exc:  # fail closed on any read failure
        print(f"cannot read R2 usage, not running: {exc}", file=sys.stderr)
        return 2
    verdict = evaluate(usage)
    print(
        json.dumps(
            {
                "as_of": usage.as_of.isoformat(),
                "class_a": usage.class_a,
                "class_b": usage.class_b,
                "storage_gb": round(usage.storage_bytes / 1e9, 3),
                "allowed": verdict.allowed,
                "reasons": verdict.reasons,
                "warnings": verdict.warnings,
            }
        )
    )
    return 0 if verdict.allowed else 3


if __name__ == "__main__":
    sys.exit(main())
