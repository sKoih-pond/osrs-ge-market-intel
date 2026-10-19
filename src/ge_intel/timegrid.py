"""UTC bucket arithmetic. All intervals are half-open [start, end) (ADR-0004)."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta

FIVE_MIN = timedelta(minutes=5)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)


def parse_utc(text: str) -> datetime:
    """ISO-8601 with an explicit offset; naive times are refused."""
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"{text!r} has no UTC offset")
    return moment.astimezone(UTC)


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def floor(moment: datetime, step: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return epoch + ((moment - epoch) // step) * step


def grid(start: datetime, end: datetime, step: timedelta = FIVE_MIN) -> list[datetime]:
    """Bucket starts in [start, end), aligned to `step`."""
    t = floor(start, step)
    if t < start:
        t += step
    out = []
    while t < end:
        out.append(t)
        t += step
    return out


def coalesce_ranges(
    starts: Iterable[datetime], step: timedelta = FIVE_MIN
) -> list[tuple[datetime, datetime]]:
    """Merge bucket starts into contiguous half-open ranges."""
    ranges: list[tuple[datetime, datetime]] = []
    for t in sorted(set(starts)):
        if ranges and ranges[-1][1] == t:
            ranges[-1] = (ranges[-1][0], t + step)
        else:
            ranges.append((t, t + step))
    return ranges


def days_touched(starts: Iterable[datetime]) -> list[date]:
    return sorted({t.date() for t in starts})


def sql_ts(moment: datetime) -> str:
    """Timestamp literal accepted by delta-rs predicates and DuckDB."""
    return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S+00:00")
