"""The history job's inputs on local disk, and the rules the rebuild shares with it.

Written again here, not imported from `ge_intel`: the engines read the same files under the
same rules, so a parity check against the DuckDB build (ADR-0018) tests the rules as well as
the engine (ADR-0011, amended). The constants below are `ge_intel.history`'s; a drift between
the two shows up as a parity failure, not as silence.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

DUMP_DIR = Path("kaggle/antonygarand/osrs-ge-5m/v1")
KAGGLE_SOURCE = "kaggle:antonygarand/osrs-ge-5m@v1"
API_SOURCE = "wiki_api"
# History ends where the live series begins; 5-minute rows exist from 2 July 2026 (ADR-0005).
LIVE_START = datetime(2026, 10, 18, 23, tzinfo=UTC)
WINDOW_5M_START = datetime(2026, 7, 2, tzinfo=UTC)
# Months whose hours take `/1h`'s rows where the dump disagrees (docs/plans/M2.md, 5.1).
REPAIR_FROM_1H = ("2026-01",)
REL_TOL = 0.001
ABS_TOL = {"integer": 1.01, "decimal": 0.02}

MONTH_FILE = re.compile(r"prices_5m/year=(\d{4})/month=(\d{2})/data\.parquet")
RAW_FILE = re.compile(r"(\d+)-[0-9a-f]+\.json\.gz")
SEED = Path(__file__).resolve().parents[3] / "src" / "ge_intel" / "seeds" / "regimes.csv"


@dataclass(frozen=True)
class Regime:
    regime_id: str
    starts: int  # epoch seconds, inclusive
    ends: int | None  # epoch seconds, exclusive; None: still in force


def epoch(moment: datetime) -> int:
    return int(moment.timestamp())


def parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def measurement_regimes(seed: Path = SEED) -> list[Regime]:
    with seed.open() as fh:
        return [
            Regime(
                r["regime_id"],
                epoch(parse_utc(r["starts_at"])),
                epoch(parse_utc(r["ends_at"])) if r["ends_at"] else None,
            )
            for r in csv.DictReader(fh)
            if r["kind"] == "measurement"
        ]


def month_bounds(month: str) -> tuple[int, int]:
    """[start, end) of a `YYYY-MM` month, in epoch seconds."""
    year, mon = map(int, month.split("-"))
    start = datetime(year, mon, 1, tzinfo=UTC)
    end = datetime(year + mon // 12, mon % 12 + 1, 1, tzinfo=UTC)
    return epoch(start), epoch(end)


def month_of(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m")


def month_files(history_dir: Path) -> dict[str, Path]:
    """month -> dump file, every month the dump holds, in order."""
    root = Path(history_dir) / DUMP_DIR
    out = {}
    for path in root.glob("prices_5m/year=*/month=*/data.parquet"):
        m = MONTH_FILE.search(path.relative_to(root).as_posix())
        if m:
            out[f"{m[1]}-{m[2]}"] = path
    return dict(sorted(out.items()))


def raw_files(history_dir: Path, endpoint: str) -> dict[int, Path]:
    """Bucket start (epoch seconds) -> stored Wiki response for `endpoint` (5m or 1h). Two
    bodies for one bucket are refused, as the history job refuses them."""
    out: dict[int, Path] = {}
    for path in sorted((Path(history_dir) / "bronze" / "wiki" / endpoint).glob("date=*/*.json.gz")):
        m = RAW_FILE.fullmatch(path.name)
        if not m:
            raise ValueError(f"unexpected stored response {path}")
        start = int(m[1])
        if start in out:
            raise ValueError(f"two {endpoint} bodies for bucket {start}")
        out[start] = path
    return out
