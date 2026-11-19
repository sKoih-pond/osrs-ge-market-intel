"""The PySpark rebuild of history/prices_1h against the DuckDB build (ADR-0011, amended), on the
fixture month: the same inputs on disk, two engines, and parity must be exact.

Runs only with `pytest -m spark` and a Java 17 runtime, against the engines project's own
environment (`engines/.venv`, or the `ge-spark` command named by GE_SPARK).
"""

import dataclasses
import json
import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from conftest import FIXTURE_NOW, Clock, FixtureTransport
from test_history import KAGGLE, LIVE, OUTAGE, fixture_pins

from ge_intel import history, kaggle, parity, wiki

pytestmark = pytest.mark.spark

REPO = Path(__file__).resolve().parents[1]
GE_SPARK = Path(os.environ.get("GE_SPARK", REPO / "engines" / ".venv" / "bin" / "ge-spark"))


@pytest.fixture
def hist(tmp_path):
    """The fixture month as the history job holds it (as `test_history.h`)."""
    root = tmp_path / "history"
    shutil.copytree(KAGGLE, root / "kaggle" / kaggle.DATASET / "v1")
    client = wiki.WikiClient(
        transport=FixtureTransport(drop_buckets=[OUTAGE]),
        clock=Clock(FIXTURE_NOW),
        min_interval_s=0,
    )
    return history.History(root, client=client, live_start=LIVE, pins=fixture_pins())


def spark(h, out: Path, *args: str) -> dict:
    if not GE_SPARK.exists() or not shutil.which("java"):
        pytest.skip("needs the engines environment and Java 17")
    done = subprocess.run(
        [
            str(GE_SPARK),
            "reprocess",
            "--history-dir",
            str(h.root),
            "--out",
            str(out),
            "--live-start",
            history.iso(h.live_start),
            "--threads",
            "2",
            "--memory",
            "2g",
            "--local-dir",
            str(out.parent / "spark-tmp"),
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert done.returncode == 0, done.stderr[-4000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_spark_rebuild_matches_the_duckdb_build_exactly(hist, tmp_path):
    h = hist
    history.build(h)
    history.check_1h(h)
    history.fill(h)  # a 27-bucket seam, three gaps (one an outage) and a mixed hour
    report = spark(h, tmp_path / "spark", "--repair-months", "")
    assert report["rows"] == h.lake.read(history.PRICES_1H).num_rows

    found = parity.run(h, tmp_path / "spark")["duckdb_vs_other"]
    assert found["exact"] and found["checksums_equal"], found
    assert found["totals"]["rows_a"] == found["totals"]["rows_b"] == report["rows"]


def test_spark_repairs_the_same_hours_from_1h(hist, tmp_path, monkeypatch):
    """The 2026-01 repair, on the fixture month made hourly-only: one tampered `/1h` hour and
    the outage hour disagree with the dump; both engines replace exactly those hours."""
    monkeypatch.setattr(history, "REPAIR_FROM_1H", frozenset({"2026-10"}))
    h = dataclasses.replace(hist, window_5m_start=LIVE)
    history.build(h)
    history.fill(h)
    tampered = datetime(2026, 10, 19, 5, tzinfo=UTC)
    payload = h.client.transport.hours[str(int(tampered.timestamp()))]
    del payload["data"][min(payload["data"], key=int)]
    duck = history.check_1h(h)["build"]["repaired"]["2026-10"]
    assert duck["replaced"] == [history.iso(tampered), history.iso(OUTAGE.replace(minute=0))]

    report = spark(
        h,
        tmp_path / "spark",
        "--repair-months",
        "2026-10",
        "--window-5m-start",
        history.iso(LIVE),
    )
    assert report["repaired"]["2026-10"]["replaced"] == duck["replaced"]
    assert report["repaired"]["2026-10"]["hours"] == duck["hours"] == 24
    found = parity.run(h, tmp_path / "spark")["duckdb_vs_other"]
    assert found["exact"] and found["checksums_equal"], found
