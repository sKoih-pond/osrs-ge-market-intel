"""Parity between two builds of history/prices_1h (ADR-0011, amended): each kind of difference
is counted where it belongs, a one-step price difference is a rounding flip and not an error,
and R2's own record (its Delta log and manifest) is compared without reading data."""

import json
import shutil
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from ge_intel import history, parity
from ge_intel.lake import Lake
from ge_intel.objects import LocalStore

JAN = datetime(2026, 1, 1, tzinfo=UTC)


def rows():
    out = []
    for month, start in (("2026-01", JAN), ("2026-02", datetime(2026, 2, 1, tzinfo=UTC))):
        for hour in range(3):
            for item in (2, 10, 11):
                out.append(
                    {
                        "bucket_start": start + timedelta(hours=hour),
                        "bucket_month": month,
                        "item_id": item,
                        "avg_high_price": 100.25 + item if item != 11 else None,
                        "high_volume": 5 * item,
                        "avg_low_price": 99.5 + hour,
                        "low_volume": item,
                        "regime_id": "measurement-runelite",
                        "source": "kaggle:antonygarand/osrs-ge-5m@v1",
                    }
                )
    return out


def lake(path, data):
    store = Lake(root=str(path), store=LocalStore(path))
    store.write(
        history.PRICES_1H,
        pa.Table.from_pylist(data, schema=history.PRICES_1H_SCHEMA),
        partition_by=["bucket_month"],
    )
    return store


def compare(tmp_path, a, b):
    con = parity.connect(tmp_path / "tmp")
    parity.register_delta(con, "a", lake(tmp_path / "a", a).uri(history.PRICES_1H))
    parity.register_delta(con, "b", lake(tmp_path / "b", b).uri(history.PRICES_1H))
    return parity.compare(con, "a", "b")


def test_identical_builds_are_exact(tmp_path):
    found = compare(tmp_path, rows(), rows())
    assert found["exact"] and found["checksums_equal"] and found["months"] == 2
    assert found["totals"]["rows_a"] == found["totals"]["rows_b"] == 18


def test_every_kind_of_difference_is_counted(tmp_path):
    other = rows()
    del other[0]  # only in a
    other[1]["high_volume"] += 1  # volume
    other[2]["source"] = "mixed"  # label
    other[3]["avg_low_price"] += 0.01  # a rounding flip: equivalent, not exact
    other[4]["avg_low_price"] += 0.5  # a real error
    other[5]["avg_high_price"] = 1.0  # null on one side only
    found = compare(tmp_path, rows(), other)
    jan = found["by_month"]["2026-01"]
    assert (jan["only_a"], jan["only_b"], jan["volume"], jan["labels"]) == (1, 0, 1, 1)
    assert (jan["rounding_flips"], jan["price_errors"]) == (1, 2)
    assert (
        not found["exact"] and not found["equivalent"] and found["months_not_exact"] == ["2026-01"]
    )
    assert found["by_month"]["2026-02"]["exact"]


def test_a_rounding_flip_alone_is_reported_but_equivalent(tmp_path):
    other = rows()
    other[0]["avg_high_price"] += 0.01
    found = compare(tmp_path, rows(), other)
    assert found["equivalent"] and not found["exact"]
    assert found["totals"]["rounding_flips"] == 1 and not found["checksums_equal"]
    assert found["by_month"]["2026-01"]["flip_samples"] == [
        {
            "bucket_start": "2026-01-01 00:00:00+00:00",
            "item_id": 2,
            "source": "kaggle:antonygarand/osrs-ge-5m@v1",
        }
    ]


def test_run_ties_both_builds_to_r2s_log_and_manifest(tmp_path):
    h = history.History(tmp_path / "history", pins=[])
    data = rows()
    h.lake.write(
        history.PRICES_1H,
        pa.Table.from_pylist(data, schema=history.PRICES_1H_SCHEMA),
        partition_by=["bucket_month"],
    )
    lake(tmp_path / "spark", data)
    r2 = lake(tmp_path / "r2", data)  # R2 stands in as a local lake: same Delta log, same API
    manifest = [
        {"bucket_month": m, "hourly_rows": 9, "status": "validated"} for m in ("2026-01", "2026-02")
    ]
    r2.write(
        history.MONTHS,
        pa.Table.from_pylist(manifest).cast(
            pa.schema(
                [
                    ("bucket_month", pa.string()),
                    ("hourly_rows", pa.int64()),
                    ("status", pa.string()),
                ]
            )
        ),
    )
    summary = parity.run(h, tmp_path / "spark", r2=r2)
    assert summary["duckdb_vs_other"]["exact"]
    assert summary["r2_counts"]["log_equals_manifest"] and summary["r2_counts"]["log_equals_build"]
    saved = json.loads((h.root / "parity.json").read_text())
    assert saved["r2_counts"]["by_month"] == {"2026-01": 9, "2026-02": 9}
    assert parity.agrees(summary)

    r2.write(
        history.MONTHS,
        pa.Table.from_pylist([{**manifest[0], "hourly_rows": 8}, manifest[1]]).cast(
            pa.schema(
                [
                    ("bucket_month", pa.string()),
                    ("hourly_rows", pa.int64()),
                    ("status", pa.string()),
                ]
            )
        ),
    )
    assert not parity.agrees(parity.run(h, tmp_path / "spark", r2=r2))


def test_copied_data_files_read_like_the_table(tmp_path):
    source = lake(tmp_path / "a", rows())
    files = tmp_path / "copy"
    shutil.copytree(
        source.uri(history.PRICES_1H), files, ignore=shutil.ignore_patterns("_delta_log")
    )
    con = parity.connect(tmp_path / "tmp")
    parity.register_delta(con, "tbl", source.uri(history.PRICES_1H))
    parity.register_files(con, "files", files)
    assert parity.compare(con, "tbl", "files")["exact"]


def test_a_duplicated_row_fails_even_when_every_key_matches(tmp_path):
    found = compare(tmp_path, rows(), rows() + [rows()[0]])
    jan = found["by_month"]["2026-01"]
    assert (jan["rows_a"], jan["rows_b"], jan["duplicates_b"]) == (9, 10, 1)
    assert not jan["equivalent"] and not found["equivalent"]


def test_flips_are_one_step_in_whole_cents_at_any_price(tmp_path):
    """Above about 5.4e8 GP two neighbouring cents differ by more than 0.01 as doubles; a
    one-step flip there is still a flip. A difference off the 0.01 grid is an error however
    small."""
    a, b = rows(), rows()
    for r in (a[0], b[0]):
        r["avg_high_price"] = 1_534_217_843.27
    b[0]["avg_high_price"] = 1_534_217_843.28  # one step up, both on the grid
    b[1]["avg_low_price"] += 0.004  # off the grid: no engine rounding to 0.01 gives this
    jan = compare(tmp_path, a, b)["by_month"]["2026-01"]
    assert (jan["rounding_flips"], jan["price_errors"]) == (1, 1)
    assert jan["flip_samples"] == [
        {
            "bucket_start": "2026-01-01 00:00:00+00:00",
            "item_id": 2,
            "source": "kaggle:antonygarand/osrs-ge-5m@v1",
        }
    ]
