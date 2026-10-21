from datetime import date

import pyarrow as pa

from ge_intel.lake import Lake
from ge_intel.objects import LocalStore


def test_merge_write_adds_a_column_and_keeps_the_protocol_baseline(tmp_path):
    lake = Lake(root=str(tmp_path), store=LocalStore(tmp_path))
    lake.write(
        "gold/t", pa.table({"d": ["2026-10-20"], "item": [1], "x": [1.0]}), partition_by=["d"]
    )
    newer = pa.table({"d": ["2026-10-20"], "item": [1], "x": [2.0], "dev": [3.5]})
    lake.write(
        "gold/t", newer, predicate="d = '2026-10-20'", partition_by=["d"], schema_mode="merge"
    )
    assert lake.read("gold/t").to_pylist() == [{"item": 1, "x": 2.0, "d": "2026-10-20", "dev": 3.5}]
    protocol = lake.table("gold/t").protocol()
    assert (protocol.min_reader_version, protocol.min_writer_version) == (1, 2)
    assert not protocol.writer_features


def test_a_delete_on_the_partition_column_rewrites_no_file(tmp_path):
    lake = Lake(root=str(tmp_path), store=LocalStore(tmp_path))
    days = pa.array([date(2026, 10, 20), date(2026, 10, 21)], type=pa.date32())
    lake.write("silver/t", pa.table({"d": days, "x": [1, 2]}), partition_by=["d"])

    metrics = lake.delete("silver/t", "d < '2026-10-21'", metadata={"task": "maintain"})

    assert (metrics["num_deleted_rows"], metrics["num_added_files"]) == (1, 0)
    assert lake.read("silver/t").column("x").to_pylist() == [2]
    assert lake.commits[-1] == {"table": "silver/t", "version": 1, "rows_deleted": 1}
    assert lake.delete("silver/t", "d < '2026-10-21'")["num_deleted_rows"] == 0
    assert lake.table("silver/t").version() == 1  # nothing matched: no commit


def test_checkpoints_every_nth_version_when_asked(tmp_path):
    def checkpoints(lake):
        log = tmp_path / lake.root.rsplit("/", 1)[-1] / "silver" / "t" / "_delta_log"
        return sorted(int(f.name.split(".")[0]) for f in log.glob("*.checkpoint.parquet"))

    every = Lake(root=str(tmp_path / "every"), store=LocalStore(tmp_path))
    tenth = Lake(root=str(tmp_path / "tenth"), store=LocalStore(tmp_path), checkpoint_every=10)
    assert every.checkpoint_every == 1  # S1 rule 2 until ADR-0019 is accepted
    for lake in (every, tenth):
        for i in range(12):
            lake.write("silver/t", pa.table({"x": [i]}), mode="append")
    assert checkpoints(every) == list(range(12))
    assert checkpoints(tenth) == [0, 10]
    assert sorted(tenth.read("silver/t").column("x").to_pylist()) == list(range(12))
