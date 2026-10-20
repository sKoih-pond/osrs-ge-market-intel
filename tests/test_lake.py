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
