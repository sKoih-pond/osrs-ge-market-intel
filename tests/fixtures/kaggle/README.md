# OSRS Grand Exchange 5-minute price series

One row per item per 5-minute bucket, exactly as the wiki 5m mapping
reports it: avg low/high price and traded volume on each side of the book.
No derived columns.

## Source & license

[OSRS Wiki — RuneScape: Real-time Prices](https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices)
(5-minute mapping averages from the realtime API). Source wiki content is
© Weird Gloop contributors; this dataset is distributed under the same
terms — [CC BY-NC-SA 3.0](https://meta.weirdgloop.org/w/Licensing).

## Files

- `prices_5m/year=YYYY/month=MM/data.parquet` — hive-partitioned by month,
  sorted by `(itemId, timestamp)`, zstd-compressed.
- `items.parquet` — optional lookup: `itemId, name, ge_limit, members, tradeable`.

## Schema (prices_5m)

Column names are the wiki API field names verbatim.

| column           | type                | meaning                                           |
| ---------------- | ------------------- | ------------------------------------------------- |
| itemId           | int32               | wiki item id (see items.parquet)                  |
| timestamp        | int32               | wiki field of the same name: bucket start, epoch seconds (300s-aligned) |
| avgLowPrice      | int32/int64/double? | avg low-side (instasell) price; NULL if no trades |
| avgHighPrice     | int32/int64/double? | avg high-side (instabuy) price; NULL if no trades |
| lowPriceVolume   | int32/int64         | units traded on the low side in the bucket        |
| highPriceVolume  | int32/int64         | units traded on the high side in the bucket       |

Avg prices can exceed max cash (2,147,483,647 gp) and — for recent data —
carry a fractional part, so the price column widths vary per month file:
the two most recent months are always double; each older month is int32 if
every value in it is whole and fits, int64 if only the range needs it, and
double as soon as any value is fractional. Readers must unify schemas
across files — DuckDB: `union_by_name = true` (see Reading);
pyarrow/pandas dataset APIs unify by default.

Rows where neither side traded are omitted. Timestamps are UTC;
`to_timestamp("timestamp")` in DuckDB (or `pd.to_datetime(df["timestamp"], unit="s")`)
reconstructs them.

## Snapshot stats

- generated: 2026-10-17T13:19:33.568Z
- rows: 876754705
- items: 4539
- range: 1970-03-04 03:32:43-04 .. 1970-03-10 23:29:02-04

## Reading

Always pass `union_by_name = true` — month files carry different int widths
for the same column (see the note above), and DuckDB otherwise takes the
schema of the first file:

```python
import duckdb
df = duckdb.sql("""
	SELECT to_timestamp("timestamp") AS t, *
	FROM read_parquet("prices_5m/**/data.parquet", hive_partitioning = true, union_by_name = true)
	WHERE itemId = 2
	ORDER BY "timestamp"
""").df()
```
