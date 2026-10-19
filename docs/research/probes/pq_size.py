import duckdb, glob, json, os, sys
src, out = sys.argv[1], sys.argv[2]
rows = []
for f in sorted(glob.glob(os.path.join(src, "*.json"))):
    t = int(os.path.basename(f).split(".")[0]); d = json.load(open(f))["data"]
    for k, v in d.items():
        rows.append((int(k), t, v.get("avgHighPrice"), v.get("highPriceVolume"), v.get("avgLowPrice"), v.get("lowPriceVolume")))
con = duckdb.connect()
con.execute("create table h (item_id INTEGER, ts BIGINT, avg_high DOUBLE, high_vol BIGINT, avg_low DOUBLE, low_vol BIGINT)")
con.executemany("insert into h values (?,?,?,?,?,?)", rows)
n = con.execute("select count(*) from h").fetchone()[0]
for codec in ("zstd", "snappy"):
    p = os.path.join(out, f"h_{codec}.parquet")
    con.execute(f"copy (select item_id, to_timestamp(ts) as bucket_start, avg_high, high_vol, avg_low, low_vol from h order by item_id, ts) to '{p}' (format parquet, compression {codec})")
    print(codec, "rows", n, "bytes", os.path.getsize(p), "bytes/row", round(os.path.getsize(p)/n, 2))
print("null avg_high", con.execute("select count(*) filter (where avg_high is null), count(*) filter (where avg_low is null) from h").fetchone())
print("items/hour min/avg/max", con.execute("select min(c), avg(c), max(c) from (select ts, count(*) c from h group by ts)").fetchone())
print("items seen in all 24 hours", con.execute("select count(*) from (select item_id from h group by item_id having count(*)=24)").fetchone()[0], "of", con.execute("select count(distinct item_id) from h").fetchone()[0])
