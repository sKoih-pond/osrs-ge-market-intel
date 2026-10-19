import json, time, urllib.request, sys, os, statistics as st, datetime as dt
UA = "osrs-ge-market-intel research probe (portfolio project, github.com/sKoih-pond)"
API = sys.argv[1]
def get(path):
    req = urllib.request.Request("https://prices.runescape.wiki/api/" + path, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r: body = r.read()
    time.sleep(0.6); return json.loads(body)
U = lambda y,m,d,h=12,mi=0: int(dt.datetime(y,m,d,h,mi,tzinfo=dt.timezone.utc).timestamp())
F = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
# 1) late-arriving data: refetch the 5m bucket saved earlier
old = json.load(open(os.path.join(API, "5m_latest.json")))
new = get(f"v2/osrs/5m?timestamp={old['timestamp']}")
od, nd = old["data"], new["data"]
chg = sum(1 for k in set(od)|set(nd) if od.get(k) != nd.get(k))
print("5m bucket", F(old["timestamp"]), "items then/now", len(od), len(nd), "items changed", chg)
# 2) v1 vs v2 same bucket
t = U(2026,10,18)
v1 = get(f"v1/osrs/1h?timestamp={t}")["data"]; v2 = get(f"v2/osrs/1h?timestamp={t}")["data"]
diff = [k for k in v2 if k in v1 and (round(v2[k]["avgHighPrice"] or 0) != (v1[k]["avgHighPrice"] or 0) or v2[k]["highPriceVolume"] != v1[k]["highPriceVolume"])]
print("v1 items", len(v1), "v2 items", len(v2), "rows differing beyond rounding", len(diff), "e.g. whip v1", v1.get("4151"), "v2", v2.get("4151"))
# 3) reconcile 1h vs 12x5m
fives = [get(f"v2/osrs/5m?timestamp={t+300*i}")["data"] for i in range(12)]
ok_vol = bad_vol = ok_px = bad_px = 0; worst = []
for k, h in v2.items():
    hv = sum((f.get(k) or {}).get("highPriceVolume") or 0 for f in fives)
    lv = sum((f.get(k) or {}).get("lowPriceVolume") or 0 for f in fives)
    if hv == h["highPriceVolume"] and lv == h["lowPriceVolume"]: ok_vol += 1
    else: bad_vol += 1; worst.append((k, h["highPriceVolume"], hv, h["lowPriceVolume"], lv))
    num = sum(((f.get(k) or {}).get("avgHighPrice") or 0)*((f.get(k) or {}).get("highPriceVolume") or 0) for f in fives)
    if hv and h["avgHighPrice"]:
        vw = num/hv
        if abs(vw - h["avgHighPrice"])/h["avgHighPrice"] < 0.001: ok_px += 1
        else: bad_px += 1
print("1h vs sum(12x5m) volume match", ok_vol, "mismatch", bad_vol, "| VWAP(high) match", ok_px, "mismatch", bad_px, "sample mismatches", worst[:5])
# 4) timeseries pre/post switch
items = {561:"nature rune",4151:"whip",385:"shark",2:"cannonball",13190:"bond",12934:"zulrah scales",560:"death rune",11832:"bandos chestplate",6585:"amulet of fury",20997:"twisted bow",3024:"super restore(4)",453:"coal"}
cut = U(2026,9,30,0)
print("== timeseries lookback=6m: mean vol per step and median rel spread, 30d pre vs post switch")
for i, name in items.items():
    r = get(f"v2/osrs/timeseries?lookback=6m&id={i}")
    rows = r["data"]; step = rows[1]["timestamp"]-rows[0]["timestamp"] if len(rows)>1 else None
    pre = [x for x in rows if cut-30*86400 <= x["timestamp"] < cut]; post = [x for x in rows if x["timestamp"] >= cut+86400]
    vol = lambda xs: st.mean([(x.get("highPriceVolume") or 0)+(x.get("lowPriceVolume") or 0) for x in xs]) if xs else float('nan')
    spr = lambda xs: st.median([(x["avgHighPrice"]-x["avgLowPrice"])/((x["avgHighPrice"]+x["avgLowPrice"])/2) for x in xs if x.get("avgHighPrice") and x.get("avgLowPrice")] or [float('nan')])
    print(f"{name:18s} step={step}s n={len(rows)} first={F(rows[0]['timestamp'])} vol pre={vol(pre):,.0f} post={vol(post):,.0f} ratio={vol(post)/vol(pre):.2f} spread pre={spr(pre):.4f} post={spr(post):.4f}")
r = get("v2/osrs/timeseries?lookback=1y&id=561"); rows = r["data"]
print("1y lookback: n", len(rows), "step", rows[1]["timestamp"]-rows[0]["timestamp"], "first", F(rows[0]["timestamp"]), "keys", sorted(r.keys()))
