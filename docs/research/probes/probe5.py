import json, time, urllib.request, datetime as dt, collections
UA = "osrs-ge-market-intel research probe (portfolio project, github.com/sKoih-pond)"
def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r: body = r.read()
    time.sleep(0.8); return json.loads(body)
for iid, name in [(4151,"whip"),(561,"nature rune"),(385,"shark"),(13190,"bond")]:
    wg = get(f"https://api.weirdgloop.org/exchange/history/osrs/all?id={iid}")[str(iid)]
    wk = get(f"https://prices.runescape.wiki/api/v2/osrs/timeseries?lookback=1y&id={iid}")["data"]
    wgm = collections.defaultdict(int); wkm = collections.defaultdict(int)
    for x in wg:
        d = dt.datetime.fromtimestamp(x["timestamp"]/1000, dt.timezone.utc)
        if x.get("volume") is not None: wgm[d.strftime("%Y-%m") if d < dt.datetime(2026,9,30,tzinfo=dt.timezone.utc) else "2026-10*"] += x["volume"]
    for x in wk:
        d = dt.datetime.fromtimestamp(x["timestamp"], dt.timezone.utc)
        key = d.strftime("%Y-%m") if d < dt.datetime(2026,9,30,tzinfo=dt.timezone.utc) else "2026-10*"
        wkm[key] += (x.get("highPriceVolume") or 0) + (x.get("lowPriceVolume") or 0)
    keys = [k for k in sorted(wkm) if k in wgm][-14:]
    print(name, " | ".join(f"{k}: {wkm[k]/wgm[k]:.2f}" for k in keys))
