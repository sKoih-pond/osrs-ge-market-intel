import json, time, urllib.request, os, sys, datetime as dt, hashlib
UA = "osrs-ge-market-intel research probe (portfolio project, github.com/sKoih-pond)"
API = sys.argv[1]
def get(path):
    req = urllib.request.Request("https://prices.runescape.wiki/api/" + path, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r: body = r.read(); hdr = dict(r.headers)
    time.sleep(0.6); return json.loads(body), hdr
print("now", dt.datetime.now(dt.timezone.utc).strftime("%H:%M UTC"))
for fn, ep in (("5m_latest.json", "5m"), ("1h_latest.json", "1h")):
    old = json.load(open(os.path.join(API, fn))); new, h = get(f"v2/osrs/{ep}?timestamp={old['timestamp']}")
    od, nd = old["data"], new["data"]
    chg = [k for k in set(od) | set(nd) if od.get(k) != nd.get(k)]
    print(ep, "bucket", dt.datetime.fromtimestamp(old["timestamp"], dt.timezone.utc).strftime("%H:%M"), "items", len(od), "->", len(nd), "changed", len(chg), "age", h.get("Age"))
