import json, time, urllib.request, sys, os, datetime as dt
UA = "osrs-ge-market-intel research probe (portfolio project, github.com/sKoih-pond)"
OUT = sys.argv[1]
def get(path, save=None):
    req = urllib.request.Request("https://prices.runescape.wiki/api/" + path, headers={"User-Agent": UA})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read(); hdr = dict(r.headers)
    el = time.time() - t0
    if save:
        open(os.path.join(OUT, save), "wb").write(body)
    time.sleep(0.6)
    return json.loads(body), hdr, len(body), el
def ts(t): return dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat()

m, h, n, el = get("v2/osrs/mapping", "mapping.json")
print("mapping items", len(m), "bytes", n, "keys", sorted(set(k for x in m for k in x)))
print("mapping missing limit:", sum(1 for x in m if x.get("limit") is None), "members:", sum(1 for x in m if x.get("members")))
print("headers sample", {k: v for k, v in h.items() if k.lower() in ("cache-control","age","cf-cache-status","last-modified","expires","x-ratelimit-limit","content-type")})
l, h, n, el = get("v2/osrs/latest", "latest.json")
d = l["data"]; now = time.time()
print("latest items", len(d), "bytes", n)
hn = sum(1 for v in d.values() if v.get("high") is None); ln = sum(1 for v in d.values() if v.get("low") is None)
print("null high", hn, "null low", ln)
ages = sorted(now - max(v.get("highTime") or 0, v.get("lowTime") or 0) for v in d.values())
q = lambda p: ages[int(p*(len(ages)-1))]
print("age since last trade (s) p10,p50,p75,p90,p99:", [round(q(p)) for p in (.1,.5,.75,.9,.99)])
inv = sum(1 for v in d.values() if v.get("high") and v.get("low") and v["low"] > v["high"])
print("low>high (inverted) count", inv)
for path in ("v2/osrs/5m", "v2/osrs/1h"):
    r, h, n, el = get(path, path.split('/')[-1] + "_latest.json")
    dd = r["data"]
    print(path, "timestamp", r.get("timestamp"), ts(r["timestamp"]), "items", len(dd), "bytes", n, "keys", sorted(set(k for v in dd.values() for k in v)))
    print("  headers", {k: v for k, v in h.items() if k.lower() in ("cache-control","age","cf-cache-status","expires")})
