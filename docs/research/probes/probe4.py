import json, time, urllib.request, urllib.error, sys, os, datetime as dt
UA = "osrs-ge-market-intel research probe (portfolio project, github.com/sKoih-pond)"
OUT = sys.argv[1]
def get(path):
    req = urllib.request.Request("https://prices.runescape.wiki/api/" + path, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as r: body = r.read(); hdr = dict(r.headers)
    except urllib.error.HTTPError as e:
        time.sleep(0.6); return None, {"status": e.code}, b""
    time.sleep(0.6); return json.loads(body), hdr, body
F = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
now = int(time.time()); print("now", F(now), now)
h0 = now - now % 3600; m0 = now - now % 300
for back in (1, 2):
    t = h0 - back*3600; r, h, _ = get(f"v2/osrs/1h?timestamp={t}")
    print("1h bucket", F(t), "items", len((r or {}).get("data", {})), {k: v for k, v in h.items() if k.lower() in ("age","cf-cache-status","expires","status")})
for back in (1, 2, 3):
    t = m0 - back*300; r, h, _ = get(f"v2/osrs/5m?timestamp={t}")
    print("5m bucket", F(t), "items", len((r or {}).get("data", {})), {k: v for k, v in h.items() if k.lower() in ("age","cf-cache-status","expires","status")})
base = int(dt.datetime(2026,10,18,0,tzinfo=dt.timezone.utc).timestamp())
for i in range(24):
    t = base + 3600*i; r, h, body = get(f"v2/osrs/1h?timestamp={t}")
    open(os.path.join(OUT, f"{t}.json"), "wb").write(body)
print("saved 24 hourly buckets")
