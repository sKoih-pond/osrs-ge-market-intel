import json, time, urllib.request, sys, os, datetime as dt
UA = "osrs-ge-market-intel research probe (portfolio project, github.com/sKoih-pond)"
def get(path):
    req = urllib.request.Request("https://prices.runescape.wiki/api/" + path, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as r: body = r.read()
    except Exception as e:
        time.sleep(0.6); return {"error": str(e)}
    time.sleep(0.6); return json.loads(body)
U = lambda y,m,d,h=12: int(dt.datetime(y,m,d,h,tzinfo=dt.timezone.utc).timestamp())
F = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
WATCH = {4151:"whip", 561:"nature rune", 385:"shark", 20997:"twisted bow", 13190:"bond", 2:"cannonball"}
def summarize(r):
    d = r.get("data") or {}
    tv = sum((v.get("highPriceVolume") or 0)+(v.get("lowPriceVolume") or 0) for v in d.values())
    w = {WATCH[i]: ((d.get(str(i)) or {}).get("highPriceVolume"), (d.get(str(i)) or {}).get("lowPriceVolume")) for i in WATCH}
    return len(d), tv, w
print("== earliest coverage (1h @12:00 UTC)")
for (y,m,d) in [(2021,2,1),(2021,3,1),(2021,3,8),(2021,3,14),(2021,3,20),(2021,4,1)]:
    r = get(f"v2/osrs/1h?timestamp={U(y,m,d)}"); n, tv, _ = summarize(r); print(F(U(y,m,d)), "items", n, "totvol", tv, r.get("error",""))
print("== earliest coverage (5m)")
for (y,m,d) in [(2021,3,8),(2021,3,14)]:
    r = get(f"v2/osrs/5m?timestamp={U(y,m,d)}"); n, tv, _ = summarize(r); print(F(U(y,m,d)), "items", n, "totvol", tv)
print("== daily 1h@12:00 UTC around 30 Sep 2026 source switch")
for day in [dt.date(2026,9,1)+dt.timedelta(days=i) for i in range(0,49,1) if i in (0,7,14,21,24,25,26,27,28,29,30,31,32,33,34,35,42,47)]:
    t = U(day.year, day.month, day.day); r = get(f"v2/osrs/1h?timestamp={t}"); n, tv, w = summarize(r)
    print(F(t), day.strftime("%a"), "items", n, "totvol", f"{tv:,}", w)
