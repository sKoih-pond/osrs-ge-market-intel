# Spike S2 results: Worker static assets on a path of kohstack.tech

- **Run:** 20 October 2026, 22:05 to 22:15 UTC, on the temporary path `kohstack.tech/ge-spike/`
- **Plan:** `S2_worker_static_assets.md`. Harness: `spikes/s2/` (git-ignored): snapshots of the live site before, during and after (`before.json`, `after_deploy.json`, `after_cleanup.json`).
- **Verdict: GO for ADR-0012**, with the configuration below. No change to the zone's cache rules is needed. View Transitions remain unverified on a real device and are carried to M6.

## Tooling

| Component | Version or setting |
|---|---|
| Wrangler | 4.150.0 (released 13 October 2026), run through `npx`, telemetry off |
| Worker | static assets only, no script; `compatibility_date` 2026-10-01 |
| Token | the trimmed Workers token (Workers Scripts Edit, Account Settings Read, Workers Routes Edit on kohstack.tech, User Details Read, Memberships Read) was sufficient for deploy, routes, rollback and delete |

## Results by test

| # | Test | Result | Evidence |
|---|---|---|---|
| 1 | Deploy assets-only Worker on `kohstack.tech/ge-spike/*` | **Pass** | Live after a 28 s first deploy (about 8 s of upload and route setup); Wrangler confirmed the route serves `public/ge-spike/*` |
| 2 | Blast radius | **Pass** | `/`, `/sgn/`, `/sgn/flight_lookup.json`, the CV and `www` home: bodies and key headers identical before, during and after |
| 3 | Deep links | **Pass with a design change** | Single-page-app fallback needs an `index.html` at the asset root and then answers **every** unmatched request with 200 HTML, including `fetch()` calls for missing data files. `404-page` mode returns honest 404s for missing data and pages, and serves item pages by query string (`/ge-spike/item/?id=4151`, 200) |
| 4 | Caching | **Pass** | Repeat requests are `cf-cache-status: HIT` from the Workers asset cache, yet changed content was live **1 s** after a redeploy. The zone's 30-day rule does not pin Worker responses. `_headers` controls `Cache-Control` per path (HTML `max-age=0`, hashed JS one year `immutable`, data 60 s) |
| 5 | Security headers | **Pass, one fix needed** | The spike's own CSP, HSTS, nosniff, referrer, permissions and frame headers came from `_headers`. In Chrome the CSP blocked a deliberate cross-origin `fetch` (good) and also blocked Cloudflare's injected Web Analytics beacon, so the production policy must allow `https://static.cloudflareinsights.com` in `script-src`, as the portfolio's does |
| 6 | `www` and no-slash URLs | **Pass with extra routes** | With only `kohstack.tech/ge-spike/*`, both `kohstack.tech/ge-spike` and `www.kohstack.tech/ge-spike/` fell through to the origin's 404. Adding exact and wildcard routes for both hosts gives a 307 redirect from `/ge-spike` to `/ge-spike/` and serves `www` |
| 7 | Data as static assets | **Pass** | 4,013 files (4,000 per-item JSON files) deployed in 24 s, uploads 9.4 s. Changing 10 files then uploaded only those 10 (3,998 skipped); redeploy 16 s. Content-addressed: identical files upload once. Free-plan limit 20,000 files per version |
| 8 | Same-site View Transitions | **Inconclusive in this environment** | Both pages opt in (`@view-transition { navigation: auto }`), but the in-app browser (Chrome 152 engine) reported no transition (`pageswap` with no `viewTransition`) both for the spike and for the live portfolio-to-`/sgn/` morph, which the vault records as device-verified. The pane does not run cross-document transitions. Verify on iPhone Safari and desktop Chrome in M6 |
| 9 | Preview links | **Not run** | Preview URLs need a `workers.dev` subdomain; the account has none, and creating one is an account setting. Decide in M1 |
| 10 | Rollback | **Pass** | `wrangler rollback <version>` took 6 s; the previous data was served within 5 s |
| Clean-up | Delete Worker and routes | **Done** | Account has no Workers and the zone has no Worker routes (checked through the API); live pages identical to the before snapshot |

## Configuration for production (`kohstack.tech/ge/`)

```jsonc
{
  "name": "ge-dashboard",
  "compatibility_date": "<pinned at M6>",
  "assets": { "directory": "./dist", "not_found_handling": "404-page" },
  "routes": [
    { "pattern": "kohstack.tech/ge", "zone_name": "kohstack.tech" },
    { "pattern": "kohstack.tech/ge/*", "zone_name": "kohstack.tech" },
    { "pattern": "www.kohstack.tech/ge", "zone_name": "kohstack.tech" },
    { "pattern": "www.kohstack.tech/ge/*", "zone_name": "kohstack.tech" }
  ],
  "workers_dev": false
}
```

- Build output nests everything under `dist/ge/`, with a `404.html` there and `_headers` at `dist/`.
- Routing by query string (`/ge/item/?id=…`), not client-side paths, so missing files stay real 404s.
- `_headers`: the spike's set, plus `https://static.cloudflareinsights.com` in `script-src`, and `connect-src 'self'` (data is same-origin static assets per ADR-0015).
- Cache: HTML revalidates, hashed assets one year `immutable`, data 60 s. No zone cache-rule change.
- Hourly publish: one `wrangler deploy` uploads only changed data files (about 16 s for a handful of changes).

## Not settled here

- Real-device View Transitions (M6).
- Preview links and the `workers.dev` subdomain (M1 decision).
- Whether `www` should redirect to the apex for `/ge/` rather than serve it (both work; the portfolio currently serves `www` without a redirect).
