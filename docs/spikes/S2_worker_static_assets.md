# Spike S2: Worker static assets on a path of kohstack.tech

- **Decides:** ADR-0012 (dashboard hosting)
- **Time box:** half a working day
- **Status:** Planned. Not started: needs the prerequisites below.

## Question

Can a Cloudflare Worker with static assets serve a site under a path of kohstack.tech, next to the existing VentraIP origin, with correct caching, security headers, single-page-app routing and preview deploys, without changing anything else on the site?

To keep production untouched, the spike uses **`kohstack.tech/ge-spike/*`**, not `/ge/`.

## What is already known (checked 2026-10-20)

| Fact | Source |
|---|---|
| Static asset requests are free and unlimited; only script invocations count toward 100,000 a day. | Workers static assets billing page |
| Over the limit, a route either fails open (bypasses the Worker) or fails closed (error 1027). | Workers limits page |
| Assets for `example.com/blog/*` must sit under a directory that mirrors the path. | "Serving a subdirectory" page |
| `_headers` in the asset directory sets headers (up to 100 rules); not applied to Worker-generated responses. | Workers headers page |
| How the zone's existing 30-day "cache everything" rule interacts with Worker asset responses: **not verified**. | To test |

## Prerequisites (Sylvester)

1. A Cloudflare API token with Workers Scripts Edit (account) and Workers Routes Edit (zone kohstack.tech) only, exported in the shell session (`CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`). Alternatively, an interactive `wrangler login` that you complete yourself.
2. ~~A screenshot or export of the zone's current cache rules.~~ Read through the API on 2026-10-20 (zone `kohstack.tech`, Free plan):
   - Cache rule 1, "Cache the site at the edge for 30 days (deploys purge it); browsers keep the origin's headers": `http.host in {"kohstack.tech" "www.kohstack.tech"}`, cache on, edge TTL 2,592,000 s (override origin), browser TTL respects origin.
   - Cache rule 2, "SGN data files: always from the origin": `starts_with(http.request.uri.path, "/sgn/") and http.request.uri.path.extension eq "json"`, cache off.
   - No page rules, dynamic redirects, origin rules, response-header transforms or Worker routes on the zone.
   Test 4 must establish whether rule 1 affects responses served by the Worker; the likely exclusion is a third rule bypassing `starts_with(http.request.uri.path, "/ge")`.
3. Approval to create one temporary Worker and one route on kohstack.tech.

## Set-up

- `spikes/s2/` with a pinned Wrangler version (run through `npx`), a configuration file, and an asset directory `public/ge-spike/` containing `index.html`, a content-hashed `app-<hash>.js`, `data/status.json` and a second page for the View Transitions test.
- Before deploying, record the response headers and body hashes of `/`, `/sgn/` and the CV PDF for a before-and-after comparison.

## Steps

| # | Test | Pass condition |
|---|---|---|
| 1 | Deploy the Worker with the route `kohstack.tech/ge-spike/*`. | `/ge-spike/` returns 200 from the Worker. |
| 2 | Blast radius: re-check `/`, `/sgn/`, `/sgn/*.json`, the CV. | Bodies and key headers identical to the "before" record. |
| 3 | Deep links: `/ge-spike/item/4151` with single-page-app fallback on, then off. | Fallback serves `index.html`; off returns the configured 404. Pick one for ADR-0012. |
| 4 | Caching: check `cf-cache-status`, `cache-control` and `age` on HTML, the hashed JS and `status.json`; change `status.json`, redeploy, time until it is visible. | Hashed assets long-lived; HTML and data visible within 5 minutes of a deploy. If the 30-day rule interferes, add a `/ge-spike/*` bypass and re-test. |
| 5 | Headers: `_headers` sets CSP, HSTS, `X-Content-Type-Options`, `Referrer-Policy`, `Permissions-Policy`. Compare with the portfolio's live headers. | Headers present on HTML and assets; CSP blocks an off-list fetch in the browser console. |
| 6 | `www.kohstack.tech/ge-spike/`: second route or redirect. | Same content or a clean redirect, no loop. |
| 7 | Data as static assets (ADR-0015): change `data/status.json`, redeploy, and time until it is live; repeat with 4,000 small per-item files to check upload time and the 20,000-file limit. Confirm no R2 binding or custom domain exists anywhere. | Redeploy finishes inside the hourly run budget and only changed files upload. If not, use the VentraIP fallback in ADR-0012. |
| 8 | Same-site View Transitions between the two spike pages with `@view-transition { navigation: auto }`. | Cross-fade works in Chrome and Safari 18+; reduced motion respected. |
| 9 | Preview: upload a version without deploying and open its preview URL. | Preview serves correctly; note any base-path difference. |
| 10 | Rollback: deploy a change, then roll back. | Previous version live within a minute. |

## Outputs

- `docs/spikes/S2_results.md`: configuration (no secrets), header set, cache findings, data redeploy timings, chosen 404 or fallback behaviour, and go or no-go for ADR-0012.
- The exact cache-rule change needed for `/ge/*`, written for Sylvester to apply or approve.

## Fallback

If the path route cannot coexist cleanly with the zone's cache rules or origin, supersede ADR-0012 with `ge.kohstack.tech` on Workers static assets (same build, no path coordination; loses the same-tab morph).

## Clean-up

Delete the spike Worker, its route and any test bucket. Re-run step 2 to confirm the site matches the "before" record.
