# ADR-0005: Ingestion grain, backfill scope and API etiquette

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decisions 4 and 5)

> **Implemented in M1 (ADR-0016, 2026-10-20):** gap-fill runs newest first, at most 96 buckets per run; facts commit before the manifest so a crash cannot duplicate rows; outage = under 0.6 of the trailing 24-hour median item count, with 15 minutes' grace for unpublished buckets (`docs/methodology/outage_rule.md`); bronze keys add a content hash; `/latest` and `/mapping` refuse intervals they cannot reproduce.

> **Amended by ADR-0015 (2026-10-20):** backfill raw responses are stored on local disk with their SHA-256, not uploaded to R2; only the typed tables built from them are. Live raw responses go to `bronze/` in R2 and expire after 30 days.

> **Amended 2026-10-20 (Sylvester):** the historical backfill comes from the Kaggle dump `antonygarand/osrs-ge-5m` (version 1, CC BY-NC-SA 3.0), not from about 81,000 Wiki requests. Rules: pin version 1 with per-file SHA-256; convert `timestamp` × 300 (it is a bucket number); tag provenance per row; check 2 random hours per month against the Wiki `/1h` endpoint and quarantine any month that disagrees; fill missing buckets from the API with the gap-finder; keep the 4.8 GB archive on local disk and put derived hourly history plus recent 5-minute data in R2. Wiki requests for history drop to a few hundred to low thousands. Evidence: `docs/research/2026-10-20_kaggle_osrs_ge_5m_assessment.md`.

## Context

Measured on 19 Oct 2026: `/1h` is an exact roll-up of twelve `/5m` buckets (volumes matched on 3,234 of 3,234 items; high-side VWAP within 0.1% on 2,638 of 2,640). Data starts on 8 Mar 2021. Buckets appear about 10 to 20 seconds after they close and are cached by Cloudflare (`s-maxage=300`). The un-timestamped "latest" responses can lag by a bucket because of caching. The Wiki has no hard rate limit but asks for a descriptive User-Agent, blocks default library User-Agents, and asks real-time pipeline builders to join #api-discussion on its Discord.

## Decision

**Grain**
- From 30 Sep 2026 onward the atomic grain is the 5-minute bucket. Hourly figures are derived from it, and the API's `/1h` is fetched as an independent reconciliation check.
- Before 30 Sep 2026 (minus the 90-day 5-minute window) the grain is the hourly bucket.
- `/mapping` is snapshotted daily into a slowly changing item dimension (type 2). `/latest` is snapshotted hourly for staleness grading only.
- Official guide price and volume come from Weird Gloop's exchange history, once per item for backfill and incrementally after.

**Requests**
- Only explicit-timestamp requests for closed buckets. Never loop `/latest?id=` over items.
- Header `Accept-Encoding: gzip` on every request.
- User-Agent: `osrs-ge-market-intel/<version> (+https://github.com/sKoih-pond/osrs-ge-market-intel; sylvester@kohstack.au)`, held in configuration. The client refuses to start if the User-Agent is missing or matches a blocked default.
- Steady state: about 14 requests per hour. Backfill: at most 1 request per second, resumable, run by Sylvester after the Discord post.

**Backfill scope**
- Hourly: 8 Mar 2021 to now (about 49k requests).
- 5-minute: 2 Jul 2026 (90 days before the switch) to now (about 32k requests by late October).
- Weird Gloop: about 4k requests; ask whether a bulk route exists.

**Correctness**
- Deterministic object keys from (endpoint, bucket start). Raw bodies are stored with their SHA-256, request URL, HTTP status and fetch time.
- Gap detection compares expected buckets with committed ones; every run fills all gaps up to the newest closed bucket.
- An item missing from a bucket means no trades. A bucket whose item count falls far below its trailing median is flagged as a source outage (for example, weekly update downtime).
- Revision audit: a random sample of buckets is re-fetched at +1 h, +24 h and +7 d and compared by hash. If revisions appear, a bounded re-fetch window is added (supersede this ADR).
- All times are UTC, bucket-start semantics, stored as timezone-aware timestamps. Prices are stored as 64-bit or decimal values (the API warns they can exceed 32-bit).

## Consequences

- About 145M hourly rows and about 200M 5-minute rows per year. Fits DuckDB on one machine.
- Requests mostly hit the Wiki's CDN cache, keeping load off its origin.
- Historical buy limits before the first mapping snapshot are unknown; current limits are used and flagged.

## Alternatives considered

- Full 5-minute history since 2021 (about 590k requests, about 10 GB): rejected as heavy for a community API and over the free tier, for little gain.
- Two years of hourly history only: lighter, but loses the 1% tax era and early regime changes.

## References

- OSRS Wiki Real-time Prices (API v2, acceptable use): https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices
- Probe scripts: `docs/research/probes/probe1.py` to `probe6.py`.
