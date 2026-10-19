# ADR-0015: R2 stays inside the free tier by construction

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester ("ensure it stays within the free limit; an alert after billing is no use")
- **Amends:** ADR-0002, ADR-0003, ADR-0005, ADR-0011, ADR-0012 (each carries a note)

## Context

R2 has no hard spending cap. Cloudflare's budget alerts are informational, fire the day after a threshold is crossed, and do not pause usage. Prevention therefore has to come from the design.

Billing facts (Cloudflare R2 pricing page, read 2026-10-20):

| Item | Free each month | Notes |
|---|---|---|
| Storage | 10 GB-month | Average of each day's peak storage. Standard class only; Infrequent Access has no free tier, a 30-day minimum and a retrieval fee. |
| Class A operations | 1,000,000 | `ListBuckets`, `PutBucket`, `ListObjects`, `PutObject`, `CopyObject`, all multipart operations except abort, `LifecycleStorageTierTransition`, `PutBucket*` configuration |
| Class B operations | 10,000,000 | `HeadBucket`, `HeadObject`, `GetObject`, `UsageSummary`, `GetBucket*` configuration |
| Free operations | unlimited | `DeleteObject`, `DeleteBucket`, `AbortMultipartUpload` |
| Egress | free | |

Cost vectors, from unbounded to bounded:
1. **Public reads.** A public bucket (r2.dev or a custom domain) turns every visitor cache miss into a Class B operation, and anyone can force misses with junk query strings. This is the only vector nobody on the project controls.
2. **Pipeline bugs.** A loop that lists or reads repeatedly.
3. **Chatty engines.** Spark and S3 clients issue many `HEAD` and `LIST` calls per file.
4. **Storage creep.** Raw responses, small files, and old Delta file versions that are never cleaned up.
5. **Infrequent Access.** Any object written to, or moved to, that class is billed from the first byte.

## Decision

Four layers. The first two cannot be bypassed by anyone outside the project; the last two stop the pipeline itself.

**Layer 1: nothing in R2 is reachable by the public (by construction)**
- No r2.dev URL, no custom domain, no Worker R2 binding on a public route, on any bucket. The only R2 credentials are project-scoped API tokens held as secrets.
- Published dashboard data is shipped as **Workers static assets** (free and unlimited requests, 20,000 files and 25 MiB per file on the free plan), redeployed by the pipeline. This replaces the public bucket in ADR-0002 and the R2 data-origin options in ADR-0012.
- One bucket, `ge-intel-lake`, private. Tokens: Object Read & Write, limited to that bucket; no admin tokens in the pipeline.

**Layer 2: storage cannot creep (by construction)**
- Every object is Standard class. No code sets a storage class; no lifecycle rule transitions to Infrequent Access.
- Lifecycle rules: `bronze/` objects expire after 30 days; incomplete multipart uploads abort after 1 day.
- Backfill raw responses (about 3 GB) are kept on the developer's disk with their hashes, not uploaded. Only the typed tables built from them go to R2.
- Delta maintenance: compaction plus `VACUUM` with 7-day retention, scheduled weekly.

**Layer 3: a usage check before every run (fails closed)**
- `ge_intel.cost_guard` reads month-to-date account usage from Cloudflare's GraphQL Analytics API (`r2OperationsAdaptiveGroups`, `r2StorageAdaptiveGroups`) using a token with Account Analytics Read only.
- The run does not start if Class A or Class B usage has reached **50% of the free amount**, if stored bytes have reached **6 GB**, or if usage cannot be read. Unknown operation types are counted as Class A.
- A warning (run continues) when the month-end projection, made after 72 hours of data, reaches 50%.
- A blocked run exits non-zero, which fails the workflow and opens an issue through the watchdog; the dashboard keeps serving its last published assets.
- The analytics API allows about 300 queries per 5 minutes per user or token, shared with dashboard browsing for a user. The pipeline uses its own token and one query per run. A rate-limit refusal counts as "usage unreadable", so that run is skipped and the next run catches up.

**Layer 4: per-run caps**
- Every R2 request the project's own code makes goes through a counter that refuses the request (raises before sending) once the run's cap is reached. Hourly-run caps: **500 Class A, 5,000 Class B, 50 MB written**. At 744 runs a month, every run hitting its cap would total 372,000 Class A (37%) and 3.72M Class B (37%).
- Traffic generated inside engines (delta-rs, DuckDB, Spark) is not visible to that counter. It is bounded instead by job timeouts (10 minutes for the hourly run), measured per operation in spike S1, and checked after each run against Cloudflare's analytics; a run that exceeded its budget blocks the next run until reviewed.
- Spark jobs and backfills copy the partitions they need to local disk once, compute locally, and upload results as a few large files. R2 is the archive, not working storage.

**Expected usage (design budget)**

| Resource | Expected per month | Share of free tier | Halt at |
|---|---|---|---|
| Class A | about 45,000 (hourly runs) | about 5% | 500,000 |
| Class B | about 150,000 | about 2% | 5,000,000 |
| Storage | under 4 GB in year one | under 40% | 6 GB |

## Consequences

- The only way to be billed is for the project's own pipeline to exceed its limits despite layers 3 and 4, or for someone with account access to change bucket settings by hand.
- Publishing dashboard data means redeploying the Worker each hour. Spike S2 confirms deploy behaviour and the file-count budget (the data contract must stay well under 20,000 files).
- Delta time travel is limited to the 7-day VACUUM window. Reproducibility of training sets rests on partition immutability, the recorded table version and a row hash, not on unlimited time travel.
- The analytics token is one more secret (read-only, account analytics only).
- Cloudflare's default budget alert stays as a last-resort indicator that these layers failed; it is not part of the control.

## Alternatives considered

- **Budget alerts only:** after the fact; rejected by Sylvester.
- **Public bucket behind a custom domain with caching:** cheap in normal use, but cache-busting requests make reads unbounded.
- **Publishing data to the VentraIP host over FTP (the /sgn/ pattern):** also zero marginal cost; kept as the fallback if hourly Worker deploys prove unsuitable in S2.
- **Routing all engine traffic through a counting proxy:** exact counts, but heavy; measurement in S1 plus timeouts gives a sufficient bound.

## References

- R2 pricing: https://developers.cloudflare.com/r2/pricing/
- R2 public buckets and r2.dev limits: https://developers.cloudflare.com/r2/buckets/public-buckets/ and https://developers.cloudflare.com/r2/platform/limits/
- Budget alerts: https://developers.cloudflare.com/billing/manage/budget-alerts/
- Workers limits (static assets): https://developers.cloudflare.com/workers/platform/limits/
- Workers static assets billing: https://developers.cloudflare.com/workers/static-assets/billing-and-limitations/
