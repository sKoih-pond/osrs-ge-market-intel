# ADR-0002: Storage: Cloudflare R2, private lake and public bucket

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decisions 2 and 10)

## Context

Hourly runs in Actions start with an empty disk, so history must live off the runner. The dashboard needs to download published data directly from the browser. Expected size after year one is about 5 GB. kohstack.tech is already on Cloudflare. BigQuery's sandbox expires tables after 60 days, which rules it out as an archive.

## Decision

Two R2 buckets:

| Bucket | Access | Contents |
|---|---|---|
| `ge-intel-lake` | Private (scoped API tokens only) | `bronze/` raw API responses as gzipped JSON, partitioned by source, endpoint and date, deleted after 90 days by an R2 lifecycle rule; `silver/` typed tables; `gold/` features, scores, anomalies, reliability, backtests; `ops/` run records |
| `ge-intel-public` | Public read, through the Worker (ADR-0012) or an R2 custom domain | `v1/` the published data contract only |

- Formats: Delta tables for silver and gold (ADR-0003); JSON and small Parquet files in `v1/`.
- Credentials: one R2 API token per purpose, each limited to the bucket it needs (pipeline write to both; spike token to a temporary bucket). Stored as GitHub Actions secrets; never in files.
- A usage notification is set in Cloudflare. There is no hard spending cap, so usage is kept at about a tenth of the free tier.

## Consequences

- Downloads from R2 are free, so the browser can fetch published data directly.
- Storage survives loss of any compute host, which de-risks Phase C.
- R2's S3 API works with DuckDB, delta-rs and Spark (S3A), subject to spike S1.
- Raw responses are replayable for 90 days; after that, provenance rests on stored content hashes and the typed tables.
- Year two will need a decision on 5-minute retention if storage approaches 8 GB.

## Alternatives considered

- **GitHub Releases plus Pages:** no sign-up, but whole-file access only and large binaries in GitHub.
- **Local archive, marts only in the cloud:** runners could not see history.
- **Backblaze B2, Hugging Face Datasets:** viable, but another account, and Hugging Face implies public raw data (see ADR-0013).

## References

- Cloudflare R2 pricing: https://developers.cloudflare.com/r2/pricing/
- Discovery report section 2.3 (sizing).
