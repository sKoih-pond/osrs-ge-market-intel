# Architecture Decision Records

Each record captures one decision: the context, what was decided, the consequences and the alternatives rejected. Records are never edited to change a decision; a new record supersedes the old one and both stay.

Status values: **Accepted**, **Accepted, pending spike** (holds unless the named spike fails), **Superseded by ADR-NNNN**, **Proposed**.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-runtime-actions-now-vm-after-v1.md) | Runtime: GitHub Actions now, always-on VM after v1 | Accepted |
| [0002](0002-storage-cloudflare-r2.md) | Storage: Cloudflare R2, private lake and public bucket | Accepted, amended by 0015 |
| [0003](0003-table-format-delta-lake.md) | Table format: Delta Lake | Accepted (S1 passed); amended by 0015 |
| [0004](0004-orchestration-airflow-at-phase-c.md) | Orchestration: Airflow-ready tasks now, Airflow 3 at Phase C | Accepted, amended by 0016 |
| [0005](0005-ingestion-grain-backfill-etiquette.md) | Ingestion grain, backfill scope and API etiquette | Accepted, amended by 0015 and 0016 |
| [0006](0006-regimes-as-data.md) | Market and measurement regimes as first-class data | Accepted |
| [0007](0007-tax-model.md) | Grand Exchange tax model | Accepted (exemption seed built in M1) |
| [0008](0008-execution-policies.md) | Execution policies: Flipper, Casual, Holder | Accepted (parameters provisional until M4) |
| [0009](0009-reliability-dimensions.md) | Reliability shown as separate, evidenced dimensions | Accepted, amended by 0016 (Data grade v1) |
| [0010](0010-ml-promotion-gate.md) | Pre-registered ML promotion gate | Accepted |
| [0011](0011-spark-scope.md) | Spark scope: backtest grid and reprocessing only | Accepted, amended by 0015 |
| [0012](0012-dashboard-worker-static-assets.md) | Dashboard served by a Cloudflare Worker at kohstack.tech/ge/ | Accepted (S2 passed); amended by 0015 and 0016 |
| [0013](0013-data-publication-licensing.md) | Data publication and licensing | Accepted, amended by 0015 |
| [0014](0014-repository-ci-secrets.md) | Repository, CI, secrets and code licence (MIT) | Accepted, amended by 0016 |
| [0015](0015-r2-free-tier-by-construction.md) | R2 stays inside the free tier by construction | Accepted, amended by 0016 |
| [0016](0016-m1-live-screener-implementation.md) | M1 live screener: implementation decisions | Accepted |

Source of the decisions: `docs/decisions/2026-10-20_kickoff_decisions.md`. Evidence: `docs/research/2026-10-20_discovery_report.md`.

## Template

```markdown
# ADR-NNNN: Title

- **Status:**
- **Date:**
- **Decided by:**

## Context
## Decision
## Consequences
## Alternatives considered
## References
```
