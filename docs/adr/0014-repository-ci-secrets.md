# ADR-0014: Repository, CI and secrets

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 11; code licence chosen 2026-10-20)

## Context

Hourly Actions runs need unlimited minutes, which GitHub provides only for public repositories (a private repo on the free plan gets 2,000 minutes a month; hourly runs alone need about 2,200 to 3,600). Reviewers should see the project's history. The pipeline handles R2 and Cloudflare credentials.

## Decision

**Repository**
- Public from the first push at `github.com/sKoih-pond/osrs-ge-market-intel` (created by Sylvester, or by an agent on his explicit go).
- Trunk-based: short branches, pull requests into `main`, CI must pass to merge. Branch protection on `main`.
- Secret scanning and push protection enabled on day one.

**Python and dependencies**
- Python 3.12 (`requires-python >=3.12`), the same version locally (Homebrew), in CI and in the container.
- M0 uses a project virtualenv with pip. A lockfile (uv) is introduced in M1 when the first runtime dependencies arrive.
- Lint and format with ruff; tests with pytest.

**CI (GitHub Actions)**
- On every pull request and push to `main`: ruff check, ruff format check, pytest. Later milestones add dbt build on fixtures, data-contract schema tests and the dashboard build.
- Workflow permissions default to `contents: read`; jobs request more only when needed.
- Actions pinned to major versions now (`actions/checkout@v7`, `actions/setup-python@v7`); pinned to commit SHAs before any workflow handles secrets (M1).
- Scheduled pipeline workflows are separate from CI and use concurrency groups so two runs never write the same table.
- A container image is built and pushed to GitHub Container Registry from M3.

**Secrets**
- Stored only as GitHub Actions secrets (and, in Phase C, on the VM in a root-only env file). Never in the repo, in logs or in chat.
- One token per purpose, least privilege: R2 tokens limited to their bucket; the Cloudflare deploy token limited to Workers scripts and routes on the kohstack.tech zone.
- `.gitignore` covers `.env*`, local data, DuckDB files and Spark metastore files.

**Code licence**
- **MIT** (`LICENSE` at the repo root; `license = "MIT"` in `pyproject.toml`). Chosen over Apache-2.0 because its extra terms (patent grant, change marking, contribution and trademark clauses) do not apply to a solo project with no patents, and MIT is the shortest and most widely understood permissive licence.
- MIT covers the code and documentation only. The sample data (ADR-0013) stays CC BY-NC-SA 3.0; when it is added in M1 it lives in its own folder with its own licence notice, and the README states the split.

## Consequences

- Early rough edges are public; the decision records and tests show the reasoning behind them.
- The 60-day inactivity rule for scheduled workflows applies (ADR-0001 watchdog).

## Alternatives considered

- Private until v1: over the free Actions quota once hourly runs start.

## References

- Kickoff decisions log, decision 11.
