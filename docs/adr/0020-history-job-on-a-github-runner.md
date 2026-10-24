# ADR-0020: The history job runs on a GitHub runner started on request; its raw responses live in R2

- **Status:** Proposed (accepted when Sylvester merges the PR that carries it)
- **Date:** 2026-10-21
- **Decided by:** Sylvester chose a free cloud host over the laptop ("I'd like to keep space on my MacBook", 21 October 2026); design by the agent. Amends ADR-0015 (layer 2's rule on backfill raw responses), ADR-0018 (decision 4 and its laptop context) and ADR-0019 (where option C2 would move data); each carries a note.

## Context

The M2 plan ran the history job on the laptop: about 9 GB at the peak of a run (the Kaggle dump 4.8 GB, the local `history/` lake about 2 GB, DuckDB's spill up to 2 GB) and, for good, the raw Wiki responses the job fetched. ADR-0019's option C2 would later have moved 5-minute months older than a year to local disk too, about 1.9 GB a year.

Very little of that needs a permanent home:
- **The dump** stays on Kaggle; version 1 never changes and every file is pinned by size and SHA-256 (`src/ge_intel/sources/kaggle_osrs_ge_5m_v1.json`), so it can be fetched again and checked.
- **The local lake** is derived (ADR-0018 decision 4) and R2 holds its upload.
- **The raw Wiki responses** are inputs and cannot be fetched again without asking the Wiki again: about 541 known requests (405 seam buckets, 136 `/1h` checks) plus the dump's gaps, at most about 60 KB a gzipped response (the discovery report's `/1h` estimate: 49,300 requests, about 3 GB), so well under 0.1 GB.
- **C2's archive** comes later, if at all (ADR-0019).

The constraints: no card on file anywhere that could be charged (ADR-0015's free tier by construction), the raw archive and full history stay private (ADR-0013), one writer per table (ADR-0018), and downloads, Wiki requests and the upload are Sylvester's to start (delegation plan), which he may hand to an agent (he asked for that on 21 October 2026, so that runs can go ahead without him). The Wiki asks for a note in its Discord channel before a backfill; that note comes first either way.

## Decision

### 1. A workflow that runs only on request

`.github/workflows/history.yml`, `workflow_dispatch` only: Sylvester starts it from the Actions tab, or an agent he has asked to run it starts it through GitHub's API and schedules any later runs with its own scheduled tasks. There is no GitHub schedule; GitHub's scheduler never fired reliably here (ADR-0017). Three inputs: the step (`plan`, `build`, `check-1h`, `fill`, `check-and-fill`, which runs the month checks and then the fill in one run, or `upload`), `dry_run` (on by default, so a bare request sends and writes nothing) and `limit` for `fill`. An agent stops at the same decision points as a person: a quarantined month, a revision, and (until ADR-0019 was decided, 24 October 2026) the upload.
- A standard GitHub-hosted runner: free and unlimited for a public repository, 4 CPUs, 16 GB of memory and a 14 GB disk; the job first removes SDKs it never uses, for margin over the 9 GB peak. Timeout 4 hours.
- Its own concurrency group, `ge-intel-history-writer`: the job writes only `history/`, so it never queues behind or blocks the hourly run (ADR-0018 decision 1).
- R2 secrets reach only the steps that read or write R2 (ADR-0014; a test checks it).

### 2. Each run rebuilds on the runner and keeps only the inputs

1. `ge-intel history pull`: restore the raw responses from R2.
2. The dump: restored from the Actions cache (fixed key: version 1 never changes, and `download` checks every restored file against its pin); on a miss, downloaded from Kaggle and cached as soon as it is verified.
3. `ge-intel history build`, then the chosen step.
4. `ge-intel history push`: save any new raw responses to R2, also when the step stopped partway, so nothing fetched is fetched twice.
5. `upload`, when chosen, unchanged (ADR-0018, behind the cost guard).

Hashes the first download records in the pins file are printed to the run log and summary, to commit in a small PR (the runner's checkout is thrown away).

### 3. Raw responses in `history/raw_wiki`

A Delta table in R2, written by delta-rs and partitioned by endpoint: `key` (the file's path under the history directory), `endpoint`, `sha256` of the body, `body_gz` (the file itself) and `meta` (the request metadata file beside it, verbatim).
- `push`: one commit holding only the responses R2 does not have yet (replace-where on their keys, so a re-run converges); nothing new, no commit; behind the same fail-closed cost guard as `upload`.
- `pull`: writes missing files byte for byte, metadata beside them, after checking each body against its SHA-256; a different local copy stops the run as a revision, as `build` would.
- An input, not a derived table: `build` never writes it, `upload` never copies it, and the weekly maintenance never touches it (`history.RAW`, outside `history.TABLES`; tested).

### 4. The laptop still works

Nothing in the job depends on where it runs. `GE_HISTORY_DIR` still points at local files; `pull` and `push` let a laptop run and a runner run carry on from each other.

### 5. Where C2's archive would go

When option C2 comes up (ADR-0019: not before late 2027), its months go to a private Hugging Face dataset: 100 GB of private storage free, no card. The account is Sylvester's to create, then.

## Cost

- **GitHub:** free for a public repository; the dump takes 4.8 GB of the repository's 10 GB Actions cache, which drops it after 7 days unused (the next run downloads it again).
- **R2:** under 0.1 GB stored (counted by the 6 GB halt); one commit per `push` (about 10 Class A), a few reads per `pull`.
- **Kaggle:** one 4.8 GB download, and another only after 7 days without a run.

## Consequences

- **The MacBook needs no disk for M2.**
- The Wiki requests come from GitHub's address ranges, with the project's User-Agent at 1 a second; the note in the Wiki's Discord channel should say so.
- Every run spends 15 to 30 minutes restoring and rebuilding before its step.
- ADR-0015's rule that backfill raw responses stay on the developer's disk was written for about 3 GB of raw `/1h` backfill; the Kaggle dump replaced that backfill, and what remains is well under 0.1 GB.
- **Not verified from the agent's session**, whose network blocks Kaggle: that Kaggle serves the pinned files to a runner without a login, as `kaggle.download` assumes (it sends only a User-Agent; the README pin was recorded that way). If Kaggle refuses, the download step fails with the HTTP status; the fix is a Kaggle API token as two repository secrets and basic authentication in `kaggle.download`, a small change.
- Settling a revision (two bodies for one bucket) may mean deleting a row of `history/raw_wiki`: a delete in R2, so Sylvester's call.

## Alternatives considered

- **The laptop, as planned:** about 9 GB at the peak and the raw responses for good. Still possible (decision 4).
- **A private Hugging Face dataset now** (100 GB free, no card): sound, but a new account and token for well under 0.1 GB; the right home for C2's gigabytes later.
- **A private Kaggle dataset:** a private quota of about 100 to 200 GB by third-party accounts; each upload is a new version of the whole dataset, awkward for small additions.
- **Backblaze B2:** S3-compatible and 10 GB free, but a payment method past 10 GB, so a weaker guarantee.
- **AWS, Google Cloud, Azure and Oracle free tiers:** a card on file, and some expire after 12 months.
- **Actions artifacts or the Actions cache for the raw responses:** artifacts expire within 90 days and cache entries after 7 days unused; not durable.
- **Anything public** (public Hugging Face or Kaggle datasets, GitHub Releases, the repository): ADR-0013 keeps the raw archive private.

## References

- Code: `.github/workflows/history.yml`, `src/ge_intel/history.py` (`RAW`, `push_raw`, `pull_raw`), `src/ge_intel/cli.py`. Tests: `tests/test_history.py`, `tests/test_workflows.py`.
- GitHub-hosted runners: https://docs.github.com/en/actions/reference/runners/github-hosted-runners
- Hugging Face storage limits: https://huggingface.co/docs/hub/en/storage-limits
