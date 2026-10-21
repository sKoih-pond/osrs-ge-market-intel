# Runbook (M1)

How the hourly pipeline runs, what can go wrong, and what to do. Decisions behind it: ADR-0001, 0004, 0015 and 0016.

## What runs

| Workflow | When | Does |
|---|---|---|
| `hourly` | `:06` and `:26` every hour, or by hand | Cost guard, then every task in `ge_intel.tasks.REGISTRY` for the newest closed hour: `/mapping` (daily), `/5m` gap-fill, `/latest`, dbt mart, publish, duplicate check (daily), maintenance (weekly, Monday 00:xx UTC) |
| `watchdog` | `:13` and `:43` every hour | Reads the public `status.json` and the `hourly` workflow's state; opens one issue labelled `watchdog` when data is over 2 hours old or the schedule is off, and closes it on recovery |
| `ci` | Every pull request and push to `main` | Lint, unit, contract and end-to-end tests, Wrangler dry run |
| `deploy-clock` | By hand | Deploys `ge-clock` (ADR-0017), the Cloudflare Worker whose Cron Triggers dispatch `hourly` (`:06`, `:26`) and `watchdog` (`:13`, `:43`) |

Repository settings the workflows read: secrets `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ANALYTICS_TOKEN`; variables `CLOUDFLARE_ACCOUNT_ID`, `GE_DEPLOY_ENABLED` (`true` to deploy to `kohstack.tech/ge/`), `GE_GUARD_ACK_RUN_ID` (see below).

## Exit codes

`0` committed success; `1` a task failed (the log names it, and the rest of the run stops); `2` R2 usage unreadable, run not started; `3` halted by the cost guard, run not started.

## A run failed

1. Open the run's log. Each line is JSON; find `"status": "failed"` and its `error`.
2. Nothing needs undoing. Every task is idempotent and catch-up comes from storage, so the next scheduled run retries and fills any gap. Re-run by hand from the Actions tab (`hourly`, Run workflow) if you do not want to wait.
3. If the same task fails twice in a row, it is not transient. Common causes:
   - `UpstreamIncomplete`: the hour's 5-minute buckets are not all recorded, usually because the Wiki API failed for those buckets. Check `prices.runescape.wiki` responds, then re-run.
   - `ContractViolation`: the published files would break the v1 contract. Nothing was deployed; the page keeps serving the last good data. Fix the code and add a test.
   - `ContractError` from the Wiki client: the API's response shape changed. Do not loosen the parser blindly; read what changed first.

## The cost guard stopped a run (exit 2 or 3)

- **Exit 2, usage unreadable:** the analytics API refused or timed out (it rate-limits after about six heavy queries in five minutes). The next run retries. If it persists, check the `CLOUDFLARE_ANALYTICS_TOKEN` secret.
- **Exit 3, month-to-date usage at 50% of a free allowance or 6 GB stored:** stop and investigate before anything else runs. Read usage in the Cloudflare dashboard (R2, Metrics). Nothing restarts on its own until usage drops back (next month) or the cause is fixed.
- **Exit 3, previous run over its per-run budget:** the log names the run id and its counts. Look at that run: what did it do differently (a large catch-up, maintenance, a bug)? When satisfied, set the repository variable `GE_GUARD_ACK_RUN_ID` to that run id. Runs resume on the next slot. The acknowledgement covers that one run only.

## The watchdog opened an issue

- **Data is stale:** look at the latest `hourly` runs. If they are failing, see above. If they are not running at all, check the schedule (next item) and the Actions status page.
- **Schedule disabled (`disabled_inactivity` or `disabled_manually`):** GitHub switches scheduled workflows off after 60 days without repository activity. Re-enable `hourly` (and `watchdog`) in the Actions tab, workflow page, Enable workflow. A commit to `main` resets the 60-day clock.
- The issue closes itself once data is fresh and the schedule is active.

## The schedule never fires

Seen on 20 October 2026: after `hourly.yml` and `watchdog.yml` first landed on `main`, GitHub ran no `schedule` event for either workflow for four hours, with no GitHub incident open and manual runs working. Check with `gh api "repos/sKoih-pond/osrs-ge-market-intel/actions/runs?event=schedule&per_page=1" --jq .total_count`.

1. Keep the data fresh by hand meanwhile: Actions tab, `hourly`, Run workflow (each run catches up every missed bucket).
2. Make GitHub register the schedule again by changing the `cron:` line itself (a different minute is enough) in a pull request and merging it. Changes elsewhere in the repository do not re-register it.
3. That did not help on 20 October, so the clock is now `ge-clock` (ADR-0017); the GitHub schedules stay as a second clock.

## ge-clock: setting up and rotating its token

1. Sylvester creates a fine-grained personal access token on GitHub (Settings, Developer settings, Fine-grained tokens): resource owner `sKoih-pond`, repository access **only** `osrs-ge-market-intel`, repository permission **Actions: Read and write** (Metadata read-only comes with it), nothing else. No expiry date: a persistent token is the accepted choice (ADR-0017), so there is no renewal deadline.
2. Store it as the repository secret `GH_DISPATCH_TOKEN` (repository Settings, Secrets and variables, Actions). Never paste it anywhere else.
3. Run the `deploy-clock` workflow (Actions tab). It deploys `ge-clock` and pipes the secret into the Worker, so the value never appears in a log. Cron Triggers need the account's `workers.dev` subdomain to exist (`kohstack.workers.dev`, created 20 October); if Cloudflare answers error 10063, it has been deleted. Neither Worker is served on it: both keep `workers_dev` and preview URLs off.
4. Check: within 20 minutes an `hourly` run with event `workflow_dispatch` appears at `:06` or `:26`. In the Cloudflare dashboard, Workers & Pages, `ge-clock`, Logs, each tick logs `dispatched`.

To replace it (on any suspicion of exposure, or if it is revoked): create a new token with the same settings, replace the repository secret, re-run `deploy-clock`, then delete the old token on GitHub. A revoked token stops both workflows and no watchdog issue opens; the page's staleness warning is the signal (ADR-0017).

## Pausing the pipeline

Disable the `hourly` workflow in the Actions tab. The page keeps serving the last published data, and after 2 hours every page shows the staleness warning (computed in the browser, no deploy needed). The watchdog will open an issue; that is expected. Re-enable the workflow to resume: the first run catches up up to 8 hours of buckets, newest first, and later runs fill the rest, up to 7 days back (ADR-0018).

## A live gap older than 7 days

The scheduler heals the live 5-minute series 7 days back and no further, so a longer outage leaves older buckets missing (the status page's completeness shows them). Filling them is a backfill, started by hand:

1. List them: `ge-intel gaps --start <UTC ISO-8601>`.
2. Disable the `hourly` workflow (one writer at a time).
3. From the laptop, with the R2 keys in the environment: `ge-intel run ingest_5m --start <UTC> --end <UTC>`. Each run fetches at most 96 buckets, newest first; repeat until `gaps` reports none.
4. Re-enable `hourly`.

## History (M2)

The history job (`ge-intel history`, ADR-0018, `docs/plans/M2.md`) runs on the laptop, by hand, and writes only the `history/` tables. Downloads, Wiki requests and the upload are Sylvester's to start. Order: `plan`, `download`, `build`, `check-1h`, `fill`, `upload`; each takes `--dry-run`. Files live under `GE_HISTORY_DIR` (default `data/history`, git-ignored). Exit code 4: a step's inputs are not ready; the log says which.

- **A month is quarantined** (`build` or `check-1h` reports it; details in `history/months.check_detail`): it stays out of R2. Look at the mismatching items before deciding on a fallback (plan, section 5.1).
- **`RevisionFound`:** two different Wiki bodies are stored for one bucket under `data/history/bronze/`. Compare them; delete the one that should not stand, then `build` again.
- **`PinMismatch`:** a downloaded or local file differs from its pin. Do not edit the pin to make it pass; a changed file is a new source.

## Deploying and rolling back the page

- **First deploy:** set `GE_DEPLOY_ENABLED` to `true`. The next `hourly` run deploys with Wrangler (`site/wrangler.jsonc`: assets only, four routes on `kohstack.tech/ge` and `www.kohstack.tech/ge`, `404-page` mode, `workers_dev: false`).
- **Stop deploying** without stopping ingestion: set `GE_DEPLOY_ENABLED` to anything else. Publishes are then recorded as `built`, not `success`, so the freshness figure stays honest.
- **Roll back:** from a machine with the deploy token, `npx --prefix site wrangler deployments list --config site/wrangler.jsonc`, then `npx --prefix site wrangler rollback <version-id> --config site/wrangler.jsonc` (about 6 seconds, measured in S2). The next hourly deploy rolls forward again, so set `GE_DEPLOY_ENABLED` off first if the problem is in the code.

## Running locally

```bash
.venv/bin/uv sync --locked --extra dev
.venv/bin/uv run pytest
```

A local run against a local lake, no credentials needed (writes to `data/lake`, builds `site/dist`):

```bash
GE_LAKE_ROOT=data/lake .venv/bin/uv run ge-intel hourly --skip-guard
```

Against the real lake, read the R2 key and secret from the Keychain into the command's environment (never into a file), set `CLOUDFLARE_ACCOUNT_ID`, and use `ge-intel run <task> --start ... --end ...`. `ge-intel hourly` against R2 requires the analytics token, which lives only in GitHub; check usage first through the Cloudflare dashboard when running tasks by hand.

## Useful commands

- `ge-intel gaps`: how many 5-minute buckets are missing since the first recorded bucket.
- `ge-intel tasks`: the registry, as JSON.
- `ge-intel run check_duplicates --start <day> --end <next day>`: re-check one day's partition for duplicate (item, bucket) rows.
