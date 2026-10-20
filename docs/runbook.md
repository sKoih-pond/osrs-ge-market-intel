# Runbook (M1)

How the hourly pipeline runs, what can go wrong, and what to do. Decisions behind it: ADR-0001, 0004, 0015 and 0016.

## What runs

| Workflow | When | Does |
|---|---|---|
| `hourly` | `:07` and `:27` every hour, or by hand | Cost guard, then every task in `ge_intel.tasks.REGISTRY` for the newest closed hour: `/mapping` (daily), `/5m` gap-fill, `/latest`, dbt mart, publish, duplicate check (daily), maintenance (weekly, Monday 00:xx UTC) |
| `watchdog` | `:11` and `:41` every hour | Reads the public `status.json` and the `hourly` workflow's state; opens one issue labelled `watchdog` when data is over 2 hours old or the schedule is off, and closes it on recovery |
| `ci` | Every pull request and push to `main` | Lint, unit, contract and end-to-end tests, Wrangler dry run |

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

## Pausing the pipeline

Disable the `hourly` workflow in the Actions tab. The page keeps serving the last published data, and after 2 hours every page shows the staleness warning (computed in the browser, no deploy needed). The watchdog will open an issue; that is expected. Re-enable the workflow to resume: the first run catches up up to 8 hours of buckets, newest first, and later runs fill the rest.

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
