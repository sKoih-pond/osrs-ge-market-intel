# ADR-0017: The hourly clock is a Cloudflare Cron Trigger, not GitHub's scheduler

- **Status:** Accepted; deployed 2026-10-20 13:29 UTC
- **Date:** 2026-10-20
- **Decided by:** Sylvester (fall back to a Cloudflare cron trigger if re-registering the GitHub schedule failed); amends ADR-0001

## Context

ADR-0001 made GitHub Actions scheduled workflows the Phase B clock. After `hourly.yml` and `watchdog.yml` reached `main` on 20 October 2026 (02:20 UTC), GitHub fired **no `schedule` event for either workflow**: none in the first four hours, and none in the two hours after the cron lines were changed to make GitHub register the schedule again (sKoih-pond/osrs-ge-market-intel#6). GitHub reported no incident; the repository is public, Actions is enabled, `main` is the default branch, both workflows are `active`, and manual `workflow_dispatch` runs work every time. The repository was created the evening before, which may be why; GitHub does not say.

A clock that never ticks fails AT4 (95% of hours published within 45 minutes) outright. Compute on Actions works, so only the trigger needs replacing.

## Decision

- **ge-clock**, a Cloudflare Worker with two Cron Triggers (`6,26 * * * *` for `hourly.yml`, `13,43 * * * *` for `watchdog.yml`), calls GitHub's `workflow_dispatch` API for the matching workflow on `main`. Code: `clock/src/index.js`; config: `clock/wrangler.jsonc`.
- **No HTTP surface.** No `fetch` handler, no route, `workers_dev: false`, no preview URLs, no bindings. It does not touch R2 or `kohstack.tech`.
- **One secret:** `GITHUB_DISPATCH_TOKEN`, a fine-grained GitHub personal access token (`ge-clock dispatch`) limited to this repository with **Actions: read and write** and nothing else, with **no expiry date** (Sylvester, 2026-10-20). Sylvester creates it and stores it as the repository secret `GH_DISPATCH_TOKEN`; the `deploy-clock` workflow pipes it into the Worker secret, so the value never appears in a file, log or chat.
- **The GitHub schedules stay.** If GitHub starts firing them, the two clocks overlap harmlessly: runs queue on the `ge-intel-lake-writer` concurrency group and a second run in the same hour finds every task complete.
- A dispatch that GitHub refuses throws, so it shows as a failed invocation in the Worker's logs (observability on).
- **Prerequisite found at deploy:** Cloudflare refuses Cron Triggers on an account without a `workers.dev` subdomain (API error 10063), even for a Worker with `workers_dev: false`. Sylvester approved creating it; the account subdomain is `kohstack.workers.dev`. Both `ge-dashboard` and `ge-clock` keep `workers_dev` and preview URLs off (checked through the API after creation), so neither is served there.

## Consequences

- Cost: 96 invocations a day of the free plan's 100,000, 2 of the account's 5 free Cron Triggers, CPU well under the 10 ms limit (the time spent waiting on GitHub does not count). $0.
- The clock now has two dependencies: Cloudflare Cron Triggers and the token.
- **The token does not expire, by choice.** An expiring token would stop the clock on a fixed date with no alert (see the next point), and the scope is narrow. If it leaked, its holder could start, cancel, re-run or disable this repository's workflow runs and delete their logs, but could not read or change code or secrets, deploy, or reach R2 or Cloudflare; the cost guard bounds what extra runs can spend. It exists in two places only: the repository secret `GH_DISPATCH_TOKEN` and ge-clock's Worker secret. On any suspicion of exposure, revoke and replace it (runbook).
- The watchdog shares the clock it watches. If ge-clock stops (token revoked, Worker deleted), neither workflow runs and no issue opens; the page's own staleness warning (AT3) is then the only signal. Accepted for M1; M3 should add an alert that does not depend on either clock.
- ADR-0012's "assets only, no per-request script" still holds for `/ge/`: ge-clock is a separate Worker with no route.
- Phase C (M7) replaces both clocks with Airflow's scheduler; ge-clock is then deleted.

## Alternatives considered

- **Keep waiting for GitHub's scheduler:** no evidence it will start, and every hour waiting is a late hour.
- **The laptop (launchd):** it sleeps, which is why ADR-0001 rejected it.
- **A third-party cron service:** another account and another credential holder, for no gain over Cloudflare, which already hosts the dashboard.
- **Cloudflare Workflows with schedules:** more machinery than one `fetch` per tick needs.

## References

- Cloudflare Cron Triggers: https://developers.cloudflare.com/workers/configuration/cron-triggers/
- Workers limits (Cron Triggers per account, CPU time): https://developers.cloudflare.com/workers/platform/limits/
- GitHub, create a workflow dispatch event: https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
