# ADR-0001: Runtime: GitHub Actions now, always-on VM after v1

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 1)

> **Amended by ADR-0017 (2026-10-20):** GitHub never fired this repository's `schedule` events, so the hourly clock is a Cloudflare Cron Trigger (`ge-clock`) that dispatches the workflows. Actions still provides the compute; the GitHub schedules stay as a second clock.

## Context

The pipeline must run unattended every hour at $0. The developer laptop (8 GB Intel MacBook) sleeps and cannot host a service. A persistent orchestrator also needs a persistent host. The source publishes closed 5-minute buckets that can be requested by timestamp at any later time, so a late or skipped run loses no data provided the next run catches up.

## Decision

- **Phase B (until v1):** GitHub Actions scheduled workflows provide the hourly clock and compute (public repository, standard Ubuntu runners: 4 vCPU, 16 GB). Each run executes the task commands defined in ADR-0004 and catches up every missing bucket since the last committed watermark.
- **Phase C (committed milestone M7, after v1):** move scheduling and compute to an Oracle Cloud Always Free VM running Docker Compose (Airflow 3, Postgres), keeping storage in R2 (ADR-0002) so the move does not depend on the VM's disk.
- The same container image runs locally, in Actions and on the VM.

## Consequences

- Freshness target is hourly, not 5-minutely, in Phase B. The v1 objective is 95% of hours published within 45 minutes of the hour closing (AT4).
- Actions schedules can start late or be skipped under load; catch-up logic makes this a freshness issue, not a data-loss issue.
- Scheduled workflows in public repos switch off after 60 days without repository activity. The independent watchdog checks both data freshness and that the schedule is still enabled.
- GitHub's terms restrict Actions to activity related to the project. Producing this project's published data is project activity; if that interpretation is challenged, Phase C moves earlier.
- Before M7, re-check the Oracle Always Free allowance (secondary reports of a 2026 reduction are unverified) and its idle-reclamation rule.

## Alternatives considered

- **Always-on VM from day one:** most production-like, but adds an account with a card, reclamation risk and patching before any product exists.
- **Laptop only:** zero setup, but not live and weak evidence of operating a system.
- **Cloudflare Workers cron:** free, but CPU limits and no Python data stack.

## References

- Discovery report sections 6.1 to 6.3.
- GitHub docs, disabling and enabling scheduled workflows: https://docs.github.com/en/actions/how-tos/manage-workflow-runs/disable-and-enable-workflows
- Oracle Always Free resources: https://docs.oracle.com/en-us/iaas/Content/FreeTier/resourceref.htm
