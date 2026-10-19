# ADR-0013: Data publication and licensing

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 10)

## Context

OSRS Wiki content is licensed CC BY-NC-SA 3.0 (non-commercial, attribution, share-alike). The Wiki's API page grants use "for interesting projects and community-facing tools" but states no separate data licence. Since 30 Sep 2026 the live feed comes from Jagex. Republishing the whole raw archive would amount to redistributing their dataset. Reviewers benefit from being able to run the code on real data.

## Decision

- **Published:** the dashboard's data contract (`ge-intel-public/v1`): scores, ideas, anomalies, reliability grades, summaries and the paper record.
- **In the repo:** a small real-data sample (about 30 days x 100 items, chosen across liquidity tiers and spanning the 30 Sep 2026 switch) for tests and a runnable demo.
- **Private:** the raw archive and full typed history (`ge-intel-lake`).
- **Attribution** on every published file's metadata and on the site: OSRS Wiki real-time prices (data from RuneLite before 30 Sep 2026, Jagex after), Weird Gloop exchange history, Jagex. Published data carries the CC BY-NC-SA 3.0 notice.
- **No real-money values** anywhere, and no commercial use.
- A full dataset release stays open. It requires the Wiki team's agreement first, and a new ADR.

## Consequences

- The sample is CC BY-NC-SA, so the repo's README states the split: code under the code licence (ADR-0014), sample data under CC BY-NC-SA 3.0.
- Reproducing results beyond the sample requires running the backfill with one's own descriptive User-Agent.

## Alternatives considered

- Outputs only: reviewers could not run on real data.
- Full public dataset now: needs permission first, plus documentation and upkeep.

## References

- OSRS Wiki site rights (`api.php?meta=siteinfo&siprop=rightsinfo`): CC BY-NC-SA 3.0.
- Rules of RuneScape: https://legal.jagex.com/docs/rules/rules-of-runescape
