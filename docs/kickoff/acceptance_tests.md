# Acceptance Tests (v1)

**Date:** 20 October 2026 (M0)
**Format:** AI Fluency Build Brief, kickoff deliverable 3. Each statement can be scored pass or fail by someone who did not build the system.

v1 = milestones M1 to M3, the M5 data-quality gate with simple anomalies, the Casual screener with Data and Execution grades, and the live paper record (see `docs/decisions/2026-10-20_kickoff_decisions.md`).

| # | Statement | How to score it |
|---|---|---|
| AT1 | On a phone (375 px wide), a first-time visitor finds today's top 10 Casual ideas, each showing net GP after tax, a feasible quantity, and its Data and Execution grades, in under 30 seconds, without an account. | Timed task with three people who have not seen the site; pass if all three succeed. |
| AT2 | Every number on an idea card links to its definition and shows the UTC time of the data it came from. | Click through every number on three cards; pass if none is missing a definition or timestamp. |
| AT3 | If the newest published data is more than 2 hours old, every page says so at the top, and the status page shows the last successful run and the freshness and completeness figures for the last 30 days. | Pause the scheduled workflow for 3 hours; inspect every page and the status page. |
| AT4 | Over any 30-day window, at least 95% of hours are published within 45 minutes of the hour closing, at a running cost of $0. | Read the ops table and the Cloudflare and GitHub billing pages for the window. |
| AT5 | Deleting any one day of typed data and re-running the pipeline reproduces identical outputs (matching content hashes), and killing a run mid-write never leaves a partial partition visible to readers. | Scripted chaos test in CI plus one manual run against R2. |
| AT6 | The anomaly feed never presents a data-quality problem as a market anomaly, and every alert lists its evidence: size of move, persistence, volume, and data grade. | Automated test on injected bad data; manual review of 20 live alerts. |
| AT7 | The methodology page shows the live paper record for the Casual baseline: every published idea scored 24 hours later under the published execution policy, losing ideas included, with sample sizes. | Pick five ideas from the published history and trace each to its recorded outcome. |

## Notes

- AT1 and AT2 also carry the accessibility floors: contrast at least 4.5:1, no colour-only meaning, usable at 200% text and at 320 px.
- Milestone-level acceptance criteria live in the discovery report (section 7) as amended by the decision log. These seven define whether v1 as a whole is done.
- Any percentage shown to a user must state its sample size and how it was validated. A number that cannot meet that rule is withheld.
