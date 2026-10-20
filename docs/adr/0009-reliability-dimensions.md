# ADR-0009: Reliability shown as separate, evidenced dimensions

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Agreed in the kickoff walkthrough (brief section 3C)

> **Data grade v1 set in M1 (ADR-0016, 2026-10-20):** thresholds derived from measured next-hour price drift and two-sided trading on 680,069 post-switch item-hours; volume was tested and not used. Definitions and tables: `docs/methodology/data_grade.md`.

## Context

The brief makes reliability the defining feature and warns against arbitrary percentages. A single blended score would hide which part is weak: stale data, an order too large to fill, or a model that has not earned trust. The principle carried over from earlier work is to show the deviation and let the user decide.

## Decision

Four dimensions, never blended into one percentage:

| Dimension | Question | Inputs (stored with each grade) | Shown as | Validated by |
|---|---|---|---|---|
| **Data** | Is the observation sound? | Minutes since the last trade on each side; share of the last 24 hours with both sides traded; volume; regime; reconciliation status | Grade A, B or C plus a one-line reason | Monthly check that grade C rows show higher error and revision rates in backtests |
| **Execution** | Can you actually do this trade? | Quantity as a share of 4-hour volume; buy limit; historical fill rate and time to fill for this policy and item; spread stability | "Estimated fill within 4 h: N% (n past setups)" | Realised fill rates in the live paper record and the M4 manual-trade log |
| **Prediction** | How sure is the call? | Calibrated P(net > 0); interval; hit rate in that probability band over the last 8 weeks; drift flag | Probability with its band's observed hit rate and n | Reliability diagram, Brier score, interval coverage, weekly |
| **Evidence** (anomalies) | Is this move real? | Size, persistence, breadth, both sides moving, data grade | Severity level plus evidence list | Weekly blind audit of 50 alerts |

Rules:
- Every grade is reproducible from stored inputs and a versioned grading function.
- Thresholds for A, B and C are set from data in M1 and M2 and published; they are not invented.
- Where evidence is thin (new item, too few past setups), the display says "insufficient history" instead of a number.
- If a sort order is needed, it uses conservative expected GP per slot-day (a low quantile of net return x feasible quantity x fill probability), and the page says so.
- Colour is never the only signal: each grade carries a letter and a label (accessibility floor).

## Consequences

- v1 ships Data and Execution only. Prediction appears per persona only after the ML gate (ADR-0010) is passed, or for a baseline once its own calibration is measured.
- More to show per idea, so the card shows the grades and the detail opens on demand.

## Alternatives considered

- One composite "confidence %": easy to read but unvalidatable and misleading.

## References

- Discovery report section 5.7.
