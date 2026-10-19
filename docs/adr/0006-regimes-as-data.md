# ADR-0006: Market and measurement regimes as first-class data

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Agreed in the kickoff walkthrough; recorded here as a standalone decision

## Context

The series this project models changes at known instants:
- **Measurement:** the Wiki feed came from RuneLite users from 8 Mar 2021 and from Jagex (all clients) from 30 Sep 2026. Measured effect: per-item volume up 1.25x to 1.80x, median relative spreads wider (for example coal 1.39% to 2.05%). History was not restated.
- **Market rules:** tax introduced at 1% on 9 Dec 2021, raised to 2% on 29 May 2025 (ADR-0007); item-sink list changes on both dates; Beyond Max Cash and the 3-hourly guide-price cadence on 30 Sep 2026.
- **Content:** major releases (for example Sailing, reported as 19 Nov 2025) and weekly update downtime.

Treating these as noise would mix incomparable data and hide the project's most important data-quality story.

## Decision

- A `regimes` reference table (a versioned seed file) records each change: id, kind (`measurement`, `tax`, `item_sink`, `ge_mechanics`, `content`), start and end in UTC, scope (global or a list of item ids), description and source URL.
- Every hourly and 5-minute fact row carries the measurement regime id.
- Volume-based features are normalised within regime (ratio to the item's own trailing median in the same regime). Spread-based features are regime-aware. No model sees raw volumes across the 30 Sep 2026 boundary.
- Evaluation is always reported per regime; post-switch results are the headline.
- The dashboard marks regime boundaries on item charts, and every reliability panel shows the regime of its inputs.
- Content updates come from the Wiki's `Category:Game_updates` pages; release dates are parsed from page content, not category timestamps.

## Consequences

- Detecting the 30 Sep switch from the data itself (volume ratio against official volumes, spread shift) becomes a regression test: the detector must find it.
- New regime rows can be added without code changes. A missed regime is a data bug, fixed by adding a row and reprocessing (ADR-0011).

## Alternatives considered

- Training only on post-switch data and ignoring history: rejected as the default. Whether pre-switch data helps is tested in M4, not assumed.
- Dummy variables without normalisation: leaves scale shifts for the model to absorb, which it does poorly with little post-switch data.

## References

- Discovery report sections 1.1, 2.4 and 2.5.
