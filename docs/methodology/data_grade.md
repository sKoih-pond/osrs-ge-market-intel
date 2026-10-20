# Data grade (`data-grade-v2`)

**Status:** v2 in use from 20 October 2026 (v1 was live for its first hour only; see below). **Decided:** 20 October 2026, from the measurements below. **Code:** `ge_intel.policies.DATA_GRADE_V2`, applied in `dbt/models/marts/market_snapshot.sql`. **Reproduce:** `python scripts/fetch_raw.py --end 2026-10-19T22:00:00Z --hours-5m 24 --hours-1h 336`, then `python scripts/derive_data_grade.py`.

The Data grade answers one question from ADR-0009: **is the observation sound?** For a screener idea, the observation is the pair of reference prices taken from the last closed hour (instant-sell VWAP to buy at, instant-buy VWAP to sell at). The grade says how far those prices can be trusted as a basis for the next few hours. It says nothing about whether an offer will fill (the Execution grade, later in v1) or whether the price will rise (the Prediction grade, after the ML gate).

## How the thresholds were derived

The thresholds were not chosen by feel. For every item-hour in the sample, the inputs the grade can see at decision time were measured against what happened in the following hour:

- **Next-hour drift:** the larger of the relative changes in the instant-buy and instant-sell VWAPs from the price hour to the next hour.
- **Both sides trade next hour:** whether the item traded on both sides in the next hour at all.

Two criteria, each tied to the product:

- **Grade A** where the median next-hour drift is **below the 2% tax rate**. Below that, a quoted margin larger than the tax is unlikely to be price noise.
- **Grade C** where both sides trade again in the next hour **less than two times in three**. Above that rate, the quoted prices are still being traded at.

Samples: 680,069 item-hours of `/1h` data (6 to 19 October 2026, after the 30 September source switch) and 48,257 item-hours of `/5m` data (18 to 19 October 2026). Generated 20 October 2026.

### Input 1: hours in the last 24 with trades on both sides

| Two-sided hours in last 24 | Item-hours | Median next-hour drift | Both sides trade next hour |
|---:|---:|---:|---:|
| 12 | 10,352 | 4.03% | 52.2% |
| 14 | 12,243 | 3.98% | 61.3% |
| 15 | 12,873 | 3.64% | 64.9% |
| 16 | 14,556 | 3.28% | 69.1% |
| 18 | 17,512 | 3.27% | 75.2% |
| 20 | 23,250 | 3.21% | 83.5% |
| 22 | 37,426 | 3.01% | 91.2% |
| 23 | 67,920 | 2.05% | 95.6% |
| 24 | 328,071 | 1.28% | 99.1% |

(Rows 1 to 24 in full come from the script; the pattern is monotone.) Only 24 of 24 clears the A criterion (23 sits just above, at 2.05%). Below 16, both sides trade again less than two times in three.

### Input 2: minutes since the staler side last traded

| Minutes | Item-hours | Median next-hour drift | Both sides trade next hour |
|---|---:|---:|---:|
| 0 to 5 | 15,047 | 1.33% | 97.8% |
| 5 to 10 | 8,172 | 1.99% | 93.4% |
| 10 to 15 | 5,472 | 2.21% | 86.9% |
| 15 to 20 | 3,989 | 2.40% | 81.0% |
| 20 to 30 | 5,774 | 2.96% | 74.9% |
| 30 to 45 | 5,652 | 3.41% | 63.8% |
| 45 to 60 | 4,151 | 4.14% | 55.5% |

A: 10 minutes or fewer. C: more than 30.

### Input 3: how far the price hour sits from the item's own 24-hour median

Added in v2. In its first live hour, v1 ranked Yellow boots first: one stray instant-buy trade at 326,073 GP against an instant-sell price of 3,821 GP, on an item that traded on both sides in 23 of 24 hours. Coverage and staleness cannot see a single outlying trade, so v2 compares each side's price-hour VWAP with the median of that side's hourly VWAPs over the last 24 hours and takes the worse side. Measured on the same `/1h` sample (item-hours with at least 16 two-sided hours):

| Price hour vs own 24-hour median | Item-hours | Median next-hour drift | Share of hours with 24 of 24 coverage |
|---|---:|---:|---:|
| within 2% | 232,320 | 0.69% | 48.9% |
| 2% to 5% | 119,647 | 1.76% | 22.8% |
| 5% to 10% | 71,671 | 3.98% | 11.8% |
| 10% to 15% | 32,984 | 6.97% | 4.8% |
| 15% to 25% | 32,674 | 10.50% | 4.4% |
| 25% to 50% | 30,333 | 16.67% | 3.7% |
| 50% to 100% | 18,212 | 24.85% | 2.1% |
| 2 to 3 times | 8,563 | 33.29% | 0.9% |
| over 3 times | 6,790 | 50.36% | 0.6% |

A: 5% or less (drift under the 2% tax). C: over 10%, where the next hour moves more than twice the tax (prices that far out snap back). Yellow boots sat 5,300% out.

### Volume was tested and not used

At 24 of 24 two-sided hours, the thinner side's volume in the price hour does **not** separate stable from unstable prices: median drift is 1.1% to 1.9% across every volume band, with no consistent direction (mid volumes, 50 to 499 a side, drift more than both thin and deep markets). A volume threshold would have looked principled and added nothing, so the grade has none. Volume still limits the feasible quantity (ADR-0008).

## The rules

| Grade | Rule | Shown as |
|---|---|---|
| **A** (sound) | both sides traded in all 24 of the last 24 hours **and** the staler side last traded 10 minutes or less before publication **and** the price hour is within 5% of the item's 24-hour median on both sides | "Data A" in a solid box, with the reason |
| **B** (usable with care) | priced, not A, not C | "Data B" in a dashed box, with what kept it from A |
| **C** (withheld) | one side did not trade in the price hour; **or** fewer than 16 two-sided hours; **or** the staler side last traded more than 30 minutes before publication; **or** the price hour is more than 10% from the item's 24-hour median on either side; **or** the price hour is missing a bucket or hit a source outage; **or** fewer than 259 of the 288 buckets in the 24-hour window are recorded | never published as an idea; counted on the page as withheld |

The last two C rules are operating rules about the pipeline's own completeness, not derived thresholds.

## Known limits

- **Staleness is measured at publication, the derivation at the hour boundary.** The live input comes from `/latest`, fetched a few minutes after the hour closes, so it includes the run's delay. That makes the live grade slightly stricter than the derivation, never looser.
- **The envelope uses the same 24 hours it judges.** A genuine re-pricing (an update that changes an item's value) is graded C for up to a day until the median catches up. That errs on the side of withholding.
- **One regime, two weeks.** The sample is post-switch only (Jagex data). The thresholds should be re-derived when more history arrives (M2) and checked monthly against the ADR-0009 validation: grade C rows should show higher error rates in backtests.
- **Drift is not error.** A price that moves is not wrong. The criterion asks whether the quoted margin is likely to outlast price noise, which is what a screener user needs.
- **No reconciliation input yet.** ADR-0009 lists `/1h` reconciliation as an input; it arrives with the M2 reconciliation check.
