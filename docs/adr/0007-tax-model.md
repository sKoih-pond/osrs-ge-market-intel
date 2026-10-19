# ADR-0007: Grand Exchange tax model

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Agreed in the kickoff walkthrough; rules taken from Jagex's update notes

## Context

Every profit figure, label and backtest depends on the tax. The rules changed twice and the exemption list changed with them.

Confirmed from Jagex's update notes, as copied on the OSRS Wiki:
- **9 Dec 2021:** 1% tax on GE sales, deducted from the seller's proceeds, applied per item and rounded down. Items sold below 100 GP pay nothing. Tax is capped at 5,000,000 GP per item. Exempt: bonds and beginner tools (chisel, seed dibber, gardening trowel, glassblowing pipe, hammer, needle, pestle and mortar, rake, saw, secateurs, shears, spade, watering can (0)).
- **29 May 2025:** the rate for **new offers** rose to 2%. Below 50 GP the tax rounds to zero. The cap stays at 5M per item. Further low-value and early-game items were exempted (energy potion, low-level arrows, darts and mind runes, low-level food, teleport tablets, games necklace(8), ring of dueling(8)).
- **30 Sep 2026:** the 5M cap still applies to trades above max cash.
- The tax applies to the actual transaction price, which under GE matching is the price of the resting offer.

## Decision

- `src/ge_intel/tax.py` implements the rule: tax per item = `min(floor(price x rate), 5,000,000)` with integer arithmetic. The rate is chosen by the **time the sell offer was placed**: 0 before 9 Dec 2021, 1% from then, 2% from 29 May 2025.
- Era boundaries are applied at 00:00 UTC on the update day. Worlds were offline for each update, so this misclassifies at most the hours before the update on that day. This approximation is documented on the methodology page.
- Prices must be whole coins; simulated offer prices are rounded to integers before tax.
- Exemption is the caller's flag. An exemption seed (item id, era from, era to, source) is built in M1 from `/mapping` and the two published lists. Names that map to several item ids (for example dose variants) are resolved by hand and recorded.
- Net proceeds = (price minus tax per item) x quantity. The buyer pays no tax.

## Consequences

- Backtests before 29 May 2025 use 1% and the older exemption list, so pre-2025 results are not directly comparable with post-2025 results. Both are reported by regime (ADR-0006).
- Unit tests pin the edge cases: 49 and 50 GP at 2%, 99 and 100 GP at 1%, the cap, exemptions, era boundaries, and rejection of non-integer prices and timezone-naive times.

## Alternatives considered

- Applying the current 2% rule to all history: simpler, but wrong for 3.5 years of data.
- Using the trade time instead of the offer time: wrong for offers placed before 29 May 2025 that filled after it.

## References

- Update: Grand Exchange Tax & Item Sink (9 Dec 2021): https://oldschool.runescape.wiki/w/Update:Grand_Exchange_Tax_%26_Item_Sink
- Grand Exchange, convenience fee and changelog: https://oldschool.runescape.wiki/w/Grand_Exchange
- Update: GE Improvements: Beyond Max Cash: https://oldschool.runescape.wiki/w/Update:GE_Improvements:_Beyond_Max_Cash
