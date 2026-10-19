# ADR-0008: Execution policies: Flipper, Casual, Holder

- **Status:** Accepted (parameters provisional until the M4 sensitivity grid)
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 6)

## Context

The GE shows no order book, so whether an offer would have filled must be simulated. Profit labels, backtests, rankings and the live paper record all depend on the simulation's rules, so the rules must be explicit, versioned and published. Sylvester chose Casual as the default with Flipper and Holder as alternatives on a three-way switch.

## Decision

An **execution policy** is a versioned rule set with id `<persona>-v<n>`. Every label, backtest row, score and paper-record row carries its `policy_id`.

| Parameter | flipper-v1 | casual-v1 (default) | holder-v1 |
|---|---|---|---|
| Entry window (buy offer may fill) | 1 h | 4 h | 24 h |
| Exit horizon (position must be closed by) | 6 h | 24 h | 7 days |
| Buy reference price | trailing 1 h instant-sell VWAP | trailing 1 h instant-sell VWAP | trailing 24 h instant-sell VWAP |
| Sell target | instant-buy VWAP at entry | instant-buy VWAP at entry | model or baseline target |
| Baseline ranking before ML | net margin after tax x feasible quantity | same | mean reversion (MAD z-score vs 7-day median) |

**Fill rules (all policies)**
- A buy fills if, within the entry window, later instant-sell trades occur at or below the buy price **and** our quantity is at most 10% of that side's volume in the window (queue haircut). Partial fills scale with volume.
- A sell fills if instant-buy trades reach the target within the remaining horizon under the same volume rule. Otherwise the position is sold at the horizon at the instant-sell VWAP (a conservative forced exit).
- Feasible quantity = `min(buy limit, capital / price, 10% of recent instant-sell volume)`. Capital defaults to 50M GP and is adjustable in the browser (10M to 500M), as is the number of slots.
- Net result = proceeds after tax (ADR-0007) minus cost, per slot-day.

**Known biases, published on the methodology page**
- Bucket VWAPs hide intra-bucket extremes, so touch fills are under-detected.
- Queue position is unknown, so fills are over-detected; the 10% haircut is an assumption tested in the M4 sensitivity grid.
- Adverse selection is only partly captured by conditioning on the real subsequent path.
- Buy limits before the first mapping snapshot are taken from the current mapping.

**Build order:** casual-v1 first through M1 to M6, then flipper-v1, then holder-v1.

## Consequences

- Changing a parameter creates a new policy version. Old labels and paper-record rows stay attached to their version, so comparisons remain honest.
- Three policies triple the evaluation reporting, not the architecture.
- The M4 manual-trade log (kickoff decision 12) checks casual-v1's fill assumptions against reality.

## Alternatives considered

- One persona only: simpler, but Sylvester chose three styles for product reach.
- Calling the next price move instead of simulating execution: ignores tax and fills, so the numbers would not be achievable.

## References

- Discovery report sections 5.2 to 5.5.
