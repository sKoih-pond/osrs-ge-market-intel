# Problem Brief

**Project:** OSRS Grand Exchange Market Intelligence
**Date:** 20 October 2026 (M0)
**Format:** AI Fluency Build Brief, kickoff deliverable 1

## Who the users are

**Primary: members-account players who trade on the Grand Exchange alongside normal play.** They have some spare capital (roughly 10M to 500M GP) and offer slots they are not using. They check the GE a few times a day, not every few minutes. Three trading styles share the same tool:

- **Flipper:** checks often, wants offers that fill within the hour and sell within a few hours.
- **Casual (default):** checks two or three times a day, gives a buy offer about 4 hours to fill, and wants out within a day.
- **Holder:** checks once a day and is happy to hold for up to a week.

**Secondary: people reviewing the build.** Some skim for the problem, a working product, the architecture and evidence of reliability. Engineers dig into decisions, tests and failure handling.

Out of scope as users: ironmen (they can only buy bonds), new trade-restricted accounts, and anyone wanting automated trading.

## What they actually need

- "Where should my next few slots go for about the next day, roughly how much could I make **after tax**, and **how far should I trust that**?"
- "Something odd is happening to an item's price. Is it a real market move, or bad data?"
- For reviewers: "Show me that trustworthy output came from messy, changing data, and how you know."

The need is a trust-calibrated shortlist, not a price prediction. A correct forecast of a 0.5% rise is worthless to a player who pays 2% tax on the sale.

## What a great solution feels like

- **Calm and quick.** The shortlist loads fast on a phone; the main answer is visible without scrolling.
- **Honest.** Every number is net of tax and limited to what the player could actually buy. Uncertainty is shown, not hidden. Losing ideas appear in the track record alongside winning ones.
- **Self-explaining.** Each idea says why it was picked in one line, and every grade can be opened to show its evidence.
- **Current, and says when it isn't.** If data is stale, the page says so before anything else.

## Real constraints

| Constraint | Detail |
|---|---|
| Cost | $0 per month. No paid services without Sylvester's approval. |
| People | Solo developer. About 15 to 30 minutes a week of upkeep once stable. |
| Hardware | 2019 Intel MacBook Pro, 4 cores, 8 GB RAM. Python 3.12 available; Java 8 only (Spark needs 17). |
| Data source | OSRS Wiki prices API v2 and Weird Gloop history. Community APIs: descriptive User-Agent, polite request rates, explicit timestamps. |
| Data quality | The live feed changed source on 30 Sep 2026 (RuneLite to Jagex), a structural break. Only about 20 days of post-switch data exist at kickoff. |
| Market mechanics | No order book; fills must be modelled. 2% tax, 5M cap per item, buy limits every 4 hours, 8 slots. |
| Rules | Jagex rules forbid bots, game-client automation and real-world trading. Decision support only; no real-money values anywhere. |
| Licence | OSRS Wiki content is CC BY-NC-SA 3.0: non-commercial, attribution, share-alike for derived data. |
| Accessibility | Floors: 4.5:1 contrast, no colour-only meaning, 200% text, keyboard reachable, reduced motion respected. |

## Non-goals

- Automated trading, offer placement, or any game-client interaction.
- Real-money pricing, account trading or anything touching real-world trading.
- Claims of manipulation. Anomalies are described by evidence, not motive.
- Point price forecasts as the product.
- Real-time (sub-hour) alerting in v1.

## How we will know it worked

The v1 acceptance tests in `acceptance_tests.md`, plus three measured outcomes reported on the status and methodology pages: freshness objective attainment, completeness of the historical record, and the live paper record of published ideas.
