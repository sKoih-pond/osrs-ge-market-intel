# Recorded OSRS Wiki responses (test fixtures)

These files are real responses from the OSRS Wiki real-time prices API (`prices.runescape.wiki/api/v2/osrs`), trimmed to 66 items:

- `5m.json.gz`: the 288 five-minute buckets from 18 October 2026 22:00 UTC to 19 October 2026 22:00 UTC (Jagex-sourced data).
- `latest.json.gz` and `mapping.json.gz`: `/latest` and `/mapping` as fetched at about 22:29 UTC on 19 October 2026.
- `fixtures.json`: which items were kept and why. `scripts/make_fixtures.py` rebuilds the trimmed files from the raw responses.

**Licence:** OSRS Wiki content is licensed [CC BY-NC-SA 3.0](https://creativecommons.org/licenses/by-nc-sa/3.0/). This folder is covered by that licence, not by the repository's MIT licence (ADR-0013, ADR-0014). Attribution: OSRS Wiki real-time prices; data from Jagex since 30 September 2026. Non-commercial use only.
