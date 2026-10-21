# Kaggle dump fixtures (M2 history tests)

- `README.md`: the README of the Kaggle dataset `antonygarand/osrs-ge-5m` (version 1, by Antony Garand), as downloaded on 21 October 2026, byte for byte. Its SHA-256 is pinned in `src/ge_intel/sources/kaggle_osrs_ge_5m_v1.json`; it is excluded from formatters for that reason.
- `prices_5m/year=2026/month=10/data.parquet`: not a copy of the dump. It is the recorded Wiki 5-minute fixture (`tests/fixtures/wiki/5m.json.gz`) written the way the dump writes a month, ending at 19 October 2026 19:40 UTC and missing three buckets so the tests have a seam and gaps. Rebuilt by `scripts/make_history_fixtures.py`.

**Licence:** CC BY-NC-SA 3.0 (the dataset's and the OSRS Wiki's terms), not the repository's MIT licence (ADR-0013, ADR-0014). Attribution: Antony Garand, OSRS Grand Exchange 5m price series (Kaggle); OSRS Wiki real-time prices; data from Jagex since 30 September 2026. Non-commercial use only.
