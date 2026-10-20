# Seeds

Reference data read by both the Python package and the dbt project (`dbt/dbt_project.yml` points its `seed-paths` here), so each rule has one source.

| File | Contents | Rebuilt by |
|---|---|---|
| `regimes.csv` | Measurement, tax and market-rule regimes (ADR-0006). Boundaries are at 00:00 UTC on the update day, the same approximation as the tax eras (ADR-0007). | Hand-edited; a new row needs no code change |
| `tax_exemptions.csv` | Items exempt from the GE tax and the era each exemption starts in (ADR-0007): the 9 Dec 2021 list and the 29 May 2025 additions | `scripts/build_tax_exemptions.py` from a `/mapping` snapshot |
