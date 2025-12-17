# Documentation Index (newer data stocks)

This folder contains a mix of **current reference docs** and **historical session/status logs**.

## Current “source of truth”

- Data pipeline (SQL → Parquet v3 + spot): `docs/wiki/DATA_PIPELINE_WIKI.md`
- Strategy framework + how-to: `docs/wiki/OPTIONS_BACKTESTING_FRAMEWORK_WIKI.md`
- Theta strategy designs: `docs/wiki/THETA_STRATEGIES_SYSTEMATIC.md`
- Spot enrichment quick guide: `docs/guides/SPOT_ENRICHMENT_GUIDE.md`

## Current project layout (quick)

- Packed data (primary input): `data/options_date_packed_FULL_v3_SPOT_ENRICHED/<YYYY-MM-DD>/<UNDERLYING>/part-*.parquet`
- Processing scripts: `scripts/` (subfolders: `sql_extraction/`, `spot_extraction/`, `data_processing/`, `verification/`, `batch/`)
- Strategies (runners): `strategies/`
  - Buying: `strategies/buying/*.py`
  - Selling: `strategies/selling/**.py`
  - Canonical strategy outputs: `strategies/strategy_results/**`
- Market Truth framework (per-second feature store + events + API): `market_truth_framework/` (see `market_truth_framework/README.md`)

## Historical / status docs

Files under `docs/status/` and `docs/activity_logs/` are snapshots from earlier sessions. They may contain older paths (e.g., legacy `options_date_packed_FULL/`) and are best read as **project history**, not current configuration.

