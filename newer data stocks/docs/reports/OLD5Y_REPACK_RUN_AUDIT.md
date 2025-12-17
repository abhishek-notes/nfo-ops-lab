# OLD5Y Repack — Run Audit

- Output dir: `/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT`
- Trading days (from spot parquet): `1495`

## BANKNIFTY

- Written days: `1339` / `1495`
- Missing days: `156`
- First written: `2019-04-09`
- Last written: `2025-04-24`

## NIFTY

- Written days: `1424` / `1495`
- Missing days: `71`
- First written: `2019-06-06`
- Last written: `2025-07-31`

## Missing breakdown

- BANKNIFTY: internal_gap = `86`
- BANKNIFTY: after_last_written = `68`
- BANKNIFTY: no_candidates = `1`
- BANKNIFTY: before_first_written = `1`
- NIFTY: before_first_written = `38`
- NIFTY: internal_gap = `33`

## Why the “No rows written” warnings happen

These warnings are not a repacker bug by default — they mean **the raw dataset has no ticks for that underlying/date**.

Two common causes seen in this dataset:

1) **Empty/placeholder contract files**
- Example: `data/raw/options/banknifty22o0634900ce.parquet` has `0` rows (schema only).
- Example: `data/raw/options/banknifty25jul49000ce.parquet` has `2` rows but `timestamp = null` (effectively empty for date filtering).

2) **Underlying coverage ends earlier**
- BANKNIFTY packed output ends on `2025-04-24` (after that, the BANKNIFTY raw files for later expiries appear to be placeholders/empty).

## Files

- Missing dates CSV: `newer data stocks/docs/reports/old5y_repack_missing_dates.csv`

## Spot-check verification (raw → packed)

Verified samples using:
`newer data stocks/scripts/data_processing/verify_old5y_packed_vs_raw.py`

PASS samples:
- BANKNIFTY `2019-04-10`: `banknifty1941128500ce.parquet`, `banknifty1941128500pe.parquet`
- NIFTY `2019-06-06`: `nifty1960610800ce.parquet`, `nifty1960610800pe.parquet`
- BANKNIFTY `2024-11-21`: `banknifty24nov45000ce.parquet`, `banknifty24nov45000pe.parquet`
- NIFTY `2024-11-21`: `nifty24nov22000ce.parquet`, `nifty24nov22000pe.parquet`
- BANKNIFTY `2025-04-24`: `banknifty25apr44000ce.parquet`, `banknifty25apr44000pe.parquet`
