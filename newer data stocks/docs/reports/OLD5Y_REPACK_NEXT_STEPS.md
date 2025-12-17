# Old 5Y Repack — Next Steps (after verification)

## What to verify now

1) **Filename → expiry mapping**
- Summary: `newer data stocks/docs/reports/OLD_OPTIONS_NAMING_AND_EXPIRY_MAPPING.md`
- Full table (85,278 rows): `newer data stocks/docs/reports/old_options_filename_expiry_mapping.csv`

2) **One-day packed output (58 cols, v2 sorted, no spot)**
- BANKNIFTY: `newer data stocks/data/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT_TEST/2024-11-21/BANKNIFTY/part-banknifty-0.parquet`
- NIFTY: `newer data stocks/data/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT_TEST/2024-11-21/NIFTY/part-nifty-0.parquet`

3) **HDD SQL coverage (spot + futures)**
- `newer data stocks/docs/reports/HDD_SQL_SPOT_FUTURES_COVERAGE.md`

## Re-run the one-day options repack for any date

```bash
python "newer data stocks/scripts/data_processing/repack_raw_to_date_v2_SORTED_STREAMING.py" \
  --date 2019-04-09 \
  --underlyings BANKNIFTY,NIFTY \
  --overwrite
```

Output default:
`newer data stocks/data/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT_TEST/<DATE>/<UNDERLYING>/part-*.parquet`

## Full 5Y run (batch mode)

Batch mode builds a **contract index once** (so it doesn’t re-scan ~85k filenames per day), then processes a date range.

Example (recommended: use spot parquet to get the exact trading-day list):

```bash
python "newer data stocks/scripts/data_processing/repack_raw_to_date_v2_SORTED_STREAMING.py" \
  --start-date 2019-04-08 --end-date 2025-07-31 \
  --trading-days-parquet "newer data stocks/data/spot_data_OLD5Y/NIFTY_all.parquet" \
  --underlyings BANKNIFTY,NIFTY \
  --output-dir "/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT" \
  --max-expiry-days 70
```

Notes:
- Batch mode **skips existing outputs by default** (safe resume); use `--overwrite` to rebuild.
- Keep output on an external drive (Mac internal disk may not have enough space).

Monitor progress:

```bash
bash "newer data stocks/scripts/data_processing/watch_old5y_repack_progress.sh" \
  "/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT" \
  "newer data stocks/logs/old5y_repack.log"
```

## Add spot-dependent columns (64-col pack)

This reads the 58-col packed files and writes a new 64-col dataset with:
`spot_price, mid_price, distance_from_spot, moneyness_pct, intrinsic_value, time_value`.

One-day test (writes to workspace by default):

```bash
python "newer data stocks/scripts/data_processing/enrich_old5y_packed_with_spot.py" \
  --date 2024-11-21 \
  --underlyings BANKNIFTY,NIFTY \
  --overwrite
```

Full run (recommended output to external drive):

```bash
python "newer data stocks/scripts/data_processing/enrich_old5y_packed_with_spot.py" \
  --input-root "/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT" \
  --output-root "/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v3_SPOT_ENRICHED" \
  --spot-dir "newer data stocks/data/spot_data_OLD5Y" \
  --underlyings BANKNIFTY,NIFTY
```

## Extract spot series from the HDD SQL dumps (REPLACE INTO)

One-day example:

```bash
mkdir -p "newer data stocks/data/spot_data_OLD5Y_TEST"

python "newer data stocks/scripts/spot_extraction/extract_price_series_from_replace_sql.py" \
  --sql "/Volumes/Abhishek-HD/BNF NF/BNF/banknifty.sql.gz" \
  --table banknifty \
  --start-date 2024-11-21 --end-date 2024-11-21 \
  --output "newer data stocks/data/spot_data_OLD5Y_TEST/BANKNIFTY_all.parquet"

python "newer data stocks/scripts/spot_extraction/extract_price_series_from_replace_sql.py" \
  --sql "/Volumes/Abhishek 5T/Raw SQL Stocks Rajesh Data/NF/nifty.sql.gz" \
  --table nifty \
  --start-date 2024-11-21 --end-date 2024-11-21 \
  --output "newer data stocks/data/spot_data_OLD5Y_TEST/NIFTY_all.parquet"
```

## Full 5Y run (disk-safe strategy)

- **Do not write the full packed dataset to the Mac internal disk** (low free space).
- Recommended:
  - Write packed outputs to an external drive, e.g. `/Volumes/Abhishek-HD/...`
  - Process **one date at a time** (or small batches) to keep peak disk usage low.
- When you confirm the one-day outputs look correct, we can:
  1) Extract full 5Y spot series to external drive once (so it isn’t re-scanned per date).
  2) Add spot-enrichment on top of the 58-col output (to match the 64-col v3 schema) if you want.
