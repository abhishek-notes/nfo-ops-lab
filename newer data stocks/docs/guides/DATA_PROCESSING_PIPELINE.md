# NFO Options Data Processing Pipeline (v3 - current)

This is the quick-start for producing the **current packed dataset** used by strategies and the Market Truth framework.

For the full “why/how” and schema details, prefer: `docs/wiki/DATA_PIPELINE_WIKI.md`.

---

## Inputs (per raw folder)

Each raw folder under `data/new 2025 data/` typically contains:
- `das_bankopt_mod.sql.gz` (BANKNIFTY options)
- `das_niftyopt_mod.sql.gz` (NIFTY options)
- `das_nse_mod.sql.gz` (spot indices)

---

## Outputs (current)

### Packed options (sorted + spot-enriched)

```
data/options_date_packed_FULL_v3_SPOT_ENRICHED/
  <YYYY-MM-DD>/
    BANKNIFTY/part-*.parquet
    NIFTY/part-*.parquet
```

**On-disk sort order (required for fast backtests)**: `expiry → opt_type → strike → timestamp`

### Consolidated spot data (input to v3 repacker)

```
data/spot_data/
  NIFTY_all.parquet
  BANKNIFTY_all.parquet
```

---

## Scripts (current)

- Options SQL extraction: `scripts/sql_extraction/extract_sql_fast.py`
- Spot extraction (from SQL dumps): `scripts/spot_extraction/extract_spot_data.py`
- v3 repacking + spot enrichment: `scripts/data_processing/repack_raw_to_date_v3_SPOT_ENRICHED.py`
- Verification helpers: `scripts/verification/verify_data.py`, `scripts/verification/verify_output.py`

---

## End-to-end steps (recommended)

### 1) Extract raw options (per folder)

```bash
# Example folder:
FOLDER="data/new 2025 data/nov 4 to nov 18 new stocks data"

mkdir -p "$FOLDER/processed_output/raw_options"

python scripts/sql_extraction/extract_sql_fast.py \
  "$FOLDER/das_bankopt_mod.sql.gz" \
  -o "$FOLDER/processed_output/raw_options"

python scripts/sql_extraction/extract_sql_fast.py \
  "$FOLDER/das_niftyopt_mod.sql.gz" \
  -o "$FOLDER/processed_output/raw_options"
```

### 2) Build consolidated spot series (one-time)

```bash
python scripts/spot_extraction/extract_spot_data.py \
  --data-dirs "data/new 2025 data" \
  --output-dir "data/spot_data" \
  --symbols NIFTY BANKNIFTY
```

### 3) Repack to v3 (per folder)

```bash
python scripts/data_processing/repack_raw_to_date_v3_SPOT_ENRICHED.py \
  --input-dir "$FOLDER/processed_output/raw_options" \
  --output-dir "data/options_date_packed_FULL_v3_SPOT_ENRICHED" \
  --expiry-calendar "config/expiry_calendar.csv" \
  --spot-dir "data/spot_data"
```

---

## Quick verification checks

```bash
# Verify there is one file per (date, underlying)
find data/options_date_packed_FULL_v3_SPOT_ENRICHED -name '*.parquet' | wc -l
```

```python
import polars as pl
df = pl.read_parquet("data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-08-01/BANKNIFTY/part-banknifty-0.parquet")
assert df["expiry"].is_sorted()
assert df["opt_type"].is_sorted()  # within expiry blocks
```
