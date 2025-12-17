# Output File Location

## Where Files Are Created

When you run:
```bash
python scripts/data_processing/repack_raw_to_date_v3_SPOT_ENRICHED.py \
  --input-dir "data/new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options" \
  --output-dir "data/options_date_packed_FULL_v3_SPOT_ENRICHED" \
  --expiry-calendar "config/expiry_calendar.csv" \
  --spot-dir "data/spot_data"
```

**Output goes to the `--output-dir` path you specify**.

## Directory Structure Created

```
data/options_date_packed_FULL_v3_SPOT_ENRICHED/     ← Your --output-dir
├── 2025-11-18/                                    ← Date partition (YYYY-MM-DD)
│   ├── BANKNIFTY/
│   │   └── part-banknifty-0.parquet               ← ALL strikes for this day
│   └── NIFTY/
│       └── part-nifty-0.parquet
├── 2025-11-19/
│   ├── BANKNIFTY/part-banknifty-0.parquet
│   └── NIFTY/part-nifty-0.parquet
└── ... (one folder per date)
```

## How Partitions Are Written (current)

The v3 repacker writes one file per `(date, underlying)` partition explicitly (via Polars). This preserves the on-disk sort order required for high-performance backtests.

## File Naming

- Files are named: `part-{underlying}-{i}.parquet`
- Example: `part-banknifty-0.parquet`, `part-nifty-0.parquet`
- The `{i}` is a counter if the writer splits files (usually stays `0`)

## Expiry Date Logic - CORRECTED

### Last Tuesday (Since Aug 2024)
- **Monthly expiry** = Last Tuesday of the month
- Before Aug 2024 = Last Thursday (legacy)

### Extraction Priority
1. **Numeric filename** (e.g., `nifty2511518...`) → USE EXACT DATE from filename
2. **Month-name filename** (e.g., `banknifty25nov...`) → CALCULATE last Tuesday of Nov 2025

This ensures we use the actual expiry date when it's in the filename, not a calculated guess!
