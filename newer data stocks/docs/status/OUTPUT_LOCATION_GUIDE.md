# Output File Location

## Where Files Are Created

When you run:
```bash
python repack_raw_to_date.py \
  --input-dir "new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options" \
  --output-dir "date_packed_raw_test"
```

**Output goes to the `--output-dir` path you specify**.

## Directory Structure Created

```
date_packed_raw_test/                    ← Your --output-dir
├── 2025-11-18/                          ← Date partition (date=YYYY-MM-DD format)
│   ├── BANKNIFTY/                       ← Underlying partition
│   │   └── part-banknifty-0.parquet    ← Single file with ALL strikes for this day
│   └── NIFTY/
│       └── part-nifty-0.parquet
├── 2025-11-19/
│   ├── BANKNIFTY/
│   │   └── part-banknifty-0.parquet
│   └── NIFTY/
│       └── part-nifty-0.parquet
└── ... (one folder per date)
```

## How PyArrow Creates Partitions

The script uses:
```python
ds.write_dataset(
    df.to_arrow(),
    base_dir=str(output_dir),           # Your --output-dir
    format="parquet",
    partitioning=["date", "underlying"],  # Creates nested folders
    ...
)
```

PyArrow automatically:
1. Creates `date=YYYY-MM-DD/` folders for each unique date in the data
2. Within each date, creates `underlying=BANKNIFTY/` and `underlying=NIFTY/` subfolders
3. Writes `part-*.parquet` files inside

## File Naming

- Files are named: `part-{underlying}-{i}.parquet`
- Example: `part-banknifty-0.parquet`, `part-nifty-0.parquet`
- The `{i}` is a counter from PyArrow if files are split (usually stays 0)

## Expiry Date Logic - CORRECTED

### Last Tuesday (Since Aug 2024)
- **Monthly expiry** = Last Tuesday of the month
- Before Aug 2024 = Last Thursday (legacy)

### Extraction Priority
1. **Numeric filename** (e.g., `nifty2511518...`) → USE EXACT DATE from filename
2. **Month-name filename** (e.g., `banknifty25nov...`) → CALCULATE last Tuesday of Nov 2025

This ensures we use the actual expiry date when it's in the filename, not a calculated guess!
