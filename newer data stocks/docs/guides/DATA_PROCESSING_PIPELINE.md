# NFO Options Data Processing Pipeline

## Overview

This document describes the complete pipeline for processing NSE NIFTY/BANKNIFTY options tick data from MySQL SQL.gz dumps to the final partitioned parquet format used for backtesting.

---

## Input Data

### Source Files (per date range folder)
Each data folder contains three SQL.gz files:

| File | Description | Typical Size |
|------|-------------|--------------|
| `das_bankopt_mod.sql.gz` | BANKNIFTY options tick data | 1.1-1.3 GB |
| `das_niftyopt_mod.sql.gz` | NIFTY options tick data | 1.2-1.3 GB |
| `das_nse_mod.sql.gz` | Spot indices (NIFTY, BANKNIFTY, NIFTYFUT, BANKNIFTYFUT) | 1.5-1.6 GB |

### SQL Table Naming Convention
Tables in the SQL dumps follow this pattern:
```
{SYMBOL}{YY}{MMM}{STRIKE}{TYPE}
```

Examples:
- `BANKNIFTY25DEC43500PE` → BANKNIFTY, 2025, December, Strike 43500, Put
- `NIFTY25JAN24000CE` → NIFTY, 2025, January, Strike 24000, Call

---

## Scripts Location

All processing scripts are in:
```
/Users/abhishek/workspace/nfo/newer data stocks/
```

### Scripts

| Script | Purpose |
|--------|---------|
| `extract_sql_fast.py` | Extracts options data from SQL.gz → raw parquet |
| `extract_spot_indices.py` | Extracts spot/futures indices from SQL.gz → parquet |
| `pack_raw_options.py` | Converts raw options → final partitioned format |
| `verify_output.py` | Verifies output schema matches expected format |

---

## Processing Steps

### Step 1: Extract BANKNIFTY Options

```bash
cd "/Users/abhishek/workspace/nfo/newer data stocks"

python3 extract_sql_fast.py \
    "/path/to/data/das_bankopt_mod.sql.gz" \
    -o "/path/to/data/processed_output/raw_options"
```

### Step 2: Extract NIFTY Options

```bash
python3 extract_sql_fast.py \
    "/path/to/data/das_niftyopt_mod.sql.gz" \
    -o "/path/to/data/processed_output/raw_options"
```

### Step 3: Extract Spot Indices

```bash
python3 extract_spot_indices.py \
    "/path/to/data/das_nse_mod.sql.gz" \
    -o "/path/to/data/processed_output/spot"
```

### Step 4: Pack Raw Options to Final Format

```bash
python3 pack_raw_options.py \
    --raw-dir "/path/to/data/processed_output/raw_options" \
    --out-dir "/path/to/data/processed_output/packed_options" \
    --calendar "/workspace/meta/expiry_calendar.csv"
```

---

## Data Schema

### Raw Options Schema (52 columns)

The `extract_sql_fast.py` script produces raw parquet files with these columns:

```python
SQL_COLUMNS = [
    'timestamp',      # Trade timestamp (datetime)
    'price',          # Last traded price
    'qty',            # Trade quantity
    'avgPrice',       # Average price
    'volume',         # Cumulative volume
    'bQty',           # Buy quantity
    'sQty',           # Sell quantity
    'open',           # Open price
    'high',           # High price
    'low',            # Low price
    'close',          # Close price
    'changeper',      # Change percentage
    'lastTradeTime',  # Last trade time
    'oi',             # Open interest
    'oiHigh',         # OI high
    'oiLow',          # OI low
    # Bid depth (5 levels)
    'bq0', 'bp0', 'bo0',  # Bid qty, price, orders (level 0)
    'bq1', 'bp1', 'bo1',  # Level 1
    'bq2', 'bp2', 'bo2',  # Level 2
    'bq3', 'bp3', 'bo3',  # Level 3
    'bq4', 'bp4', 'bo4',  # Level 4
    # Ask depth (5 levels)
    'sq0', 'sp0', 'so0',  # Ask qty, price, orders (level 0)
    'sq1', 'sp1', 'so1',  # Level 1
    'sq2', 'sp2', 'so2',  # Level 2
    'sq3', 'sp3', 'so3',  # Level 3
    'sq4', 'sp4', 'so4',  # Level 4
]

# Additional metadata columns added:
'symbol',    # NIFTY or BANKNIFTY
'opt_type',  # CE or PE
'strike',    # Strike price (Int32)
'year',      # Expiry year
'month',     # Expiry month
'ts',        # Copy of timestamp (for 1970 bug handling)
```

### Packed Options Schema (13 columns)

The `pack_raw_options.py` script produces the final format:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | Datetime[μs, Asia/Kolkata] | Trade timestamp with IST timezone |
| `symbol` | String | NIFTY or BANKNIFTY |
| `opt_type` | String | CE (Call) or PE (Put) |
| `strike` | Int32 | Strike price |
| `open` | Float64 | Open price (OHLC) |
| `high` | Float64 | High price |
| `low` | Float64 | Low price |
| `close` | Float64 | Close price |
| `vol_delta` | Int64 | Volume change (diff of cumulative) |
| `expiry` | Date | Option expiry date |
| `expiry_type` | String | "weekly" or "monthly" |
| `is_monthly` | Int8 | 1 if monthly expiry, 0 otherwise |
| `is_weekly` | Int8 | 1 if weekly expiry, 0 otherwise |

### Spot Indices Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | Datetime[μs, Asia/Kolkata] | Tick timestamp |
| `price` | Float64 | Index/futures price |
| `symbol` | String | NIFTY, BANKNIFTY, NIFTYFUT, BANKNIFTYFUT |

---

## Output Directory Structure

### Packed Options
```
processed_output/
└── packed_options/
    ├── BANKNIFTY/
    │   ├── 202508/
    │   │   ├── exp=2025-08-14/
    │   │   │   ├── type=CE/
    │   │   │   │   ├── strike=50000.parquet
    │   │   │   │   ├── strike=50100.parquet
    │   │   │   │   └── ...
    │   │   │   └── type=PE/
    │   │   │       └── ...
    │   │   └── exp=2025-08-21/
    │   │       └── ...
    │   └── 202509/
    │       └── ...
    └── NIFTY/
        └── (same structure)

### Spot Data
```
processed_output/
└── spot/
    ├── nifty_spot.parquet
    ├── banknifty_spot.parquet
    ├── niftyfut_spot.parquet
    └── bankniftyfut_spot.parquet
```

---

## Key Processing Details

### Timestamp Handling

1. **Timezone**: All timestamps use `Asia/Kolkata` timezone
2. **Method**: Uses `replace_time_zone()` (NOT `convert_time_zone()`) to label existing timestamps as IST without shifting
3. **1970 Bug Fix**: Some vendor data has 1970 timestamps; the `ts` column is used as fallback

```python
# Correct approach - label as IST without conversion
df = df.with_columns(
    pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata")
)
```

### Market Hours Filtering

| Data Type | Market Hours (IST) |
|-----------|-------------------|
| Options | 09:15 - 15:30 |
| Spot Indices | 08:30 - 15:35 |

### OHLC Repair

The packer handles missing/zero OHLC values:
- If `open` is 0 or null → use `close`
- If `high` is 0 or null → use `max(open, close)`
- If `low` is 0 or null → use `min(open, close)`
- Enforce: `low <= open, close <= high`

### Volume Delta Calculation

Volume is stored cumulatively in raw data. The packer computes delta:
```python
vol_delta = volume.diff().clip(lower_bound=0).fill_null(0)
```

### Expiry Calendar Mapping

Uses `/workspace/meta/expiry_calendar.csv` to map trade dates to expiry dates:
- `join_asof` with `strategy="forward"` to find next expiry
- Determines `expiry_type` (weekly/monthly)
- Sets `is_weekly`, `is_monthly` flags

---

## Verification Process

### Schema Verification

```bash
python3 verify_output.py \
    --packed-dir "/path/to/processed_output/packed_options" \
    --sample-dir "/path/to/sample_packed_data"
```

### Manual Verification Steps

1. **Check file count**:
   ```bash
   find processed_output/packed_options -name "*.parquet" | wc -l
   ```

2. **Verify schema matches**:
   ```python
   import polars as pl
   df = pl.read_parquet("path/to/packed/file.parquet")
   print(df.schema)
   # Expected: 13 columns with correct types
   ```

3. **Check timestamp timezone**:
   ```python
   print(df['timestamp'].dtype)
   # Expected: Datetime(time_unit='us', time_zone='Asia/Kolkata')
   ```

4. **Check data quality**:
   ```python
   print(f"Null timestamps: {df['timestamp'].null_count()}")
   print(f"Null expiry: {df['expiry'].null_count()}")
   print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
   ```

5. **Verify expiry mapping**:
   ```python
   # All trade dates should be <= expiry date
   print(df.filter(
       pl.col("timestamp").dt.date() > pl.col("expiry")
   ).height)  # Should be 0
   ```

---

## Example Processing Session

```bash
# Set data folder
DATA_FOLDER="/Users/abhishek/workspace/nfo/aug 13 to aug 29 new stocks data"
SCRIPTS="/Users/abhishek/workspace/nfo/newer data stocks"

cd "$SCRIPTS"

# Step 1: Extract BANKNIFTY options
mkdir -p "$DATA_FOLDER/processed_output/raw_options"
python3 extract_sql_fast.py "$DATA_FOLDER/das_bankopt_mod.sql.gz" \
    -o "$DATA_FOLDER/processed_output/raw_options"

# Step 2: Extract NIFTY options
python3 extract_sql_fast.py "$DATA_FOLDER/das_niftyopt_mod.sql.gz" \
    -o "$DATA_FOLDER/processed_output/raw_options"

# Step 3: Extract spot indices
mkdir -p "$DATA_FOLDER/processed_output/spot"
python3 extract_spot_indices.py "$DATA_FOLDER/das_nse_mod.sql.gz" \
    -o "$DATA_FOLDER/processed_output/spot"

# Step 4: Pack raw options
python3 pack_raw_options.py \
    --raw-dir "$DATA_FOLDER/processed_output/raw_options" \
    --out-dir "$DATA_FOLDER/processed_output/packed_options" \
    --calendar "/workspace/meta/expiry_calendar.csv"

# Verify results
find "$DATA_FOLDER/processed_output/packed_options" -name "*.parquet" | wc -l
ls "$DATA_FOLDER/processed_output/spot/"
```

---

## Processed Data Folders Summary

| Folder | Date Range | Status |
|--------|------------|--------|
| `nov 4-18 new stocks data` | Nov 4-18, 2025 | ✅ Processed |
| `oct 20 to nov 3 new stocks data` | Oct 20 - Nov 3, 2025 | ✅ Processed |
| `oct 7 to oct 20 new stocks data` | Oct 7-20, 2025 | ✅ Processed |
| `sep 23 to oct 6 new stocks data` | Sep 23 - Oct 6, 2025 | ✅ Processed |
| `aug 29 to sep 23 new stocks data` | Aug 29 - Sep 23, 2025 | ✅ Processed |
| `aug 13 to aug 29 new stocks data` | Aug 13-29, 2025 | ⏳ Pending |
| `aug 1 to aug 13 new stocks data` | Until Aug 13, 2025 | ⏳ Pending |
| `aug 14 to 10 sep new stocks data` | Until Sep 10, 2025 | ⏳ Pending |

---

## Troubleshooting

### Common Issues

1. **"Sortedness of columns cannot be checked" warning**
   - This is informational, not an error
   - Occurs during `join_asof` when 'by' groups are used

2. **No rows after processing**
   - Check market hours filtering
   - Verify expiry calendar has entries for the date range

3. **Timestamp timezone issues**
   - Ensure using `replace_time_zone()` not `convert_time_zone()`
   - IST is UTC+5:30, market opens at 09:15 IST

4. **Missing expiry mapping**
   - Update expiry calendar with new expiry dates
   - Calendar at: `/workspace/meta/expiry_calendar.csv`

---

## Compression

All output parquet files use:
- **Algorithm**: ZSTD
- **Level**: 3 (good balance of speed/compression)
- **Statistics**: Enabled for query optimization

```python
df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)
```

---

## Dependencies

```
polars>=0.20.0
python>=3.10
```

---

*Last updated: November 2025*
