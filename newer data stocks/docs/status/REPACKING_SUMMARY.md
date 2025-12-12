# Data Repacking Script - Summary and Usage Guide

## Overview

I've created a comprehensive data repacking solution based on the detailed requirements from your reference chat. The scripts transform your current per-contract option files into a **date-partitioned format optimized for backtesting**.

## What Was Created

### 1. **`repack_options_by_date.py`** - Main Repacking Script
**Purpose**: Transform data from time-series (one file per contract) to cross-sectional (all strikes per day)

**Key Features**:
- **Parallel processing**: Uses ProcessPoolExecutor with configurable workers
- **Filename parsing**: Regex extracts underlying, strike, opt_type, expiry from filename
  - Example: `nifty2520623950ce` → NIFTY, 2025-02-06 expiry, 23950 strike, CE
- **Date partitioning**: Creates `/date=YYYY-MM-DD/underlying=NIFTY/` structure  
- **Type optimization**: Converts to Float64, Categorical for speed
- **Column filtering**: Keeps only essential columns for backtesting
- **Batch processing**: Processes files in chunks to manage memory

**Current → New Structure**:
```
Current: nifty2520623950ce.parquet (one contract, multiple days, 180k rows)

New:     /repacked/
           date=2025-01-27/
             underlying=NIFTY/
               part-xyz.parquet  (all strikes for this day)
```

### 2. **`verify_repacked_data.py`** - Verification Script
**Purpose**: Validate data integrity after repacking

**Checks**:
- ✓ Row counts match between input/output
- ✓ Metadata correctly parsed from filenames
- ✓ Partition structure is correct
- ✓ Loading speed test (single day query)

## Quick Start

### Test Run (Recommended First)
```bash
cd "/Users/abhishek/workspace/nfo/newer data stocks"

# Test on sample data
python repack_options_by_date.py \
  --input-dir "new 2025 data/Processed Samples/raw options data samples parquet" \
  --output-dir "test_repacked_output" \
  --sample 10 \
  --workers 4

# Verify the test output
python verify_repacked_data.py \
  --input-dir "new 2025 data/Processed Samples/raw options data samples parquet" \
  --output-dir "test_repacked_output" \
  --sample-files 10
```

### Full Production Run (After Test Validation)
```bash
# Full repack on all files
python repack_options_by_date.py \
  --input-dir /path/to/all/85k/option/files \
  --output-dir /path/to/repacked/output \
  --workers 16 \
  --batch-size 2000

# Verify complete dataset  
python verify_repacked_data.py \
  --output-dir /path/to/repacked/output
```

## Why This Format is Better for Backtesting

Based on the reference chat, this format enables:

### 1. **Fast Strike Selection**
```python
# Load single day with all strikes
df = pl.read_parquet("repacked/date=2025-01-27/**/*.parquet")

# Find ATM strike instantly
spot_price = 23950
atm_strike = df.filter(
    (pl.col("strike") == spot_price) & 
    (pl.col("opt_type") == "CE")
)
```

### 2. **Time-Based Filtering**
```python
# Filter to specific hours (12pm-2pm)
df = df.filter(
    pl.col("timestamp").dt.hour().is_between(12, 14)
)

# Filter to expiry days only
df = df.filter(pl.col("date") == pl.col("expiry_date"))
```

### 3. **Backtesting Example Strategy**
```python
# Your EMA5/EMA21 strategy with dynamic strike selection
import polars as pl

# Load one day
day = pl.read_parquet("repacked/date=2025-01-27/**/*.parquet")

# Calculate spot EMA (assuming you have spot data)
# Find ATM+100 strike
# Check spread < threshold
# Execute strategy with Numba for speed
```

## Column Schema

The repacked files contain:

**Core Data**:
- `timestamp` (Datetime), `date` (Date), `price` (Float64), `volume`, `qty`

**Metadata** (extracted from filename):
- `underlying` (String): "NIFTY" or "BANKNIFTY"
- `strike` (Int32): Strike price
- `opt_type` (Categorical): "CE" or "PE"
- `expiry_date` (Date): Contract expiry

**Order Book** (depth 0-4):
- `bp0-4`, `sp0-4` (Float64): Bid/ask prices
- `bq0-4`, `sq0-4`: Bid/ask quantities

**Other**:
- `oi`, `oiHigh`, `oiLow`: Open interest
- `open`, `high`, `low`, `close`: OHLC

## Performance Expectations

Based on the reference chat benchmarks:

- **Repacking speed**: ~100M rows/sec with 16 workers (your system achieved this)
- **Query speed**: Load entire day (all strikes) in <1 second with PyArrow
- **Storage**: Similar or smaller size vs original (better compression)
- **Backtesting**: 5.8B rows in 58 seconds with Numba (your previous result)

## Next Steps - What You Should Do

1. **Review Files**:
   - [ ] Check `implementation_plan.md` for detailed design
   - [ ] Review `repack_options_by_date.py` - confirm column selection
   - [ ] Verify filename regex pattern matches your files

2. **Run Test**:
   ```bash
   # Start with just 10 files
   python repack_options_by_date.py --input-dir "..." --output-dir "test" --sample 10
   ```

3. **Verify Test Output**:
   ```bash
   python verify_repacked_data.py --input-dir "..." --output-dir "test"
   ```

4. **Confirm Before Full Run**:
   - Check that strikes/expiries are correctly parsed
   - Verify partition structure looks correct
   - Confirm you have enough disk space (~same as source data)

## Important Notes

> **Disk Space**: Ensure you have at least the same amount of free space as your source data (~several hundred GB for 85k files)

> **Spot Index Data**: The chat mentioned you'll need spot index prices (NIFTY/BANKNIFTY) for ATM calculations. Make sure to repack those separately or include them in the same pipeline.

> **Expiry Day Changes**: The script stores explicit `expiry_date` so you don't need to worry about Thursday→Tuesday changes over time.

## Questions to Confirm

1. **Where are your 85k option files located?** (for the `--input-dir` path)
2. **Where should the repacked data go?** (for the `--output-dir` path)  
3. **Do you have spot index data files?** (NIFTY/BANKNIFTY index values)
4. **Any specific columns you want to drop** to save space?

Let me know when you're ready to run the test, or if you'd like any modifications to the scripts!
