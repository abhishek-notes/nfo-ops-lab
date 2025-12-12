# Data Sorting Fix - Summary

## Problem Identified
The packing script (`repack_raw_to_date_FINAL.py`) sorts data correctly in memory (line 236), but **PyArrow's `write_dataset()` doesn't preserve sort order** when writing partitioned datasets. It re-arranges rows for file splitting.

Result: Data appears unsorted on disk, causing every benchmark run to re-sort 475M rows.

## Solution

### 1. Fix Existing Data (One-Time)
**Command:**
```bash
cd "/Users/abhishek/workspace/nfo/newer data stocks"
python resort_packed_data.py
```

**What it does:**
- Reads each of 115 parquet files
- Sorts by: `expiry → opt_type → strike → timestamp`
- Writes with row_group statistics (enables efficient filtering)
- Overwrites original files

**Time:** ~5-10 minutes

### 2. Future Packing (Use New Script)
**Script:** `repack_raw_to_date_v2_SORTED.py`

**Key Improvement:** Writes files **per date/underlying** using Polars directly instead of PyArrow dataset API. This preserves the sort order.

**Changes from v1:**
```python
# OLD (v1): Uses PyArrow write_dataset - LOSES sort order
ds.write_dataset(
    combined.to_arrow(),
    partitioning=["date", "underlying"]
)

# NEW (v2): Writes per partition with Polars - PRESERVES sort order  
for date_val in unique_dates:
    date_df = combined.filter(pl.col("date") == date_val)
    date_df.write_parquet(
        output_path / f"part-{underlying}-0.parquet",
        statistics=True,           # Row group min/max stats
        row_group_size=100_000     # Optimal chunk size
    )
```

**Additional Benefits:**
- Row group statistics enable "row group skipping" (Polars can skip entire chunks without reading)
- Optimal row group size (100K rows) for filtering performance

## Expected Performance After Fix

### Before (Unsorted Data)
- Throughput: 2.5-3.0M rows/sec
- Bottleneck: Sorting 475M rows on every run

### After (Sorted Data)
- Throughput: **50-100M rows/sec**
- Bottleneck: Disk I/O only (no sorting needed)

## Commands Summary

**1. Fix existing data:**
```bash
python resort_packed_data.py
```

**2. For future data packing, use:**
```bash
python repack_raw_to_date_v2_SORTED.py \
  --input-dir "path/to/raw_options" \
  --output-dir "options_date_packed_FULL"
```

**3. Run benchmark after sorting:**
```bash
python strategy_benchmark_OPTIMIZED.py --workers 24
```

Expected result: **50-100M rows/sec** (20-40x faster than current 2.5M rows/sec)
