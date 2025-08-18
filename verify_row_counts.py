#!/usr/bin/env python3
"""
Verify row counts between raw and packed data
"""
import polars as pl
import glob
import random
from pathlib import Path
import time

print("Row Count Verification")
print("=" * 60)

# Sample raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))

print(f"Total raw files: {len(raw_files)}")
print(f"Total packed files: {len(packed_files)}")

# Sample approach - check a subset
sample_size = 100
sampled_raw = random.sample(raw_files, min(sample_size, len(raw_files)))

print(f"\nSampling {len(sampled_raw)} raw files...")

# Track totals
total_raw_rows = 0
total_raw_market_hours = 0
total_packed_rows = 0
files_checked = 0

# Also check column counts
raw_columns = set()
packed_columns = set()

start_time = time.time()

for i, raw_file in enumerate(sampled_raw):
    if i % 10 == 0:
        print(f"Progress: {i}/{len(sampled_raw)}")
    
    try:
        # Read raw file
        df_raw = pl.read_parquet(raw_file)
        raw_rows = df_raw.height
        total_raw_rows += raw_rows
        
        # Update columns
        raw_columns.update(df_raw.columns)
        
        # Check market hours in raw
        if "timestamp" in df_raw.columns:
            # Count rows within market hours
            market_hours_df = df_raw.filter(
                (pl.col("timestamp").dt.time() >= pl.time(9, 15)) &
                (pl.col("timestamp").dt.time() <= pl.time(15, 30))
            )
            total_raw_market_hours += market_hours_df.height
        
        files_checked += 1
        
    except Exception as e:
        print(f"Error reading {raw_file}: {e}")

# Now sample packed files
sampled_packed = random.sample(packed_files, min(sample_size, len(packed_files)))
print(f"\nSampling {len(sampled_packed)} packed files...")

for i, packed_file in enumerate(sampled_packed):
    if i % 10 == 0:
        print(f"Progress: {i}/{len(sampled_packed)}")
    
    try:
        df_packed = pl.read_parquet(packed_file)
        total_packed_rows += df_packed.height
        packed_columns.update(df_packed.columns)
    except Exception as e:
        print(f"Error reading {packed_file}: {e}")

elapsed = time.time() - start_time

# Extrapolate to full dataset
avg_raw_rows = total_raw_rows / files_checked if files_checked > 0 else 0
avg_raw_market = total_raw_market_hours / files_checked if files_checked > 0 else 0
avg_packed_rows = total_packed_rows / len(sampled_packed) if sampled_packed else 0

estimated_total_raw = avg_raw_rows * len(raw_files)
estimated_total_market = avg_raw_market * len(raw_files)
estimated_total_packed = avg_packed_rows * len(packed_files)

print("\n" + "=" * 60)
print("RESULTS")
print("=" * 60)

print(f"\nBased on {files_checked} raw files sampled:")
print(f"  Average rows per raw file: {avg_raw_rows:,.0f}")
print(f"  Average market hours rows: {avg_raw_market:,.0f}")
print(f"  Estimated total raw rows: {estimated_total_raw:,.0f}")
print(f"  Estimated market hours rows: {estimated_total_market:,.0f}")

print(f"\nBased on {len(sampled_packed)} packed files sampled:")
print(f"  Average rows per packed file: {avg_packed_rows:,.0f}")
print(f"  Estimated total packed rows: {estimated_total_packed:,.0f}")

print(f"\nData retention rate (market hours): {estimated_total_packed/estimated_total_market*100:.1f}%")

print(f"\nColumn analysis:")
print(f"  Raw file columns ({len(raw_columns)}): {sorted(raw_columns)[:10]}... (showing first 10)")
print(f"  Packed columns ({len(packed_columns)}): {sorted(packed_columns)}")

print(f"\nColumn reduction: {len(raw_columns)} -> {len(packed_columns)} ({(1-len(packed_columns)/len(raw_columns))*100:.1f}% reduction)")

print(f"\nTime taken: {elapsed:.1f} seconds")

# Let's also check a specific example in detail
print("\n" + "=" * 60)
print("DETAILED EXAMPLE")
print("=" * 60)

# Find a raw file and try to find its corresponding packed file
example_raw = raw_files[0]
print(f"\nExample raw file: {example_raw}")

try:
    df = pl.read_parquet(example_raw)
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  First few columns: {df.columns[:10]}")
    
    # Show data range
    if "timestamp" in df.columns:
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        # Check pre/post market data
        pre_market = df.filter(pl.col("timestamp").dt.time() < pl.time(9, 15)).height
        post_market = df.filter(pl.col("timestamp").dt.time() > pl.time(15, 30)).height
        market_hours = df.filter(
            (pl.col("timestamp").dt.time() >= pl.time(9, 15)) &
            (pl.col("timestamp").dt.time() <= pl.time(15, 30))
        ).height
        
        print(f"  Pre-market rows: {pre_market:,}")
        print(f"  Market hours rows: {market_hours:,}")
        print(f"  Post-market rows: {post_market:,}")
        print(f"  Percentage in market hours: {market_hours/df.height*100:.1f}%")
        
except Exception as e:
    print(f"  Error: {e}")