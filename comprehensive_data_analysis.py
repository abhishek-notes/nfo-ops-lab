#!/usr/bin/env python3
"""
Comprehensive analysis of data processing from raw to packed
"""
import polars as pl
import glob
import os
from pathlib import Path
from collections import defaultdict
import datetime

print("COMPREHENSIVE DATA ANALYSIS")
print("=" * 80)

# 1. Analyze what simple_pack.py filters out
print("\n1. FILTERING CONDITIONS IN simple_pack.py:")
print("-" * 60)
print("   a) Files skipped:")
print("      - Cannot parse filename (no symbol/strike/opt_type)")
print("      - Cannot read parquet file")
print("      - No timestamp column")
print("   b) Rows filtered:")
print("      - Outside market hours (9:15 AM - 3:30 PM IST)")
print("      - Duplicate timestamps (keeps first)")
print("      - Cannot map to future expiry date")
print("      - NULL timestamps")

# 2. Count actual total rows
print("\n2. TOTAL ROW COUNT ANALYSIS:")
print("-" * 60)

# Sample to estimate total rows
sample_size = 500
raw_files = glob.glob("./data/raw/options/*.parquet")
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))

print(f"Total files - Raw: {len(raw_files)}, Packed: {len(packed_files)}")

# Process sample of raw files
raw_sample = raw_files[:sample_size]
raw_total_rows = 0
raw_market_rows = 0
raw_empty_files = 0
raw_file_sizes = []

print(f"\nProcessing {len(raw_sample)} raw files...")
for i, rf in enumerate(raw_sample):
    if i % 100 == 0:
        print(f"  Progress: {i}/{len(raw_sample)}")
    try:
        df = pl.read_parquet(rf)
        rows = df.height
        raw_total_rows += rows
        
        if rows == 0:
            raw_empty_files += 1
        else:
            # Count market hours rows
            if "timestamp" in df.columns:
                market_df = df.filter(
                    (pl.col("timestamp").dt.time() >= pl.time(9, 15)) &
                    (pl.col("timestamp").dt.time() <= pl.time(15, 30))
                )
                raw_market_rows += market_df.height
        
        # File size
        raw_file_sizes.append(os.path.getsize(rf))
    except:
        pass

# Extrapolate
avg_raw_rows = raw_total_rows / len(raw_sample)
avg_raw_market = raw_market_rows / len(raw_sample)
est_total_raw_rows = int(avg_raw_rows * len(raw_files))
est_total_market_rows = int(avg_raw_market * len(raw_files))

print(f"\nRaw data statistics (based on {len(raw_sample)} files):")
print(f"  Estimated total rows: {est_total_raw_rows:,}")
print(f"  Estimated market hours rows: {est_total_market_rows:,}")
print(f"  Empty files found: {raw_empty_files}")
print(f"  Average file size: {sum(raw_file_sizes)/len(raw_file_sizes)/1024/1024:.1f} MB")

# Process sample of packed files
packed_sample = packed_files[:min(sample_size, len(packed_files))]
packed_total_rows = 0
packed_file_sizes = []

print(f"\nProcessing {len(packed_sample)} packed files...")
for i, pf in enumerate(packed_sample):
    if i % 100 == 0:
        print(f"  Progress: {i}/{len(packed_sample)}")
    try:
        df = pl.read_parquet(pf)
        packed_total_rows += df.height
        packed_file_sizes.append(os.path.getsize(pf))
    except:
        pass

avg_packed_rows = packed_total_rows / len(packed_sample)
est_total_packed_rows = int(avg_packed_rows * len(packed_files))

print(f"\nPacked data statistics (based on {len(packed_sample)} files):")
print(f"  Estimated total rows: {est_total_packed_rows:,}")
print(f"  Average file size: {sum(packed_file_sizes)/len(packed_file_sizes)/1024:.1f} KB")

retention_rate = est_total_packed_rows / est_total_market_rows * 100 if est_total_market_rows > 0 else 0
print(f"\nRow retention rate (packed/market hours): {retention_rate:.1f}%")

# 3. Column analysis
print("\n3. COLUMN REDUCTION ANALYSIS:")
print("-" * 60)

# Get column info from first non-empty files
raw_cols = set()
packed_cols = set()

for rf in raw_files[:10]:
    try:
        df = pl.read_parquet(rf)
        if df.height > 0:
            raw_cols = set(df.columns)
            break
    except:
        pass

for pf in packed_files[:10]:
    try:
        df = pl.read_parquet(pf)
        if df.height > 0:
            packed_cols = set(df.columns)
            break
    except:
        pass

if raw_cols and packed_cols:
    print(f"Raw columns ({len(raw_cols)}):")
    for col in sorted(raw_cols):
        retained = "✓" if col in packed_cols else "✗"
        print(f"  {retained} {col}")
    
    print(f"\nPacked columns ({len(packed_cols)}):")
    for col in sorted(packed_cols):
        new = "NEW" if col not in raw_cols else "   "
        print(f"  {new} {col}")
    
    removed_cols = raw_cols - packed_cols
    print(f"\nRemoved columns ({len(removed_cols)}):")
    print(f"  Order book: {[c for c in removed_cols if 'bq' in c or 'bp' in c or 'sq' in c or 'sp' in c][:10]}...")
    print(f"  OI related: {[c for c in removed_cols if 'oi' in c.lower()]}")
    print(f"  Others: {[c for c in removed_cols if not any(x in c for x in ['bq','bp','sq','sp','oi'])]}")

# 4. Check specific months
print("\n4. MONTH-SPECIFIC ANALYSIS:")
print("-" * 60)

# Group files by year-month
month_files = defaultdict(list)
for rf in raw_files:
    basename = os.path.basename(rf)
    # Try to extract date patterns
    if '23aug' in basename:
        month_files['2023-08'].append(rf)
    elif '23sep' in basename:
        month_files['2023-09'].append(rf)
    elif '2308' in basename:
        month_files['2023-08'].append(rf)
    elif '2309' in basename:
        month_files['2023-09'].append(rf)

for month, files in sorted(month_files.items()):
    total_rows = 0
    empty_files = 0
    for f in files[:10]:  # Sample
        try:
            df = pl.read_parquet(f)
            if df.height == 0:
                empty_files += 1
            total_rows += df.height
        except:
            pass
    
    print(f"\n{month}: {len(files)} files")
    if len(files[:10]) > 0:
        print(f"  Sample shows {empty_files}/{len(files[:10])} empty files")
        print(f"  Average rows in non-empty: {total_rows/(len(files[:10])-empty_files):.0f}" if empty_files < len(files[:10]) else "  All sampled files empty")

# 5. Size reduction analysis
print("\n5. SIZE REDUCTION ANALYSIS (230GB → 16GB):")
print("-" * 60)
print("Main factors:")
print(f"  a) Column reduction: {len(raw_cols)} → {len(packed_cols)} columns ({(1-len(packed_cols)/len(raw_cols))*100:.0f}% reduction)")
print("  b) Removed data:")
print("     - Complete order book (bid/ask 10 levels × prices/quantities/orders = 30+ columns)")
print("     - Open Interest data")
print("     - Redundant columns (ts, year, month, changeper, lastTradeTime)")
print("  c) Compression: ZSTD level 3 on Parquet format")
print("  d) Data type optimization (Int8 for booleans, efficient string encoding)")
print(f"  e) Some data filtering (~{100-retention_rate:.0f}% of market hours rows)")

total_raw_size = sum(os.path.getsize(f) for f in raw_files) / 1024 / 1024 / 1024
total_packed_size = sum(os.path.getsize(str(f)) for f in packed_files) / 1024 / 1024 / 1024
print(f"\nActual sizes:")
print(f"  Raw: {total_raw_size:.1f} GB")
print(f"  Packed: {total_packed_size:.1f} GB")
print(f"  Reduction: {(1-total_packed_size/total_raw_size)*100:.1f}%")