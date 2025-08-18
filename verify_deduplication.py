#!/usr/bin/env python3
"""
Verify that deduplication is working correctly - same strike on different dates should NOT be removed
"""
import polars as pl
import glob
import os
from pathlib import Path
import re

print("DEDUPLICATION VERIFICATION")
print("=" * 80)

# First, let's check the compression of raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
print(f"\nChecking raw file compression...")
sample_file = raw_files[0]
file_size = os.path.getsize(sample_file)
print(f"Sample file: {os.path.basename(sample_file)}")
print(f"File size: {file_size / 1024 / 1024:.2f} MB")

# Try to read and check compression
try:
    import pyarrow.parquet as pq
    parquet_file = pq.ParquetFile(sample_file)
    metadata = parquet_file.metadata
    print(f"Compression: {metadata.row_group(0).column(0).compression}")
except Exception as e:
    print(f"Could not determine compression: {e}")

# Now let's trace specific strikes through the process
print("\n" + "="*80)
print("TRACING SPECIFIC STRIKES")
print("="*80)

# Find strikes that appear in multiple raw files
strike_map = {}
for rf in raw_files[:5000]:  # Sample
    basename = os.path.basename(rf).lower()
    match = re.search(r'(\d{4,6})(ce|pe)', basename)
    if match:
        strike = int(match.group(1)[-5:]) if len(match.group(1)) >= 5 else int(match.group(1))
        opt_type = match.group(2).upper()
        
        if "banknifty" in basename:
            symbol = "BANKNIFTY"
        elif "nifty" in basename:
            symbol = "NIFTY"
        else:
            continue
            
        key = (symbol, strike, opt_type)
        if key not in strike_map:
            strike_map[key] = []
        strike_map[key].append(rf)

# Find strikes in multiple files
multi_file_strikes = [(k, v) for k, v in strike_map.items() if len(v) > 1]
print(f"\nFound {len(multi_file_strikes)} strikes in multiple raw files")

# Analyze a few examples
for (symbol, strike, opt_type), files in multi_file_strikes[:3]:
    print(f"\n{symbol} {strike} {opt_type} - Found in {len(files)} raw files:")
    
    all_timestamps = []
    file_details = []
    
    # Read each file
    for f in files[:3]:  # Limit to 3 files
        try:
            df = pl.read_parquet(f)
            if df.height > 0 and "timestamp" in df.columns:
                # Get timestamp info
                ts_min = df["timestamp"].min()
                ts_max = df["timestamp"].max()
                unique_dates = df.select(pl.col("timestamp").dt.date()).unique().sort("timestamp")
                
                file_details.append({
                    'file': os.path.basename(f),
                    'rows': df.height,
                    'dates': unique_dates["timestamp"].to_list(),
                    'date_range': f"{ts_min} to {ts_max}"
                })
                
                # Collect all timestamps
                all_timestamps.extend(df["timestamp"].to_list())
        except:
            pass
    
    # Show file details
    for fd in file_details:
        print(f"\n  File: {fd['file']}")
        print(f"    Rows: {fd['rows']:,}")
        print(f"    Date range: {fd['date_range']}")
        print(f"    Unique dates: {len(fd['dates'])}")
    
    # Check for overlapping timestamps
    from collections import Counter
    ts_counts = Counter(all_timestamps)
    duplicates = sum(1 for count in ts_counts.values() if count > 1)
    print(f"\n  Total duplicate timestamps across files: {duplicates:,}")
    
    # Now check packed files for this strike
    packed_files = list(Path("./data/packed/options").rglob(f"*strike={strike}.parquet"))
    matching_packed = [pf for pf in packed_files if symbol in str(pf) and f"type={opt_type}" in str(pf)]
    
    print(f"\n  Packed files for this strike: {len(matching_packed)}")
    
    total_packed_rows = 0
    for pf in matching_packed[:5]:
        try:
            df_packed = pl.read_parquet(pf)
            total_packed_rows += df_packed.height
            # Extract expiry from path
            exp_match = re.search(r'exp=(\d{4}-\d{2}-\d{2})', str(pf))
            exp = exp_match.group(1) if exp_match else "unknown"
            print(f"    {exp}: {df_packed.height:,} rows")
        except:
            pass
    
    print(f"\n  Total raw rows: {sum(fd['rows'] for fd in file_details):,}")
    print(f"  Total packed rows (sample): {total_packed_rows:,}")

# Now let's examine the packing logic in detail
print("\n" + "="*80)
print("EXAMINING PACKING LOGIC")
print("="*80)

print("\nThe simple_pack.py deduplication works as follows:")
print("1. Each raw file is processed independently")
print("2. Duplicate timestamps WITHIN each file are removed (line 255)")
print("3. Data is grouped by (symbol, expiry, opt_type, strike)")
print("4. Multiple files with same strike but DIFFERENT expiries go to different output files")
print("5. Multiple files with same strike AND same expiry get MERGED with deduplication")

# Let's verify this with a specific example
print("\n" + "="*80)
print("VERIFICATION: Same strike, different expiries")
print("="*80)

# Find BANKNIFTY 45000 CE files
bn_45000_ce = [f for f in raw_files if "banknifty" in f.lower() and "45000ce" in f.lower()]
print(f"\nFound {len(bn_45000_ce)} files for BANKNIFTY 45000 CE")

if len(bn_45000_ce) >= 2:
    # Check first two files
    for i, f in enumerate(bn_45000_ce[:2]):
        try:
            df = pl.read_parquet(f)
            if df.height > 0 and "timestamp" in df.columns:
                dates = df.select(pl.col("timestamp").dt.date()).unique().sort("timestamp")
                print(f"\n{os.path.basename(f)}:")
                print(f"  Rows: {df.height:,}")
                print(f"  Date range: {dates['timestamp'].min()} to {dates['timestamp'].max()}")
                
                # This is the key - which expiry would this map to?
                # The packing process uses join_asof with strategy="forward"
                # to map each trade_date to the NEXT expiry
                print(f"  These dates would map to expiry AFTER {dates['timestamp'].max()}")
        except:
            pass