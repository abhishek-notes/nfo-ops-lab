#!/usr/bin/env python3
"""
Investigate the REAL reason for 35% retention rate
"""
import polars as pl
import glob
import os
from pathlib import Path

print("INVESTIGATING REAL RETENTION RATE")
print("=" * 80)

# Let's trace through the entire process for a few files
raw_files = glob.glob("./data/raw/options/*.parquet")
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))

print(f"Total files: {len(raw_files)} raw, {len(packed_files)} packed")

# Pick a few specific files to trace
test_files = [
    "banknifty2360143200pe.parquet",
    "nifty2112113200pe.parquet",
    "banknifty24o0953500ce.parquet"
]

for test_file in test_files:
    # Find the raw file
    raw_file = None
    for rf in raw_files:
        if test_file in rf:
            raw_file = rf
            break
    
    if not raw_file:
        continue
        
    print(f"\n{'='*60}")
    print(f"TRACING: {test_file}")
    print(f"{'='*60}")
    
    # Read raw file
    df_raw = pl.read_parquet(raw_file)
    print(f"\n1. RAW FILE:")
    print(f"   Rows: {df_raw.height:,}")
    
    # Check what simple_pack.py does step by step
    
    # Step 1: Timestamp normalization
    if "timestamp" in df_raw.columns:
        ts_col = "timestamp"
    elif "ts" in df_raw.columns:
        ts_col = "ts"
    else:
        print("   No timestamp column!")
        continue
    
    # Convert to datetime
    if df_raw[ts_col].dtype == pl.Utf8:
        df = df_raw.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("timestamp"))
    else:
        df = df_raw.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("timestamp"))
    
    # Apply timezone
    df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata"))
    
    # Step 2: Remove duplicates
    df_unique = df.unique(["timestamp"])
    print(f"\n2. AFTER DUPLICATE REMOVAL:")
    print(f"   Rows: {df_unique.height:,}")
    print(f"   Duplicates removed: {df.height - df_unique.height:,}")
    
    # Step 3: Market hours filter
    df_market = df_unique.filter(
        (pl.col("timestamp").dt.time() >= pl.time(9, 15)) &
        (pl.col("timestamp").dt.time() <= pl.time(15, 30))
    )
    print(f"\n3. AFTER MARKET HOURS FILTER:")
    print(f"   Rows: {df_market.height:,}")
    print(f"   Removed: {df_unique.height - df_market.height:,}")
    
    # Step 4: Check expiry mapping
    # Extract dates
    dates = df_market.select(pl.col("timestamp").dt.date().alias("date")).unique().sort("date")
    print(f"\n4. DATE RANGE IN FILE:")
    print(f"   From: {dates['date'].min()}")
    print(f"   To: {dates['date'].max()}")
    print(f"   Unique dates: {dates.height}")
    
    # Now find the corresponding packed file
    # Extract symbol, strike, opt_type from filename
    basename = os.path.basename(raw_file).lower()
    
    if "banknifty" in basename:
        symbol = "BANKNIFTY"
    else:
        symbol = "NIFTY"
    
    if "ce" in basename:
        opt_type = "CE"
    else:
        opt_type = "PE"
    
    # Find strike
    import re
    strike_match = re.search(r'(\d+)(ce|pe)', basename)
    if strike_match:
        digits = strike_match.group(1)
        if symbol == "BANKNIFTY" and len(digits) >= 5:
            strike = int(digits[-5:])
        else:
            strike = int(digits[-4:]) if len(digits) >= 4 else 0
    
    print(f"\n5. LOOKING FOR PACKED FILE:")
    print(f"   Symbol: {symbol}")
    print(f"   Strike: {strike}")
    print(f"   Type: {opt_type}")
    
    # Find matching packed files
    matching_packed = []
    for pf in packed_files:
        pf_str = str(pf)
        if (symbol in pf_str and 
            f"strike={strike}.parquet" in pf_str and 
            f"type={opt_type}" in pf_str):
            matching_packed.append(pf)
    
    print(f"\n6. PACKED FILES FOUND: {len(matching_packed)}")
    
    total_packed_rows = 0
    for pf in matching_packed:
        df_packed = pl.read_parquet(pf)
        total_packed_rows += df_packed.height
        print(f"   {pf}: {df_packed.height:,} rows")
    
    print(f"\n7. SUMMARY:")
    print(f"   Raw rows: {df_raw.height:,}")
    print(f"   After processing: {df_market.height:,}")
    print(f"   Expected retention: {df_market.height / df_raw.height * 100:.1f}%")
    print(f"   Packed rows (total): {total_packed_rows:,}")
    print(f"   Actual retention: {total_packed_rows / df_raw.height * 100:.1f}%")

# Now let's check the overall statistics more carefully
print("\n" + "="*80)
print("OVERALL STATISTICS CHECK")
print("="*80)

# Sample raw files to get better estimate
sample_size = 1000
sample_raw = raw_files[:sample_size]

total_raw_rows = 0
total_processed_rows = 0
files_with_no_expiry = 0

for rf in sample_raw:
    try:
        df = pl.read_parquet(rf)
        if df.height == 0:
            continue
            
        total_raw_rows += df.height
        
        # Apply same processing as simple_pack.py
        if "timestamp" in df.columns:
            ts_col = "timestamp"
        elif "ts" in df.columns:
            ts_col = "ts"
        else:
            continue
        
        # Process
        df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("timestamp"))
        df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata"))
        df = df.unique(["timestamp"])
        df = df.filter(
            (pl.col("timestamp").dt.time() >= pl.time(9, 15)) &
            (pl.col("timestamp").dt.time() <= pl.time(15, 30))
        )
        
        if df.height > 0:
            total_processed_rows += df.height
        else:
            files_with_no_expiry += 1
            
    except:
        pass

print(f"\nBased on {sample_size} files:")
print(f"  Total raw rows: {total_raw_rows:,}")
print(f"  Total processed rows: {total_processed_rows:,}")
print(f"  Processing retention: {total_processed_rows / total_raw_rows * 100:.1f}%")
print(f"  Files with no data after processing: {files_with_no_expiry}")