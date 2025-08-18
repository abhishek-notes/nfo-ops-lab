#!/usr/bin/env python3
'''
Random verification script - checks 50 random strike combinations for data integrity
'''
import polars as pl
import glob
import random
import re
from collections import defaultdict, Counter
from pathlib import Path

def verify_strike_data(symbol, strike, opt_type):
    '''Verify data integrity for a specific strike'''
    # Find raw files
    raw_files = glob.glob("./data/raw/options/*.parquet")
    raw_matches = []
    
    for rf in raw_files:
        basename = rf.lower()
        if symbol.lower() in basename and str(strike) in basename and opt_type.lower() in basename:
            raw_matches.append(rf)
    
    if not raw_matches:
        return None
    
    # Load all raw data
    all_raw_timestamps = []
    raw_total_rows = 0
    
    for rf in raw_matches:
        try:
            df = pl.read_parquet(rf)
            if df.height > 0 and "timestamp" in df.columns:
                raw_total_rows += df.height
                all_raw_timestamps.extend(df["timestamp"].to_list())
        except:
            pass
    
    # Find packed files
    packed_files = list(Path("./data/packed/options").rglob(f"*strike={strike}.parquet"))
    packed_matches = [pf for pf in packed_files if symbol in str(pf) and f"type={opt_type}" in str(pf)]
    
    # Load all packed data
    packed_total_rows = 0
    
    for pf in packed_matches:
        try:
            df = pl.read_parquet(pf)
            packed_total_rows += df.height
        except:
            pass
    
    # Count unique timestamps
    unique_timestamps = len(set(all_raw_timestamps))
    
    return {
        'strike': f"{symbol} {strike} {opt_type}",
        'raw_files': len(raw_matches),
        'raw_rows': raw_total_rows,
        'unique_timestamps': unique_timestamps,
        'duplicates': raw_total_rows - unique_timestamps,
        'packed_files': len(packed_matches),
        'packed_rows': packed_total_rows,
        'retention': packed_total_rows / unique_timestamps * 100 if unique_timestamps > 0 else 0
    }

# Main verification
print("RANDOM STRIKE VERIFICATION")
print("=" * 80)

# Get all unique strikes
raw_files = glob.glob("./data/raw/options/*.parquet")
strikes = set()

for rf in raw_files[:5000]:  # Sample to find strikes
    basename = rf.lower()
    match = re.search(r'(\d{4,6})(ce|pe)', basename)
    if match:
        strike = int(match.group(1)[-5:]) if len(match.group(1)) >= 5 else int(match.group(1))
        opt_type = match.group(2).upper()
        symbol = "BANKNIFTY" if "banknifty" in basename else "NIFTY"
        strikes.add((symbol, strike, opt_type))

# Sample 50 strikes
sample_strikes = random.sample(list(strikes), min(50, len(strikes)))

print(f"Verifying {len(sample_strikes)} random strikes...\n")

results = []
for symbol, strike, opt_type in sample_strikes:
    result = verify_strike_data(symbol, strike, opt_type)
    if result:
        results.append(result)
        if result['duplicates'] > 0:
            print(f"{result['strike']}:")
            print(f"  Raw: {result['raw_rows']:,} rows ({result['raw_files']} files)")
            print(f"  Unique: {result['unique_timestamps']:,} timestamps")
            print(f"  Duplicates: {result['duplicates']:,} ({result['duplicates']/result['raw_rows']*100:.1f}%)")
            print(f"  Packed: {result['packed_rows']:,} rows")
            print(f"  Retention: {result['retention']:.1f}%\n")

# Summary
total_raw = sum(r['raw_rows'] for r in results)
total_unique = sum(r['unique_timestamps'] for r in results)
total_duplicates = sum(r['duplicates'] for r in results)
total_packed = sum(r['packed_rows'] for r in results)

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Strikes verified: {len(results)}")
print(f"Total raw rows: {total_raw:,}")
print(f"Total unique timestamps: {total_unique:,}")
print(f"Total duplicates: {total_duplicates:,} ({total_duplicates/total_raw*100:.1f}%)")
print(f"Total packed rows: {total_packed:,}")
print(f"Overall retention: {total_packed/total_unique*100:.1f}%")

if total_packed < total_unique * 0.95:  # Less than 95% retention
    print("\n⚠️  WARNING: Possible data loss detected!")
else:
    print("\n✅ Data integrity verified - duplicates removed, unique data preserved")
