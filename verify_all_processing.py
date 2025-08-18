#!/usr/bin/env python3
"""
Comprehensive verification of all data processing
"""
import polars as pl
import glob
import os
from datetime import datetime

print("=" * 80)
print("DATA PROCESSING VERIFICATION")
print(f"Time: {datetime.now()}")
print("=" * 80)

# 1. Options data
print("\n1. OPTIONS DATA")
print("-" * 60)
options_packed = glob.glob("data/packed/options/*/*/exp=*/type=*/strike=*.parquet")
options_bars = glob.glob("data/bars/options/*/*/exp=*/type=*/strike=*/bars_1m.parquet")
print(f"Packed tick files: {len(options_packed):,}")
print(f"1-minute bar files: {len(options_bars):,}")

if options_bars:
    # Sample one options bar file
    sample = options_bars[0]
    df = pl.read_parquet(sample)
    print(f"\nSample options bar file: {os.path.basename(os.path.dirname(sample))}")
    print(f"Shape: {df.shape}")
    print(df.head(3))

# 2. Spot data
print("\n2. SPOT DATA")
print("-" * 60)
spot_packed = glob.glob("data/packed/spot/*/202*/date=*/ticks.parquet")
spot_bars = glob.glob("data/bars/spot/*/202*/date=*/bars_1m.parquet")
print(f"Packed tick files: {len(spot_packed):,}")
print(f"1-minute bar files: {len(spot_bars):,}")

if spot_packed:
    # Show spot coverage
    spot_symbols = set()
    spot_dates = []
    for sp in spot_packed:
        if "BANKNIFTY" in sp:
            spot_symbols.add("BANKNIFTY")
        else:
            spot_symbols.add("NIFTY")
        # Extract date
        import re
        date_match = re.search(r'date=(\d{4}-\d{2}-\d{2})', sp)
        if date_match:
            spot_dates.append(date_match.group(1))
    
    print(f"\nSymbols: {sorted(spot_symbols)}")
    print(f"Date range: {min(spot_dates)} to {max(spot_dates)}")
    print(f"Total trading days: {len(set(spot_dates))}")

if spot_bars:
    # Sample spot bar
    sample = spot_bars[0]
    df = pl.read_parquet(sample)
    print(f"\nSample spot bar file")
    print(f"Shape: {df.shape}")
    print(df.head(3))

# 3. Futures data
print("\n3. FUTURES DATA")
print("-" * 60)
futures_packed = glob.glob("data/packed/futures/*/202*/exp=*/ticks.parquet")
futures_bars = glob.glob("data/bars/futures/*/202*/exp=*/bars_1m.parquet")
print(f"Packed tick files: {len(futures_packed):,}")
print(f"1-minute bar files: {len(futures_bars):,}")

if futures_packed:
    print("Futures data successfully packed!")
else:
    print("No futures data packed yet (large files, may need special handling)")

# 4. Summary
print("\n" + "="*80)
print("SUMMARY")
print("="*80)

total_tick_files = len(options_packed) + len(spot_packed) + len(futures_packed)
total_bar_files = len(options_bars) + len(spot_bars) + len(futures_bars)

print(f"Total tick files: {total_tick_files:,}")
print(f"Total bar files: {total_bar_files:,}")

if total_bar_files < total_tick_files:
    print(f"\nBar building in progress: {total_bar_files/total_tick_files*100:.1f}% complete")
else:
    print("\n✅ All data processing complete!")

# Check disk usage
print("\nDisk usage:")
for path in ["data/packed", "data/bars"]:
    if os.path.exists(path):
        size = os.popen(f"du -sh {path} 2>/dev/null").read().strip()
        if size:
            print(f"  {path}: {size}")

print("\n" + "="*80)
print("Ready for backtesting with:")
print("- Options: 1-minute bars with symbol/expiry/strike/type")
print("- Spot: 1-minute bars by date")
print("- Futures: Pending processing")
print("="*80)