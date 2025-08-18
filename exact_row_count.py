#!/usr/bin/env python3
"""
Count EVERY row in EVERY file - raw vs packed
This will take time but will give exact numbers
"""
import polars as pl
import glob
from pathlib import Path
import time

print("EXACT ROW COUNT ANALYSIS")
print("=" * 80)
print("This will take several minutes as we count every row in every file...")
print()

# Get all files
raw_files = glob.glob("./data/raw/options/*.parquet")
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))

print(f"Total files to process: {len(raw_files)} raw, {len(packed_files)} packed")

# Count raw files
print("\nCounting raw files...")
start_time = time.time()
raw_total_rows = 0
raw_empty_files = 0
raw_error_files = 0

for i, rf in enumerate(raw_files):
    if i % 1000 == 0:
        elapsed = time.time() - start_time
        rate = i / elapsed if elapsed > 0 else 0
        eta = (len(raw_files) - i) / rate if rate > 0 else 0
        print(f"  Progress: {i}/{len(raw_files)} files ({i/len(raw_files)*100:.1f}%) - ETA: {eta/60:.1f} minutes")
    
    try:
        df = pl.read_parquet(rf)
        rows = df.height
        raw_total_rows += rows
        if rows == 0:
            raw_empty_files += 1
    except Exception as e:
        raw_error_files += 1

raw_time = time.time() - start_time
print(f"\nRaw files complete in {raw_time/60:.1f} minutes")
print(f"  Total rows: {raw_total_rows:,}")
print(f"  Empty files: {raw_empty_files:,}")
print(f"  Error files: {raw_error_files:,}")
print(f"  Average rows per non-empty file: {raw_total_rows/(len(raw_files)-raw_empty_files-raw_error_files):,.0f}")

# Count packed files
print("\nCounting packed files...")
start_time = time.time()
packed_total_rows = 0
packed_empty_files = 0
packed_error_files = 0

for i, pf in enumerate(packed_files):
    if i % 1000 == 0:
        elapsed = time.time() - start_time
        rate = i / elapsed if elapsed > 0 else 0
        eta = (len(packed_files) - i) / rate if rate > 0 else 0
        print(f"  Progress: {i}/{len(packed_files)} files ({i/len(packed_files)*100:.1f}%) - ETA: {eta/60:.1f} minutes")
    
    try:
        df = pl.read_parquet(pf)
        rows = df.height
        packed_total_rows += rows
        if rows == 0:
            packed_empty_files += 1
    except Exception as e:
        packed_error_files += 1

packed_time = time.time() - start_time
print(f"\nPacked files complete in {packed_time/60:.1f} minutes")
print(f"  Total rows: {packed_total_rows:,}")
print(f"  Empty files: {packed_empty_files:,}")
print(f"  Error files: {packed_error_files:,}")
print(f"  Average rows per non-empty file: {packed_total_rows/(len(packed_files)-packed_empty_files-packed_error_files):,.0f}")

# Final summary
print("\n" + "="*80)
print("FINAL SUMMARY")
print("="*80)
print(f"Raw data:")
print(f"  Files: {len(raw_files):,}")
print(f"  Total rows: {raw_total_rows:,}")

print(f"\nPacked data:")
print(f"  Files: {len(packed_files):,}")
print(f"  Total rows: {packed_total_rows:,}")

print(f"\nRETENTION RATE: {packed_total_rows / raw_total_rows * 100:.2f}%")
print(f"Data reduction: {(1 - packed_total_rows / raw_total_rows) * 100:.2f}%")

# Save results
with open("row_count_results.txt", "w") as f:
    f.write(f"Exact Row Count Results\n")
    f.write(f"======================\n\n")
    f.write(f"Raw files: {len(raw_files):,}\n")
    f.write(f"Raw total rows: {raw_total_rows:,}\n")
    f.write(f"Raw empty files: {raw_empty_files:,}\n")
    f.write(f"Raw error files: {raw_error_files:,}\n")
    f.write(f"\nPacked files: {len(packed_files):,}\n")
    f.write(f"Packed total rows: {packed_total_rows:,}\n")
    f.write(f"Packed empty files: {packed_empty_files:,}\n")
    f.write(f"Packed error files: {packed_error_files:,}\n")
    f.write(f"\nRetention rate: {packed_total_rows / raw_total_rows * 100:.2f}%\n")

print("\nResults saved to row_count_results.txt")