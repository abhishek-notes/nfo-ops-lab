#!/usr/bin/env python3
"""
Count rows in parallel for faster processing
"""
import polars as pl
import glob
from pathlib import Path
import concurrent.futures
import time

def count_file_rows(filepath):
    """Count rows in a single file"""
    try:
        df = pl.scan_parquet(filepath)
        return df.select(pl.count()).collect().item(), False  # rows, is_error
    except:
        return 0, True

def process_files_parallel(files, desc="files"):
    """Process files in parallel"""
    total_rows = 0
    empty_files = 0
    error_files = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(count_file_rows, f): f for f in files}
        
        # Process results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
            if i % 1000 == 0:
                print(f"  Processed {i}/{len(files)} {desc} ({i/len(files)*100:.1f}%)")
            
            rows, is_error = future.result()
            if is_error:
                error_files += 1
            elif rows == 0:
                empty_files += 1
            else:
                total_rows += rows
    
    return total_rows, empty_files, error_files

print("PARALLEL ROW COUNT ANALYSIS")
print("=" * 80)

# Get all files
raw_files = glob.glob("./data/raw/options/*.parquet")
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))

print(f"Files to process: {len(raw_files)} raw, {len(packed_files)} packed")

# Process raw files
print("\nProcessing raw files...")
start = time.time()
raw_total, raw_empty, raw_errors = process_files_parallel(raw_files, "raw files")
raw_time = time.time() - start

print(f"\nRaw files complete in {raw_time:.1f} seconds")
print(f"  Total rows: {raw_total:,}")
print(f"  Empty files: {raw_empty:,}")
print(f"  Error files: {raw_errors:,}")

# Process packed files
print("\nProcessing packed files...")
start = time.time()
packed_total, packed_empty, packed_errors = process_files_parallel(packed_files, "packed files")
packed_time = time.time() - start

print(f"\nPacked files complete in {packed_time:.1f} seconds")
print(f"  Total rows: {packed_total:,}")
print(f"  Empty files: {packed_empty:,}")
print(f"  Error files: {packed_errors:,}")

# Summary
print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Raw: {raw_total:,} rows in {len(raw_files):,} files")
print(f"Packed: {packed_total:,} rows in {len(packed_files):,} files")
print(f"\nRETENTION RATE: {packed_total / raw_total * 100:.2f}%")

# Also do a detailed check on data characteristics
print("\n" + "="*80)
print("INVESTIGATING THE DIFFERENCE")
print("="*80)

# Sample some files to understand the data
print("\nSampling files to understand data characteristics...")
sample_raw = raw_files[:100]
sample_packed = packed_files[:100]

# Check a raw file in detail
for rf in sample_raw[:5]:
    try:
        df = pl.read_parquet(rf)
        if df.height > 1000:  # Good size file
            print(f"\nRaw file: {Path(rf).name}")
            print(f"  Rows: {df.height:,}")
            
            if "timestamp" in df.columns:
                # Check date range
                dates = df.select(pl.col("timestamp").dt.date()).unique()
                print(f"  Unique dates: {dates.height}")
                print(f"  Date range: {dates['timestamp'].min()} to {dates['timestamp'].max()}")
                
                # Check for duplicates
                dups = df.height - df.unique(["timestamp"]).height
                print(f"  Duplicate timestamps: {dups:,} ({dups/df.height*100:.1f}%)")
                
                # Check time distribution
                df_time = df.with_columns(pl.col("timestamp").dt.time().alias("time"))
                outside = df_time.filter(
                    (pl.col("time") < pl.time(9, 15)) | 
                    (pl.col("time") > pl.time(15, 30))
                ).height
                print(f"  Outside market hours: {outside:,} ({outside/df.height*100:.1f}%)")
            break
    except:
        pass

# Now check if raw files contain overlapping data
print("\n" + "="*80)
print("CHECKING FOR OVERLAPPING DATA IN RAW FILES")
print("="*80)

# Find files for the same strike but different expiries
strike_files = {}
for rf in raw_files[:1000]:  # Sample
    filename = Path(rf).name
    # Extract strike and type
    import re
    match = re.search(r'(\d{4,6})(ce|pe)', filename)
    if match:
        strike = match.group(1)
        opt_type = match.group(2)
        key = f"{strike}_{opt_type}"
        if key not in strike_files:
            strike_files[key] = []
        strike_files[key].append(rf)

# Find strikes that appear in multiple files
multi_file_strikes = {k: v for k, v in strike_files.items() if len(v) > 1}
print(f"\nStrikes appearing in multiple files: {len(multi_file_strikes)}")

# Check one example
if multi_file_strikes:
    example_key = list(multi_file_strikes.keys())[0]
    example_files = multi_file_strikes[example_key][:3]
    
    print(f"\nExample: Strike {example_key}")
    print(f"  Appears in {len(multi_file_strikes[example_key])} files")
    
    # Load each file and check for overlapping dates
    all_dates = []
    for ef in example_files:
        try:
            df = pl.read_parquet(ef)
            if df.height > 0 and "timestamp" in df.columns:
                dates = df.select(pl.col("timestamp").dt.date()).unique()["timestamp"].to_list()
                print(f"\n  {Path(ef).name}:")
                print(f"    Rows: {df.height:,}")
                print(f"    Dates: {min(dates)} to {max(dates)}")
                all_dates.extend(dates)
        except:
            pass
    
    # Check for date overlaps
    from collections import Counter
    date_counts = Counter(all_dates)
    overlapping_dates = [d for d, count in date_counts.items() if count > 1]
    print(f"\n  Overlapping dates: {len(overlapping_dates)}")
    if overlapping_dates:
        print(f"  Sample overlaps: {overlapping_dates[:5]}")

print("\nAnalysis complete!")