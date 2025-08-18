#!/usr/bin/env python3
"""
Verify packed data by randomly sampling 50 files from all packed files
Implements the same checks as the shell script verification
"""
import polars as pl
import random
from pathlib import Path
import sys

def verify_packed_files(sample_size=50):
    # Find all packed parquet files
    packed_files = list(Path("data/packed/options").rglob("*.parquet"))
    
    if not packed_files:
        print("❌ No packed files found in data/packed/options/")
        return
    
    print(f"Found {len(packed_files)} total packed files")
    
    # Randomly sample files
    sample_files = random.sample(packed_files, min(sample_size, len(packed_files)))
    print(f"Randomly sampling {len(sample_files)} files for verification\n")
    
    # Track results
    all_passed = True
    issues = []
    
    for i, file_path in enumerate(sample_files, 1):
        print(f"[{i}/{len(sample_files)}] Checking: {file_path}")
        
        try:
            df = pl.read_parquet(str(file_path))
            
            # A. No duplicate seconds
            dup_count = df.group_by("timestamp").len().select(pl.col("len").max())[0, 0]
            if dup_count > 1:
                issues.append(f"{file_path}: Found {dup_count} duplicate timestamps")
                all_passed = False
            else:
                print("  ✅ No duplicate seconds")
            
            # B. Only one expiry inside the file
            unique_expiries = df.select(pl.col("expiry").n_unique())[0, 0]
            if unique_expiries != 1:
                issues.append(f"{file_path}: Multiple expiries ({unique_expiries}) in single file")
                all_passed = False
            else:
                print(f"  ✅ Single expiry: {df['expiry'].unique()[0]}")
            
            # C. IST times look sane (no 20:xx times)
            min_ts = df.select(pl.min("timestamp"))[0, 0]
            max_ts = df.select(pl.max("timestamp"))[0, 0]
            
            # Check for late evening times
            late_hours = df.filter(pl.col("timestamp").dt.hour() >= 20).height
            if late_hours > 0:
                issues.append(f"{file_path}: Found {late_hours} rows with time >= 20:00")
                all_passed = False
            else:
                print(f"  ✅ Timestamp range OK: {min_ts} to {max_ts}")
            
            # D. Calendar tags are consistent
            cal_info = df.select(["expiry", "expiry_type", "is_monthly", "is_weekly"]).unique()
            if cal_info.height != 1:
                issues.append(f"{file_path}: Inconsistent calendar tags")
                all_passed = False
            else:
                row = cal_info.row(0, named=True)
                print(f"  ✅ Consistent tags: {row['expiry_type']} (monthly={row['is_monthly']}, weekly={row['is_weekly']})")
            
            # E. Schema check
            expected_cols = {
                "timestamp", "symbol", "opt_type", "strike",
                "open", "high", "low", "close", "vol_delta",
                "expiry", "expiry_type", "is_monthly", "is_weekly"
            }
            actual_cols = set(df.columns)
            missing = expected_cols - actual_cols
            extra = actual_cols - expected_cols
            
            if missing:
                issues.append(f"{file_path}: Missing columns: {missing}")
                all_passed = False
            elif extra:
                # Extra columns are OK as long as required ones are present
                print(f"  ⚠️  Extra columns (OK): {extra}")
            else:
                print("  ✅ Schema correct")
            
            # F. Data quality
            # Check for price sanity
            price_issues = df.filter(
                (pl.col("close") <= 0) | 
                (pl.col("low") > pl.col("high")) |
                (pl.col("low") > pl.col("close")) |
                (pl.col("high") < pl.col("close"))
            ).height
            
            if price_issues > 0:
                issues.append(f"{file_path}: {price_issues} rows with price issues")
                all_passed = False
            else:
                print("  ✅ Price data looks good")
            
            print()  # blank line between files
            
        except Exception as e:
            issues.append(f"{file_path}: Error reading file - {e}")
            all_passed = False
            print(f"  ❌ Error: {e}\n")
    
    # Summary
    print("="*60)
    print("VERIFICATION SUMMARY")
    print("="*60)
    print(f"Files checked: {len(sample_files)}")
    print(f"Status: {'✅ ALL PASSED' if all_passed else '❌ ISSUES FOUND'}")
    
    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues:
            print(f"  - {issue}")
    
    # Show a sample of good data
    if sample_files and all_passed:
        print("\n" + "="*60)
        print("SAMPLE DATA FROM LAST FILE")
        print("="*60)
        df = pl.read_parquet(str(sample_files[-1]))
        print(f"File: {sample_files[-1]}")
        print(f"Shape: {df.shape}")
        print("\nFirst 10 rows:")
        print(df.head(10))
        
        # Stats
        print("\nQuick stats:")
        print(f"  Symbol: {df['symbol'].unique().to_list()}")
        print(f"  Strike: {df['strike'].unique().to_list()}")
        print(f"  Option Type: {df['opt_type'].unique().to_list()}")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"  Price range: {df['close'].min():.2f} to {df['close'].max():.2f}")

if __name__ == "__main__":
    # Check if sample size provided
    sample_size = 50
    if len(sys.argv) > 1:
        try:
            sample_size = int(sys.argv[1])
        except ValueError:
            print(f"Invalid sample size: {sys.argv[1]}, using default 50")
    
    verify_packed_files(sample_size)