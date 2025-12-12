#!/usr/bin/env python3
"""
Verification script for repacked data.

Validates that the repacking process:
1. Preserved all rows from source files
2. Correctly parsed metadata from filenames  
3. Created proper date partitions
4. Maintains data integrity
"""

import argparse
from pathlib import Path
from typing import Dict, Tuple

import polars as pl


def verify_row_counts(input_dir: Path, output_dir: Path, sample_files: int = 0) -> Tuple[int, int]:
    """
    Compare total row counts between input and output.
    
    Returns:
        (input_rows, output_rows)
    """
    print("\n" + "=" * 60)
    print("ROW COUNT VERIFICATION")
    print("=" * 60)
    
    # Count input rows
    input_files = list(input_dir.glob("*.parquet"))
    if sample_files > 0:
        input_files = input_files[:sample_files]
    
    print(f"\nCounting rows in {len(input_files)} input files...")
    input_rows = 0
    for f in input_files:
        try:
            df = pl.read_parquet(f, columns=["timestamp"])
            input_rows += len(df)
        except Exception as e:
            print(f"  Warning: Could not read {f.name}: {e}")
    
    print(f"Input total: {input_rows:,} rows")
    
    # Count output rows
    print(f"\nCounting rows in output directory...")
    try:
        # Read all partitions
        df_out = pl.scan_parquet(str(output_dir / "**/*.parquet"))
        output_rows = df_out.select(pl.count()).collect().item()
        print(f"Output total: {output_rows:,} rows")
    except Exception as e:
        print(f"Error reading output: {e}")
        output_rows = 0
    
    # Compare
    if input_rows == output_rows:
        print(f"\n✓ Row counts match: {input_rows:,} rows")
    else:
        diff = abs(input_rows - output_rows)
        pct = (diff / input_rows * 100) if input_rows > 0 else 0
        print(f"\n✗ Row count mismatch!")
        print(f"  Difference: {diff:,} rows ({pct:.2f}%)")
    
    return input_rows, output_rows


def verify_metadata(output_dir: Path) -> Dict:
    """
    Verify that metadata columns are correctly populated.
    
    Returns:
        Dictionary with metadata statistics
    """
    print("\n" + "=" * 60)
    print("METADATA VERIFICATION")
    print("=" * 60)
    
    stats = {}
    
    try:
        # Load a sample of the data
        df = pl.scan_parquet(str(output_dir / "**/*.parquet")).head(100000).collect()
        
        # Check required columns exist
        required_cols = ["date", "underlying", "strike", "opt_type", "expiry_date", "timestamp", "price"]
        missing = [c for c in required_cols if c not in df.columns]
        
        if missing:
            print(f"✗ Missing required columns: {missing}")
            return stats
        
        print(f"✓ All required columns present")
        
        # Statistics
        stats["total_rows_sampled"] = len(df)
        stats["unique_dates"] = df["date"].n_unique()
        stats["unique_underlyings"] = df["underlying"].unique().to_list()
        stats["unique_strikes"] = df["strike"].n_unique()
        stats["opt_types"] = df["opt_type"].unique().to_list()
        stats["unique_expiries"] = df["expiry_date"].n_unique()
        
        print(f"\nSample statistics (first 100k rows):")
        print(f"  Unique dates: {stats['unique_dates']}")
        print(f"  Underlyings: {stats['unique_underlyings']}")
        print(f"  Unique strikes: {stats['unique_strikes']}")
        print(f"  Option types: {stats['opt_types']}")
        print(f"  Unique expiries: {stats['unique_expiries']}")
        
        # Show sample data
        print(f"\nSample rows:")
        sample = df.select([
            "date", "timestamp", "underlying", "strike", 
            "opt_type", "expiry_date", "price"
        ]).head(5)
        print(sample)
        
        # Validate data types
        print(f"\nData types:")
        for col in ["date", "timestamp", "underlying", "strike", "opt_type", "expiry_date", "price"]:
            if col in df.columns:
                print(f"  {col}: {df[col].dtype}")
        
    except Exception as e:
        print(f"Error during metadata verification: {e}")
    
    return stats


def verify_partitioning(output_dir: Path) -> Dict:
    """
    Verify partition structure.
    
    Returns:
        Dictionary with partition statistics
    """
    print("\n" + "=" * 60)
    print("PARTITION STRUCTURE VERIFICATION")
    print("=" * 60)
    
    stats = {
        "date_partitions": [],
        "underlying_partitions": {}
    }
    
    # Find all date partitions
    date_dirs = sorted([d for d in output_dir.glob("date=*") if d.is_dir()])
    
    print(f"\nFound {len(date_dirs)} date partitions")
    
    for date_dir in date_dirs[:5]:  # Show first 5
        date_val = date_dir.name.replace("date=", "")
        stats["date_partitions"].append(date_val)
        
        # Find underlying partitions within this date
        underlying_dirs = sorted([d for d in date_dir.glob("underlying=*") if d.is_dir()])
        underlyings = [d.name.replace("underlying=", "") for d in underlying_dirs]
        
        stats["underlying_partitions"][date_val] = underlyings
        
        # Count parquet files
        parquet_files = list(date_dir.glob("**/*.parquet"))
        
        print(f"\n  {date_val}:")
        print(f"    Underlyings: {underlyings}")
        print(f"    Parquet files: {len(parquet_files)}")
    
    if len(date_dirs) > 5:
        print(f"\n  ... and {len(date_dirs) - 5} more date partitions")
    
    return stats


def verify_data_loading_speed(output_dir: Path):
    """
    Test that we can quickly load a single day's data.
    """
    print("\n" + "=" * 60)
    print("LOADING SPEED TEST")
    print("=" * 60)
    
    import time
    
    # Find first date partition
    date_dirs = sorted([d for d in output_dir.glob("date=*") if d.is_dir()])
    if not date_dirs:
        print("No date partitions found")
        return
    
    test_date_dir = date_dirs[0]
    date_val = test_date_dir.name.replace("date=", "")
    
    print(f"\nTesting load speed for {date_val}...")
    
    try:
        start = time.time()
        
        # Load all data for this date
        df = pl.read_parquet(str(test_date_dir / "**/*.parquet"))
        
        elapsed = time.time() - start
        
        print(f"✓ Loaded {len(df):,} rows in {elapsed:.3f} seconds")
        print(f"  Throughput: {len(df) / elapsed:,.0f} rows/sec")
        print(f"  Unique strikes: {df['strike'].n_unique()}")
        print(f"  Underlyings: {df['underlying'].unique().to_list()}")
        
    except Exception as e:
        print(f"✗ Error loading data: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify repacked option data"
    )
    
    parser.add_argument(
        "--input-dir",
        type=Path,
        help="Original input directory (for row count comparison)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Repacked output directory to verify"
    )
    
    parser.add_argument(
        "--sample-files",
        type=int,
        default=0,
        help="Number of input files to sample for row count (0 = all)"
    )
    
    args = parser.parse_args()
    
    if not args.output_dir.exists():
        print(f"Error: Output directory does not exist: {args.output_dir}")
        return 1
    
    print("=" * 60)
    print("REPACKED DATA VERIFICATION")
    print("=" * 60)
    print(f"Output directory: {args.output_dir}")
    
    # Run verifications
    partition_stats = verify_partitioning(args.output_dir)
    metadata_stats = verify_metadata(args.output_dir)
    verify_data_loading_speed(args.output_dir)
    
    if args.input_dir:
        if args.input_dir.exists():
            row_counts = verify_row_counts(args.input_dir, args.output_dir, args.sample_files)
        else:
            print(f"\nWarning: Input directory not found: {args.input_dir}")
    
    print("\n" + "=" * 60)
    print("VERIFICATION COMPLETE")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    exit(main())
