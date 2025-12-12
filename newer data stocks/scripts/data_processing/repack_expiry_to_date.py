#!/usr/bin/env python3
"""
Repack Options: From Expiry-Partitioned to Date-Partitioned

Current structure:
  packed_options/BANKNIFTY/202512/exp=2025-12-30/type=CE/strike=48000.parquet

Target structure:
  /date=2024-11-18/underlying=BANKNIFTY/part-0.parquet (ALL strikes, both CE & PE)
"""

from __future__ import annotations

import argparse
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import polars as pl
import pyarrow.dataset as ds


def collect_files_by_underlying(input_dir: Path) -> Dict[str, List[Path]]:
    """
    Scan the packed_options directory and group files by underlying.
    
    Returns:
        Dict mapping underlying (BANKNIFTY/NIFTY) to list of all parquet files
    """
    files_by_underlying = defaultdict(list)
    
    # Find all parquet files recursively
    for underlying_dir in input_dir.glob("*"):
        if not underlying_dir.is_dir():
            continue
        
        underlying = underlying_dir.name.upper()
        
        # Recursively find all parquet files for this underlying
        parquet_files = list(underlying_dir.rglob("*.parquet"))
        
        if parquet_files:
            files_by_underlying[underlying] = parquet_files
            print(f"Found {len(parquet_files)} files for {underlying}")
    
    return dict(files_by_underlying)


def repack_by_date(
    files: List[Path],
    underlying: str,
    output_dir: Path,
    sample_date: str = None
) -> Dict:
    """
    Read all files for an underlying and repack by date with schema optimization.
    
    Schema optimizations applied:
    - timestamp: Int64 (Unix nanoseconds for Numba speed)
    - Prices (price, avgPrice, bp0-4, sp0-4): Float64
    - Quantities/Volumes: Int64
    - Strings (symbol): Categorical
    - Strike: Float32
    - opt_type: Categorical
    
    Data is sorted by: timestamp, strike, opt_type for efficient querying
    
    Args:
        files: List of parquet files to process
        underlying: BANKNIFTY or NIFTY
        output_dir: Output directory
        sample_date: If provided, only process this date (e.g., "2024-11-18")
        
    Returns:
        Statistics dictionary
    """
    print(f"\nProcessing {underlying}...")
    
    stats = {
        "files_read": 0,
        "total_rows": 0,
        "dates_written": set(),
        "errors": []
    }
    
    # Step 1: Read ALL files for this underlying and combine
    print(f"  Reading {len(files)} files...")
    dfs = []
    
    for file_path in files:
        try:
            df = pl.read_parquet(file_path)
            
            # Ensure required columns exist
            if "timestamp" not in df.columns:
                stats["errors"].append(f"No timestamp in {file_path}")
                continue
            
            dfs.append(df)
            stats["files_read"] += 1
            
        except Exception as e:
            stats["errors"].append(f"Error reading {file_path}: {e}")
    
    if not dfs:
        print(f"  No valid data found for {underlying}")
        return stats
    
    # Step 2: Combine all data
    print(f"  Combining {len(dfs)} dataframes...")
    combined = pl.concat(dfs, how="diagonal")
    stats["total_rows"] = len(combined)
    
    # Step 3: Apply Schema Optimizations
    print(f"  Applying schema optimizations...")
    
    # Price columns to Float64
    price_cols = ["price", "avgPrice", "open", "high", "low", "close", "changeper"]
    price_cols += [f"bp{i}" for i in range(5)] + [f"sp{i}" for i in range(5)]
    
    # Quantity/Volume columns to Int64
    qty_cols = ["qty", "volume", "bQty", "sQty", "oi", "oiHigh", "oiLow"]
    qty_cols += [f"bq{i}" for i in range(5)] + [f"sq{i}" for i in range(5)]
    qty_cols += [f"bo{i}" for i in range(5)] + [f"so{i}" for i in range(5)]
    
    # Build transformation expressions
    transforms = []
    
    # Convert timestamp to Int64 (Unix nanoseconds) for Numba speed
    if "timestamp" in combined.columns:
        transforms.append(
            pl.col("timestamp").dt.epoch(time_unit="ns").alias("timestamp_ns")
        )
        # Keep original timestamp too for readability
        transforms.append(pl.col("timestamp"))
        # Add date column for partitioning
        transforms.append(pl.col("timestamp").dt.date().alias("date"))
    
    # Convert price columns to Float64
    for col in price_cols:
        if col in combined.columns:
            transforms.append(pl.col(col).cast(pl.Float64))
    
    # Convert quantity columns to Int64
    for col in qty_cols:
        if col in combined.columns:
            transforms.append(pl.col(col).cast(pl.Int64))
    
    # Convert string columns to Categorical
    if "symbol" in combined.columns:
        transforms.append(pl.col("symbol").cast(pl.Categorical))
    
    if "opt_type" in combined.columns:
        transforms.append(pl.col("opt_type").cast(pl.Categorical))
    
    # Convert strike to Float32 for space efficiency
    if "strike" in combined.columns:
        transforms.append(pl.col("strike").cast(pl.Float32))
    
    # Convert year/month to Int16 for space efficiency
    if "year" in combined.columns:
        transforms.append(pl.col("year").cast(pl.Int16))
    if "month" in combined.columns:
        transforms.append(pl.col("month").cast(pl.Int16))
    
    # Add underlying column
    transforms.append(pl.lit(underlying).alias("underlying"))
    
    # Keep all other columns as-is
    existing_cols = set(combined.columns)
    handled_cols = set(price_cols + qty_cols + ["symbol", "opt_type", "strike", "year", "month", "timestamp"])
    for col in existing_cols:
        if col not in handled_cols and col != "date":
            transforms.append(pl.col(col))
    
    combined = combined.with_columns(transforms)
    
    # Step 4: Filter to sample date if specified
    if sample_date:
        print(f"  Filtering to date: {sample_date}")
        sample_date_parsed = pl.lit(sample_date).str.strptime(pl.Date, "%Y-%m-%d")
        combined = combined.filter(pl.col("date") == sample_date_parsed)
        print(f"  Rows for {sample_date}: {len(combined):,}")
        
        if combined.is_empty():
            print(f"  No data found for date {sample_date}")
            return stats
    
    # Step 5: Sort by opt_type (CE first), then strike, then timestamp
    # This groups all CE options together, then all PE options
    # Within each opt_type, strikes are in ascending order
    # Within each strike, timestamps are in chronological order
    print(f"  Sorting by opt_type, strike, timestamp...")
    sort_cols = []
    if "opt_type" in combined.columns:
        sort_cols.append("opt_type")
    if "strike" in combined.columns:
        sort_cols.append("strike") 
    sort_cols.append("timestamp")
    
    combined = combined.sort(sort_cols)
    
    # Step 6: Write partitioned by date
    print(f"  Writing partitioned data...")
    
    # Get unique dates to track
    unique_dates = combined["date"].unique().sort()
    stats["dates_written"] = set(str(d) for d in unique_dates.to_list())
    
    try:
        ds.write_dataset(
            combined.to_arrow(),
            base_dir=str(output_dir),
            format="parquet",
            partitioning=["date", "underlying"],
            existing_data_behavior="overwrite_or_ignore",
            file_options=ds.ParquetFileFormat().make_write_options(compression="zstd"),
            basename_template=f"part-{underlying.lower()}-{{i}}.parquet"
        )
        
        print(f"  ✓ Wrote {len(combined):,} rows across {len(stats['dates_written'])} dates")
        
    except Exception as e:
        stats["errors"].append(f"Error writing dataset: {e}")
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Repack options from expiry-partitioned to date-partitioned format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single date for testing
  python repack_expiry_to_date.py \\
    --input-dir "new 2025 data/nov 18 to 1 dec new stocks data/processed_output/packed_options" \\
    --output-dir "date_partitioned_output" \\
    --sample-date "2024-11-18"
  
  # Process all dates
  python repack_expiry_to_date.py \\
    --input-dir "new 2025 data/nov 18 to 1 dec new stocks data/processed_output/packed_options" \\
    --output-dir "date_partitioned_output"
        """
    )
    
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing packed_options (with BANKNIFTY/NIFTY subdirs)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for date-partitioned data"
    )
    
    parser.add_argument(
        "--sample-date",
        type=str,
        default=None,
        help="Process only this date (YYYY-MM-DD format, e.g., 2024-11-18)"
    )
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}")
        return 1
    
    print("=" * 60)
    print("Expiry-to-Date Repacking Script")
    print("=" * 60)
    print(f"Input: {args.input_dir}")
    print(f"Output: {args.output_dir}")
    if args.sample_date:
        print(f"Sample date: {args.sample_date}")
    print()
    
    # Collect all files by underlying
    files_by_underlying = collect_files_by_underlying(args.input_dir)
    
    if not files_by_underlying:
        print("No data found!")
        return 1
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each underlying
    start_time = time.time()
    all_stats = {}
    
    for underlying, files in files_by_underlying.items():
        stats = repack_by_date(files, underlying, args.output_dir, args.sample_date)
        all_stats[underlying] = stats
    
    elapsed = time.time() - start_time
    
    # Print summary
    print("\n" + "=" * 60)
    print("REPACKING COMPLETE")
    print("=" * 60)
    
    for underlying, stats in all_stats.items():
        print(f"\n{underlying}:")
        print(f"  Files read: {stats['files_read']}")
        print(f"  Total rows: {stats['total_rows']:,}")
        print(f"  Dates written: {len(stats['dates_written'])}")
        if stats['dates_written']:
            dates_list = sorted(stats['dates_written'])
            print(f"    Range: {dates_list[0]} to {dates_list[-1]}")
        
        if stats['errors']:
            print(f"  Errors: {len(stats['errors'])}")
            for err in stats['errors'][:3]:
                print(f"    - {err}")
    
    print(f"\nTime elapsed: {elapsed:.2f} seconds")
    print(f"Output written to: {args.output_dir}")
    
    return 0


if __name__ == "__main__":
    exit(main())
