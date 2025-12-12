#!/usr/bin/env python3
"""
RAW Options to Date-Partitioned Repacking

Transforms raw option files (52 columns with full order book) into date-partitioned format
optimized for backtesting.

Features:
- Preserves ALL 52 columns from raw data
- Applies schema optimizations (Int64 timestamps, Float64 prices, Categorical strings)
- Computes vol_delta (volume delta from cumulative volume)
- Sorts by: opt_type → strike → timestamp (all CE strikes, then all PE strikes)
- Partitions by: date → underlying
"""

from __future__ import annotations

import argparse
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import polars as pl
import pyarrow.dataset as ds


# Regex patterns for filename parsing
FILENAME_REGEX_MONTHNAME = re.compile(
    r"([a-z]+)(\d{2})([a-z]{3})(\d+)(ce|pe)",
    re.IGNORECASE
)
FILENAME_REGEX_NUMERIC = re.compile(
    r"([a-z]+)(\d{2})(\d{1,2})(\d{2})(\d+)(ce|pe)",
    re.IGNORECASE
)

MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}


def calculate_monthly_expiry(year: int, month: int) -> str:
    """
    Calculate the monthly expiry date (last Tuesday of the month).
    
    For NIFTY/BANKNIFTY options:
    - Monthly expiry: Last Tuesday of the expiry month (since Aug 2024)
    - Before Aug 2024: Last Thursday (legacy)
    - If last Tuesday is a holiday, it's the previous trading day
    
    Returns:
        Expiry date as YYYY-MM-DD string
    """
    import calendar
    from datetime import date, timedelta
    
    # Get last day of month
    last_day = calendar.monthrange(year, month)[1]
    expiry = date(year, month, last_day)
    
    # Determine which day to use based on date
    # Aug 2024 onwards = Tuesday (weekday 1), before = Thursday (weekday 3)
    target_weekday = 1 if (year > 2024 or (year == 2024 and month >= 8)) else 3
    
    # Backtrack to target weekday
    # Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4
    while expiry.weekday() != target_weekday:
        expiry -= timedelta(days=1)
    
    return expiry.strftime("%Y-%m-%d")


def parse_expiry_from_filename(filename: str) -> tuple[str, str, bool, bool]:
    """
    Extract expiry metadata from filename.
    
    PRIORITY: If filename has exact date (numeric format), use that.
    Otherwise, calculate monthly expiry for month-name format.
    
    Returns:
        (expiry_date, expiry_type, is_monthly, is_weekly)
    
    Example:
        nifty2511518...pe.parquet → ("2025-11-05", "weekly", False, True) - EXACT DATE
        banknifty25nov...pe.parquet → ("2025-11-26", "monthly", True, False) - CALCULATED
    """
    stem = filename.stem.lower() if hasattr(filename, 'stem') else Path(filename).stem.lower()
    
    # Try numeric format FIRST (has exact date in filename)
    match = FILENAME_REGEX_NUMERIC.match(stem)
    if match:
        underlying, yy, m, dd, strike, opt_type = match.groups()
        year = 2000 + int(yy)
        month = int(m)
        day = int(dd)
        
        from datetime import date
        # USE THE DATE FROM FILENAME - don't calculate!
        expiry_date = date(year, month, day).strftime("%Y-%m-%d")
        
        # Determine if it's monthly expiry by comparing to calculated monthly
        monthly_expiry = calculate_monthly_expiry(year, month)
        is_monthly = (expiry_date == monthly_expiry)
        is_weekly = not is_monthly
        expiry_type = "monthly" if is_monthly else "weekly"
        
        return expiry_date, expiry_type, is_monthly, is_weekly
    
    # Try month name format (only has month, calculate last Tuesday)
    match = FILENAME_REGEX_MONTHNAME.match(stem)
    if match:
        underlying, yy, month_str, strike, opt_type = match.groups()
        year = 2000 + int(yy)
        month = MONTH_MAP.get(month_str.lower())
        
        if month:
            # Calculate monthly expiry (last Tuesday)
            expiry_date = calculate_monthly_expiry(year, month)
            # Month-name format is always monthly
            return expiry_date, "monthly", True, False
    
    # Fallback if parsing fails
    return None, None, False, False


def collect_files_by_underlying(input_dir: Path) -> Dict[str, List[Path]]:
    """Group parquet files by underlying (NIFTY/BANKNIFTY)."""
    files_by_underlying = defaultdict(list)
    
    for file_path in input_dir.glob("*.parquet"):
        # Extract underlying from filename
        stem = file_path.stem.lower()
        
        if "banknifty" in stem or "banknifty" in stem:
            files_by_underlying["BANKNIFTY"].append(file_path)
        elif "nifty" in stem:
            # Exclude banknifty matches
            if "banknifty" not in stem and "banknifty" not in stem:
                files_by_underlying["NIFTY"].append(file_path)
    
    for underlying, files in files_by_underlying.items():
        print(f"Found {len(files)} {underlying} files")
    
    return dict(files_by_underlying)


def repack_raw_options(
    files: List[Path],
    underlying: str,
    output_dir: Path,
    sample_date: str = None
) -> Dict:
    """
    Repack raw option files with full schema optimization.
    
    Schema transformations:
    - timestamp: Int64 (Unix nanoseconds) for Numba + keep original
    - Prices (price, avgPrice, bp0-4, sp0-4, open/high/low/close): Float64
    - Quantities (qty, volume, bQty, sQty, oi, bq0-4, sq0-4, bo0-4, so0-4): Int64
    - Strings (symbol, opt_type): Categorical
    - Strike: Float32
    - Computed: vol_delta (volume change between ticks)
    
    Sort order: opt_type (CE first) → strike → timestamp
    """
    print(f"\nProcessing {underlying} RAW options...")
    
    stats = {
        "files_read": 0,
        "total_rows": 0,
        "dates_written": set(),
        "errors": []
    }
    
    # Step 1: Read all files and add expiry metadata
    print(f"  Reading {len(files)} files...")
    dfs = []
    
    for file_path in files:
        try:
            df = pl.read_parquet(file_path)
            
            if "timestamp" not in df.columns:
                stats["errors"].append(f"No timestamp in {file_path}")
                continue
            
            # Extract expiry metadata from filename
            expiry_date, expiry_type, is_monthly, is_weekly = parse_expiry_from_filename(file_path)
            
            if expiry_date:
                # Add expiry metadata as columns
                df = df.with_columns([
                    pl.lit(expiry_date).str.strptime(pl.Date, "%Y-%m-%d").alias("expiry"),
                    pl.lit(expiry_type).alias("expiry_type"),
                    pl.lit(is_monthly).alias("is_monthly"),
                    pl.lit(is_weekly).alias("is_weekly")
                ])
            
            dfs.append(df)
            stats["files_read"] += 1
            
        except Exception as e:
            stats["errors"].append(f"Error reading {file_path}: {e}")
    
    if not dfs:
        print(f"  No valid data found for {underlying}")
        return stats
    
    # Step 2: Combine
    print(f"  Combining {len(dfs)} dataframes...")
    combined = pl.concat(dfs, how="diagonal")
    stats["total_rows"] = len(combined)
    
    # Step 3: Apply Schema Optimizations
    print(f"  Applying schema optimizations...")
    
    # Define column groups
    price_cols = ["price", "avgPrice", "open", "high", "low", "close", "changeper"]
    price_cols += [f"bp{i}" for i in range(5)] + [f"sp{i}" for i in range(5)]
    
    qty_cols = ["qty", "volume", "bQty", "sQty", "oi", "oiHigh", "oiLow"]
    qty_cols += [f"bq{i}" for i in range(5)] + [f"sq{i}" for i in range(5)]
    qty_cols += [f"bo{i}" for i in range(5)] + [f"so{i}" for i in range(5)]
    
    # Build transformations
    transforms = []
    
    # Timestamp handling
    if "timestamp" in combined.columns:
        # Add Unix nanosecond timestamp for Numba
        transforms.append(
            pl.col("timestamp").dt.epoch(time_unit="ns").alias("timestamp_ns")
        )
        # Keep original timestamp
        transforms.append(pl.col("timestamp"))
        # Add date for partitioning
        transforms.append(pl.col("timestamp").dt.date().alias("date"))
    
    # Convert prices to Float64
    for col in price_cols:
        if col in combined.columns:
            transforms.append(pl.col(col).cast(pl.Float64))
    
    # Convert quantities to Int64
    for col in qty_cols:
        if col in combined.columns:
            transforms.append(pl.col(col).cast(pl.Int64))
    
    # Categorical for strings
    if "symbol" in combined.columns:
        transforms.append(pl.col("symbol").cast(pl.Categorical))
    if "opt_type" in combined.columns:
        transforms.append(pl.col("opt_type").cast(pl.Categorical))
    
    # Strike to Float32
    if "strike" in combined.columns:
        transforms.append(pl.col("strike").cast(pl.Float32))
    
    # Year/month to Int16
    if "year" in combined.columns:
        transforms.append(pl.col("year").cast(pl.Int16))
    if "month" in combined.columns:
        transforms.append(pl.col("month").cast(pl.Int16))
    
    # Add underlying
    transforms.append(pl.lit(underlying).alias("underlying"))
    
    # Keep all other columns
    existing_cols = set(combined.columns)
    handled_cols = set(price_cols + qty_cols + ["symbol", "opt_type", "strike", "year", "month", "timestamp"])
    for col in existing_cols:
        if col not in handled_cols and col != "date":
            transforms.append(pl.col(col))
    
    combined = combined.with_columns(transforms)
    
    # Step 4: Compute vol_delta (volume change between ticks)
    print(f"  Computing vol_delta...")
    if "volume" in combined.columns and "strike" in combined.columns and "opt_type" in combined.columns:
        combined = combined.sort(["strike", "opt_type", "timestamp"])
        combined = combined.with_columns([
            (pl.col("volume") - pl.col("volume").shift(1).over(["strike", "opt_type"]))
            .fill_null(0)
            .alias("vol_delta")
        ])
    
    # Step 5: Filter to sample date if specified
    if sample_date:
        print(f"  Filtering to date: {sample_date}")
        sample_date_parsed = pl.lit(sample_date).str.strptime(pl.Date, "%Y-%m-%d")
        combined = combined.filter(pl.col("date") == sample_date_parsed)
        print(f"  Rows for {sample_date}: {len(combined):,}")
        
        if combined.is_empty():
            print(f"  No data found for date {sample_date}")
            return stats
    
    # Step 6: Sort by opt_type → strike → timestamp
    # This ensures all CE strikes appear first, then all PE strikes
    print(f"  Sorting by opt_type, strike, timestamp...")
    sort_cols = []
    if "opt_type" in combined.columns:
        sort_cols.append("opt_type")
    if "strike" in combined.columns:
        sort_cols.append("strike")
    sort_cols.append("timestamp")
    
    combined = combined.sort(sort_cols)
    
    # Step 7: Write partitioned dataset
    print(f"  Writing partitioned data...")
    
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
        description="Repack RAW option data (52 columns) to date-partitioned format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single date for testing
  python repack_raw_to_date.py \\
    --input-dir "new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options" \\
    --output-dir "date_packed_raw_test" \\
    --sample-date "2024-11-18"
  
  # Process all dates
  python repack_raw_to_date.py \\
    --input-dir "new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options" \\
    --output-dir "date_packed_raw"
        """
    )
    
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing raw option parquet files (52 columns)"
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
        help="Process only this date (YYYY-MM-DD format)"
    )
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}")
        return 1
    
    print("=" * 60)
    print("RAW Options to Date-Partitioned Repacking")
    print("=" * 60)
    print(f"Input: {args.input_dir}")
    print(f"Output: {args.output_dir}")
    if args.sample_date:
        print(f"Sample date: {args.sample_date}")
    print()
    
    # Collect files by underlying
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
        stats = repack_raw_options(files, underlying, args.output_dir, args.sample_date)
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
