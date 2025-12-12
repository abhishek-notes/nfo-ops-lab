#!/usr/bin/env python3
"""
PRODUCTION: RAW Options to Date-Partitioned Repacking

Final version with:
- Expiry calendar integration (expiry_calendar.csv)
- Vol_delta fix for volume resets
- Changeper rounding to 2 decimals
- Full 52-column preservation
- Schema optimizations
"""

from __future__ import annotations

import argparse
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import pyarrow.dataset as ds


def load_expiry_calendar(calendar_path: Path) -> pl.DataFrame:
    """Load and prepare expiry calendar for lookups."""
    df = pl.read_csv(calendar_path)
    
    # Parse dates
    df = df.with_columns([
        pl.col("Final_Expiry").str.strptime(pl.Date, "%Y-%m-%d").alias("expiry_date")
    ])
    
    # Create lookup key: BANKNIFTY_monthly_2025-11 -> expiry_date
    df = df.with_columns([
        (pl.col("Instrument") + "_" + 
         pl.col("Expiry_Type").str.to_lowercase() + "_" + 
         pl.col("Contract_Month")).alias("lookup_key")
    ])
    
    return df.select(["lookup_key", "expiry_date", "Expiry_Type", "Final_Day"])


def collect_files_by_underlying(input_dir: Path) -> Dict[str, List[Path]]:
    """Group parquet files by underlying (NIFTY/BANKNIFTY)."""
    files_by_underlying = defaultdict(list)
    
    for file_path in input_dir.glob("*.parquet"):
        stem = file_path.stem.lower()
        
        if "banknifty" in stem:
            files_by_underlying["BANKNIFTY"].append(file_path)
        elif "nifty" in stem and "banknifty" not in stem:
            files_by_underlying["NIFTY"].append(file_path)
    
    for underlying, files in files_by_underlying.items():
        print(f"Found {len(files)} {underlying} files")
    
    return dict(files_by_underlying)


def repack_raw_options(
    files: List[Path],
    underlying: str,
    output_dir: Path,
    expiry_calendar: Optional[pl.DataFrame] = None,
    sample_date: str = None
) -> Dict:
    """
    Repack raw option files with full optimizations.
    """
    print(f"\nProcessing {underlying} RAW options...")
    
    stats = {
        "files_read": 0,
        "total_rows": 0,
        "dates_written": set(),
        "errors": []
    }
    
    # Step 1: Read all files
    print(f"  Reading {len(files)} files...")
    dfs = []
    
    for file_path in files:
        try:
            df = pl.read_parquet(file_path)
            
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
    
    # Step 2: Combine
    print(f"  Combining {len(dfs)} dataframes...")
    combined = pl.concat(dfs, how="diagonal")
    stats["total_rows"] = len(combined)
    
    # Step 3: Apply Schema Optimizations
    print(f"  Applying schema optimizations...")
    
    price_cols = ["price", "avgPrice", "open", "high", "low", "close", "changeper"]
    price_cols += [f"bp{i}" for i in range(5)] + [f"sp{i}" for i in range(5)]
    
    qty_cols = ["qty", "volume", "bQty", "sQty", "oi", "oiHigh", "oiLow"]
    qty_cols += [f"bq{i}" for i in range(5)] + [f"sq{i}" for i in range(5)]
    qty_cols += [f"bo{i}" for i in range(5)] + [f"so{i}" for i in range(5)]
    
    transforms = []
    
    # Timestamp handling
    if "timestamp" in combined.columns:
        transforms.append(pl.col("timestamp").dt.epoch(time_unit="ns").alias("timestamp_ns"))
        transforms.append(pl.col("timestamp"))
        transforms.append(pl.col("timestamp").dt.date().alias("date"))
    
    # Convert prices to Float64
    for col in price_cols:
        if col in combined.columns:
            if col == "changeper":
                # Round changeper to 2 decimals
                transforms.append(
                    pl.col(col).cast(pl.Float64).round(2).alias(col)
                )
            else:
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
    
    # Step 4: Add expiry metadata from calendar
    if expiry_calendar is not None and "year" in combined.columns and "month" in combined.columns:
        print(f"  Adding expiry metadata from calendar...")
        
        # Create lookup key for each row
        combined = combined.with_columns([
            (pl.lit(underlying) + "_monthly_" + 
             pl.col("year").cast(pl.String) + "-" + 
             pl.col("month").cast(pl.String).str.zfill(2)).alias("lookup_key")
        ])
        
        # Join with calendar
        combined = combined.join(
            expiry_calendar.select(["lookup_key", "expiry_date"]).rename({"expiry_date": "expiry"}),
            on="lookup_key",
            how="left"
        ).drop("lookup_key")
        
        # Add expiry type flags
        combined = combined.with_columns([
            pl.lit("monthly").alias("expiry_type"),
            pl.lit(True).alias("is_monthly"),
            pl.lit(False).alias("is_weekly")
        ])
    
    # Step 5: Compute vol_delta (FIXED for volume resets)
    print(f"  Computing vol_delta (with reset handling)...")
    if "volume" in combined.columns and "strike" in combined.columns and "opt_type" in combined.columns:
        combined = combined.sort(["strike", "opt_type", "timestamp"])
        
        # Calculate delta, then set to 0 if negative (volume reset)
        combined = combined.with_columns([
            pl.when(
                (pl.col("volume") - pl.col("volume").shift(1).over(["strike", "opt_type"])) < 0
            ).then(0)
            .otherwise(
                pl.col("volume") - pl.col("volume").shift(1).over(["strike", "opt_type"])
            )
            .fill_null(0)
            .alias("vol_delta")
        ])
    
    # Step 6: Filter to sample date if specified
    if sample_date:
        print(f"  Filtering to date: {sample_date}")
        sample_date_parsed = pl.lit(sample_date).str.strptime(pl.Date, "%Y-%m-%d")
        combined = combined.filter(pl.col("date") == sample_date_parsed)
        print(f"  Rows for {sample_date}: {len(combined):,}")
        
        if combined.is_empty():
            print(f"  No data found for date {sample_date}")
            return stats
    
    # Step 7: Sort by expiry → opt_type → strike → timestamp
    # CRITICAL: Must sort by expiry FIRST to prevent mixing different expiry contracts!
    # Example: Don't mix Nov 58300 CE with Dec 58300 CE at same timestamp
    print(f"  Sorting by expiry, opt_type, strike, timestamp...")
    sort_cols = []
    if "expiry" in combined.columns:
        sort_cols.append("expiry")
    if "opt_type" in combined.columns:
        sort_cols.append("opt_type")
    if "strike" in combined.columns:
        sort_cols.append("strike")
    sort_cols.append("timestamp")
    
    combined = combined.sort(sort_cols)
    
    # Step 8: Write partitioned dataset
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
        description="PRODUCTION: Repack RAW options with expiry calendar and optimizations"
    )
    
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expiry-calendar", type=Path, default=Path("expiry_calendar.csv"))
    parser.add_argument("--sample-date", type=str, default=None)
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}")
        return 1
    
    # Load expiry calendar
    expiry_cal = None
    if args.expiry_calendar.exists():
        print(f"Loading expiry calendar from {args.expiry_calendar}...")
        expiry_cal = load_expiry_calendar(args.expiry_calendar)
        print(f"  Loaded {len(expiry_cal)} expiry entries")
    else:
        print(f"Warning: Expiry calendar not found at {args.expiry_calendar}")
    
    print("=" * 60)
    print("PRODUCTION: RAW Options Repacking")
    print("=" * 60)
    print(f"Input: {args.input_dir}")
    print(f"Output: {args.output_dir}")
    if args.sample_date:
        print(f"Sample date: {args.sample_date}")
    print()
    
    files_by_underlying = collect_files_by_underlying(args.input_dir)
    
    if not files_by_underlying:
        print("No data found!")
        return 1
    
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    start_time = time.time()
    all_stats = {}
    
    for underlying, files in files_by_underlying.items():
        stats = repack_raw_options(files, underlying, args.output_dir, expiry_cal, args.sample_date)
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
