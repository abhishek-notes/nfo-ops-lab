#!/usr/bin/env python3
"""
PRODUCTION v3: RAW Options to Date-Partitioned Repacking (SPOT-ENRICHED)

New features in v3:
- Spot price enrichment (NIFTY/BANKNIFTY)
- Additional computed columns: distance_from_spot, moneyness_pct
- Intrinsic/extrinsic value, mid_price
- Preserves sort order on disk (from v2)
- Row group statistics for efficient filtering (from v2)

Performance target: 50-100M rows/sec benchmarking (maintained from v2)
"""

from __future__ import annotations

import argparse
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl


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


def load_spot_data(spot_file: Path, underlying: str) -> pl.DataFrame:
    """
    Load spot data for a specific underlying.
    
    Args:
        spot_file: Path to consolidated spot parquet (e.g., NIFTY_all.parquet)
        underlying: 'NIFTY' or 'BANKNIFTY'
    
    Returns:
        DataFrame with columns: timestamp, price (renamed to spot_price)
    """
    print(f"  Loading spot data for {underlying} from {spot_file}...")
    
    spot = pl.read_parquet(spot_file).select([
        'timestamp',
        'price'
    ]).rename({'price': 'spot_price'})
    
    print(f"    Loaded {len(spot):,} spot ticks")
    
    # Resample to 1-second intervals with forward fill
    # This ensures every options tick can find a nearby spot price
    spot_resampled = spot.group_by_dynamic(
        'timestamp',
        every='1s',
        start_by='datapoint',
        closed='left',
        label='left'
    ).agg([
        pl.col('spot_price').last().alias('spot_price')
    ])
    
    # Forward fill missing values (up to 10 seconds)
    spot_resampled = spot_resampled.with_columns([
        pl.col('spot_price').fill_null(strategy='forward', limit=10)
    ])
    
    # Remove timezone to match options data (which is timezone-naive)
    spot_resampled = spot_resampled.with_columns([
        pl.col('timestamp').dt.replace_time_zone(None)
    ])
    
    # Sort by timestamp (required for join_asof)
    spot_resampled = spot_resampled.sort('timestamp')
    
    print(f"    Resampled to {len(spot_resampled):,} 1-second intervals")
    
    return spot_resampled


def enrich_with_spot(
    options: pl.DataFrame,
    spot: pl.DataFrame
) -> pl.DataFrame:
    """
    Join spot prices with options data and compute derived fields.
    
    Adds columns:
    - spot_price (Float32): Spot/index price at timestamp
    - distance_from_spot (Float32): strike - spot (for ATM detection)
    - moneyness_pct (Float32): (strike - spot) / spot * 100
    - intrinsic_value (Float32): max(0, (spot - strike) * sign)
    - time_value (Float32): price - intrinsic_value
    - mid_price (Float32): (bp0 + sp0) / 2
    
    Args:
        options: Options DataFrame with timestamp column
        spot: Spot DataFrame with timestamp and spot_price columns
    
    Returns:
        Enriched options DataFrame
    """
    print(f"  Enriching options with spot data...")
    
    initial_rows = len(options)
    
    # Sort options by timestamp (required for join_asof)
    # Note: options may be sorted by other keys from vol_delta computation
    options = options.sort('timestamp')
    
    # Join spot using asof (nearest timestamp within 1 second)
    enriched = options.join_asof(
        spot,
        on='timestamp',
        strategy='nearest',
        tolerance='1s'
    )
    
    # Check join success rate
    joined_rows = enriched.filter(pl.col('spot_price').is_not_null()).height
    success_rate = joined_rows / initial_rows * 100 if initial_rows > 0 else 0
    print(f"    Spot join success: {success_rate:.1f}% ({joined_rows:,}/{initial_rows:,} rows)")
    
    if success_rate < 95:
        print(f"    WARNING: Low join success rate! Check timestamp alignment.")
    
    # Compute derived fields
    enriched = enriched.with_columns([
        # ===== TIER 1: MUST HAVE =====
        
        # Distance from spot (for ATM detection via argmin)
        (pl.col('strike').cast(pl.Float64) - pl.col('spot_price'))
            .alias('distance_from_spot')
            .cast(pl.Float32),
        
        # Moneyness as percentage (positive = OTM for CE, ITM for PE)
        (((pl.col('strike').cast(pl.Float64) - pl.col('spot_price')) / 
          pl.col('spot_price')) * 100.0)
            .alias('moneyness_pct')
            .cast(pl.Float32),
        
        # ===== TIER 2: NICE TO HAVE =====
        
        # Intrinsic value
        # CE: max(0, spot - strike)
        # PE: max(0, strike - spot)
        pl.when(pl.col('opt_type') == 'CE')
            .then(pl.max_horizontal([
                pl.lit(0.0), 
                pl.col('spot_price').cast(pl.Float64) - pl.col('strike').cast(pl.Float64)
            ]))
            .otherwise(pl.max_horizontal([
                pl.lit(0.0), 
                pl.col('strike').cast(pl.Float64) - pl.col('spot_price').cast(pl.Float64)
            ]))
            .alias('intrinsic_value')
            .cast(pl.Float32),
        
        # Mid price (average of best bid and ask)
        pl.when((pl.col('bp0').is_not_null()) & (pl.col('sp0').is_not_null()))
            .then((pl.col('bp0').cast(pl.Float64) + pl.col('sp0').cast(pl.Float64)) / 2.0)
            .otherwise(pl.col('price').cast(pl.Float64))
            .alias('mid_price')
            .cast(pl.Float32),
        
        # Cast spot_price to Float32 (save space)
        pl.col('spot_price').cast(pl.Float32)
    ])
    
    # Time value (extrinsic) = price - intrinsic
    enriched = enriched.with_columns([
        (pl.col('price').cast(pl.Float64) - pl.col('intrinsic_value').cast(pl.Float64))
            .alias('time_value')
            .cast(pl.Float32)
    ])
    
    print(f"    Added 6 spot-derived columns:")
    print(f"      - spot_price, distance_from_spot, moneyness_pct")
    print(f"      - intrinsic_value, time_value, mid_price")
    
    return enriched


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
    spot_data: Optional[pl.DataFrame] = None,
    sample_date: str = None
) -> Dict:
    """
    Repack raw option files with full optimizations + SPOT ENRICHMENT.
    
    New in v3: Spot price enrichment with 6 additional columns
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
    
    # Step 3: Apply Schema Optimizations (same as v2)
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
                transforms.append(pl.col(col).cast(pl.Float64).round(2).alias(col))
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
    
    # Step 4: Add expiry metadata from calendar (same as v2)
    if expiry_calendar is not None and "year" in combined.columns and "month" in combined.columns:
        print(f"  Adding expiry metadata from calendar...")
        
        combined = combined.with_columns([
            (pl.lit(underlying) + "_monthly_" + 
             pl.col("year").cast(pl.String) + "-" + 
             pl.col("month").cast(pl.String).str.zfill(2)).alias("lookup_key")
        ])
        
        combined = combined.join(
            expiry_calendar.select(["lookup_key", "expiry_date"]).rename({"expiry_date": "expiry"}),
            on="lookup_key",
            how="left"
        ).drop("lookup_key")
        
        combined = combined.with_columns([
            pl.lit("monthly").alias("expiry_type"),
            pl.lit(True).alias("is_monthly"),
            pl.lit(False).alias("is_weekly")
        ])
    
    # Step 5: Compute vol_delta (same as v2)
    print(f"  Computing vol_delta (with reset handling)...")
    if "volume" in combined.columns and "strike" in combined.columns and "opt_type" in combined.columns:
        combined = combined.sort(["strike", "opt_type", "timestamp"])
        
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
    
    # Step 6: SPOT ENRICHMENT (NEW in v3!)
    if spot_data is not None:
        combined = enrich_with_spot(combined, spot_data)
    else:
        print(f"  WARNING: No spot data provided, skipping enrichment!")
        # Add empty columns so schema is consistent
        combined = combined.with_columns([
            pl.lit(None).cast(pl.Float32).alias('spot_price'),
            pl.lit(None).cast(pl.Float32).alias('distance_from_spot'),
            pl.lit(None).cast(pl.Float32).alias('moneyness_pct'),
            pl.lit(None).cast(pl.Float32).alias('intrinsic_value'),
            pl.lit(None).cast(pl.Float32).alias('time_value'),
            pl.lit(None).cast(pl.Float32).alias('mid_price'),
        ])
    
    # Step 7: Filter to sample date if specified
    if sample_date:
        print(f"  Filtering to date: {sample_date}...")
        sample_date_parsed = pl.lit(sample_date).str.strptime(pl.Date, "%Y-%m-%d")
        combined = combined.filter(pl.col("date") == sample_date_parsed)
        print(f"  Rows for {sample_date}: {len(combined):,}")
        
        if combined.is_empty():
            print(f"  No data found for date {sample_date}")
            return stats
    
    # Step 8: Sort by expiry → opt_type → strike → timestamp (CRITICAL!)
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
    
    # Step 9: Write SORTED partitioned data (preserves sort order from v2)
    print(f"  Writing SORTED partitioned data...")
    
    unique_dates = combined["date"].unique().sort()
    stats["dates_written"] = set(str(d) for d in unique_dates.to_list())
    
    # Write per date/underlying to preserve sort order
    for date_val in unique_dates:
        date_df = combined.filter(pl.col("date") == date_val)
        
        # Create output path
        date_str = str(date_val)
        output_path = output_dir / date_str / underlying
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Write with proper settings for sorted data
        file_path = output_path / f"part-{underlying.lower()}-0.parquet"
        
        try:
            date_df.drop("date", "underlying").write_parquet(
                file_path,
                compression="zstd",
                statistics=True,  # Enable row group statistics
                row_group_size=100_000,  # Optimize for filtering
                use_pyarrow=True
            )
        except Exception as e:
            stats["errors"].append(f"Error writing {file_path}: {e}")
    
    print(f"  ✓ Wrote {len(combined):,} rows across {len(stats['dates_written'])} dates")
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="PRODUCTION v3: Repack RAW options with SPOT ENRICHMENT"
    )
    
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expiry-calendar", type=Path, default=Path("expiry_calendar.csv"))
    parser.add_argument("--spot-dir", type=Path, default=Path("../data/spot_data"))
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
    
    print("="*70)
    print("PRODUCTION v3: RAW Options Repacking (SPOT-ENRICHED)")
    print("="*70)
    print(f"Input: {args.input_dir}")
    print(f"Output: {args.output_dir}")
    print(f"Spot data: {args.spot_dir}")
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
        # Load spot data for this underlying
        spot_file = args.spot_dir / f"{underlying}_all.parquet"
        
        spot_data = None
        if spot_file.exists():
            spot_data = load_spot_data(spot_file, underlying)
        else:
            print(f"\nWARNING: Spot data not found at {spot_file}")
            print(f"Will pack without spot enrichment (columns will be null)")
        
        # Repack with spot enrichment
        stats = repack_raw_options(
            files, underlying, args.output_dir, 
            expiry_cal, spot_data, args.sample_date
        )
        all_stats[underlying] = stats
    
    elapsed = time.time() - start_time
    
    # Print summary
    print("\n" + "="*70)
    print("REPACKING COMPLETE")
    print("="*70)
    
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
    print("\n✓ Data written with SORTED + SPOT-ENRICHED parquet files")
    print("  New columns: spot_price, distance_from_spot, moneyness_pct,")
    print("               intrinsic_value, time_value, mid_price")
    
    return 0


if __name__ == "__main__":
    exit(main())
