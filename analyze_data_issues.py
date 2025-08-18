#!/usr/bin/env python3
"""
Comprehensive analysis of NFO data processing issues:
1. Files that couldn't be mapped to expiry dates
2. Market hours row retention (65% being dropped)
"""

import os
import re
import glob
import polars as pl
from datetime import date, time, datetime
from collections import defaultdict, Counter
import json

# Paths
RAW_DIR = "./data/raw/options"
PACKED_DIR = "./data/packed/options"
CAL_PATH = "./meta/expiry_calendar.csv"

# Parse filename function (from simple_pack.py)
def parse_filename(path: str):
    """Extract (symbol, opt_type, strike) from filenames."""
    base = os.path.basename(path).lower()
    if not base.endswith(".parquet"):
        return None
    stem = base[:-8]  # drop ".parquet"

    # symbol
    if stem.startswith("banknifty"):
        symbol, rest = "BANKNIFTY", stem[len("banknifty"):]
    elif stem.startswith("nifty"):
        symbol, rest = "NIFTY", stem[len("nifty"):]
    else:
        return None

    # opt type
    if rest.endswith("ce"):
        opt_type, core = "CE", rest[:-2]
    elif rest.endswith("pe"):
        opt_type, core = "PE", rest[:-2]
    else:
        return None

    # trailing digits before ce/pe
    m = re.search(r"(\d+)$", core)
    if not m:
        return None
    digits = m.group(1)

    strike = None
    if symbol == "BANKNIFTY":
        # prefer 5 digits (modern BN strikes)
        if len(digits) >= 5:
            v5 = int(digits[-5:])
            if 10000 <= v5 <= 100000:
                strike = v5
        # fallbacks
        if strike is None and len(digits) >= 6:
            v6 = int(digits[-6:])
            if 100000 <= v6 <= 999999:
                strike = v6
        if strike is None and len(digits) >= 4:
            v4 = int(digits[-4:])
            if 1000 <= v4 <= 9999:
                strike = v4
    else:  # NIFTY
        if len(digits) >= 5:
            v5 = int(digits[-5:])
            if 10000 <= v5 <= 50000:
                strike = v5
        if strike is None and len(digits) >= 4:
            v4 = int(digits[-4:])
            if 1000 <= v4 <= 9999:
                strike = v4

    if strike is None:
        return None
    return {"symbol": symbol, "opt_type": opt_type, "strike": int(strike), "filename": base}


def load_calendar(path: str) -> pl.DataFrame:
    """Load and process expiry calendar."""
    cal = pl.read_csv(path)
    cal = cal.rename({
        "Instrument": "symbol",
        "Final_Expiry": "expiry",
        "Expiry_Type": "kind",
    })
    cal = cal.select(
        pl.col("symbol").str.to_uppercase(),
        pl.col("kind").str.to_lowercase(),
        pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"),
    ).drop_nulls(["symbol","kind","expiry"])
    
    return cal.sort(["symbol","expiry"])


def analyze_failed_files():
    """Analyze files that couldn't be mapped to expiry dates."""
    print("\n" + "="*80)
    print("ANALYZING FILES THAT FAILED EXPIRY MAPPING")
    print("="*80)
    
    # Load calendar
    cal = load_calendar(CAL_PATH)
    
    # Get all raw files
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    print(f"\nTotal raw files: {len(files):,}")
    
    failed_files = []
    empty_files = []
    parse_failures = []
    date_ranges = defaultdict(lambda: {'min': None, 'max': None, 'count': 0})
    
    # Sample some files to understand date patterns
    print("\nAnalyzing sample of files...")
    for i, path in enumerate(files):
        if i % 5000 == 0:
            print(f"... {i}/{len(files)}")
        
        # Parse filename
        meta = parse_filename(path)
        if not meta:
            parse_failures.append(path)
            continue
        
        try:
            # Read file
            df = pl.read_parquet(path)
            
            # Check if empty
            if df.height == 0:
                empty_files.append(path)
                continue
            
            # Check timestamp columns
            if "timestamp" in df.columns:
                ts_col = "timestamp"
            elif "ts" in df.columns:
                ts_col = "ts"
            else:
                failed_files.append((path, "No timestamp column"))
                continue
            
            # Convert timestamp to datetime
            if df[ts_col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("timestamp"))
            else:
                df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("timestamp"))
            
            # Get date range
            dates = df.select(pl.col("timestamp").dt.date()).unique()
            if dates.height > 0:
                min_date = dates["timestamp"].min()
                max_date = dates["timestamp"].max()
                
                # Track date ranges by symbol
                symbol = meta["symbol"]
                if date_ranges[symbol]['min'] is None or min_date < date_ranges[symbol]['min']:
                    date_ranges[symbol]['min'] = min_date
                if date_ranges[symbol]['max'] is None or max_date > date_ranges[symbol]['max']:
                    date_ranges[symbol]['max'] = max_date
                date_ranges[symbol]['count'] += 1
                
                # Check if dates can be mapped
                cal_symbol = cal.filter(pl.col("symbol") == symbol)
                if cal_symbol.height == 0:
                    failed_files.append((path, f"No calendar entries for {symbol}"))
                    continue
                
                # Check if any date in file can map to an expiry
                can_map = False
                for trade_date in dates["timestamp"]:
                    future_expiries = cal_symbol.filter(pl.col("expiry") >= trade_date)
                    if future_expiries.height > 0:
                        can_map = True
                        break
                
                if not can_map:
                    failed_files.append((path, f"No future expiries for dates {min_date} to {max_date}"))
                    
        except Exception as e:
            failed_files.append((path, f"Error: {str(e)}"))
    
    # Print results
    print(f"\nFiles that couldn't parse: {len(parse_failures):,}")
    print(f"Empty files: {len(empty_files):,}")
    print(f"Files with mapping issues: {len(failed_files):,}")
    
    print("\nDate ranges by symbol:")
    for symbol, dates in date_ranges.items():
        print(f"  {symbol}: {dates['min']} to {dates['max']} ({dates['count']:,} files)")
    
    # Analyze specific problem periods
    print("\nAnalyzing September 2023 files specifically...")
    sept_2023_files = []
    for path in files[:10000]:  # Sample for efficiency
        try:
            df = pl.read_parquet(path)
            if df.height > 0 and "timestamp" in df.columns:
                # Check for September 2023 dates
                if df["timestamp"].dtype == pl.Utf8:
                    df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
                else:
                    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime, strict=False))
                
                dates = df.select(pl.col("timestamp").dt.date()).unique()
                for d in dates["timestamp"]:
                    if d and d.year == 2023 and d.month == 9:
                        sept_2023_files.append((path, d))
                        break
        except:
            pass
    
    print(f"Found {len(sept_2023_files)} files with September 2023 data")
    
    # Check calendar for September 2023
    sept_2023_cal = cal.filter(
        (pl.col("expiry").dt.year() == 2023) & 
        (pl.col("expiry").dt.month() >= 9) &
        (pl.col("expiry").dt.month() <= 10)
    )
    print("\nSeptember-October 2023 expiries in calendar:")
    print(sept_2023_cal)
    
    return failed_files, empty_files, parse_failures


def analyze_duplicate_timestamps():
    """Analyze duplicate timestamps and row retention."""
    print("\n" + "="*80)
    print("ANALYZING DUPLICATE TIMESTAMPS AND ROW RETENTION")
    print("="*80)
    
    # Sample some files to analyze timestamp patterns
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    
    # Take a representative sample
    sample_files = files[:100]  # Analyze first 100 files
    
    total_raw_rows = 0
    total_unique_timestamps = 0
    total_market_hours_rows = 0
    duplicate_patterns = []
    
    print(f"\nAnalyzing {len(sample_files)} sample files...")
    
    for i, path in enumerate(sample_files):
        if i % 10 == 0:
            print(f"... {i}/{len(sample_files)}")
        
        try:
            df = pl.read_parquet(path)
            if df.height == 0:
                continue
            
            # Get timestamp column
            if "timestamp" in df.columns:
                ts_col = "timestamp"
            elif "ts" in df.columns:
                ts_col = "ts"
            else:
                continue
            
            # Convert to datetime
            if df[ts_col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("timestamp"))
            else:
                df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("timestamp"))
            
            # Count rows
            raw_rows = df.height
            
            # Count unique timestamps
            unique_ts = df.select("timestamp").unique().height
            
            # Count market hours rows
            market_df = df.filter(
                (pl.col("timestamp").dt.time() >= time(9, 15, 0)) &
                (pl.col("timestamp").dt.time() <= time(15, 30, 0))
            )
            market_rows = market_df.height
            
            # Analyze duplicates
            dup_counts = df.group_by("timestamp").agg(pl.count().alias("count")).filter(pl.col("count") > 1)
            
            if dup_counts.height > 0:
                max_dups = dup_counts["count"].max()
                avg_dups = dup_counts["count"].mean()
                duplicate_patterns.append({
                    "file": os.path.basename(path),
                    "raw_rows": raw_rows,
                    "unique_timestamps": unique_ts,
                    "market_hours_rows": market_rows,
                    "duplicate_timestamps": dup_counts.height,
                    "max_duplicates": max_dups,
                    "avg_duplicates": avg_dups,
                    "retention_rate": unique_ts / raw_rows if raw_rows > 0 else 0,
                    "market_hours_retention": market_rows / raw_rows if raw_rows > 0 else 0
                })
            
            total_raw_rows += raw_rows
            total_unique_timestamps += unique_ts
            total_market_hours_rows += market_rows
            
        except Exception as e:
            print(f"Error processing {path}: {e}")
    
    # Print analysis
    print(f"\nOverall statistics from sample:")
    print(f"  Total raw rows: {total_raw_rows:,}")
    print(f"  Total unique timestamps: {total_unique_timestamps:,}")
    print(f"  Total market hours rows: {total_market_hours_rows:,}")
    print(f"  Overall retention rate: {total_unique_timestamps/total_raw_rows:.1%}")
    print(f"  Market hours retention: {total_market_hours_rows/total_raw_rows:.1%}")
    
    if duplicate_patterns:
        print(f"\nFiles with duplicates ({len(duplicate_patterns)}):")
        # Sort by retention rate
        duplicate_patterns.sort(key=lambda x: x['retention_rate'])
        
        print("\nWorst retention rates:")
        for pattern in duplicate_patterns[:10]:
            print(f"  {pattern['file']}: {pattern['retention_rate']:.1%} retention")
            print(f"    Raw: {pattern['raw_rows']:,}, Unique: {pattern['unique_timestamps']:,}")
            print(f"    Max dups per timestamp: {pattern['max_duplicates']}")
            print(f"    Market hours retention: {pattern['market_hours_retention']:.1%}")
    
    # Analyze a specific file in detail
    if duplicate_patterns:
        worst_file = duplicate_patterns[0]
        print(f"\nDetailed analysis of worst file: {worst_file['file']}")
        
        # Re-read the file
        worst_path = os.path.join(RAW_DIR, worst_file['file'])
        df = pl.read_parquet(worst_path)
        
        # Process timestamp
        if "timestamp" in df.columns:
            ts_col = "timestamp"
        elif "ts" in df.columns:
            ts_col = "ts"
        
        if df[ts_col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("timestamp"))
        else:
            df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("timestamp"))
        
        # Show sample of duplicates
        dup_timestamps = df.group_by("timestamp").agg(pl.count().alias("count")).filter(pl.col("count") > 1).sort("count", descending=True)
        
        print("\nTop duplicate timestamps:")
        print(dup_timestamps.head(10))
        
        # Show what data varies in duplicates
        if dup_timestamps.height > 0:
            sample_ts = dup_timestamps["timestamp"][0]
            dup_rows = df.filter(pl.col("timestamp") == sample_ts)
            print(f"\nSample duplicate rows for timestamp {sample_ts}:")
            print(dup_rows)


def check_specific_dates():
    """Check for specific problematic dates mentioned by user."""
    print("\n" + "="*80)
    print("CHECKING SPECIFIC DATE ISSUES")
    print("="*80)
    
    # Check for August 2025 data (shouldn't exist)
    print("\nChecking for August 2025 data...")
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    
    aug_2025_files = []
    for i, path in enumerate(files[:5000]):  # Sample
        try:
            df = pl.read_parquet(path)
            if df.height > 0:
                if "timestamp" in df.columns:
                    ts_col = "timestamp"
                elif "ts" in df.columns:
                    ts_col = "ts"
                else:
                    continue
                
                # Convert to datetime
                if df[ts_col].dtype == pl.Utf8:
                    df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("timestamp"))
                else:
                    df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("timestamp"))
                
                # Check for 2025 dates
                dates = df.select(pl.col("timestamp").dt.date()).unique()
                for d in dates["timestamp"]:
                    if d and d.year == 2025 and d.month == 8:
                        aug_2025_files.append((path, d))
                        break
        except:
            pass
    
    if aug_2025_files:
        print(f"WARNING: Found {len(aug_2025_files)} files with August 2025 data!")
        for path, date in aug_2025_files[:5]:
            print(f"  {os.path.basename(path)}: {date}")
    else:
        print("No August 2025 data found (as expected)")


def main():
    """Run all analyses."""
    print("NFO Data Processing Issue Analysis")
    print("==================================")
    
    # 1. Analyze files that failed expiry mapping
    failed_files, empty_files, parse_failures = analyze_failed_files()
    
    # 2. Analyze duplicate timestamps and retention
    analyze_duplicate_timestamps()
    
    # 3. Check specific date issues
    check_specific_dates()
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nTotal files analyzed: 85,280")
    print(f"Files that couldn't parse: {len(parse_failures):,}")
    print(f"Empty files: {len(empty_files):,}")
    print(f"Files with expiry mapping issues: {len(failed_files):,}")
    print(f"\nPotential issues identified:")
    print("1. Many files have duplicate timestamps in raw data")
    print("2. Duplicate removal + market hours filter = significant row reduction")
    print("3. Some files may have dates beyond calendar range")
    print("4. September 2023 needs specific investigation")


if __name__ == "__main__":
    main()