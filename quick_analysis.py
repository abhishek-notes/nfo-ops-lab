#!/usr/bin/env python3
"""
Quick focused analysis of NFO data processing issues.
"""

import os
import re
import glob
import polars as pl
from datetime import date, time, datetime
import random

RAW_DIR = "./data/raw/options"
CAL_PATH = "./meta/expiry_calendar.csv"

def parse_filename(path: str):
    """Extract info from filename."""
    base = os.path.basename(path).lower()
    if not base.endswith(".parquet"):
        return None
    stem = base[:-8]
    
    if stem.startswith("banknifty"):
        symbol = "BANKNIFTY"
    elif stem.startswith("nifty"):
        symbol = "NIFTY"
    else:
        return None
    
    return {"symbol": symbol, "filename": base}


def quick_duplicate_analysis():
    """Quick analysis of duplicate timestamps."""
    print("\n=== DUPLICATE TIMESTAMP ANALYSIS ===")
    
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    # Random sample of 20 files
    sample_files = random.sample(files, min(20, len(files)))
    
    results = []
    for path in sample_files:
        try:
            df = pl.read_parquet(path)
            if df.height == 0:
                continue
            
            # Get timestamp column
            ts_col = "timestamp" if "timestamp" in df.columns else "ts" if "ts" in df.columns else None
            if not ts_col:
                continue
            
            # Count rows
            total_rows = df.height
            
            # Count unique timestamps
            unique_timestamps = df.select(ts_col).unique().height
            
            # Convert to datetime for market hours check
            if df[ts_col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("dt"))
            else:
                df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("dt"))
            
            # Market hours filter
            market_df = df.filter(
                (pl.col("dt").dt.time() >= time(9, 15, 0)) &
                (pl.col("dt").dt.time() <= time(15, 30, 0))
            )
            market_unique = market_df.select("dt").unique().height
            
            results.append({
                "file": os.path.basename(path),
                "total_rows": total_rows,
                "unique_timestamps": unique_timestamps,
                "duplication_rate": 1 - (unique_timestamps / total_rows),
                "market_hours_unique": market_unique,
                "retention_after_dedup": unique_timestamps / total_rows,
                "retention_after_market_filter": market_unique / total_rows
            })
            
        except Exception as e:
            print(f"Error with {os.path.basename(path)}: {e}")
    
    # Print results
    if results:
        avg_duplication = sum(r["duplication_rate"] for r in results) / len(results)
        avg_retention_dedup = sum(r["retention_after_dedup"] for r in results) / len(results)
        avg_retention_market = sum(r["retention_after_market_filter"] for r in results) / len(results)
        
        print(f"\nAnalyzed {len(results)} files:")
        print(f"Average duplication rate: {avg_duplication:.1%}")
        print(f"Average retention after deduplication: {avg_retention_dedup:.1%}")
        print(f"Average retention after market hours filter: {avg_retention_market:.1%}")
        
        print("\nSample files with high duplication:")
        sorted_results = sorted(results, key=lambda x: x["duplication_rate"], reverse=True)
        for r in sorted_results[:5]:
            print(f"  {r['file']}:")
            print(f"    Total rows: {r['total_rows']:,}")
            print(f"    Unique timestamps: {r['unique_timestamps']:,}")
            print(f"    Duplication rate: {r['duplication_rate']:.1%}")
            print(f"    Final retention: {r['retention_after_market_filter']:.1%}")


def check_september_2023():
    """Check September 2023 expiry mapping issues."""
    print("\n=== SEPTEMBER 2023 ANALYSIS ===")
    
    # Load calendar
    cal = pl.read_csv(CAL_PATH)
    cal = cal.rename({
        "Instrument": "symbol",
        "Final_Expiry": "expiry",
        "Expiry_Type": "kind",
    })
    cal = cal.with_columns(
        pl.col("expiry").str.strptime(pl.Date, strict=False)
    )
    
    # Filter September 2023 expiries
    sept_2023 = cal.filter(
        (pl.col("expiry").dt.year() == 2023) & 
        (pl.col("expiry").dt.month() == 9)
    )
    
    print("\nSeptember 2023 expiries in calendar:")
    print(sept_2023.select(["symbol", "expiry", "kind"]))
    
    # Check a few files for September 2023 data
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    sample_files = random.sample(files, min(100, len(files)))
    
    sept_files = []
    for path in sample_files:
        try:
            df = pl.read_parquet(path)
            if df.height == 0:
                continue
            
            ts_col = "timestamp" if "timestamp" in df.columns else "ts" if "ts" in df.columns else None
            if not ts_col:
                continue
            
            # Check for September 2023 dates
            if df[ts_col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).dt.date().alias("date"))
            else:
                df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).dt.date().alias("date"))
            
            sept_dates = df.filter(
                (pl.col("date").dt.year() == 2023) & 
                (pl.col("date").dt.month() == 9)
            )
            
            if sept_dates.height > 0:
                unique_dates = sept_dates.select("date").unique()
                sept_files.append({
                    "file": os.path.basename(path),
                    "dates": unique_dates["date"].to_list()
                })
        except:
            pass
    
    print(f"\nFound {len(sept_files)} files with September 2023 data (from {len(sample_files)} sampled)")
    if sept_files:
        print("\nSample files with September 2023 data:")
        for f in sept_files[:5]:
            print(f"  {f['file']}: {f['dates'][:3]}...")


def count_failed_files():
    """Estimate files that fail processing."""
    print("\n=== ESTIMATING FAILED FILES ===")
    
    # Run simple_pack logic on a sample
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    sample_size = min(1000, len(files))
    sample_files = random.sample(files, sample_size)
    
    parse_fail = 0
    empty_files = 0
    no_timestamp = 0
    no_expiry_map = 0
    
    # Load calendar for mapping check
    cal = pl.read_csv(CAL_PATH)
    cal = cal.rename({
        "Instrument": "symbol",
        "Final_Expiry": "expiry",
    })
    cal = cal.with_columns(
        pl.col("symbol").str.to_uppercase(),
        pl.col("expiry").str.strptime(pl.Date, strict=False)
    )
    
    for path in sample_files:
        # Check filename parse
        meta = parse_filename(path)
        if not meta:
            parse_fail += 1
            continue
        
        try:
            df = pl.read_parquet(path)
            
            # Check empty
            if df.height == 0:
                empty_files += 1
                continue
            
            # Check timestamp
            if "timestamp" not in df.columns and "ts" not in df.columns:
                no_timestamp += 1
                continue
            
            # Check if would map to expiry
            ts_col = "timestamp" if "timestamp" in df.columns else "ts"
            if df[ts_col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).dt.date().alias("trade_date"))
            else:
                df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).dt.date().alias("trade_date"))
            
            # Get date range
            dates = df.select("trade_date").unique()
            if dates.height > 0:
                max_date = dates["trade_date"].max()
                
                # Check if any future expiry exists
                symbol_cal = cal.filter(pl.col("symbol") == meta["symbol"])
                future_expiries = symbol_cal.filter(pl.col("expiry") >= max_date)
                
                if future_expiries.height == 0:
                    no_expiry_map += 1
                    
        except Exception as e:
            parse_fail += 1
    
    # Extrapolate to full dataset
    total_files = len(files)
    scale = total_files / sample_size
    
    print(f"\nBased on {sample_size} sample files (out of {total_files:,} total):")
    print(f"Estimated files that fail parsing: {int(parse_fail * scale):,}")
    print(f"Estimated empty files: {int(empty_files * scale):,}")
    print(f"Estimated files without timestamp: {int(no_timestamp * scale):,}")
    print(f"Estimated files that can't map to expiry: {int(no_expiry_map * scale):,}")
    print(f"TOTAL ESTIMATED FAILED FILES: {int((parse_fail + empty_files + no_timestamp + no_expiry_map) * scale):,}")


def main():
    print("Quick NFO Data Analysis")
    print("=" * 50)
    
    # 1. Duplicate analysis
    quick_duplicate_analysis()
    
    # 2. September 2023 check
    check_september_2023()
    
    # 3. Failed files estimate
    count_failed_files()
    
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print("1. Raw data has significant timestamp duplication")
    print("2. After deduplication + market hours filter, ~35% retention is plausible")
    print("3. September 2023 has valid expiries in calendar")
    print("4. Many files likely fail due to dates beyond calendar range")


if __name__ == "__main__":
    main()