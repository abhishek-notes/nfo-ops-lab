#!/usr/bin/env python3
"""
Verify the actual failure patterns by running a mini version of simple_pack.
"""

import os
import re
import glob
import polars as pl
from datetime import date, time
import random

RAW_DIR = "./data/raw/options"
CAL_PATH = "./meta/expiry_calendar.csv"

# Copy functions from simple_pack.py
def parse_filename(path: str):
    """Extract (symbol, opt_type, strike) from filenames."""
    base = os.path.basename(path).lower()
    if not base.endswith(".parquet"):
        return None
    stem = base[:-8]

    if stem.startswith("banknifty"):
        symbol, rest = "BANKNIFTY", stem[len("banknifty"):]
    elif stem.startswith("nifty"):
        symbol, rest = "NIFTY", stem[len("nifty"):]
    else:
        return None

    if rest.endswith("ce"):
        opt_type, core = "CE", rest[:-2]
    elif rest.endswith("pe"):
        opt_type, core = "PE", rest[:-2]
    else:
        return None

    m = re.search(r"(\d+)$", core)
    if not m:
        return None
    digits = m.group(1)

    strike = None
    if symbol == "BANKNIFTY":
        if len(digits) >= 5:
            v5 = int(digits[-5:])
            if 10000 <= v5 <= 100000:
                strike = v5
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
    return {"symbol": symbol, "opt_type": opt_type, "strike": int(strike)}


def load_calendar(path: str) -> pl.DataFrame:
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


def process_file_check(path: str, cal: pl.DataFrame):
    """Process file and return failure reason if any."""
    meta = parse_filename(path)
    if not meta:
        return "parse_fail"

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        return "read_error"

    if df.height == 0:
        return "empty_file"

    # Add metadata columns
    df = df.with_columns([
        pl.lit(meta["symbol"]).alias("symbol"),
        pl.lit(meta["opt_type"]).alias("opt_type"),
        pl.lit(meta["strike"]).alias("strike"),
    ])

    # Check timestamp
    if "timestamp" not in df.columns and "ts" not in df.columns:
        return "no_timestamp"

    # Normalize timestamp
    if "timestamp" not in df.columns and "ts" in df.columns:
        df = df.with_columns(pl.col("ts").alias("timestamp"))

    # Convert to datetime
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt in (pl.Int16, pl.Int32, pl.Int64, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float64, pl.Float32):
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))

    # Filter nulls and duplicates
    df = df.filter(pl.col("timestamp").is_not_null()).unique(["timestamp"]).sort("timestamp")

    # Market hours filter
    df = df.filter(
        (pl.col("timestamp").dt.time() >= time(9, 15, 0)) &
        (pl.col("timestamp").dt.time() <= time(15, 30, 0))
    )

    if df.height == 0:
        return "no_market_hours_data"

    # Add trade_date and try to map to expiry
    df = df.with_columns(pl.col("timestamp").dt.date().alias("trade_date"))
    cal_sorted = cal.sort(["symbol","expiry"])
    df_sorted = df.sort(["symbol","trade_date"])
    
    dfj = df_sorted.join_asof(
        cal_sorted,
        left_on="trade_date",
        right_on="expiry",
        by="symbol",
        strategy="forward",
    )

    # Check if any rows mapped
    mapped_rows = dfj.filter(pl.col("expiry").is_not_null()).height
    
    if mapped_rows == 0:
        # Get the date range to understand why
        min_date = df["trade_date"].min()
        max_date = df["trade_date"].max()
        
        # Check calendar
        symbol_cal = cal.filter(pl.col("symbol") == meta["symbol"])
        if symbol_cal.height == 0:
            return f"no_calendar_for_{meta['symbol']}"
        
        last_expiry = symbol_cal["expiry"].max()
        if max_date > last_expiry:
            return f"dates_beyond_calendar_{max_date}_vs_{last_expiry}"
        
        return f"no_forward_expiry_found"
    
    return None  # Success


def main():
    print("VERIFYING ACTUAL FAILURE PATTERNS")
    print("=" * 50)
    
    # Load calendar
    cal = load_calendar(CAL_PATH)
    
    # Get sample of files
    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    sample_size = min(2000, len(files))
    sample_files = random.sample(files, sample_size)
    
    # Track failures
    failures = {
        "parse_fail": 0,
        "read_error": 0,
        "empty_file": 0,
        "no_timestamp": 0,
        "no_market_hours_data": 0,
        "dates_beyond_calendar": 0,
        "no_forward_expiry": 0,
        "other": 0,
        "success": 0
    }
    
    failure_examples = {}
    
    print(f"\nProcessing {sample_size} sample files...")
    for i, path in enumerate(sample_files):
        if i % 200 == 0:
            print(f"... {i}/{sample_size}")
        
        result = process_file_check(path, cal)
        
        if result is None:
            failures["success"] += 1
        else:
            if result.startswith("dates_beyond_calendar"):
                failures["dates_beyond_calendar"] += 1
                if "dates_beyond_calendar" not in failure_examples:
                    failure_examples["dates_beyond_calendar"] = []
                failure_examples["dates_beyond_calendar"].append((os.path.basename(path), result))
            elif result in failures:
                failures[result] += 1
            else:
                failures["other"] += 1
            
            # Store examples
            if result not in failure_examples:
                failure_examples[result] = []
            if len(failure_examples[result]) < 3:
                failure_examples[result].append(os.path.basename(path))
    
    # Calculate rates and extrapolate
    total_files = len(files)
    scale = total_files / sample_size
    
    print(f"\nResults from {sample_size} samples (extrapolated to {total_files:,} files):")
    print("\nFailure reasons:")
    
    total_failures = 0
    for reason, count in failures.items():
        if reason != "success" and count > 0:
            estimated = int(count * scale)
            total_failures += count
            print(f"  {reason}: {count} samples → ~{estimated:,} files")
            
            if reason in failure_examples and failure_examples[reason]:
                print(f"    Examples: {failure_examples[reason][:3]}")
    
    success_rate = failures["success"] / sample_size
    failure_rate = total_failures / sample_size
    
    print(f"\nSuccess rate: {success_rate:.1%}")
    print(f"Failure rate: {failure_rate:.1%}")
    print(f"\nEstimated total failures: {int(total_failures * scale):,}")
    print(f"Estimated successful files: {int(failures['success'] * scale):,}")
    
    # Show some dates beyond calendar examples
    if "dates_beyond_calendar" in failure_examples:
        print("\nFiles with dates beyond calendar:")
        for filename, details in failure_examples["dates_beyond_calendar"][:5]:
            print(f"  {filename}: {details}")


if __name__ == "__main__":
    main()