#!/usr/bin/env python3
"""
Check if the timezone conversion is causing the 65% data drop
"""
import polars as pl
import glob
import random
from datetime import time
import os

# Sample some raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
sample_files = random.sample(raw_files, min(10, len(raw_files)))

print("TIMEZONE ANALYSIS")
print("=" * 80)

for i, rf in enumerate(sample_files):
    print(f"\nFile {i+1}: {os.path.basename(rf)}")
    print("-" * 60)
    
    try:
        df = pl.read_parquet(rf)
        if df.height == 0:
            print("  Empty file")
            continue
            
        # Find timestamp column
        ts_col = None
        for col in ['timestamp', 'ts']:
            if col in df.columns:
                ts_col = col
                break
        
        if not ts_col:
            print("  No timestamp column found")
            continue
            
        # Convert to datetime if needed
        if df[ts_col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias("ts_parsed"))
        else:
            df = df.with_columns(pl.col(ts_col).cast(pl.Datetime, strict=False).alias("ts_parsed"))
        
        # Sample some timestamps
        sample_ts = df.select("ts_parsed").filter(pl.col("ts_parsed").is_not_null()).head(20)
        
        print(f"  Total rows: {df.height:,}")
        print(f"  Sample timestamps (raw):")
        for ts in sample_ts["ts_parsed"][:5]:
            print(f"    {ts}")
        
        # Now let's check what simple_pack.py does
        # It applies dt.replace_time_zone("Asia/Kolkata")
        df_ist = df.with_columns(
            pl.col("ts_parsed").dt.replace_time_zone("Asia/Kolkata").alias("ts_ist")
        )
        
        # Check time distribution
        df_with_hour = df_ist.with_columns(
            pl.col("ts_ist").dt.hour().alias("hour"),
            pl.col("ts_ist").dt.time().alias("time_only")
        )
        
        # Count by hour
        hour_dist = df_with_hour.group_by("hour").count().sort("hour")
        print("\n  Hour distribution (after IST conversion):")
        for row in hour_dist.iter_rows():
            hour, count = row
            pct = count / df.height * 100
            print(f"    Hour {hour:02d}: {count:,} rows ({pct:.1f}%)")
        
        # Check market hours
        market_hours = df_with_hour.filter(
            (pl.col("time_only") >= time(9, 15)) &
            (pl.col("time_only") <= time(15, 30))
        ).height
        
        outside_hours = df.height - market_hours
        
        print(f"\n  Market hours (9:15-15:30 IST): {market_hours:,} rows ({market_hours/df.height*100:.1f}%)")
        print(f"  Outside market hours: {outside_hours:,} rows ({outside_hours/df.height*100:.1f}%)")
        
        # Show some examples of timestamps outside market hours
        outside_df = df_with_hour.filter(
            (pl.col("time_only") < time(9, 15)) |
            (pl.col("time_only") > time(15, 30))
        )
        
        if outside_df.height > 0:
            print("\n  Sample timestamps OUTSIDE market hours:")
            for ts in outside_df.select("ts_ist").head(5)["ts_ist"]:
                print(f"    {ts}")
                
        # Also check if the original data might be in UTC
        # IST is UTC+5:30
        df_utc = df.with_columns(
            pl.col("ts_parsed").dt.replace_time_zone("UTC").alias("ts_utc")
        )
        
        # Convert UTC to IST
        df_utc_to_ist = df_utc.with_columns(
            pl.col("ts_utc").dt.convert_time_zone("Asia/Kolkata").alias("ts_utc_to_ist")
        )
        
        # Check if this gives different results
        df_utc_hours = df_utc_to_ist.with_columns(
            pl.col("ts_utc_to_ist").dt.hour().alias("hour_from_utc"),
            pl.col("ts_utc_to_ist").dt.time().alias("time_from_utc")
        )
        
        market_hours_from_utc = df_utc_hours.filter(
            (pl.col("time_from_utc") >= time(9, 15)) &
            (pl.col("time_from_utc") <= time(15, 30))
        ).height
        
        print(f"\n  If data was UTC converted to IST:")
        print(f"    Market hours would be: {market_hours_from_utc:,} rows ({market_hours_from_utc/df.height*100:.1f}%)")
        
        # Show the difference
        print("\n  Comparison of interpretations:")
        print(f"    Direct IST (replace_time_zone): {market_hours:,} rows")
        print(f"    UTC→IST (convert_time_zone): {market_hours_from_utc:,} rows")
        
    except Exception as e:
        print(f"  Error: {e}")

print("\n" + "="*80)
print("KEY QUESTION: Is the raw data timezone-naive or timezone-aware?")
print("Let's check the actual dtype...")

# Check one file in detail
for rf in raw_files[:5]:
    try:
        df = pl.read_parquet(rf)
        if df.height > 0 and "timestamp" in df.columns:
            print(f"\nFile: {os.path.basename(rf)}")
            print(f"  timestamp dtype: {df['timestamp'].dtype}")
            print(f"  First value: {df['timestamp'][0]}")
            break
    except:
        continue