#!/usr/bin/env python3
"""Simplified ATM volume backtest - focusing on getting it working first"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime, date, time, timedelta
import glob

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    # Test reading spot data
    spot_pattern = f"./data/packed/spot/{args.symbol}/**/*.parquet"
    spot_files = glob.glob(spot_pattern, recursive=True)
    print(f"Found {len(spot_files)} spot files")
    
    if spot_files:
        # Read first file as test
        df = pl.read_parquet(spot_files[0])
        print(f"\nFirst file: {spot_files[0]}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns}")
        print(f"Timestamp dtype: {df['timestamp'].dtype}")
        print("\nFirst 3 rows:")
        print(df.head(3))
        
    # Test reading options data
    options_pattern = f"./data/packed/options/{args.symbol}/*/exp=*/type=*/strike=*.parquet"
    options_files = glob.glob(options_pattern, recursive=True)
    print(f"\nFound {len(options_files)} options files")
    
    if options_files:
        # Read first file as test
        df = pl.read_parquet(options_files[0])
        print(f"\nFirst options file: {options_files[0]}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns}")
        print("\nFirst 3 rows:")
        print(df.head(3))

if __name__ == "__main__":
    main()