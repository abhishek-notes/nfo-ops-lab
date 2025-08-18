#!/usr/bin/env python3
"""Check what's included in the options 1-minute bars"""
import polars as pl
import glob
import os

# Find some options bar files
bar_files = glob.glob("data/bars/options/*/*/exp=*/type=*/strike=*/bars_1m.parquet")
print(f"Found {len(bar_files)} options bar files")

if bar_files:
    # Sample one file
    sample_file = bar_files[0]
    print(f"\nSample file: {sample_file}")
    
    # Extract info from path
    path_parts = sample_file.split('/')
    symbol = path_parts[3]
    expiry = path_parts[5].split('=')[1]
    opt_type = path_parts[6].split('=')[1]
    strike = path_parts[7].split('=')[1]
    
    print(f"Symbol: {symbol}")
    print(f"Expiry: {expiry}")
    print(f"Type: {opt_type}")
    print(f"Strike: {strike}")
    
    # Read the file
    df = pl.read_parquet(sample_file)
    print(f"\nShape: {df.shape}")
    print(f"Columns: {df.columns}")
    
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nLast 5 rows:")
    print(df.tail())
    
    # Check time range
    print(f"\nTime range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Check data quality
    print(f"\nData quality checks:")
    print(f"- Null values: {df.null_count().sum(axis=1)[0]}")
    print(f"- Minutes with data: {df.height}")
    print(f"- Average volume per minute: {df['volume'].mean():.0f}")
    print(f"- Price range: {df['low'].min():.2f} to {df['high'].max():.2f}")