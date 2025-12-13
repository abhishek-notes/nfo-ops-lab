#!/usr/bin/env python3
"""
DEBUG VERSION - Order Flow Momentum Burst
Adding logging to see why no trades are being generated
"""

import polars as pl
from pathlib import Path

# Test on single date
data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
sample_dates = sorted([d for d in data_dir.glob("*") if d.is_dir() and "1970" not in d.name])[:5]

print("="*80)
print("DEBUGGING BUYING STRATEGY")
print("="*80)

for date_dir in sample_dates:
    print(f"\nTesting date: {date_dir.name}")
    
    underlying_dir = date_dir / "BANKNIFTY"
    if not underlying_dir.exists():
        print("  No BANKNIFTY data")
        continue
    
    files = list(underlying_dir.glob("*.parquet"))
    if not files:
        print("  No files")
        continue
    
    df = pl.read_parquet(files[0], columns=[
        'timestamp', 'strike', 'distance_from_spot', 'opt_type', 
        'price', 'bp0', 'sp0', 'bq0', 'sq0', 'expiry', 'spot_price'
    ]).filter(pl.col('timestamp').dt.year() > 1970)
    
    print(f"  Total rows: {len(df):,}")
    
    # Nearest expiry
    nearest_expiry = df['expiry'].min()
    df = df.filter(pl.col('expiry') == nearest_expiry)
    print(f"  Rows with nearest expiry: {len(df):,}")
    
    # Time filter
    df_time = df.filter(
        (pl.col('timestamp').dt.hour() >= 12) & 
        ((pl.col('timestamp').dt.hour() < 14) | 
         ((pl.col('timestamp').dt.hour() == 14) & (pl.col('timestamp').dt.minute() <= 30)))
    )
    print(f"  Rows in 12PM-2:30PM: {len(df_time):,}")
    
    if len(df_time) == 0:
        continue
    
    # Check filters
    # 1. Moneyness filter: within 2% of spot
    df_money = df_time.filter(
        pl.col('distance_from_spot').abs() < (pl.col('spot_price') * 0.02)
    )
    print(f"  After moneyness filter (<2%): {len(df_money):,}")
    
    # 2. Order imbalance: bq0 > sq0 * 1.5
    df_imb = df_money.filter(
        (pl.col('sq0') > 0) & 
        (pl.col('bq0') / pl.col('sq0') > 1.5)
    )
    print(f"  After imbalance filter (>1.5x): {len(df_imb):,}")
    
    # 3. Spread check
    with_spread = df_imb.with_columns([
        ((pl.col('sp0') - pl.col('bp0')) / ((pl.col('sp0') + pl.col('bp0')) / 2) * 100).alias('spread_pct')
    ])
    df_spread = with_spread.filter(
        (pl.col('bp0') > 0) & 
        (pl.col('sp0') > 0) &
        (pl.col('spread_pct') < 0.5)
    )
    print(f"  After spread filter (<0.5%): {len(df_spread):,}")
    
    # Show sample if any pass
    if len(df_spread) > 0:
        print(f"\n  Sample rows passing all filters:")
        print(df_spread.select(['timestamp', 'strike', 'opt_type', 'price', 'bq0', 'sq0', 'spread_pct']).head(3))
    
    print()

print("="*80)
print("DEBUG COMPLETE")
print("="*80)
