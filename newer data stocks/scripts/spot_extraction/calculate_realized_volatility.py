#!/usr/bin/env python3
"""
Calculate Realized Volatility (VIX Proxy) from Spot Prices
Saves daily realized vol for use in strategies
"""

from pathlib import Path
import polars as pl
import numpy as np
from datetime import date, time
import csv


def calculate_realized_vol_for_date(df: pl.DataFrame, window_days: int = 5) -> float:
    """
    Calculate realized volatility as proxy for VIX
    Uses rolling window of spot price returns
    """
    if len(df) < 10:  # Need minimum data points
        return 20.0  # Default moderate volatility
    
    # Get spot prices - sample every 60 seconds to avoid too many duplicates
    df_sorted = df.sort('timestamp')
    spot_prices = df_sorted.select('spot_price').unique().to_numpy().flatten()
    
    if len(spot_prices) < 10:
        return 20.0
    
    # Remove any NaN or zero values
    spot_prices = spot_prices[~np.isnan(spot_prices)]
    spot_prices = spot_prices[spot_prices > 0]
    
    if len(spot_prices) < 10:
        return 20.0
    
    # Calculate log returns
    log_returns = np.diff(np.log(spot_prices))
    
    # Remove any inf or nan from log returns
    log_returns = log_returns[np.isfinite(log_returns)]
    
    if len(log_returns) < 5:
        return 20.0
    
    # Calculate realized volatility (annualized)
    # For intraday second-by-second data: sqrt(252 * 6.5 * 3600) for seconds per trading day
    std_dev = np.std(log_returns)
    
    if std_dev == 0 or np.isnan(std_dev):
        return 20.0
    
    # Annualize: sqrt(252 trading days * 78 5-min periods)
    realized_vol = std_dev * np.sqrt(252 * 78)
    
    # Clamp to reasonable range (actual VIX ranges 10-80)
    realized_vol = float(np.clip(realized_vol, 10.0, 80.0))
    
    return realized_vol


def main():
    print("="*80)
    print("CALCULATING REALIZED VOLATILITY (VIX PROXY)")
    print("="*80)
    
    data_dir = Path("../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    output_dir = Path("../data/realized_volatility_cache")
    output_dir.mkdir(exist_ok=True)
    
    # Process each underlying
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\nProcessing {underlying}...")
        
        volatility_data = []
        
        date_dirs = sorted(data_dir.glob("*"))
        processed = 0
        
        for date_dir in date_dirs:
            if not date_dir.is_dir() or "1970" in date_dir.name:
                continue
            
            underlying_dir = date_dir / underlying
            if not underlying_dir.exists():
                continue
            
            files = list(underlying_dir.glob("*.parquet"))
            if not files:
                continue
            
            # Load nearest expiry data
            df = pl.read_parquet(files[0], columns=[
                'timestamp', 'spot_price', 'expiry'
            ]).filter(pl.col('timestamp').dt.year() > 1970)
            
            if df.is_empty():
                continue
            
            # Use nearest expiry only
            nearest_expiry = df['expiry'].min()
            df = df.filter(pl.col('expiry') == nearest_expiry)
            
            if df.is_empty():
                continue
            
            # Calculate realized vol for this date
            date_str = date_dir.name
            realized_vol = calculate_realized_vol_for_date(df)
            
            volatility_data.append({
                'date': date_str,
                'underlying': underlying,
                'realized_vol': realized_vol
            })
            
            processed += 1
            if processed % 10 == 0:
                print(f"  Processed {processed} dates, avg vol: {np.mean([v['realized_vol'] for v in volatility_data[-10:]]):.2f}")
        
        # Save to CSV
        output_file = output_dir / f"{underlying}_realized_volatility.csv"
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['date', 'underlying', 'realized_vol'])
            writer.writeheader()
            writer.writerows(volatility_data)
        
        print(f"\n✓ Saved {len(volatility_data)} dates to {output_file}")
        print(f"  Avg Vol: {np.mean([v['realized_vol'] for v in volatility_data]):.2f}")
        print(f"  Min Vol: {np.min([v['realized_vol'] for v in volatility_data]):.2f}")
        print(f"  Max Vol: {np.max([v['realized_vol'] for v in volatility_data]):.2f}")
    
    print("\n" + "="*80)
    print("COMPLETE - Realized volatility cached for future use")
    print("="*80)


if __name__ == "__main__":
    main()
