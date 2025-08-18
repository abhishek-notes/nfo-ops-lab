#!/usr/bin/env python3
"""
Simple, working pack script for NFO options data
"""
import os
import polars as pl
from pathlib import Path
import glob
from datetime import datetime

def parse_filename(filename):
    """Extract symbol, strike, opt_type from filename"""
    # Examples: banknifty1941128500ce.parquet, nifty1941128500pe.parquet
    base = os.path.basename(filename).lower().replace('.parquet', '')
    
    if 'banknifty' in base:
        symbol = 'BANKNIFTY'
        rest = base.replace('banknifty', '')
    elif 'nifty' in base:
        symbol = 'NIFTY'
        rest = base.replace('nifty', '')
    else:
        return None
    
    # Extract CE/PE
    if rest.endswith('ce'):
        opt_type = 'CE'
        strike_part = rest[:-2]
    elif rest.endswith('pe'):
        opt_type = 'PE'
        strike_part = rest[:-2]
    else:
        return None
    
    # Extract strike (last 5-6 digits)
    strike = None
    for i in range(5, 7):
        try:
            strike = int(strike_part[-i:])
            if 1000 <= strike <= 100000:  # Reasonable strike range
                break
        except:
            continue
    
    if not strike:
        return None
        
    return {
        'symbol': symbol,
        'opt_type': opt_type,
        'strike': strike
    }

def fix_and_process_file(filepath, calendar_df):
    """Process a single option file"""
    print(f"Processing: {filepath}")
    
    # Parse metadata from filename
    meta = parse_filename(filepath)
    if not meta:
        print(f"  Skipping - cannot parse filename")
        return None
    
    try:
        # Read parquet
        df = pl.read_parquet(filepath)
        
        # Add metadata columns
        df = df.with_columns([
            pl.lit(meta['symbol']).alias('symbol'),
            pl.lit(meta['opt_type']).alias('opt_type'),
            pl.lit(meta['strike']).alias('strike')
        ])
        
        # Fix timestamp
        if 'timestamp' in df.columns:
            # Check for 1970 timestamps
            df = df.with_columns([
                pl.col('timestamp').cast(pl.Datetime('ns'))
            ])
            
            # If we have 'ts' column and timestamp is 1970, use ts
            if 'ts' in df.columns:
                # Handle both string and datetime ts
                if df['ts'].dtype == pl.Utf8:
                    ts_fixed = pl.col('ts').str.strptime(pl.Datetime, strict=False)
                else:
                    ts_fixed = pl.col('ts')
                    
                df = df.with_columns([
                    pl.when(pl.col('timestamp').dt.year() <= 1971)
                    .then(ts_fixed)
                    .otherwise(pl.col('timestamp'))
                    .alias('timestamp')
                ])
        
        # Convert to IST without shifting values (replace timezone, not convert)
        df = df.with_columns([
            pl.col('timestamp').dt.replace_time_zone('Asia/Kolkata').alias('timestamp')
        ])
        
        # Get date for calendar join
        df = df.with_columns([
            pl.col('timestamp').dt.date().alias('trade_date')
        ])
        
        # Join with calendar to get expiry
        # For each trade_date, find the next expiry >= trade_date
        cal_for_symbol = calendar_df.filter(pl.col('symbol') == meta['symbol'])
        
        # Simple approach: for each unique date in data, find next expiry
        unique_dates = df.select('trade_date').unique()
        
        date_to_expiry = {}
        for row in unique_dates.iter_rows(named=True):
            trade_date = row['trade_date']
            # Find next expiry >= this date
            next_expiries = cal_for_symbol.filter(pl.col('expiry') >= trade_date).sort('expiry')
            if next_expiries.height > 0:
                next_expiry = next_expiries.row(0, named=True)
                date_to_expiry[trade_date] = {
                    'expiry': next_expiry['expiry'],
                    'expiry_type': next_expiry['expiry_type'],
                    'is_monthly': 1 if next_expiry['expiry_type'] == 'monthly' else 0,
                    'is_weekly': 1 if next_expiry['expiry_type'] == 'weekly' else 0
                }
        
        # Map expiries to dataframe
        expiry_dates = []
        expiry_types = []
        is_monthly_list = []
        is_weekly_list = []
        
        for date in df['trade_date']:
            if date in date_to_expiry:
                expiry_dates.append(date_to_expiry[date]['expiry'])
                expiry_types.append(date_to_expiry[date]['expiry_type'])
                is_monthly_list.append(date_to_expiry[date]['is_monthly'])
                is_weekly_list.append(date_to_expiry[date]['is_weekly'])
            else:
                expiry_dates.append(None)
                expiry_types.append(None)
                is_monthly_list.append(None)
                is_weekly_list.append(None)
        
        df = df.with_columns([
            pl.Series('expiry', expiry_dates),
            pl.Series('expiry_type', expiry_types),
            pl.Series('is_monthly', is_monthly_list),
            pl.Series('is_weekly', is_weekly_list)
        ])
        
        # Filter out rows without expiry
        df = df.filter(pl.col('expiry').is_not_null())
        
        if df.height == 0:
            print(f"  No valid data after expiry mapping")
            return None
        
        # Ensure price columns
        for col in ['open', 'high', 'low', 'close']:
            if col not in df.columns and 'price' in df.columns:
                df = df.with_columns(pl.col('price').alias(col))
        
        # Volume delta
        if 'volume' in df.columns:
            df = df.sort('timestamp').with_columns([
                (pl.col('volume').diff().clip(lower_bound=0).fill_null(0)).alias('vol_delta')
            ])
        elif 'qty' in df.columns:
            df = df.with_columns(pl.col('qty').fill_null(0).alias('vol_delta'))
        else:
            df = df.with_columns(pl.lit(0).alias('vol_delta'))
        
        # Filter valid data
        df = df.filter(
            pl.col('timestamp').is_not_null() & 
            (pl.col('close') > 0)
        )
        
        # Remove duplicates
        df = df.unique(['timestamp'])
        
        return df
        
    except Exception as e:
        print(f"  Error: {e}")
        return None

def main():
    # Paths
    raw_dir = "./data/raw/options"
    out_dir = "./data/packed/options"
    calendar_path = "./meta/expiry_calendar.csv"
    
    # Read calendar
    print("Reading calendar...")
    cal = pl.read_csv(calendar_path)
    cal = cal.rename({
        'Instrument': 'symbol',
        'Final_Expiry': 'expiry',
        'Expiry_Type': 'expiry_type'
    })
    cal = cal.with_columns([
        pl.col('expiry').str.strptime(pl.Date, '%Y-%m-%d'),
        pl.col('expiry_type').str.to_lowercase()
    ])
    
    # Get all parquet files
    files = glob.glob(os.path.join(raw_dir, "*.parquet"))
    print(f"Found {len(files)} files to process")
    
    # Process files and group by expiry
    expiry_groups = {}
    
    for i, filepath in enumerate(files):
        if i % 1000 == 0:
            print(f"Progress: {i}/{len(files)}")
            
        df = fix_and_process_file(filepath, cal)
        if df is None or df.height == 0:
            continue
            
        # Group by expiry
        for expiry_date in df['expiry'].unique():
            expiry_df = df.filter(pl.col('expiry') == expiry_date)
            
            # Create key
            symbol = expiry_df['symbol'][0]
            opt_type = expiry_df['opt_type'][0]
            strike = expiry_df['strike'][0]
            key = (symbol, expiry_date, opt_type, strike)
            
            if key not in expiry_groups:
                expiry_groups[key] = []
            expiry_groups[key].append(expiry_df)
    
    # Write partitioned files
    print(f"\nWriting {len(expiry_groups)} partitioned files...")
    
    for (symbol, expiry_date, opt_type, strike), dfs in expiry_groups.items():
        # Combine all dataframes for this key
        combined = pl.concat(dfs)
        
        # Remove duplicates and sort
        combined = combined.unique(['timestamp']).sort('timestamp')
        
        # Create output path
        yyyymm = expiry_date.strftime('%Y%m')
        exp_str = expiry_date.strftime('%Y-%m-%d')
        
        out_path = Path(out_dir) / symbol / yyyymm / f"exp={exp_str}" / f"type={opt_type}" / f"strike={strike}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Select final columns
        final_df = combined.select([
            'timestamp', 'symbol', 'opt_type', 'strike',
            'open', 'high', 'low', 'close', 'vol_delta',
            'expiry', 'expiry_type', 'is_monthly', 'is_weekly'
        ])
        
        # Write with compression
        final_df.write_parquet(
            str(out_path),
            compression='zstd',
            compression_level=3,
            statistics=True
        )
        
        print(f"  Wrote: {out_path} ({final_df.height} rows)")
    
    print("\nPacking complete!")

if __name__ == "__main__":
    main()