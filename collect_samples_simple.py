#!/usr/bin/env python3
"""
Simple NFO Ops Lab Data Sample Collector - Works around timezone issues
"""

import polars as pl
import os
from pathlib import Path

def analyze_file_safe(file_path: str) -> dict:
    """Safely analyze a parquet file without converting to dicts."""
    try:
        df = pl.read_parquet(file_path)
        
        # Get basic info without triggering timezone conversion
        shape = df.shape
        columns = list(df.columns)
        dtypes = [str(dtype) for dtype in df.dtypes]
        
        # Get first and last few rows as strings to avoid timezone issues
        first_5_str = str(df.head(5))
        last_5_str = str(df.tail(5))
        
        return {
            'file': file_path,
            'shape': shape,
            'columns': columns,
            'dtypes': dtypes,
            'first_5_preview': first_5_str[:500] + "..." if len(first_5_str) > 500 else first_5_str,
            'last_5_preview': last_5_str[:500] + "..." if len(last_5_str) > 500 else last_5_str,
            'success': True
        }
    except Exception as e:
        return {'file': file_path, 'error': str(e), 'success': False}

def find_existing_files():
    """Find existing sample files from the required years."""
    base_dirs = {
        'options': '/workspace/data/packed/options',
        'spot': '/workspace/data/packed/spot',
        'futures': '/workspace/data/packed/futures'
    }
    
    found_files = []
    
    # OPTIONS - Look for files from 2019, 2022, 2023, 2024
    opt_base = Path(base_dirs['options'])
    for symbol in ['BANKNIFTY', 'NIFTY']:
        for year_prefix in ['2019', '2022', '2023', '2024']:
            year_dirs = list(opt_base.glob(f"{symbol}/{year_prefix}*"))
            if year_dirs:
                for year_dir in year_dirs[:1]:  # Take first match per year
                    exp_dirs = list(year_dir.glob("exp=*"))
                    if exp_dirs:
                        type_dirs = list(exp_dirs[0].glob("type=*"))
                        if type_dirs:
                            strike_files = list(type_dirs[0].glob("strike=*.parquet"))
                            if strike_files:
                                found_files.append(('OPTIONS', str(strike_files[0])))
                                break
    
    # SPOT - Look for files from different years
    spot_base = Path(base_dirs['spot'])
    for symbol in ['BANKNIFTY', 'NIFTY']:
        for year_prefix in ['2019', '2022', '2023', '2024']:
            year_dirs = list(spot_base.glob(f"{symbol}/{year_prefix}*"))
            if year_dirs:
                for year_dir in year_dirs[:1]:  # Take first match per year
                    date_dirs = list(year_dir.glob("date=*"))
                    if date_dirs:
                        tick_files = list(date_dirs[0].glob("ticks.parquet"))
                        if tick_files:
                            found_files.append(('SPOT', str(tick_files[0])))
                            break
    
    # FUTURES - Look for files from different years
    fut_base = Path(base_dirs['futures'])
    for symbol in ['BANKNIFTY', 'NIFTY']:
        for year_prefix in ['2019', '2022', '2023', '2024']:
            year_dirs = list(fut_base.glob(f"{symbol}/{year_prefix}*"))
            if year_dirs:
                for year_dir in year_dirs[:1]:  # Take first match per year
                    exp_dirs = list(year_dir.glob("exp=*"))
                    if exp_dirs:
                        tick_files = list(exp_dirs[0].glob("ticks.parquet"))
                        if tick_files:
                            found_files.append(('FUTURES', str(tick_files[0])))
                            break
    
    return found_files

def main():
    print("NFO Ops Lab Data Sample Collection")
    print("=" * 60)
    
    # Find existing files
    found_files = find_existing_files()
    
    print(f"\nFound {len(found_files)} sample files:")
    
    # Group by category
    by_category = {}
    for category, file_path in found_files:
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(file_path)
    
    # Analyze each category
    for category, files in by_category.items():
        print(f"\n" + "="*60)
        print(f"📊 {category} DATA SAMPLES")
        print("="*60)
        
        for i, file_path in enumerate(files[:4], 1):  # Limit to 4 per category
            print(f"\n🔍 SAMPLE {i}: {category}")
            print(f"📁 File: {file_path}")
            print("-" * 50)
            
            result = analyze_file_safe(file_path)
            if result['success']:
                print(f"✅ Shape: {result['shape']} (rows × columns)")
                print(f"📋 Columns ({len(result['columns'])}): {result['columns']}")
                print(f"🏷️  Data Types: {list(zip(result['columns'], result['dtypes']))}")
                print(f"\n📄 First 5 rows preview:")
                print(result['first_5_preview'])
                print(f"\n📄 Last 5 rows preview:")
                print(result['last_5_preview'])
            else:
                print(f"❌ Error reading file: {result['error']}")
    
    print(f"\n✅ Analysis Complete!")
    print(f"📊 Total files analyzed: {len(found_files)}")
    for category, files in by_category.items():
        print(f"   - {category}: {len(files)} files")

if __name__ == "__main__":
    main()