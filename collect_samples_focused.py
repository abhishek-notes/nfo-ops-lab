#!/usr/bin/env python3
"""
Focused NFO Ops Lab Data Sample Collector
"""

import polars as pl
import os
from pathlib import Path

def analyze_file(file_path: str) -> dict:
    """Analyze a single parquet file and return key information."""
    try:
        df = pl.read_parquet(file_path)
        return {
            'file': file_path.split('/')[-5:],  # Show last 5 path components
            'shape': df.shape,
            'columns': list(df.columns),
            'sample_data': {
                'first_row': df.head(1).to_dicts()[0] if df.height > 0 else {},
                'last_row': df.tail(1).to_dicts()[0] if df.height > 0 else {}
            },
            'success': True
        }
    except Exception as e:
        return {'file': file_path, 'error': str(e), 'success': False}

def main():
    print("NFO Ops Lab Data Sample Collection - Focused Analysis")
    print("=" * 60)
    
    # Sample specific files from the requested years
    sample_files = [
        # OPTIONS FILES from 2019, 2022, 2023, 2024
        "/workspace/data/packed/options/BANKNIFTY/201904/exp=2019-04-11/type=CE/strike=30000.parquet",
        "/workspace/data/packed/options/BANKNIFTY/202201/exp=2022-01-06/type=PE/strike=35000.parquet",
        "/workspace/data/packed/options/BANKNIFTY/202305/exp=2023-05-05/type=CE/strike=42000.parquet",
        "/workspace/data/packed/options/BANKNIFTY/202408/exp=2024-08-01/type=PE/strike=50000.parquet",
        
        # SPOT FILES from different years
        "/workspace/data/packed/spot/BANKNIFTY/201904/date=2019-04-08/ticks.parquet",
        "/workspace/data/packed/spot/NIFTY/202202/date=2022-02-01/ticks.parquet",
        "/workspace/data/packed/spot/BANKNIFTY/202305/date=2023-05-01/ticks.parquet",
        
        # FUTURES FILES from different years
        "/workspace/data/packed/futures/BANKNIFTY/201904/exp=2019-04-25/ticks.parquet",
        "/workspace/data/packed/futures/NIFTY/202203/exp=2022-03-31/ticks.parquet",
        "/workspace/data/packed/futures/BANKNIFTY/202309/exp=2023-09-28/ticks.parquet",
        "/workspace/data/packed/futures/NIFTY/202410/exp=2024-10-31/ticks.parquet",
    ]
    
    for i, file_path in enumerate(sample_files, 1):
        print(f"\n📊 SAMPLE {i}: {file_path.split('/')[-5:]}")
        print("-" * 50)
        
        if os.path.exists(file_path):
            result = analyze_file(file_path)
            if result['success']:
                print(f"✅ Shape: {result['shape']} (rows × columns)")
                print(f"📋 Columns: {result['columns']}")
                print(f"🔍 Sample Data:")
                print(f"   First Row: {result['sample_data']['first_row']}")
                print(f"   Last Row:  {result['sample_data']['last_row']}")
            else:
                print(f"❌ Error: {result['error']}")
        else:
            print(f"❌ File not found")
            # Try to find an alternative file in the same directory
            parent_dir = Path(file_path).parent
            if parent_dir.exists():
                alt_files = list(parent_dir.glob("*.parquet"))
                if alt_files:
                    alt_file = str(alt_files[0])
                    print(f"🔄 Trying alternative: {alt_file}")
                    result = analyze_file(alt_file)
                    if result['success']:
                        print(f"✅ Shape: {result['shape']} (rows × columns)")
                        print(f"📋 Columns: {result['columns']}")

if __name__ == "__main__":
    main()