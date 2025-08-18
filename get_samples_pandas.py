#!/usr/bin/env python3
"""
Get sample data using pandas to avoid timezone issues
"""

import pandas as pd
import os
from pathlib import Path

def get_sample_data(file_path, sample_type):
    """Get sample data from a parquet file using pandas."""
    try:
        # Read with pandas which handles timezones better
        df = pd.read_parquet(file_path)
        
        # Get basic info
        shape = df.shape
        columns = list(df.columns)
        dtypes = df.dtypes.to_dict()
        
        # Get first and last 5 rows
        first_5 = df.head(5)
        last_5 = df.tail(5)
        
        # Convert timestamps to strings to avoid display issues
        for col in df.columns:
            if 'timestamp' in col.lower() or df[col].dtype.name.startswith('datetime'):
                first_5[col] = first_5[col].astype(str)
                last_5[col] = last_5[col].astype(str)
        
        return {
            'success': True,
            'file_path': file_path,
            'sample_type': sample_type,
            'shape': shape,
            'columns': columns,
            'dtypes': {k: str(v) for k, v in dtypes.items()},
            'first_5': first_5.to_dict('records'),
            'last_5': last_5.to_dict('records')
        }
    except Exception as e:
        return {
            'success': False,
            'file_path': file_path,
            'sample_type': sample_type,
            'error': str(e)
        }

def main():
    print("NFO Ops Lab Data Samples - Using Pandas")
    print("=" * 60)
    
    # Define sample files for different categories and years
    sample_files = [
        # OPTIONS data from different years
        ("/workspace/data/packed/options/BANKNIFTY/201907/exp=2019-07-11/type=CE/strike=29000.parquet", "2019 BANKNIFTY OPTIONS CE"),
        ("/workspace/data/packed/options/BANKNIFTY/202207/exp=2022-07-21/type=CE/strike=36500.parquet", "2022 BANKNIFTY OPTIONS CE"),
        ("/workspace/data/packed/options/BANKNIFTY/202311/exp=2023-11-15/type=CE/strike=41700.parquet", "2023 BANKNIFTY OPTIONS CE"),
        ("/workspace/data/packed/options/BANKNIFTY/202402/exp=2024-02-28/type=CE/strike=47900.parquet", "2024 BANKNIFTY OPTIONS CE"),
        
        # SPOT data from different years
        ("/workspace/data/packed/spot/BANKNIFTY/201907/date=2019-07-25/ticks.parquet", "2019 BANKNIFTY SPOT"),
        ("/workspace/data/packed/spot/BANKNIFTY/202207/date=2022-07-25/ticks.parquet", "2022 BANKNIFTY SPOT"),
        ("/workspace/data/packed/spot/BANKNIFTY/202311/date=2023-11-16/ticks.parquet", "2023 BANKNIFTY SPOT"),
        
        # FUTURES data from different years
        ("/workspace/data/packed/futures/BANKNIFTY/201912/exp=2019-12-26/ticks.parquet", "2019 BANKNIFTY FUTURES"),
        ("/workspace/data/packed/futures/BANKNIFTY/202207/exp=2022-07-28/ticks.parquet", "2022 BANKNIFTY FUTURES"),
        ("/workspace/data/packed/futures/BANKNIFTY/202311/exp=2023-11-30/ticks.parquet", "2023 BANKNIFTY FUTURES"),
    ]
    
    results = []
    
    for file_path, sample_type in sample_files:
        print(f"\n📊 Analyzing: {sample_type}")
        print(f"📁 File: {file_path}")
        print("-" * 60)
        
        if os.path.exists(file_path):
            result = get_sample_data(file_path, sample_type)
            results.append(result)
            
            if result['success']:
                print(f"✅ SUCCESS")
                print(f"   Shape: {result['shape']} (rows × columns)")
                print(f"   Columns: {result['columns']}")
                print(f"   Data Types: {result['dtypes']}")
                
                print(f"\n   📄 First 5 rows:")
                for i, row in enumerate(result['first_5'], 1):
                    print(f"     Row {i}: {row}")
                
                print(f"\n   📄 Last 5 rows:")
                for i, row in enumerate(result['last_5'], 1):
                    print(f"     Row {i}: {row}")
                    
            else:
                print(f"❌ ERROR: {result['error']}")
        else:
            print("❌ File not found")
            results.append({
                'success': False,
                'file_path': file_path,
                'sample_type': sample_type,
                'error': 'File not found'
            })
    
    # Summary
    print(f"\n{'='*80}")
    print("📊 SUMMARY")
    print("="*80)
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"✅ Successfully analyzed: {len(successful)} files")
    print(f"❌ Failed to analyze: {len(failed)} files")
    
    if successful:
        print(f"\n📈 Data Overview:")
        for result in successful:
            print(f"   - {result['sample_type']}: {result['shape'][0]:,} rows × {result['shape'][1]} columns")

if __name__ == "__main__":
    main()