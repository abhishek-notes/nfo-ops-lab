#!/usr/bin/env python3
"""
Get actual sample values from NFO data files
"""

import polars as pl
import os

def read_sample_data(file_path, num_rows=5):
    """Read sample data from a parquet file."""
    try:
        # Read only a small number of rows to avoid memory issues
        df = pl.scan_parquet(file_path).head(num_rows).collect()
        
        # Convert to list of dictionaries, handling datetime columns
        rows = []
        for i in range(min(num_rows, df.height)):
            row = {}
            for col in df.columns:
                val = df[col][i]
                # Convert datetime to string to avoid timezone issues
                if hasattr(val, 'strftime'):
                    row[col] = str(val)
                elif str(type(val)).startswith('<class \'polars'):
                    row[col] = str(val)
                else:
                    row[col] = val
            rows.append(row)
        return rows
    except Exception as e:
        return [{"error": str(e)}]

def main():
    print("NFO Ops Lab - Sample Data Values")
    print("=" * 50)
    
    # Select a few representative files
    sample_files = [
        ("/workspace/data/packed/options/BANKNIFTY/201907/exp=2019-07-11/type=CE/strike=29000.parquet", "2019 OPTIONS"),
        ("/workspace/data/packed/options/BANKNIFTY/202207/exp=2022-07-21/type=CE/strike=36500.parquet", "2022 OPTIONS"),
        ("/workspace/data/packed/options/BANKNIFTY/202311/exp=2023-11-15/type=CE/strike=41700.parquet", "2023 OPTIONS"),
        ("/workspace/data/packed/options/BANKNIFTY/202402/exp=2024-02-28/type=CE/strike=47900.parquet", "2024 OPTIONS"),
        ("/workspace/data/packed/spot/BANKNIFTY/201907/date=2019-07-25/ticks.parquet", "2019 SPOT"),
        ("/workspace/data/packed/spot/BANKNIFTY/202207/date=2022-07-25/ticks.parquet", "2022 SPOT"),
        ("/workspace/data/packed/futures/BANKNIFTY/201912/exp=2019-12-26/ticks.parquet", "2019 FUTURES"),
        ("/workspace/data/packed/futures/BANKNIFTY/202207/exp=2022-07-28/ticks.parquet", "2022 FUTURES"),
    ]
    
    for file_path, label in sample_files:
        print(f"\n{'='*60}")
        print(f"📊 {label}")
        print(f"📁 {file_path}")
        print('='*60)
        
        if os.path.exists(file_path):
            # Get first 3 rows
            first_rows = read_sample_data(file_path, 3)
            print(f"\n🔍 FIRST 3 ROWS:")
            for i, row in enumerate(first_rows, 1):
                print(f"  Row {i}: {row}")
            
            # Get last 3 rows (use tail)
            try:
                df = pl.scan_parquet(file_path).tail(3).collect()
                last_rows = []
                for i in range(df.height):
                    row = {}
                    for col in df.columns:
                        val = df[col][i]
                        if hasattr(val, 'strftime'):
                            row[col] = str(val)
                        elif str(type(val)).startswith('<class \'polars'):
                            row[col] = str(val)
                        else:
                            row[col] = val
                    last_rows.append(row)
                
                print(f"\n🔍 LAST 3 ROWS:")
                for i, row in enumerate(last_rows, 1):
                    print(f"  Row {i}: {row}")
                    
            except Exception as e:
                print(f"  Error reading last rows: {e}")
        else:
            print("❌ File not found")

if __name__ == "__main__":
    main()