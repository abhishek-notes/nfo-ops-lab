#!/usr/bin/env python3
"""
NFO Ops Lab Data Sample Collector

This script collects representative samples from the packed data files including:
1. OPTIONS data from different years (2019, 2022, 2023, 2024)
2. SPOT data from different years
3. FUTURES data from different years

For each file, it collects:
- First 5 rows
- Last 5 rows
- Column information and data shape
"""

import polars as pl
import os
from pathlib import Path
from typing import List, Dict, Any
import sys

def get_file_info(file_path: str) -> Dict[str, Any]:
    """Get basic information about a parquet file."""
    try:
        df = pl.read_parquet(file_path)
        return {
            'file_path': file_path,
            'shape': df.shape,
            'columns': df.columns,
            'dtypes': {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            'first_5': df.head(5),
            'last_5': df.tail(5),
            'success': True,
            'error': None
        }
    except Exception as e:
        return {
            'file_path': file_path,
            'shape': None,
            'columns': None,
            'dtypes': None,
            'first_5': None,
            'last_5': None,
            'success': False,
            'error': str(e)
        }

def find_options_files() -> List[str]:
    """Find representative options files from different years."""
    base_path = Path("/workspace/data/packed/options")
    files = []
    
    # Target years and some representative files
    targets = [
        ("BANKNIFTY", "201904", "exp=2019-04-11", "type=CE", "strike=30000.parquet"),
        ("BANKNIFTY", "201911", "exp=2019-11-07", "type=PE", "strike=29500.parquet"),
        ("BANKNIFTY", "202201", "exp=2022-01-06", "type=CE", "strike=35000.parquet"),
        ("BANKNIFTY", "202209", "exp=2022-09-08", "type=PE", "strike=38000.parquet"),
        ("BANKNIFTY", "202305", "exp=2023-05-05", "type=CE", "strike=42000.parquet"),
        ("BANKNIFTY", "202311", "exp=2023-11-03", "type=PE", "strike=44000.parquet"),
        ("BANKNIFTY", "202402", "exp=2024-02-01", "type=CE", "strike=47000.parquet"),
        ("BANKNIFTY", "202408", "exp=2024-08-01", "type=PE", "strike=50000.parquet"),
    ]
    
    for symbol, month, exp, type_dir, strike_file in targets:
        file_path = base_path / symbol / month / exp / type_dir / strike_file
        if file_path.exists():
            files.append(str(file_path))
        else:
            # Try to find any file in that directory structure
            try:
                exp_dir = base_path / symbol / month / exp / type_dir
                if exp_dir.exists():
                    strike_files = list(exp_dir.glob("strike=*.parquet"))
                    if strike_files:
                        files.append(str(strike_files[0]))
                        print(f"Using alternative file: {strike_files[0]}")
            except:
                pass
    
    return files

def find_spot_files() -> List[str]:
    """Find representative spot files from different years."""
    base_path = Path("/workspace/data/packed/spot")
    files = []
    
    # Target files from different years
    targets = [
        ("BANKNIFTY", "201904", "date=2019-04-08", "ticks.parquet"),
        ("BANKNIFTY", "202202", "date=2022-02-01", "ticks.parquet"),
        ("NIFTY", "202305", "date=2023-05-01", "ticks.parquet"),
        ("NIFTY", "202408", "date=2024-08-01", "ticks.parquet"),
    ]
    
    for symbol, month, date_dir, filename in targets:
        file_path = base_path / symbol / month / date_dir / filename
        if file_path.exists():
            files.append(str(file_path))
        else:
            # Try to find any file in that month
            try:
                month_dir = base_path / symbol / month
                if month_dir.exists():
                    date_dirs = [d for d in month_dir.iterdir() if d.is_dir() and d.name.startswith("date=")]
                    if date_dirs:
                        tick_file = date_dirs[0] / "ticks.parquet"
                        if tick_file.exists():
                            files.append(str(tick_file))
                            print(f"Using alternative spot file: {tick_file}")
            except:
                pass
    
    return files

def find_futures_files() -> List[str]:
    """Find representative futures files from different years."""
    base_path = Path("/workspace/data/packed/futures")
    files = []
    
    # Target files from different years
    targets = [
        ("BANKNIFTY", "201904", "exp=2019-04-25", "ticks.parquet"),
        ("BANKNIFTY", "202203", "exp=2022-03-31", "ticks.parquet"),
        ("NIFTY", "202309", "exp=2023-09-28", "ticks.parquet"),
        ("NIFTY", "202410", "exp=2024-10-31", "ticks.parquet"),
    ]
    
    for symbol, month, exp, filename in targets:
        file_path = base_path / symbol / month / exp / filename
        if file_path.exists():
            files.append(str(file_path))
        else:
            print(f"File not found: {file_path}")
    
    return files

def format_dataframe_output(df: pl.DataFrame, title: str) -> str:
    """Format a polars DataFrame for nice display."""
    if df is None:
        return f"{title}: No data available\n"
    
    output = f"\n{title}:\n"
    output += "=" * (len(title) + 1) + "\n"
    
    # Convert to string representation with better formatting
    df_str = str(df)
    output += df_str + "\n"
    
    return output

def print_file_analysis(info: Dict[str, Any], category: str):
    """Print formatted analysis of a file."""
    if not info['success']:
        print(f"\n❌ FAILED to read {info['file_path']}")
        print(f"   Error: {info['error']}")
        return
    
    print(f"\n📊 {category.upper()} DATA ANALYSIS")
    print("=" * 60)
    print(f"File: {info['file_path']}")
    print(f"Shape: {info['shape']} (rows × columns)")
    print(f"Columns: {info['columns']}")
    print(f"Data Types:")
    for col, dtype in info['dtypes'].items():
        print(f"  {col}: {dtype}")
    
    print(format_dataframe_output(info['first_5'], "First 5 rows"))
    print(format_dataframe_output(info['last_5'], "Last 5 rows"))

def main():
    """Main function to collect and display data samples."""
    print("NFO Ops Lab Data Sample Collection")
    print("=" * 50)
    
    # Collect OPTIONS data
    print("\n🔍 Searching for OPTIONS files...")
    options_files = find_options_files()
    print(f"Found {len(options_files)} options files")
    
    for i, file_path in enumerate(options_files[:4], 1):  # Limit to 4 files
        print(f"\nAnalyzing options file {i}/{len(options_files[:4])}")
        info = get_file_info(file_path)
        print_file_analysis(info, f"Options Sample {i}")
    
    # Collect SPOT data
    print("\n🔍 Searching for SPOT files...")
    spot_files = find_spot_files()
    print(f"Found {len(spot_files)} spot files")
    
    for i, file_path in enumerate(spot_files[:3], 1):  # Limit to 3 files
        print(f"\nAnalyzing spot file {i}/{len(spot_files[:3])}")
        info = get_file_info(file_path)
        print_file_analysis(info, f"Spot Sample {i}")
    
    # Collect FUTURES data
    print("\n🔍 Searching for FUTURES files...")
    futures_files = find_futures_files()
    print(f"Found {len(futures_files)} futures files")
    
    for i, file_path in enumerate(futures_files[:3], 1):  # Limit to 3 files
        print(f"\nAnalyzing futures file {i}/{len(futures_files[:3])}")
        info = get_file_info(file_path)
        print_file_analysis(info, f"Futures Sample {i}")
    
    print("\n✅ Data sample collection completed!")
    print("\nSUMMARY:")
    print(f"- Options files analyzed: {min(len(options_files), 4)}")
    print(f"- Spot files analyzed: {min(len(spot_files), 3)}")
    print(f"- Futures files analyzed: {min(len(futures_files), 3)}")

if __name__ == "__main__":
    main()