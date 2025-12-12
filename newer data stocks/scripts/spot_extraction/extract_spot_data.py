#!/usr/bin/env python3
"""
Extract NIFTY and BANKNIFTY spot prices from das_nse_mod.sql.gz files.

Outputs consolidated spot data as Parquet for use in options enrichment.
"""

import argparse
import gzip
import re
from pathlib import Path
from typing import List, Tuple
import polars as pl
from datetime import datetime


def extract_insert_values(insert_line: str) -> List[Tuple]:
    """
    Parse INSERT INTO statement to extract values.
    
    Example input:
    INSERT INTO `NIFTY` VALUES (1,'2025-11-04 09:15:00',24510.75,24510.75,24510.75,24510.75,0,0.0,0,0.0);
    
    Returns list of tuples: [(timestamp, price, ...)]
    """
    # Extract everything between VALUES and ;
    match = re.search(r'VALUES\s+(.+);', insert_line)
    if not match:
        return []
    
    values_str = match.group(1)
    
    # Split by "),(" to get individual rows
    rows = []
    
    # Handle format: (row1),(row2),(row3)
    for row_match in re.finditer(r'\(([^)]+)\)', values_str):
        row_str = row_match.group(1)
        
        # Split by comma (but preserve quoted strings)
        parts = []
        current = ""
        in_quotes = False
        
        for char in row_str:
            if char == "'" and (not current or current[-1] != '\\'):
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current.strip().strip("'"))
                current = ""
                continue
            current += char
        
        if current:
            parts.append(current.strip().strip("'"))
        
        rows.append(tuple(parts))
    
    return rows


def parse_sql_for_symbol(
    sql_gz_path: Path,
    symbol: str
) -> pl.DataFrame:
    """
    Extract data for a specific symbol from SQL dump.
    
    Args:
        sql_gz_path: Path to das_nse_mod.sql.gz file
        symbol: Symbol to extract (e.g., 'NIFTY', 'BANKNIFTY')
    
    Returns:
        DataFrame with columns: timestamp, price, open, high, low, close
    """
    print(f"  Extracting {symbol} from {sql_gz_path.name}...")
    
    records = []
    symbol_upper = symbol.upper()
    
    with gzip.open(sql_gz_path, 'rt', errors='ignore') as f:
        in_symbol_section = False
        line_count = 0
        
        for line in f:
            line_count += 1
            line = line.strip()
            
            # Detect table start (case-insensitive, flexible matching)
            if "Table structure for table" in line and f"`{symbol}`" in line:
                in_symbol_section = True
                print(f"    Found table at line {line_count}")
                continue
            
            # Also match CREATE TABLE as backup
            if f"CREATE TABLE" in line and f"`{symbol}`" in line:
                in_symbol_section = True
                print(f"    Found table (CREATE) at line {line_count}")
                continue
            
            # Detect table end (next table starts or UNLOCK TABLES)
            if in_symbol_section:
                if (line.startswith("--") and "Table structure for table" in line) or \
                   (line.startswith("CREATE TABLE")) or \
                   ("UNLOCK TABLES" in line):
                    if not line.endswith(f"`{symbol}`"):
                        print(f"    Reached end of {symbol} table at line {line_count}")
                        break
            
            # Parse INSERT statements (case-insensitive)
            if in_symbol_section and f"INSERT INTO `{symbol}`" in line:
                rows = extract_insert_values(line)
                
                for row in rows:
                    try:
                        # Expected format:
                        # (id, timestamp, price, open, high, low, volume, change, trades, turnover)
                        # Index columns: 0=id, 1=timestamp, 2=price, 3=open, 4=high, 5=low
                        
                        if len(row) < 6:
                            continue
                        
                        timestamp_str = row[1]  # '2025-11-04 09:15:00'
                        price = float(row[2]) if row[2] and row[2] != 'NULL' else 0.0
                        open_price = float(row[3]) if row[3] and row[3] != 'NULL' else 0.0
                        high = float(row[4]) if row[4] and row[4] != 'NULL' else 0.0
                        low = float(row[5]) if row[5] and row[5] != 'NULL' else 0.0
                        
                        # Close = price (last traded)
                        close = price
                        
                        # Skip invalid data
                        if price == 0.0:
                            continue
                        
                        records.append({
                            'timestamp': timestamp_str,
                            'price': price,
                            'open': open_price,
                            'high': high,
                            'low': low,
                            'close': close
                        })
                    
                    except (ValueError, IndexError) as e:
                        # Silently skip bad rows
                        continue
    
    if not records:
        print(f"  Warning: No data found for {symbol}")
        return pl.DataFrame()
    
    # Create DataFrame
    df = pl.DataFrame(records)
    
    # Parse timestamp
    df = df.with_columns([
        pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    ])
    
    # Add symbol column
    df = df.with_columns([
        pl.lit(symbol).alias('symbol')
    ])
    
    print(f"  Extracted {len(df):,} rows for {symbol}")
    
    return df


def extract_spot_data(
    sql_gz_files: List[Path],
    output_dir: Path,
    symbols: List[str] = ['NIFTY', 'BANKNIFTY']
):
    """
    Extract spot data for multiple symbols from multiple SQL dumps.
    
    Outputs:
    - Per-file parquet: <output_dir>/<symbol>_<date_range>.parquet
    - Consolidated parquet: <output_dir>/<symbol>_all.parquet
    """
    print("="*70)
    print("SPOT DATA EXTRACTION FROM SQL DUMPS")
    print("="*70)
    print(f"Processing {len(sql_gz_files)} SQL dump files")
    print(f"Symbols: {', '.join(symbols)}")
    print()
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Store data per symbol
    symbol_data = {sym: [] for sym in symbols}
    
    for sql_file in sql_gz_files:
        print(f"\nProcessing: {sql_file.parent.name}/{sql_file.name}")
        
        for symbol in symbols:
            df = parse_sql_for_symbol(sql_file, symbol)
            
            if not df.is_empty():
                # Write per-file output
                folder_name = sql_file.parent.name.replace(' ', '_')
                output_file = output_dir / f"{symbol}_{folder_name}.parquet"
                df.write_parquet(output_file)
                print(f"  Wrote: {output_file.name}")
                
                # Collect for consolidated output
                symbol_data[symbol].append(df)
    
    # Write consolidated files
    print("\n" + "="*70)
    print("CREATING CONSOLIDATED FILES")
    print("="*70)
    
    for symbol in symbols:
        if symbol_data[symbol]:
            combined = pl.concat(symbol_data[symbol])
            
            # Sort by timestamp
            combined = combined.sort('timestamp')
            
            # Deduplicate (in case of overlaps between files)
            combined = combined.unique(subset=['timestamp'], keep='first')
            
            # Write consolidated
            output_file = output_dir / f"{symbol}_all.parquet"
            combined.write_parquet(output_file, compression='zstd')
            
            # Get date range
            min_date = combined['timestamp'].min()
            max_date = combined['timestamp'].max()
            
            print(f"\n{symbol}:")
            print(f"  Total rows: {len(combined):,}")
            print(f"  Date range: {min_date} to {max_date}")
            print(f"  Output: {output_file}")
    
    print("\n✓ Spot data extraction complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Extract NIFTY/BANKNIFTY spot data from SQL dumps"
    )
    
    parser.add_argument(
        "--data-dirs",
        type=str,
        nargs='+',
        help="Directories containing das_nse_mod.sql.gz files"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for spot data"
    )
    
    parser.add_argument(
        "--symbols",
        type=str,
        nargs='+',
        default=['NIFTY', 'BANKNIFTY'],
        help="Symbols to extract (default: NIFTY BANKNIFTY)"
    )
    
    args = parser.parse_args()
    
    # Find all SQL dumps
    sql_files = []
    
    if args.data_dirs:
        for dir_path in args.data_dirs:
            dir_path = Path(dir_path)
            if dir_path.exists():
                found = list(dir_path.glob("**/das_nse_mod.sql.gz"))
                sql_files.extend(found)
                print(f"Found {len(found)} SQL dumps in {dir_path}")
    else:
        # Default: search in "new 2025 data"
        base_dir = Path("new 2025 data")
        if base_dir.exists():
            sql_files = list(base_dir.glob("**/das_nse_mod.sql.gz"))
            print(f"Found {len(sql_files)} SQL dumps in {base_dir}")
    
    if not sql_files:
        print("Error: No SQL dump files found!")
        return 1
    
    # Extract
    extract_spot_data(sql_files, args.output_dir, args.symbols)
    
    return 0


if __name__ == "__main__":
    exit(main())
