#!/usr/bin/env python3
"""
NFO Options Data Viewer - CLI Version

A command-line tool to view and compare data across:
- Raw parquet files (from SQL extraction)
- Packed parquet files (normalized format)
- Original SQL.gz data

Usage:
    python cli_viewer.py --folder "till 10 sep" --symbol BANKNIFTY --strike 54000 --type CE
    python cli_viewer.py --list-folders
    python cli_viewer.py --folder "till 10 sep" --list-options
"""

import argparse
import gzip
import os
import re
import sys
from pathlib import Path
from datetime import datetime
import polars as pl

# Data folder configurations
DATA_ROOT = Path(os.environ.get("NFO_DATA_ROOT", Path(__file__).resolve().parent.parent / "newer data stocks"))
NEW_DATA = DATA_ROOT / "new 2025 data"
DATA_FOLDERS = {
    "aug 1 to aug 13": NEW_DATA / "aug 1 to aug 13 new stocks data",
    "aug 13-29": NEW_DATA / "aug 13 to aug 29 new stocks data",
    "aug 29-sep 23": NEW_DATA / "aug 29 to sep 23 new stocks data",
    "aug 14 to 10 sep": NEW_DATA / "aug 14 to 10 sep new stocks data",
    "sep 23-oct 6": NEW_DATA / "sep 23 to oct 6 new stocks data",
    "oct 7 to oct 20": NEW_DATA / "oct 7 to oct 20 new stocks data",
    "oct 20 to nov 3": NEW_DATA / "oct 20 to nov 3 new stocks data",
    "main (nov+)": NEW_DATA / "nov 4 to nov 18 new stocks data",
}

MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}


def parse_filename(filename: str) -> dict | None:
    """Parse raw parquet filename into components."""
    pattern = r'^(banknifty|nifty)(\d{2})([a-z]{3})(\d+)(ce|pe)\.parquet$'
    m = re.match(pattern, filename.lower())
    if m:
        return {
            'symbol': m.group(1).upper(),
            'year': 2000 + int(m.group(2)),
            'month': MONTH_MAP.get(m.group(3)),
            'month_str': m.group(3),
            'strike': int(m.group(4)),
            'opt_type': m.group(5).upper(),
        }
    return None


def get_available_options(folder_path: str) -> list[dict]:
    """Get list of available options from raw_options folder."""
    raw_dir = Path(folder_path) / "processed_output" / "raw_options"
    if not raw_dir.exists():
        return []

    options = []
    for f in raw_dir.glob("*.parquet"):
        meta = parse_filename(f.name)
        if meta:
            meta['filename'] = f.name
            meta['path'] = str(f)
            options.append(meta)

    return sorted(options, key=lambda x: (x['symbol'], x['strike'], x['opt_type']))


def extract_sql_rows(sql_path: str, table_name: str, limit: int = 50) -> list[dict]:
    """Extract rows from SQL.gz file for a specific table."""
    rows = []

    SQL_COLUMNS = [
        'timestamp', 'price', 'qty', 'avgPrice', 'volume', 'bQty', 'sQty',
        'open', 'high', 'low', 'close', 'changeper', 'lastTradeTime',
        'oi', 'oiHigh', 'oiLow',
        'bq0', 'bp0', 'bo0', 'bq1', 'bp1', 'bo1', 'bq2', 'bp2', 'bo2',
        'bq3', 'bp3', 'bo3', 'bq4', 'bp4', 'bo4',
        'sq0', 'sp0', 'so0', 'sq1', 'sp1', 'so1', 'sq2', 'sp2', 'so2',
        'sq3', 'sp3', 'so3', 'sq4', 'sp4', 'so4'
    ]

    try:
        with gzip.open(sql_path, 'rt', encoding='utf-8', errors='replace') as f:
            for line in f:
                if f'`{table_name}`' not in line:
                    continue
                if 'REPLACE INTO' not in line and 'INSERT INTO' not in line:
                    continue

                idx = line.find('VALUES')
                if idx == -1:
                    continue

                content = line[idx + 6:].strip()
                if content.endswith(';'):
                    content = content[:-1]

                in_row = False
                in_quote = False
                current_val = []
                current_row = []

                for char in content:
                    if not in_row:
                        if char == '(':
                            in_row = True
                            current_row = []
                            current_val = []
                    else:
                        if char == "'" and not in_quote:
                            in_quote = True
                        elif char == "'" and in_quote:
                            in_quote = False
                        elif char == ',' and not in_quote:
                            current_row.append(''.join(current_val).strip())
                            current_val = []
                        elif char == ')' and not in_quote:
                            current_row.append(''.join(current_val).strip())
                            if len(current_row) == len(SQL_COLUMNS):
                                row_dict = dict(zip(SQL_COLUMNS, current_row))
                                rows.append(row_dict)
                                if len(rows) >= limit:
                                    return rows
                            in_row = False
                        else:
                            current_val.append(char)

                if rows:
                    break
    except Exception as e:
        print(f"Error reading SQL: {e}")

    return rows


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='NFO Options Data Viewer - CLI')
    parser.add_argument('--list-folders', action='store_true', help='List available data folders')
    parser.add_argument('--folder', type=str, help='Data folder name')
    parser.add_argument('--list-options', action='store_true', help='List available options in folder')
    parser.add_argument('--symbol', type=str, choices=['BANKNIFTY', 'NIFTY'], help='Symbol')
    parser.add_argument('--strike', type=int, help='Strike price')
    parser.add_argument('--type', type=str, choices=['CE', 'PE'], help='Option type')
    parser.add_argument('--date', type=str, help='Filter by date (YYYY-MM-DD)')
    parser.add_argument('--rows', type=int, default=20, help='Number of rows to display')
    parser.add_argument('--show-raw', action='store_true', help='Show raw parquet data')
    parser.add_argument('--show-packed', action='store_true', help='Show packed parquet data')
    parser.add_argument('--show-sql', action='store_true', help='Show SQL data')
    parser.add_argument('--compare', action='store_true', help='Compare raw vs packed')

    args = parser.parse_args()

    # List folders
    if args.list_folders:
        print("\nAvailable Data Folders:")
        print("-" * 40)
        for name, path in DATA_FOLDERS.items():
            exists = "✓" if Path(path).exists() else "✗"
            print(f"  {exists} {name}")
        return 0

    # Validate folder
    if args.folder and args.folder not in DATA_FOLDERS:
        print(f"Error: Unknown folder '{args.folder}'")
        print(f"Use --list-folders to see available folders")
        return 1

    folder_path = DATA_FOLDERS.get(args.folder) if args.folder else None

    # List options
    if args.list_options:
        if not folder_path:
            print("Error: --folder required with --list-options")
            return 1

        options = get_available_options(folder_path)
        if not options:
            print("No options found in this folder")
            return 1

        print(f"\nAvailable Options in '{args.folder}':")
        print("-" * 60)
        print(f"{'Symbol':<12} {'Strike':<8} {'Type':<4} {'Month':<6}")
        print("-" * 60)

        # Show first 50
        for o in options[:50]:
            print(f"{o['symbol']:<12} {o['strike']:<8} {o['opt_type']:<4} {o['month_str']:<6}")

        if len(options) > 50:
            print(f"... and {len(options) - 50} more")

        return 0

    # View specific option
    if args.symbol and args.strike and args.type:
        if not folder_path:
            print("Error: --folder required")
            return 1

        options = get_available_options(folder_path)
        matching = [o for o in options
                    if o['symbol'] == args.symbol
                    and o['strike'] == args.strike
                    and o['opt_type'] == args.type]

        if not matching:
            print(f"No matching option found: {args.symbol} {args.strike} {args.type}")
            return 1

        option = matching[0]
        filter_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None

        # Default: show all if no specific flag
        show_all = not (args.show_raw or args.show_packed or args.show_sql or args.compare)

        # Raw data
        if args.show_raw or show_all:
            print_section("RAW PARQUET DATA")
            print(f"File: {option['path']}")

            raw_df = pl.read_parquet(option['path'])
            if filter_date:
                raw_df = raw_df.filter(pl.col('timestamp').dt.date() == filter_date)

            print(f"Rows: {raw_df.height:,}")
            print(f"Columns: {len(raw_df.columns)}")
            print(f"\nFirst {args.rows} rows:")
            print(raw_df.head(args.rows))

        # Packed data
        if args.show_packed or show_all:
            print_section("PACKED PARQUET DATA")
            packed_dir = Path(folder_path) / "processed_output" / "packed_options" / args.symbol

            if packed_dir.exists():
                # Find expiry files
                found = False
                for ym_dir in packed_dir.iterdir():
                    if ym_dir.is_dir():
                        for exp_dir in ym_dir.iterdir():
                            if exp_dir.is_dir() and exp_dir.name.startswith("exp="):
                                strike_file = exp_dir / f"type={args.type}" / f"strike={args.strike}.parquet"
                                if strike_file.exists():
                                    print(f"File: {strike_file}")
                                    packed_df = pl.read_parquet(str(strike_file))
                                    if filter_date:
                                        packed_df = packed_df.filter(pl.col('timestamp').dt.date() == filter_date)
                                    print(f"Rows: {packed_df.height:,}")
                                    print(f"Columns: {len(packed_df.columns)}")
                                    print(f"\nFirst {args.rows} rows:")
                                    print(packed_df.head(args.rows))
                                    found = True
                                    break
                    if found:
                        break

                if not found:
                    print(f"No packed data found for {args.symbol} {args.strike} {args.type}")
            else:
                print("Packed options directory not found")

        # SQL data
        if args.show_sql or show_all:
            print_section("SQL SOURCE DATA")

            if args.symbol == "BANKNIFTY":
                sql_file = Path(folder_path) / "das_bankopt_mod.sql.gz"
            else:
                sql_file = Path(folder_path) / "das_niftyopt_mod.sql.gz"

            if sql_file.exists():
                yr = option['year'] % 100
                table_name = f"{args.symbol}{yr}{option['month_str'].upper()}{args.strike}{args.type}"
                print(f"SQL File: {sql_file}")
                print(f"Table: {table_name}")

                print(f"\nExtracting {args.rows} rows...")
                sql_rows = extract_sql_rows(str(sql_file), table_name, limit=args.rows)
                if sql_rows:
                    sql_df = pl.DataFrame(sql_rows)
                    print(f"Extracted: {len(sql_rows)} rows")
                    print(sql_df)
                else:
                    print("No data found in SQL file")
            else:
                print(f"SQL file not found: {sql_file}")

        # Compare
        if args.compare:
            print_section("COMPARISON: RAW vs PACKED")

            raw_df = pl.read_parquet(option['path'])
            if filter_date:
                raw_df = raw_df.filter(pl.col('timestamp').dt.date() == filter_date)

            # Find packed
            packed_dir = Path(folder_path) / "processed_output" / "packed_options" / args.symbol
            packed_df = None

            if packed_dir.exists():
                for ym_dir in packed_dir.iterdir():
                    if ym_dir.is_dir():
                        for exp_dir in ym_dir.iterdir():
                            if exp_dir.is_dir() and exp_dir.name.startswith("exp="):
                                strike_file = exp_dir / f"type={args.type}" / f"strike={args.strike}.parquet"
                                if strike_file.exists():
                                    packed_df = pl.read_parquet(str(strike_file))
                                    if filter_date:
                                        packed_df = packed_df.filter(pl.col('timestamp').dt.date() == filter_date)
                                    break
                    if packed_df is not None:
                        break

            if packed_df is not None:
                print(f"\n{'Metric':<25} {'Raw':<15} {'Packed':<15}")
                print("-" * 55)
                print(f"{'Total Rows':<25} {raw_df.height:<15,} {packed_df.height:<15,}")
                print(f"{'Columns':<25} {len(raw_df.columns):<15} {len(packed_df.columns):<15}")

                if 'close' in raw_df.columns and 'close' in packed_df.columns:
                    raw_avg = raw_df['close'].mean()
                    packed_avg = packed_df['close'].mean()
                    if raw_avg and packed_avg:
                        print(f"{'Avg Close':<25} {raw_avg:<15.2f} {packed_avg:<15.2f}")

                # Column comparison
                raw_cols = set(raw_df.columns)
                packed_cols = set(packed_df.columns)

                print(f"\n{'Common Columns:':<25} {len(raw_cols & packed_cols)}")
                print(f"{'Raw Only:':<25} {len(raw_cols - packed_cols)}")
                print(f"{'Packed Only:':<25} {len(packed_cols - raw_cols)}")
            else:
                print("Could not find packed data for comparison")

        return 0

    # No action specified
    parser.print_help()
    return 0


if __name__ == '__main__':
    sys.exit(main())
