#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract NIFTY and BANKNIFTY Spot Index Data from NSE SQL.gz

Extracts tick-level spot prices for NIFTY and BANKNIFTY indices.
"""

import os
import re
import gzip
import argparse
from pathlib import Path
from datetime import datetime
import polars as pl


# Target tables to extract
TARGET_TABLES = {'NIFTY', 'BANKNIFTY', 'NIFTYFUT', 'BANKNIFTYFUT'}


def parse_spot_values(line: str) -> list:
    """
    Parse SQL VALUES for spot data format: (timestamp, price)
    """
    rows = []

    idx = line.find('VALUES')
    if idx == -1:
        return rows

    content = line[idx + 6:].strip()
    if content.endswith(';'):
        content = content[:-1]

    # State machine parser for (timestamp, price) pairs
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
                if len(current_row) >= 2:
                    rows.append(current_row[:2])  # Only timestamp and price
                in_row = False
            else:
                current_val.append(char)

    return rows


def extract_spot_indices(sql_path: str, output_dir: str,
                         tables: set = None, verbose: bool = True) -> dict:
    """
    Extract NIFTY and BANKNIFTY spot data from SQL.gz file.

    Args:
        sql_path: Path to .sql.gz file
        output_dir: Output directory for parquet files
        tables: Set of table names to extract (default: NIFTY, BANKNIFTY, NIFTYFUT, BANKNIFTYFUT)
        verbose: Print progress

    Returns:
        Stats dict
    """
    if tables is None:
        tables = TARGET_TABLES

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    stats = {
        'tables_found': {},
        'rows_total': 0,
        'errors': []
    }

    # Accumulate data per table
    table_data = {t: [] for t in tables}

    if verbose:
        print(f"Reading: {sql_path}")
        print(f"Output:  {output_dir}")
        print(f"Looking for tables: {sorted(tables)}")
        print()

    with gzip.open(sql_path, 'rt', encoding='utf-8', errors='replace') as f:
        for line in f:
            if 'REPLACE INTO' not in line and 'INSERT INTO' not in line:
                continue

            # Extract table name
            m = re.search(r'`([^`]+)`', line)
            if not m:
                continue

            tname = m.group(1).upper()
            if tname not in tables:
                continue

            # Parse values
            try:
                rows = parse_spot_values(line)
                if rows:
                    for row in rows:
                        table_data[tname].append({
                            'timestamp': row[0],
                            'price': float(row[1]) if row[1] else None
                        })
                    stats['tables_found'][tname] = stats['tables_found'].get(tname, 0) + len(rows)
            except Exception as e:
                stats['errors'].append(f"{tname}: {str(e)[:100]}")

    # Write each table to parquet
    for tname, rows in table_data.items():
        if not rows:
            continue

        df = pl.DataFrame(rows)

        # Parse timestamp
        df = df.with_columns([
            pl.col('timestamp').str.strptime(pl.Datetime('us'), '%Y-%m-%d %H:%M:%S', strict=False),
        ])

        # Filter 1970 timestamps (bad data)
        df = df.filter(pl.col('timestamp').dt.year() > 1971)

        # Add symbol column
        df = df.with_columns([
            pl.lit(tname).alias('symbol')
        ])

        # Filter market hours (08:30-15:35 for pre-market to close)
        from datetime import time
        df = df.filter(
            (pl.col('timestamp').dt.time() >= time(8, 30, 0)) &
            (pl.col('timestamp').dt.time() <= time(15, 35, 0))
        )

        df = df.sort('timestamp').unique(['timestamp'])

        # Add timezone (after filtering to avoid issues)
        try:
            df = df.with_columns([
                pl.col('timestamp').dt.replace_time_zone('Asia/Kolkata'),
            ])
        except Exception:
            # Fallback if tzdata not available
            pass

        # Save
        out_file = out_path / f"{tname.lower()}_spot.parquet"
        df.write_parquet(str(out_file), compression='zstd', compression_level=3)

        stats['rows_total'] += df.height

        if verbose:
            # Get date range - use string conversion to avoid timezone issues
            dates = df.select(pl.col('timestamp').dt.date().cast(pl.Utf8)).unique().to_series().to_list()
            dates = sorted(dates)
            print(f"  {tname}: {df.height:,} rows ({dates[0]} to {dates[-1]}) -> {out_file}")

    return stats


def main():
    parser = argparse.ArgumentParser(description='Extract NIFTY/BANKNIFTY spot data')
    parser.add_argument('sql_file', type=str, help='Path to SQL.gz file')
    parser.add_argument('--output', '-o', type=str, required=True, help='Output directory')
    parser.add_argument('--tables', type=str, nargs='+',
                        default=['NIFTY', 'BANKNIFTY', 'NIFTYFUT', 'BANKNIFTYFUT'],
                        help='Tables to extract')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode')

    args = parser.parse_args()

    if not os.path.exists(args.sql_file):
        print(f"Error: File not found: {args.sql_file}")
        return 1

    stats = extract_spot_indices(
        args.sql_file,
        args.output,
        tables=set(t.upper() for t in args.tables),
        verbose=not args.quiet
    )

    print("\n" + "=" * 50)
    print("EXTRACTION COMPLETE")
    print("=" * 50)
    for tname, count in stats['tables_found'].items():
        print(f"  {tname}: {count:,} raw rows")
    print(f"Total processed rows: {stats['rows_total']:,}")

    if stats['errors']:
        print(f"\nErrors ({len(stats['errors'])})")
        for err in stats['errors'][:5]:
            print(f"  - {err}")

    return 0


if __name__ == '__main__':
    exit(main())
