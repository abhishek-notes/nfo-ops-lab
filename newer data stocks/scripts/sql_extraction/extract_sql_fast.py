#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fast SQL.gz to Parquet Extractor for NFO Options Data

This script efficiently extracts option tables from MySQL SQL.gz dumps
and converts them to parquet files matching the existing raw format.

Optimized for large files (1GB+) using streaming parsing.
"""

import os
import re
import gzip
import argparse
from pathlib import Path
from datetime import datetime
import polars as pl

# Column definitions matching SQL schema
SQL_COLUMNS = [
    'timestamp', 'price', 'qty', 'avgPrice', 'volume', 'bQty', 'sQty',
    'open', 'high', 'low', 'close', 'changeper', 'lastTradeTime',
    'oi', 'oiHigh', 'oiLow',
    'bq0', 'bp0', 'bo0', 'bq1', 'bp1', 'bo1', 'bq2', 'bp2', 'bo2',
    'bq3', 'bp3', 'bo3', 'bq4', 'bp4', 'bo4',
    'sq0', 'sp0', 'so0', 'sq1', 'sp1', 'so1', 'sq2', 'sp2', 'so2',
    'sq3', 'sp3', 'so3', 'sq4', 'sp4', 'so4'
]

# Table name pattern: BANKNIFTY25DEC43500PE or NIFTY25JAN24000CE
TABLE_PATTERN = re.compile(
    r'^(BANKNIFTY|NIFTY)(\d{2})([A-Z]{3})(\d+)(CE|PE)$',
    re.IGNORECASE
)

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}
MONTH_REV = {v: k.lower() for k, v in MONTH_MAP.items()}


def parse_table_name(name: str) -> dict | None:
    """Parse table name into metadata components."""
    m = TABLE_PATTERN.match(name.upper())
    if not m:
        return None
    return {
        'symbol': m.group(1).upper(),
        'year': 2000 + int(m.group(2)),
        'month': MONTH_MAP.get(m.group(3).upper()),
        'strike': int(m.group(4)),
        'opt_type': m.group(5).upper(),
        'table_name': name
    }


def parse_values_fast(line: str) -> list:
    """
    Fast parsing of SQL VALUES line into list of row tuples.
    Handles: VALUES ('val1',123,...),('val2',456,...)...;
    """
    rows = []

    # Find VALUES keyword
    idx = line.find('VALUES')
    if idx == -1:
        return rows

    content = line[idx + 6:].strip()
    if content.endswith(';'):
        content = content[:-1]

    # State machine parser for better performance
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
                    rows.append(current_row)
                in_row = False
            else:
                current_val.append(char)

    return rows


def rows_to_dataframe(rows: list, meta: dict) -> pl.DataFrame | None:
    """Convert parsed rows to Polars DataFrame with proper types."""
    if not rows:
        return None

    # Build column data
    data = {col: [] for col in SQL_COLUMNS}
    for row in rows:
        for i, col in enumerate(SQL_COLUMNS):
            data[col].append(row[i] if i < len(row) else None)

    # Create DataFrame
    df = pl.DataFrame(data)

    # Convert timestamp columns
    df = df.with_columns([
        pl.col('timestamp').str.strptime(pl.Datetime('us'), '%Y-%m-%d %H:%M:%S', strict=False),
        pl.col('lastTradeTime').str.strptime(pl.Datetime('us'), '%Y-%m-%d %H:%M:%S', strict=False),
    ])

    # Convert numeric columns
    float_cols = ['price', 'avgPrice', 'open', 'high', 'low', 'close', 'changeper',
                  'bp0', 'bp1', 'bp2', 'bp3', 'bp4', 'sp0', 'sp1', 'sp2', 'sp3', 'sp4']
    int_cols = ['qty', 'volume', 'bQty', 'sQty', 'oi', 'oiHigh', 'oiLow',
                'bq0', 'bo0', 'bq1', 'bo1', 'bq2', 'bo2', 'bq3', 'bo3', 'bq4', 'bo4',
                'sq0', 'so0', 'sq1', 'so1', 'sq2', 'so2', 'sq3', 'so3', 'sq4', 'so4']

    for col in float_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    for col in int_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    # Add metadata columns (matching raw parquet format)
    df = df.with_columns([
        pl.lit(meta['symbol']).alias('symbol'),
        pl.lit(meta['opt_type']).alias('opt_type'),
        pl.lit(meta['strike']).cast(pl.Int32).alias('strike'),
        pl.lit(meta['year']).cast(pl.Int32).alias('year'),
        pl.lit(meta['month']).cast(pl.Int32).alias('month'),
        pl.col('timestamp').alias('ts'),  # Copy for 1970 bug handling
    ])

    return df


def extract_sql_gz(sql_path: str, output_dir: str, symbol_filter: str = None,
                   table_limit: int = 0, verbose: bool = True) -> dict:
    """
    Extract tables from SQL.gz file to parquet files.

    Args:
        sql_path: Path to .sql.gz file
        output_dir: Output directory for parquet files
        symbol_filter: Filter by symbol (BANKNIFTY or NIFTY)
        table_limit: Max tables to process (0 = all)
        verbose: Print progress

    Returns:
        Stats dict
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    stats = {
        'tables_found': 0,
        'tables_processed': 0,
        'tables_skipped': 0,
        'rows_total': 0,
        'errors': []
    }

    if verbose:
        print(f"Reading: {sql_path}")
        print(f"Output:  {output_dir}")
        if symbol_filter:
            print(f"Filter:  {symbol_filter}")
        print()

    with gzip.open(sql_path, 'rt', encoding='utf-8', errors='replace') as f:
        for line in f:
            # Look for REPLACE INTO lines (they contain the data)
            if 'REPLACE INTO' not in line and 'INSERT INTO' not in line:
                continue

            # Extract table name
            m = re.search(r'`([^`]+)`', line)
            if not m:
                continue

            tname = m.group(1)
            meta = parse_table_name(tname)

            if not meta or not meta['month']:
                continue

            # Apply symbol filter
            if symbol_filter and meta['symbol'] != symbol_filter.upper():
                continue

            stats['tables_found'] += 1

            if verbose and stats['tables_found'] % 100 == 0:
                print(f"  Found {stats['tables_found']} tables...")

            # Parse values from this line
            try:
                rows = parse_values_fast(line)
                if rows:
                    df = rows_to_dataframe(rows, meta)
                    if df is not None and df.height > 0:
                        # Generate filename
                        yr = meta['year'] % 100
                        mon = MONTH_REV[meta['month']]
                        fname = f"{meta['symbol'].lower()}{yr}{mon}{meta['strike']}{meta['opt_type'].lower()}.parquet"

                        out_file = out_path / fname

                        # CRITICAL: Merge with existing file if present
                        # SQL dumps often have multiple REPLACE INTO statements for same table
                        if out_file.exists():
                            try:
                                existing_df = pl.read_parquet(str(out_file))
                                df = pl.concat([existing_df, df], how="vertical_relaxed")
                                df = df.unique(subset=["timestamp"]).sort("timestamp")
                            except Exception:
                                pass  # If read fails, just overwrite

                        df.write_parquet(str(out_file))

                        stats['tables_processed'] += 1
                        stats['rows_total'] += df.height

                        if verbose and stats['tables_processed'] % 50 == 0:
                            print(f"  Processed {stats['tables_processed']} tables ({stats['rows_total']:,} rows)")

                        # Check limit
                        if table_limit > 0 and stats['tables_processed'] >= table_limit:
                            print(f"\nReached table limit ({table_limit})")
                            return stats
            except Exception as e:
                stats['errors'].append(f"{tname}: {str(e)[:100]}")
                stats['tables_skipped'] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description='Fast SQL.gz to Parquet extractor')
    parser.add_argument('sql_file', type=str, help='Path to SQL.gz file')
    parser.add_argument('--output', '-o', type=str, required=True, help='Output directory')
    parser.add_argument('--symbol', type=str, choices=['BANKNIFTY', 'NIFTY'], help='Filter by symbol')
    parser.add_argument('--limit', type=int, default=0, help='Max tables to process (0=all)')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode')

    args = parser.parse_args()

    if not os.path.exists(args.sql_file):
        print(f"Error: File not found: {args.sql_file}")
        return 1

    stats = extract_sql_gz(
        args.sql_file,
        args.output,
        symbol_filter=args.symbol,
        table_limit=args.limit,
        verbose=not args.quiet
    )

    print("\n" + "=" * 50)
    print("EXTRACTION COMPLETE")
    print("=" * 50)
    print(f"Tables found:     {stats['tables_found']}")
    print(f"Tables processed: {stats['tables_processed']}")
    print(f"Tables skipped:   {stats['tables_skipped']}")
    print(f"Total rows:       {stats['rows_total']:,}")

    if stats['errors']:
        print(f"\nErrors ({len(stats['errors'])}):")
        for err in stats['errors'][:10]:
            print(f"  - {err}")

    return 0


if __name__ == '__main__':
    exit(main())
