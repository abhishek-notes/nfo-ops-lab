#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fast SQL.gz to Parquet Extractor for NFO Options Data - NUMERIC DATE FORMAT

This version handles NIFTY tables with numeric date encoding:
  NIFTY25{MDD}{STRIKE}{CE|PE}  (e.g., NIFTY2581422600CE = Aug 14, strike 22600, CE)

Instead of the standard 3-letter month format:
  NIFTY25{MMM}{STRIKE}{CE|PE}  (e.g., NIFTY25AUG22600CE)

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

# Table name pattern for NUMERIC date format: NIFTY2581422600CE
# Format: {SYMBOL}{YY}{M}{DD}{STRIKE}{TYPE}
# Where M is single digit month (1-9 for Jan-Sep, O/N/D or 10/11/12 for Oct-Dec)
TABLE_PATTERN_NUMERIC = re.compile(
    r'^(BANKNIFTY|NIFTY)(\d{2})(\d{3,4})(\d+)(CE|PE)$',
    re.IGNORECASE
)

# Also keep the standard 3-letter month pattern for fallback
TABLE_PATTERN_ALPHA = re.compile(
    r'^(BANKNIFTY|NIFTY)(\d{2})([A-Z]{3})(\d+)(CE|PE)$',
    re.IGNORECASE
)

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}
MONTH_REV = {v: k.lower() for k, v in MONTH_MAP.items()}


def parse_numeric_date(date_code: str) -> tuple[int, int] | None:
    """
    Parse numeric date code into (month, day).

    Format appears to be {M}{DD} where:
    - M = 1-9 for Jan-Sep, or 1X for Oct(10), Nov(11), Dec(12)
    - DD = day of month

    Examples:
    - 814 = August 14 (8, 14)
    - 807 = August 7 (8, 07)
    - 902 = September 2 (9, 02)
    - 1015 = October 15 (10, 15)
    - 1107 = November 7 (11, 07)
    """
    if len(date_code) == 3:
        # Single digit month (1-9)
        month = int(date_code[0])
        day = int(date_code[1:])
        if 1 <= month <= 9 and 1 <= day <= 31:
            return (month, day)
    elif len(date_code) == 4:
        # Could be single month + 2-digit day, or double month + 2-digit day
        # Try double-digit month first (10, 11, 12)
        month = int(date_code[:2])
        day = int(date_code[2:])
        if 10 <= month <= 12 and 1 <= day <= 31:
            return (month, day)
        # If that doesn't work, try single month with 2-digit day
        # (shouldn't happen for days 01-09, but handle edge cases)
        month = int(date_code[0])
        day = int(date_code[1:])
        if 1 <= month <= 9 and 1 <= day <= 99:  # day could be strike prefix
            return None  # Ambiguous, skip
    return None


def parse_table_name(name: str) -> dict | None:
    """Parse table name into metadata components."""
    name_upper = name.upper()

    # Try numeric date pattern first (for NIFTY in these files)
    m = TABLE_PATTERN_NUMERIC.match(name_upper)
    if m:
        symbol = m.group(1).upper()
        year = 2000 + int(m.group(2))
        date_code = m.group(3)
        strike_str = m.group(4)
        opt_type = m.group(5).upper()

        # Parse the date code
        parsed = parse_numeric_date(date_code)
        if parsed:
            month, day = parsed
            return {
                'symbol': symbol,
                'year': year,
                'month': month,
                'day': day,
                'strike': int(strike_str),
                'opt_type': opt_type,
                'table_name': name
            }

        # If date parsing failed, the "date_code" might actually be part of strike
        # Try alternative parsing: first 1-2 digits as month, rest as strike
        # This handles edge cases
        for month_len in [1, 2]:
            if len(date_code) > month_len:
                try:
                    month = int(date_code[:month_len])
                    day_and_strike = date_code[month_len:] + strike_str
                    # Assume 2-digit day followed by strike
                    if len(day_and_strike) >= 2:
                        day = int(day_and_strike[:2])
                        strike = int(day_and_strike[2:]) if len(day_and_strike) > 2 else int(strike_str)
                        if 1 <= month <= 12 and 1 <= day <= 31:
                            return {
                                'symbol': symbol,
                                'year': year,
                                'month': month,
                                'day': day,
                                'strike': strike,
                                'opt_type': opt_type,
                                'table_name': name
                            }
                except ValueError:
                    continue

    # Try standard 3-letter month pattern as fallback
    m = TABLE_PATTERN_ALPHA.match(name_upper)
    if m:
        month = MONTH_MAP.get(m.group(3).upper())
        if month:
            return {
                'symbol': m.group(1).upper(),
                'year': 2000 + int(m.group(2)),
                'month': month,
                'day': None,  # No specific day in this format
                'strike': int(m.group(4)),
                'opt_type': m.group(5).upper(),
                'table_name': name
            }

    return None


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
        'errors': [],
        'sample_tables': []  # Store sample table names for debugging
    }

    if verbose:
        print(f"Reading: {sql_path}")
        print(f"Output:  {output_dir}")
        if symbol_filter:
            print(f"Filter:  {symbol_filter}")
        print("Using NUMERIC date format parser")
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
                # Store first few unmatched tables for debugging
                if len(stats['sample_tables']) < 10 and tname.upper().startswith(('NIFTY', 'BANKNIFTY')):
                    stats['sample_tables'].append(tname)
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
                        # Generate filename using month abbreviation for consistency
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
    parser = argparse.ArgumentParser(description='Fast SQL.gz to Parquet extractor (NUMERIC date format)')
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
    print("EXTRACTION COMPLETE (NUMERIC FORMAT)")
    print("=" * 50)
    print(f"Tables found:     {stats['tables_found']}")
    print(f"Tables processed: {stats['tables_processed']}")
    print(f"Tables skipped:   {stats['tables_skipped']}")
    print(f"Total rows:       {stats['rows_total']:,}")

    if stats['sample_tables']:
        print(f"\nUnmatched sample tables (for debugging):")
        for t in stats['sample_tables']:
            print(f"  - {t}")

    if stats['errors']:
        print(f"\nErrors ({len(stats['errors'])}):")
        for err in stats['errors'][:10]:
            print(f"  - {err}")

    return 0


if __name__ == '__main__':
    exit(main())
