#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NFO New Data Processing Pipeline

This script:
1. Extracts tables from SQL.gz dumps (BANKNIFTY/NIFTY options)
2. Converts them to raw parquet files (matching existing raw format)
3. Processes them through the packing pipeline (matching existing packed format)

The output will be 100% consistent with the older processed data.
"""

import os
import re
import gzip
import argparse
from pathlib import Path
from datetime import datetime, date, time
from io import StringIO
import polars as pl

# ============== CONFIGURATION ==============

# Column definitions matching the SQL schema
SQL_COLUMNS = [
    'timestamp', 'price', 'qty', 'avgPrice', 'volume', 'bQty', 'sQty',
    'open', 'high', 'low', 'close', 'changeper', 'lastTradeTime',
    'oi', 'oiHigh', 'oiLow',
    'bq0', 'bp0', 'bo0', 'bq1', 'bp1', 'bo1', 'bq2', 'bp2', 'bo2',
    'bq3', 'bp3', 'bo3', 'bq4', 'bp4', 'bo4',
    'sq0', 'sp0', 'so0', 'sq1', 'sp1', 'so1', 'sq2', 'sp2', 'so2',
    'sq3', 'sp3', 'so3', 'sq4', 'sp4', 'so4'
]

# Regex to parse table names like BANKNIFTY25DEC43500PE or NIFTY25JAN24000CE
TABLE_NAME_PATTERN = re.compile(
    r'^(BANKNIFTY|NIFTY)(\d{2})([A-Z]{3})(\d+)(CE|PE)$',
    re.IGNORECASE
)

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}


def parse_table_name(table_name: str) -> dict | None:
    """
    Parse table name like BANKNIFTY25DEC43500PE into components.
    Returns dict with symbol, opt_type, strike, year, month or None if invalid.
    """
    m = TABLE_NAME_PATTERN.match(table_name.upper())
    if not m:
        return None

    symbol = m.group(1).upper()
    year_2digit = int(m.group(2))
    month_str = m.group(3).upper()
    strike = int(m.group(4))
    opt_type = m.group(5).upper()

    # Convert 2-digit year to 4-digit (assume 2000s)
    year = 2000 + year_2digit
    month = MONTH_MAP.get(month_str)

    if month is None:
        return None

    return {
        'symbol': symbol,
        'opt_type': opt_type,
        'strike': strike,
        'year': year,
        'month': month,
        'table_name': table_name
    }


def parse_sql_values(values_str: str) -> list:
    """
    Parse SQL VALUES string into list of tuples.
    Handles datetime strings and numeric values.
    """
    rows = []
    # Split by ),( but handle the outer parens
    # The format is: ('val1','val2',...),('val1','val2',...)

    # Remove leading/trailing whitespace
    values_str = values_str.strip()
    if values_str.endswith(';'):
        values_str = values_str[:-1]

    # Parse each row
    row_pattern = re.compile(r"\(([^)]+)\)")
    for match in row_pattern.finditer(values_str):
        row_str = match.group(1)
        values = []

        # Parse individual values (handle quoted strings and numbers)
        in_quote = False
        current = []
        for char in row_str:
            if char == "'" and not in_quote:
                in_quote = True
            elif char == "'" and in_quote:
                in_quote = False
            elif char == ',' and not in_quote:
                val = ''.join(current).strip()
                values.append(val)
                current = []
            else:
                current.append(char)

        # Don't forget the last value
        if current:
            val = ''.join(current).strip()
            values.append(val)

        if len(values) == len(SQL_COLUMNS):
            rows.append(values)

    return rows


def extract_tables_from_sql_gz(sql_gz_path: str, output_dir: str, symbol_filter: str = None) -> dict:
    """
    Extract all tables from SQL.gz file and convert to parquet.

    Args:
        sql_gz_path: Path to .sql.gz file
        output_dir: Directory to write raw parquet files
        symbol_filter: If set, only process tables for this symbol (BANKNIFTY or NIFTY)

    Returns:
        dict with stats: {tables_found, tables_processed, rows_total}
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stats = {'tables_found': 0, 'tables_processed': 0, 'rows_total': 0, 'errors': []}

    current_table = None
    current_meta = None
    collecting_data = False
    data_buffer = []

    print(f"Reading {sql_gz_path}...")

    with gzip.open(sql_gz_path, 'rt', encoding='utf-8', errors='replace') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Look for CREATE TABLE statements
            if line.startswith('CREATE TABLE'):
                # Extract table name
                m = re.search(r'`([^`]+)`', line)
                if m:
                    table_name = m.group(1)
                    meta = parse_table_name(table_name)
                    if meta:
                        if symbol_filter is None or meta['symbol'] == symbol_filter.upper():
                            current_table = table_name
                            current_meta = meta
                            stats['tables_found'] += 1
                            if stats['tables_found'] % 100 == 0:
                                print(f"  Found {stats['tables_found']} tables...")

            # Look for REPLACE INTO or INSERT INTO for current table
            if current_table and f'`{current_table}`' in line:
                if 'REPLACE INTO' in line or 'INSERT INTO' in line:
                    collecting_data = True
                    # Extract VALUES part
                    if 'VALUES' in line:
                        values_start = line.index('VALUES') + 6
                        data_buffer.append(line[values_start:])

            # Continue collecting data lines
            elif collecting_data:
                if line.startswith('UNLOCK TABLES') or line.startswith('LOCK TABLES') or line.startswith('--'):
                    # End of data for this table
                    if data_buffer and current_meta:
                        try:
                            rows = parse_sql_values(''.join(data_buffer))
                            if rows:
                                df = create_dataframe(rows, current_meta)
                                if df is not None and df.height > 0:
                                    # Generate output filename
                                    fname = f"{current_meta['symbol'].lower()}{current_meta['year'] % 100}{MONTH_MAP_REV[current_meta['month']]}{current_meta['strike']}{current_meta['opt_type'].lower()}.parquet"
                                    out_file = output_path / fname
                                    df.write_parquet(str(out_file))
                                    stats['tables_processed'] += 1
                                    stats['rows_total'] += df.height
                                    if stats['tables_processed'] % 50 == 0:
                                        print(f"  Processed {stats['tables_processed']} tables, {stats['rows_total']:,} rows...")
                        except Exception as e:
                            stats['errors'].append(f"{current_table}: {str(e)}")

                    collecting_data = False
                    data_buffer = []
                    current_table = None
                    current_meta = None
                elif not line.startswith('/*') and not line.startswith('--'):
                    data_buffer.append(line)

    # Handle last table if file doesn't end with UNLOCK
    if collecting_data and data_buffer and current_meta:
        try:
            rows = parse_sql_values(''.join(data_buffer))
            if rows:
                df = create_dataframe(rows, current_meta)
                if df is not None and df.height > 0:
                    fname = f"{current_meta['symbol'].lower()}{current_meta['year'] % 100}{MONTH_MAP_REV[current_meta['month']]}{current_meta['strike']}{current_meta['opt_type'].lower()}.parquet"
                    out_file = output_path / fname
                    df.write_parquet(str(out_file))
                    stats['tables_processed'] += 1
                    stats['rows_total'] += df.height
        except Exception as e:
            stats['errors'].append(f"{current_table}: {str(e)}")

    return stats


# Reverse month map for filename generation
MONTH_MAP_REV = {v: k.lower() for k, v in MONTH_MAP.items()}


def create_dataframe(rows: list, meta: dict) -> pl.DataFrame:
    """
    Create a Polars DataFrame from parsed SQL rows.
    Adds symbol, opt_type, strike, year, month columns to match raw parquet format.
    """
    if not rows:
        return None

    # Build column data
    data = {col: [] for col in SQL_COLUMNS}

    for row in rows:
        for i, col in enumerate(SQL_COLUMNS):
            val = row[i] if i < len(row) else None
            data[col].append(val)

    # Create DataFrame with proper types
    df = pl.DataFrame(data)

    # Convert timestamp columns
    df = df.with_columns([
        pl.col('timestamp').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False).alias('timestamp'),
        pl.col('lastTradeTime').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S', strict=False).alias('lastTradeTime'),
    ])

    # Convert numeric columns
    numeric_float = ['price', 'avgPrice', 'open', 'high', 'low', 'close', 'changeper',
                     'bp0', 'bp1', 'bp2', 'bp3', 'bp4', 'sp0', 'sp1', 'sp2', 'sp3', 'sp4']
    numeric_int = ['qty', 'volume', 'bQty', 'sQty', 'oi', 'oiHigh', 'oiLow',
                   'bq0', 'bo0', 'bq1', 'bo1', 'bq2', 'bo2', 'bq3', 'bo3', 'bq4', 'bo4',
                   'sq0', 'so0', 'sq1', 'so1', 'sq2', 'so2', 'sq3', 'so3', 'sq4', 'so4']

    for col in numeric_float:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    for col in numeric_int:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    # Add metadata columns (matching raw parquet format)
    df = df.with_columns([
        pl.lit(meta['symbol']).alias('symbol'),
        pl.lit(meta['opt_type']).alias('opt_type'),
        pl.lit(meta['strike']).cast(pl.Int32).alias('strike'),
        pl.lit(meta['year']).cast(pl.Int32).alias('year'),
        pl.lit(meta['month']).cast(pl.Int32).alias('month'),
    ])

    # Add 'ts' column (copy of timestamp for 1970 bug handling)
    df = df.with_columns(pl.col('timestamp').alias('ts'))

    return df


# ============== PACKING FUNCTIONS (from simple_pack.py) ==============

def load_calendar(path: str) -> pl.DataFrame:
    """Load and prepare expiry calendar."""
    cal = pl.read_csv(path)
    cal = cal.rename({
        "Instrument": "symbol",
        "Final_Expiry": "expiry",
        "Expiry_Type": "kind",
    })
    cal = cal.select(
        pl.col("symbol").str.to_uppercase(),
        pl.col("kind").str.to_lowercase(),
        pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"),
    ).drop_nulls(["symbol", "kind", "expiry"])

    # week_index for weekly expiries per (symbol, year, month)
    cal = (
        cal
        .with_columns([
            pl.col("expiry").dt.year().alias("_y"),
            pl.col("expiry").dt.month().alias("_m"),
        ])
        .sort(["symbol", "_y", "_m", "expiry"])
        .with_columns([
            pl.when(pl.col("kind") == "weekly")
              .then(pl.col("expiry").rank(method="dense").over(["symbol", "_y", "_m", "kind"]))
              .otherwise(0)
              .cast(pl.Int16)
              .alias("week_index"),
            (pl.col("kind") == "monthly").cast(pl.Int8).alias("is_monthly"),
            (pl.col("kind") == "weekly").cast(pl.Int8).alias("is_weekly"),
        ])
        .select(["symbol", "kind", "expiry", "week_index", "is_monthly", "is_weekly"])
        .unique()
        .sort(["symbol", "expiry"])
    )
    return cal


def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ensure df['timestamp'] is Datetime (IST). Use 'ts' when vendor left 1970.
    """
    cols = set(df.columns)
    if "timestamp" not in cols and "ts" in cols:
        df = df.with_columns(pl.col("ts").alias("timestamp"))

    if "timestamp" not in df.columns:
        return df

    # Cast timestamp to Datetime
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt in (pl.Int16, pl.Int32, pl.Int64, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float64, pl.Float32):
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))

    # Fix 1970s using 'ts' if present
    if "ts" in df.columns:
        if df["ts"].dtype == pl.Utf8:
            ts_parsed = pl.col("ts").str.strptime(pl.Datetime, strict=False)
        else:
            ts_parsed = pl.col("ts").cast(pl.Datetime, strict=False)
        df = df.with_columns(
            pl.when(pl.col("timestamp").dt.year() <= 1971)
              .then(ts_parsed)
              .otherwise(pl.col("timestamp"))
              .alias("timestamp")
        )

    # Attach timezone (no shifting) - CRITICAL: use replace_time_zone not convert
    df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata").alias("timestamp"))
    return df


def ensure_ohlc(df: pl.DataFrame) -> pl.DataFrame:
    """Guarantee OHLC exists and sanitize vendor zeros."""
    close_sources = [
        "close", "Close", "ltp", "LTP", "price", "Price", "last", "Last",
        "closePrice", "ClosePrice", "avgPrice", "AvgPrice", "avg_price"
    ]
    close_expr = None
    for c in close_sources:
        if c in df.columns:
            close_expr = pl.col(c)
            break

    if "close" not in df.columns:
        if close_expr is not None:
            df = df.with_columns(close_expr.cast(pl.Float64, strict=False).alias("close"))
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("close"))

    for name in ("open", "high", "low"):
        if name not in df.columns:
            df = df.with_columns(pl.col("close").alias(name))

    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ("open", "high", "low", "close")])

    # Repair zeros/nulls
    df = df.with_columns([
        pl.when((pl.col("open") <= 0) | pl.col("open").is_null()).then(pl.col("close")).otherwise(pl.col("open")).alias("open"),
        pl.when((pl.col("high") <= 0) | pl.col("high").is_null()).then(pl.max_horizontal("open", "close")).otherwise(pl.col("high")).alias("high"),
        pl.when((pl.col("low") <= 0) | pl.col("low").is_null()).then(pl.min_horizontal("open", "close")).otherwise(pl.col("low")).alias("low"),
    ])
    # Enforce bounds
    df = df.with_columns([
        pl.min_horizontal("low", "open", "close").alias("low"),
        pl.max_horizontal("high", "open", "close").alias("high"),
    ])
    return df


def compute_vol_delta(df: pl.DataFrame) -> pl.DataFrame:
    """Compute volume delta from cumulative volume."""
    if "volume" in df.columns:
        df = df.with_columns(pl.col("volume").cast(pl.Int64, strict=False).alias("_vol"))
        df = df.sort("timestamp").with_columns(
            pl.col("_vol").diff().clip(lower_bound=0).fill_null(0).alias("vol_delta")
        ).drop("_vol")
    elif "qty" in df.columns:
        df = df.with_columns(pl.col("qty").cast(pl.Int64, strict=False).fill_null(0).alias("vol_delta"))
    else:
        df = df.with_columns(pl.lit(0, dtype=pl.Int64).alias("vol_delta"))
    return df


def process_raw_file(path: str, cal: pl.DataFrame) -> pl.DataFrame | None:
    """Process a single raw parquet file and return packed DataFrame."""
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None

    # Get metadata from dataframe columns
    if 'symbol' not in df.columns or 'opt_type' not in df.columns or 'strike' not in df.columns:
        print(f"Skip (missing metadata): {path}")
        return None

    df = normalize_timestamp(df)
    if "timestamp" not in df.columns:
        print(f"Error {path}: no timestamp")
        return None

    df = ensure_ohlc(df)
    df = compute_vol_delta(df)

    # Keep one row per timestamp
    df = df.filter(pl.col("timestamp").is_not_null()).unique(["timestamp"]).sort("timestamp")

    # Market hours only (09:15-15:30 IST)
    df = df.filter(
        (pl.col("timestamp").dt.time() >= time(9, 15, 0)) &
        (pl.col("timestamp").dt.time() <= time(15, 30, 0))
    )

    if df.height == 0:
        return None

    # Add trade_date and map to next expiry
    df = df.with_columns(pl.col("timestamp").dt.date().alias("trade_date"))
    cal_sorted = cal.sort(["symbol", "expiry"])
    df_sorted = df.sort(["symbol", "trade_date"])
    dfj = df_sorted.join_asof(
        cal_sorted,
        left_on="trade_date",
        right_on="expiry",
        by="symbol",
        strategy="forward",
    )

    # Drop rows that failed to map
    dfj = dfj.filter(pl.col("expiry").is_not_null())

    if dfj.height == 0:
        return None

    # Rename kind -> expiry_type
    dfj = dfj.rename({"kind": "expiry_type"})

    # Final columns only
    dfj = dfj.select([
        "timestamp", "symbol", "opt_type", "strike",
        "open", "high", "low", "close", "vol_delta",
        "expiry", "expiry_type", "is_monthly", "is_weekly",
    ])
    return dfj


def write_partition(out_dir: Path, g: pl.DataFrame):
    """Write one parquet file per (symbol, expiry, opt_type, strike)."""
    symbol = g["symbol"][0]
    opt_type = g["opt_type"][0]
    strike = int(g["strike"][0])
    expiry = g["expiry"][0]  # python date
    yyyymm = f"{expiry.year:04d}{expiry.month:02d}"
    exp_str = expiry.strftime("%Y-%m-%d")

    out_path = out_dir / symbol / yyyymm / f"exp={exp_str}" / f"type={opt_type}" / f"strike={strike}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Final safety: keep only expected columns
    keep = {
        "timestamp", "symbol", "opt_type", "strike",
        "open", "high", "low", "close", "vol_delta",
        "expiry", "expiry_type", "is_monthly", "is_weekly",
    }
    if any(c not in keep for c in g.columns):
        g = g.select(sorted(list(keep), key=list(keep).index))

    try:
        if out_path.exists():
            old = pl.read_parquet(str(out_path))
            merged = pl.concat([old, g], how="vertical_relaxed").unique(["timestamp"]).sort("timestamp")
        else:
            merged = g.unique(["timestamp"]).sort("timestamp")
        merged.write_parquet(str(out_path), compression="zstd", compression_level=3, statistics=True)
    except Exception as e:
        print(f"Write error {out_path}: {e}")
        return
    print(f"  wrote {out_path} ({g.height} rows)")


def pack_raw_files(raw_dir: str, out_dir: str, cal_path: str, limit: int = 0, flush_every: int = 500):
    """
    Process all raw parquet files and write packed output.
    """
    import glob as glob_module

    cal = load_calendar(cal_path)

    files = glob_module.glob(os.path.join(raw_dir, "*.parquet"))
    files.sort()
    if limit and limit > 0:
        files = files[:limit]
    print(f"Found {len(files)} raw files to process")

    out_path = Path(out_dir)
    buckets: dict[tuple[str, date, str, int], list[pl.DataFrame]] = {}

    for i, path in enumerate(files, 1):
        if i % 100 == 0:
            print(f"... {i}/{len(files)}")

        df = process_raw_file(path, cal)
        if df is None or df.height == 0:
            continue

        # Split by expiry -> append to bucket
        for exp in df.select("expiry").unique().to_series():
            g = df.filter(pl.col("expiry") == exp)
            sym = g["symbol"][0]
            opt = g["opt_type"][0]
            k = int(g["strike"][0])
            key = (sym, exp, opt, k)
            buckets.setdefault(key, []).append(g)

        # Periodic flush
        if i % flush_every == 0:
            print(f"\nFLUSH @{i} files ...")
            flush_buckets(out_path, buckets)

    # Final flush
    print(f"\nFinal flush ({len(buckets)} partitions pending)...")
    flush_buckets(out_path, buckets)
    print("Done packing.")


def flush_buckets(out_dir: Path, buckets: dict):
    """Flush all in-memory buckets to disk and clear them."""
    if not buckets:
        return
    print(f"Flushing {len(buckets)} partitions to disk...")
    for (symbol, expiry, opt_type, strike), chunks in list(buckets.items()):
        out = pl.concat(chunks, how="vertical_relaxed").unique(["timestamp"]).sort("timestamp")
        write_partition(out_dir, out)
    buckets.clear()


# ============== MAIN ==============

def main():
    parser = argparse.ArgumentParser(description='Process NFO SQL.gz files to packed parquet')
    parser.add_argument('--input-dir', type=str,
                        default='/Users/abhishek/workspace/nfo/newer data stocks/new 2025 data/nov 4 to nov 18 new stocks data',
                        help='Directory containing SQL.gz files')
    parser.add_argument('--output-dir', type=str,
                        default='/Users/abhishek/workspace/nfo/newer data stocks/new 2025 data/nov 4 to nov 18 new stocks data/processed_output',
                        help='Output directory for processed data')
    parser.add_argument('--calendar', type=str,
                        default='/workspace/meta/expiry_calendar.csv',
                        help='Path to expiry calendar CSV')
    parser.add_argument('--step', type=str, choices=['extract', 'pack', 'all'], default='all',
                        help='Which step to run: extract (SQL to raw parquet), pack (raw to packed), or all')
    parser.add_argument('--symbol', type=str, choices=['BANKNIFTY', 'NIFTY', None], default=None,
                        help='Process only this symbol (optional)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of files to process (0 = all)')

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    raw_dir = output_dir / 'raw_options'
    packed_dir = output_dir / 'packed_options'

    print("=" * 60)
    print("NFO New Data Processing Pipeline")
    print("=" * 60)
    print(f"Input directory:  {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Calendar:         {args.calendar}")
    print(f"Step:             {args.step}")
    if args.symbol:
        print(f"Symbol filter:    {args.symbol}")
    print("=" * 60)

    # Step 1: Extract SQL.gz to raw parquet
    if args.step in ['extract', 'all']:
        print("\n[STEP 1] Extracting SQL.gz files to raw parquet...")

        sql_files = list(input_dir.glob('*.sql.gz'))
        print(f"Found {len(sql_files)} SQL.gz files")

        for sql_file in sql_files:
            fname = sql_file.name.lower()

            # Determine which symbol this file contains
            if 'bankopt' in fname:
                symbol_in_file = 'BANKNIFTY'
            elif 'niftyopt' in fname:
                symbol_in_file = 'NIFTY'
            else:
                print(f"  Skipping {sql_file.name} (not options data)")
                continue

            # Apply symbol filter if specified
            if args.symbol and symbol_in_file != args.symbol:
                print(f"  Skipping {sql_file.name} (symbol filter)")
                continue

            print(f"\nProcessing {sql_file.name}...")
            stats = extract_tables_from_sql_gz(
                str(sql_file),
                str(raw_dir),
                symbol_filter=symbol_in_file
            )
            print(f"  Tables found: {stats['tables_found']}")
            print(f"  Tables processed: {stats['tables_processed']}")
            print(f"  Total rows: {stats['rows_total']:,}")
            if stats['errors']:
                print(f"  Errors: {len(stats['errors'])}")
                for err in stats['errors'][:5]:
                    print(f"    - {err}")

    # Step 2: Pack raw parquet to final format
    if args.step in ['pack', 'all']:
        print("\n[STEP 2] Packing raw parquet files...")

        if not raw_dir.exists():
            print(f"Error: Raw directory not found: {raw_dir}")
            print("Run with --step extract first, or --step all")
            return

        pack_raw_files(
            str(raw_dir),
            str(packed_dir),
            args.calendar,
            limit=args.limit,
            flush_every=500
        )

    print("\n" + "=" * 60)
    print("Processing complete!")
    print(f"Raw parquet files:    {raw_dir}")
    print(f"Packed parquet files: {packed_dir}")
    print("=" * 60)


if __name__ == '__main__':
    main()
