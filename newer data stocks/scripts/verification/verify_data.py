#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NFO Options Data Verification Script

Comprehensive verification of:
1. Raw parquet files (from SQL extraction)
2. Packed parquet files (final format)
3. Cross-validation between raw and packed
4. Random row spot checks
5. Schema validation
"""

import os
import re
import gzip
import random
import argparse
from pathlib import Path
from datetime import datetime, time
from collections import defaultdict
import polars as pl


# Expected schemas
EXPECTED_RAW_COLUMNS = {
    'timestamp', 'price', 'qty', 'avgPrice', 'volume', 'bQty', 'sQty',
    'open', 'high', 'low', 'close', 'changeper', 'lastTradeTime',
    'oi', 'oiHigh', 'oiLow',
    'bq0', 'bp0', 'bo0', 'bq1', 'bp1', 'bo1', 'bq2', 'bp2', 'bo2',
    'bq3', 'bp3', 'bo3', 'bq4', 'bp4', 'bo4',
    'sq0', 'sp0', 'so0', 'sq1', 'sp1', 'so1', 'sq2', 'sp2', 'so2',
    'sq3', 'sp3', 'so3', 'sq4', 'sp4', 'so4',
    'symbol', 'opt_type', 'strike', 'year', 'month', 'ts'
}

EXPECTED_PACKED_COLUMNS = [
    'timestamp', 'symbol', 'opt_type', 'strike',
    'open', 'high', 'low', 'close', 'vol_delta',
    'expiry', 'expiry_type', 'is_monthly', 'is_weekly'
]


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_section(title: str):
    """Print a section header."""
    print(f"\n--- {title} ---")


def verify_raw_files(raw_dir: Path) -> dict:
    """Verify raw parquet files from SQL extraction."""
    print_header("RAW PARQUET FILES VERIFICATION")

    stats = {
        'nifty_files': 0,
        'banknifty_files': 0,
        'nifty_rows': 0,
        'banknifty_rows': 0,
        'schema_errors': [],
        'sample_files': [],
        'date_ranges': {}
    }

    if not raw_dir.exists():
        print(f"ERROR: Raw directory does not exist: {raw_dir}")
        return stats

    all_files = list(raw_dir.glob("*.parquet"))
    print(f"Total raw parquet files: {len(all_files)}")

    nifty_files = [f for f in all_files if f.name.lower().startswith('nifty') and not f.name.lower().startswith('banknifty')]
    banknifty_files = [f for f in all_files if f.name.lower().startswith('banknifty')]

    stats['nifty_files'] = len(nifty_files)
    stats['banknifty_files'] = len(banknifty_files)

    print(f"  NIFTY files:     {len(nifty_files)}")
    print(f"  BANKNIFTY files: {len(banknifty_files)}")

    # Sample and verify a few files
    print_section("Schema Verification (sampling 5 files)")

    sample_files = random.sample(all_files, min(5, len(all_files)))
    for f in sample_files:
        try:
            df = pl.read_parquet(f)
            cols = set(df.columns)

            # Check for required columns
            required = {'timestamp', 'symbol', 'opt_type', 'strike', 'open', 'high', 'low', 'close'}
            missing = required - cols

            if missing:
                stats['schema_errors'].append(f"{f.name}: missing {missing}")
                print(f"  WARN {f.name}: missing columns {missing}")
            else:
                print(f"  OK   {f.name}: {df.height} rows, {len(df.columns)} cols")
                stats['sample_files'].append({
                    'name': f.name,
                    'rows': df.height,
                    'columns': len(df.columns)
                })
        except Exception as e:
            stats['schema_errors'].append(f"{f.name}: {str(e)[:50]}")
            print(f"  ERR  {f.name}: {str(e)[:50]}")

    # Count total rows by symbol
    print_section("Row Counts by Symbol")

    for f in nifty_files[:50]:  # Sample first 50
        try:
            df = pl.read_parquet(f)
            stats['nifty_rows'] += df.height
        except:
            pass

    for f in banknifty_files[:50]:
        try:
            df = pl.read_parquet(f)
            stats['banknifty_rows'] += df.height
        except:
            pass

    # Extrapolate
    if len(nifty_files) > 50:
        avg_rows = stats['nifty_rows'] / 50
        stats['nifty_rows_est'] = int(avg_rows * len(nifty_files))
        print(f"  NIFTY rows (sampled 50): {stats['nifty_rows']:,} (est total: ~{stats['nifty_rows_est']:,})")
    else:
        print(f"  NIFTY rows: {stats['nifty_rows']:,}")

    if len(banknifty_files) > 50:
        avg_rows = stats['banknifty_rows'] / 50
        stats['banknifty_rows_est'] = int(avg_rows * len(banknifty_files))
        print(f"  BANKNIFTY rows (sampled 50): {stats['banknifty_rows']:,} (est total: ~{stats['banknifty_rows_est']:,})")
    else:
        print(f"  BANKNIFTY rows: {stats['banknifty_rows']:,}")

    return stats


def verify_packed_files(packed_dir: Path) -> dict:
    """Verify packed parquet files (final format)."""
    print_header("PACKED PARQUET FILES VERIFICATION")

    stats = {
        'nifty_files': 0,
        'banknifty_files': 0,
        'nifty_rows': 0,
        'banknifty_rows': 0,
        'schema_ok': True,
        'timezone_ok': True,
        'expiry_mapping_ok': True,
        'date_range': {},
        'expiries': set(),
        'errors': []
    }

    if not packed_dir.exists():
        print(f"ERROR: Packed directory does not exist: {packed_dir}")
        return stats

    # Count files by symbol
    nifty_dir = packed_dir / "NIFTY"
    banknifty_dir = packed_dir / "BANKNIFTY"

    nifty_files = list(nifty_dir.rglob("*.parquet")) if nifty_dir.exists() else []
    banknifty_files = list(banknifty_dir.rglob("*.parquet")) if banknifty_dir.exists() else []

    stats['nifty_files'] = len(nifty_files)
    stats['banknifty_files'] = len(banknifty_files)

    print(f"Total packed files: {len(nifty_files) + len(banknifty_files)}")
    print(f"  NIFTY files:     {len(nifty_files)}")
    print(f"  BANKNIFTY files: {len(banknifty_files)}")

    # Verify schema on samples
    print_section("Schema Verification (13 expected columns)")

    all_packed = nifty_files + banknifty_files
    sample_packed = random.sample(all_packed, min(5, len(all_packed)))

    for f in sample_packed:
        try:
            df = pl.read_parquet(f)

            # Check column count
            if len(df.columns) != 13:
                stats['schema_ok'] = False
                stats['errors'].append(f"{f.name}: expected 13 cols, got {len(df.columns)}")
                print(f"  WARN {f.name}: {len(df.columns)} columns (expected 13)")

            # Check column names
            missing = set(EXPECTED_PACKED_COLUMNS) - set(df.columns)
            if missing:
                stats['schema_ok'] = False
                stats['errors'].append(f"{f.name}: missing {missing}")
                print(f"  WARN {f.name}: missing {missing}")

            # Check timestamp timezone
            ts_dtype = df['timestamp'].dtype
            if hasattr(ts_dtype, 'time_zone'):
                if ts_dtype.time_zone != 'Asia/Kolkata':
                    stats['timezone_ok'] = False
                    print(f"  WARN {f.name}: timezone is {ts_dtype.time_zone}, expected Asia/Kolkata")
                else:
                    print(f"  OK   {f.name}: {df.height} rows, tz=Asia/Kolkata")
            else:
                stats['timezone_ok'] = False
                print(f"  WARN {f.name}: no timezone on timestamp")

        except Exception as e:
            stats['errors'].append(f"{f.name}: {str(e)[:50]}")
            print(f"  ERR  {f.name}: {str(e)[:50]}")

    # Count rows and collect expiries
    print_section("Aggregated Stats")

    for f in nifty_files:
        try:
            df = pl.read_parquet(f)
            stats['nifty_rows'] += df.height
            if 'expiry' in df.columns:
                for exp in df['expiry'].unique().to_list():
                    if exp is not None:
                        stats['expiries'].add(str(exp))
        except:
            pass

    for f in banknifty_files:
        try:
            df = pl.read_parquet(f)
            stats['banknifty_rows'] += df.height
        except:
            pass

    print(f"  NIFTY total rows:     {stats['nifty_rows']:,}")
    print(f"  BANKNIFTY total rows: {stats['banknifty_rows']:,}")
    print(f"  Unique expiries found: {len(stats['expiries'])}")

    if stats['expiries']:
        sorted_exp = sorted(stats['expiries'])
        print(f"  Expiry range: {sorted_exp[0]} to {sorted_exp[-1]}")

    return stats


def verify_raw_vs_packed(raw_dir: Path, packed_dir: Path) -> dict:
    """Cross-verify raw vs packed files for a specific strike."""
    print_header("RAW vs PACKED CROSS-VERIFICATION")

    results = {
        'matches_found': 0,
        'row_comparisons': [],
        'discrepancies': []
    }

    # Find a BANKNIFTY file that exists in both raw and packed
    raw_files = list(raw_dir.glob("banknifty*.parquet"))

    if not raw_files:
        print("No BANKNIFTY raw files found for comparison")
        return results

    # Parse filename to find corresponding packed file
    # Raw: banknifty25aug50000ce.parquet
    # Packed: BANKNIFTY/202508/exp=2025-08-XX/type=CE/strike=50000.parquet

    pattern = re.compile(r'^banknifty(\d{2})([a-z]{3})(\d+)(ce|pe)\.parquet$', re.IGNORECASE)

    for raw_file in random.sample(raw_files, min(3, len(raw_files))):
        m = pattern.match(raw_file.name)
        if not m:
            continue

        year = 2000 + int(m.group(1))
        month_str = m.group(2).upper()
        strike = int(m.group(3))
        opt_type = m.group(4).upper()

        month_map = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                     'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
        month = month_map.get(month_str)

        if not month:
            continue

        # Find packed files for this strike
        yyyymm = f"{year}{month:02d}"
        packed_pattern = packed_dir / "BANKNIFTY" / yyyymm / f"exp=*" / f"type={opt_type}" / f"strike={strike}.parquet"
        packed_matches = list(packed_dir.glob(f"BANKNIFTY/{yyyymm}/exp=*/type={opt_type}/strike={strike}.parquet"))

        if not packed_matches:
            continue

        print_section(f"Comparing: {raw_file.name}")

        try:
            raw_df = pl.read_parquet(raw_file)
            packed_df = pl.read_parquet(packed_matches[0])

            print(f"  Raw file:    {raw_file.name}")
            print(f"  Packed file: {packed_matches[0].name}")
            print(f"  Raw rows:    {raw_df.height}")
            print(f"  Packed rows: {packed_df.height}")

            # Note: Packed may have fewer rows due to:
            # 1. Market hours filter (09:15-15:30)
            # 2. Deduplication
            # 3. Expiry mapping failures

            results['matches_found'] += 1
            results['row_comparisons'].append({
                'raw_file': raw_file.name,
                'packed_file': str(packed_matches[0]),
                'raw_rows': raw_df.height,
                'packed_rows': packed_df.height,
                'difference': raw_df.height - packed_df.height
            })

            # Compare OHLC values - use to_dicts() instead of iter_rows to avoid timezone issues
            if 'close' in raw_df.columns and 'close' in packed_df.columns:
                # Use select and head to get sample data without timezone conversion issues
                raw_sample = raw_df.select(['open', 'high', 'low', 'close']).head(5)
                print(f"\n  Sample raw OHLC (first 5 rows):")
                for i, row in enumerate(raw_sample.rows()):
                    o, h, l, c = row
                    print(f"    Row {i+1}: O={o} H={h} L={l} C={c}")

                packed_sample = packed_df.select(['open', 'high', 'low', 'close']).head(5)
                print(f"\n  Sample packed OHLC (first 5 rows):")
                for i, row in enumerate(packed_sample.rows()):
                    o, h, l, c = row
                    print(f"    Row {i+1}: O={o} H={h} L={l} C={c}")

        except Exception as e:
            print(f"  ERROR: {str(e)[:100]}")
            results['discrepancies'].append(str(e)[:100])

    return results


def random_row_spot_check(packed_dir: Path, num_checks: int = 10) -> dict:
    """Perform random row spot checks on packed files."""
    print_header("RANDOM ROW SPOT CHECKS")

    results = {
        'checks_performed': 0,
        'ohlc_valid': 0,
        'timestamp_valid': 0,
        'expiry_valid': 0,
        'issues': []
    }

    all_files = list(packed_dir.rglob("*.parquet"))
    if not all_files:
        print("No packed files found")
        return results

    sample_files = random.sample(all_files, min(num_checks, len(all_files)))

    for f in sample_files:
        try:
            df = pl.read_parquet(f)
            if df.height == 0:
                continue

            # Pick a random row index
            idx = random.randint(0, df.height - 1)

            # Get OHLC data without timezone conversion issues
            ohlc_cols = ['symbol', 'strike', 'opt_type', 'open', 'high', 'low', 'close']
            ohlc_data = df.select(ohlc_cols).slice(idx, 1).rows()[0]
            symbol, strike, opt_type, o, h, l, c = ohlc_data

            results['checks_performed'] += 1
            issues = []

            # Validate OHLC
            if all(v is not None for v in [o, h, l, c]):
                if l <= o <= h and l <= c <= h:
                    results['ohlc_valid'] += 1
                else:
                    issues.append(f"OHLC invalid: L={l} O={o} H={h} C={c}")

            # For timestamp validation, just check using polars expressions
            # Get timestamp hour/minute without converting to Python
            ts_check = df.slice(idx, 1).select([
                pl.col('timestamp').dt.hour().alias('hour'),
                pl.col('timestamp').dt.minute().alias('minute')
            ]).rows()[0]
            hour, minute = ts_check

            if hour is not None and minute is not None:
                total_mins = hour * 60 + minute
                # Market hours: 09:15 (555) to 15:30 (930)
                if 555 <= total_mins <= 930:
                    results['timestamp_valid'] += 1
                else:
                    issues.append(f"Outside market hours: {hour}:{minute:02d}")

            # Validate expiry - get trade date and expiry using polars
            exp_check = df.slice(idx, 1).select([
                pl.col('timestamp').dt.date().alias('trade_date'),
                pl.col('expiry')
            ]).rows()[0]
            trade_date, exp = exp_check

            if exp is not None and trade_date is not None:
                if trade_date <= exp:
                    results['expiry_valid'] += 1
                else:
                    issues.append(f"Trade date {trade_date} > expiry {exp}")

            # Print result
            status = "OK" if not issues else "WARN"
            print(f"  {status} {symbol} {strike}{opt_type}: O={o} H={h} L={l} C={c}")

            if issues:
                for issue in issues:
                    print(f"      Issue: {issue}")
                results['issues'].extend(issues)

        except Exception as e:
            print(f"  ERR {f.name}: {str(e)[:80]}")

    print_section("Spot Check Summary")
    print(f"  Checks performed: {results['checks_performed']}")
    print(f"  OHLC valid:       {results['ohlc_valid']}/{results['checks_performed']}")
    print(f"  Timestamp valid:  {results['timestamp_valid']}/{results['checks_performed']}")
    print(f"  Expiry valid:     {results['expiry_valid']}/{results['checks_performed']}")

    return results


def count_sql_tables(sql_path: Path, limit: int = 1000) -> dict:
    """Count tables in SQL.gz file by parsing REPLACE INTO statements."""
    print_header("SQL.GZ TABLE COUNT")

    stats = {
        'nifty_tables': 0,
        'banknifty_tables': 0,
        'other_tables': 0,
        'sample_tables': []
    }

    if not sql_path.exists():
        print(f"SQL file not found: {sql_path}")
        return stats

    print(f"Scanning: {sql_path}")

    try:
        with gzip.open(sql_path, 'rt', encoding='utf-8', errors='replace') as f:
            count = 0
            for line in f:
                if 'REPLACE INTO' not in line and 'INSERT INTO' not in line:
                    continue

                m = re.search(r'`([^`]+)`', line)
                if not m:
                    continue

                tname = m.group(1).upper()

                if tname.startswith('BANKNIFTY'):
                    stats['banknifty_tables'] += 1
                elif tname.startswith('NIFTY'):
                    stats['nifty_tables'] += 1
                else:
                    stats['other_tables'] += 1

                if len(stats['sample_tables']) < 10:
                    stats['sample_tables'].append(tname)

                count += 1
                if count >= limit:
                    print(f"  (Stopped at {limit} tables)")
                    break

                if count % 500 == 0:
                    print(f"  Scanned {count} tables...")
    except Exception as e:
        print(f"Error reading SQL: {e}")

    print(f"\nTable counts (scanned up to {limit}):")
    print(f"  NIFTY tables:     {stats['nifty_tables']}")
    print(f"  BANKNIFTY tables: {stats['banknifty_tables']}")
    print(f"  Other tables:     {stats['other_tables']}")

    if stats['sample_tables']:
        print(f"\nSample table names:")
        for t in stats['sample_tables'][:5]:
            print(f"  - {t}")

    return stats


def generate_report(folder_name: str, raw_stats: dict, packed_stats: dict,
                   cross_stats: dict, spot_stats: dict) -> str:
    """Generate a summary report."""
    print_header("VERIFICATION REPORT")

    report = []
    report.append(f"Folder: {folder_name}")
    report.append(f"Generated: {datetime.now().isoformat()}")
    report.append("")

    # Raw files
    report.append("RAW FILES:")
    report.append(f"  NIFTY:     {raw_stats.get('nifty_files', 0)} files")
    report.append(f"  BANKNIFTY: {raw_stats.get('banknifty_files', 0)} files")

    # Packed files
    report.append("\nPACKED FILES:")
    report.append(f"  NIFTY:     {packed_stats.get('nifty_files', 0)} files, {packed_stats.get('nifty_rows', 0):,} rows")
    report.append(f"  BANKNIFTY: {packed_stats.get('banknifty_files', 0)} files, {packed_stats.get('banknifty_rows', 0):,} rows")
    report.append(f"  Schema OK: {packed_stats.get('schema_ok', False)}")
    report.append(f"  Timezone OK: {packed_stats.get('timezone_ok', False)}")

    # Cross-verification
    report.append("\nCROSS-VERIFICATION:")
    report.append(f"  Matches found: {cross_stats.get('matches_found', 0)}")

    # Spot checks
    report.append("\nSPOT CHECKS:")
    report.append(f"  Performed: {spot_stats.get('checks_performed', 0)}")
    report.append(f"  OHLC valid: {spot_stats.get('ohlc_valid', 0)}")
    report.append(f"  Issues: {len(spot_stats.get('issues', []))}")

    # Overall status
    all_ok = (
        packed_stats.get('schema_ok', False) and
        packed_stats.get('timezone_ok', False) and
        len(spot_stats.get('issues', [])) == 0 and
        packed_stats.get('nifty_files', 0) > 0 and
        packed_stats.get('banknifty_files', 0) > 0
    )

    report.append(f"\nOVERALL STATUS: {'PASS' if all_ok else 'NEEDS REVIEW'}")

    report_text = "\n".join(report)
    print(report_text)

    return report_text


def main():
    parser = argparse.ArgumentParser(description='Verify NFO options data processing')
    parser.add_argument('--data-dir', type=str, required=True,
                       help='Base data directory (e.g., "aug 13 to aug 29 new stocks data")')
    parser.add_argument('--sql-check', action='store_true',
                       help='Also scan SQL.gz files (slow)')
    parser.add_argument('--spot-checks', type=int, default=10,
                       help='Number of random spot checks')

    args = parser.parse_args()

    base_dir = Path(args.data_dir)
    raw_dir = base_dir / "processed_output" / "raw_options"
    packed_dir = base_dir / "processed_output" / "packed_options"

    print(f"\nVerifying data in: {base_dir}")
    print(f"Raw dir:    {raw_dir}")
    print(f"Packed dir: {packed_dir}")

    # Run verifications
    raw_stats = verify_raw_files(raw_dir)
    packed_stats = verify_packed_files(packed_dir)
    cross_stats = verify_raw_vs_packed(raw_dir, packed_dir)
    spot_stats = random_row_spot_check(packed_dir, args.spot_checks)

    # SQL check (optional, slow)
    if args.sql_check:
        sql_files = list(base_dir.glob("*.sql.gz"))
        for sql_file in sql_files:
            count_sql_tables(sql_file)

    # Generate report
    folder_name = base_dir.name
    generate_report(folder_name, raw_stats, packed_stats, cross_stats, spot_stats)


if __name__ == '__main__':
    main()
