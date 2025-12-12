#!/usr/bin/env python3
"""
One-Time Script: Re-sort Packed Data

Problem: PyArrow write_dataset() doesn't preserve sort order
Solution: Read each file, sort properly, and overwrite

This enables zero-copy sorted reads for 50-100M rows/sec performance.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import time

import polars as pl


def resort_file(file_path: Path, dry_run: bool = False) -> dict:
    """
    Re-sort a single parquet file in place.
    
    Sort order: expiry → opt_type → strike → timestamp
    """
    try:
        # Read file
        df = pl.read_parquet(file_path)
        rows = len(df)
        
        # Check if already sorted
        is_sorted = (
            df['expiry'].is_sorted() and
            all(
                df.filter(pl.col('expiry') == exp).select(pl.col('opt_type').is_sorted()).item()
                for exp in df['expiry'].unique()
            )
        )
        
        if is_sorted:
            return {"file": str(file_path), "rows": rows, "action": "already_sorted"}
        
        if dry_run:
            return {"file": str(file_path), "rows": rows, "action": "needs_sort"}
        
        # Sort
        df_sorted = df.sort(['expiry', 'opt_type', 'strike', 'timestamp'])
        
        # Overwrite file
        df_sorted.write_parquet(
            file_path,
            compression="zstd",
            statistics=True,  # Write min/max stats for row group skipping
            row_group_size=100_000  # Optimize for filtering
        )
        
        return {"file": str(file_path), "rows": rows, "action": "sorted"}
    
    except Exception as e:
        return {"file": str(file_path), "rows": 0, "action": f"error: {e}"}


def main():
    parser = argparse.ArgumentParser(
        description="Re-sort packed parquet files for optimal performance"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("../data/options_date_packed_FULL"),
        help="Root directory of packed data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check which files need sorting without modifying"
    )
    
    args = parser.parse_args()
    
    # Find all parquet files
    files = list(args.data_dir.rglob("*.parquet"))
    print(f"Found {len(files)} parquet files")
   
    if args.dry_run:
        print("DRY RUN - no files will be modified")
    
    print()
    
    # Process files
    t0 = time.time()
    results = []
    
    for i, f in enumerate(files, 1):
        result = resort_file(f, dry_run=args.dry_run)
        results.append(result)
        
        if i % 10 == 0:
            elapsed = time.time() - t0
            print(f"Progress: {i}/{len(files)} files ({elapsed:.1f}s)")
    
    elapsed = time.time() - t0
    
    # Summary
    print("\n" + "="*70)
    print("RESORT SUMMARY")
    print("="*70)
    
    already_sorted = sum(1 for r in results if r['action'] == 'already_sorted')
    needs_sort = sum(1 for r in results if r['action'] == 'needs_sort')
    sorted_now = sum(1 for r in results if r['action'] == 'sorted')
    errors = sum(1 for r in results if 'error' in r['action'])
    
    print(f"Already sorted: {already_sorted}")
    print(f"Needs sorting:  {needs_sort if args.dry_run else 'N/A'}")
    print(f"Sorted now:     {sorted_now}")
    print(f"Errors:         {errors}")
    print(f"\nTime elapsed:   {elapsed:.1f}s")
    
    if args.dry_run and needs_sort > 0:
        print(f"\nRun without --dry-run to sort {needs_sort} files")


if __name__ == "__main__":
    main()
