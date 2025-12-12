#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verify packed output matches expected format.
"""

import argparse
from pathlib import Path
import polars as pl


def verify_schema(packed_dir: str, sample_dir: str) -> bool:
    """Verify output schema matches sample."""
    packed_path = Path(packed_dir)
    sample_path = Path(sample_dir)

    # Find first packed file
    packed_files = list(packed_path.rglob("*.parquet"))
    if not packed_files:
        print("ERROR: No packed files found!")
        return False

    # Find first sample file
    sample_files = list(sample_path.rglob("*.parquet"))
    if not sample_files:
        print("ERROR: No sample files found!")
        return False

    print(f"Checking packed file: {packed_files[0]}")
    print(f"Against sample file:  {sample_files[0]}")
    print()

    packed = pl.read_parquet(packed_files[0])
    sample = pl.read_parquet(sample_files[0])

    # Check columns
    packed_cols = set(packed.columns)
    sample_cols = set(sample.columns)

    if packed_cols != sample_cols:
        print("WARNING: Column mismatch!")
        print(f"  Packed columns:  {sorted(packed_cols)}")
        print(f"  Sample columns:  {sorted(sample_cols)}")
        print(f"  Missing in packed: {sample_cols - packed_cols}")
        print(f"  Extra in packed:   {packed_cols - sample_cols}")
    else:
        print("Columns match: OK")

    # Check types
    print("\nSchema comparison:")
    all_ok = True
    for col in sorted(sample_cols):
        if col in packed.columns:
            packed_type = str(packed[col].dtype)
            sample_type = str(sample[col].dtype)
            if packed_type == sample_type:
                print(f"  {col}: {packed_type} == {sample_type} OK")
            else:
                print(f"  {col}: {packed_type} != {sample_type} MISMATCH")
                all_ok = False
        else:
            print(f"  {col}: MISSING in packed")
            all_ok = False

    # Check data quality
    print("\nData quality check:")
    print(f"  Packed file rows:     {packed.height}")
    print(f"  Null timestamps:      {packed['timestamp'].null_count()}")
    print(f"  Null expiry:          {packed['expiry'].null_count()}")
    print(f"  Timestamp has TZ:     {'Asia/Kolkata' in str(packed['timestamp'].dtype)}")

    # Show first few rows
    print("\nFirst 3 rows of packed data:")
    print(packed.head(3))

    return all_ok


def main():
    parser = argparse.ArgumentParser(description='Verify packed output')
    parser.add_argument('--packed-dir', type=str, required=True, help='Packed output directory')
    parser.add_argument('--sample-dir', type=str, required=True, help='Sample data directory')

    args = parser.parse_args()

    ok = verify_schema(args.packed_dir, args.sample_dir)

    if ok:
        print("\n" + "=" * 50)
        print("VERIFICATION PASSED")
        print("=" * 50)
        return 0
    else:
        print("\n" + "=" * 50)
        print("VERIFICATION FAILED - Check warnings above")
        print("=" * 50)
        return 1


if __name__ == '__main__':
    exit(main())
