#!/usr/bin/env python3
"""
Alternative parquet reader benchmark (keep the original script intact).

Benchmarks four paths over a file sample:
1) DuckDB full column scan
2) DuckDB projected columns
3) PyArrow dataset projected
4) Polars lazy scan over PyArrow dataset (projected)

Defaults:
- base_dir: this script's directory
- sample: first 100 parquet files (or shuffled sample)
- projection columns: timestamp, price, qty, symbol, opt_type, strike

Examples (from /Users/abhishek/workspace/nfo/data/raw/options):
  python parquet_benchmark_2.py
  python parquet_benchmark_2.py --sample 500 --shuffle
  python parquet_benchmark_2.py --columns timestamp,price,qty
"""

from __future__ import annotations

import argparse
import itertools
import random
import sys
import time
from pathlib import Path
from typing import Iterable, List, Sequence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alternate parquet benchmark runner")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory containing parquet files (default: script directory).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=100,
        help="Number of files to read. 0 or negative means all files.",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle before sampling (materializes file list once).",
    )
    parser.add_argument(
        "--columns",
        type=str,
        default="timestamp,price,qty,symbol,opt_type,strike",
        help="Comma-separated columns to project for projected benchmarks.",
    )
    return parser.parse_args()


def sample_files(base_dir: Path, sample: int, shuffle: bool) -> List[Path]:
    iterator: Iterable[Path] = base_dir.glob("*.parquet")
    if shuffle:
        files = list(iterator)
        random.shuffle(files)
        return files if sample <= 0 else files[:sample]
    if sample <= 0:
        return list(iterator)
    return list(itertools.islice(iterator, sample))


def bench(name: str, fn) -> None:
    t0 = time.perf_counter()
    rows = fn()
    elapsed = time.perf_counter() - t0
    rps = rows / elapsed if elapsed else float("inf")
    print(f"{name:<30} {rows:>12,} rows  {elapsed:6.3f} s  {rps:>10,.0f} rows/s")


def require_duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError:
        sys.exit("duckdb is required. Install with: pip install duckdb")
    return duckdb


def require_pyarrow_dataset():
    try:
        import pyarrow.dataset as ds  # type: ignore
    except ImportError:
        sys.exit("pyarrow is required. Install with: pip install pyarrow")
    return ds


def require_polars():
    try:
        import polars as pl  # type: ignore
    except ImportError:
        sys.exit("polars is required for the Polars benchmark. Install with: pip install polars")
    return pl


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir
    if not base_dir.exists():
        sys.exit(f"Base dir does not exist: {base_dir}")

    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    files = sample_files(base_dir, args.sample, args.shuffle)
    if not files:
        sys.exit("No parquet files found to benchmark.")

    file_list = [str(p) for p in files]
    print(f"Benchmarking {len(file_list)} files (sample={args.sample}, shuffle={args.shuffle})")
    print(f"Projected columns: {cols}")

    duckdb = require_duckdb()
    ds = require_pyarrow_dataset()
    pl = require_polars()

    def duckdb_full():
        con = duckdb.connect()
        sql = f"SELECT * FROM read_parquet([{', '.join(repr(p) for p in file_list)}], union_by_name=True)"
        t = con.execute(sql).fetch_arrow_table()
        rows = t.num_rows
        del t, con
        return rows

    def duckdb_project():
        con = duckdb.connect()
        sql = f"SELECT {', '.join(cols)} FROM read_parquet([{', '.join(repr(p) for p in file_list)}], union_by_name=True)"
        t = con.execute(sql).fetch_arrow_table()
        rows = t.num_rows
        del t, con
        return rows

    def pyarrow_dataset_project():
        dataset = ds.dataset(file_list, format="parquet")  # union schemas by name
        t = dataset.to_table(columns=cols, use_threads=True)
        rows = t.num_rows
        del t
        return rows

    def polars_scan_project():
        dataset = ds.dataset(file_list, format="parquet")
        t = pl.scan_pyarrow_dataset(dataset).select(cols).collect()
        rows = t.height
        del t
        return rows

    bench("duckdb full", duckdb_full)
    bench("duckdb projected", duckdb_project)
    bench("pyarrow dataset projected", pyarrow_dataset_project)
    bench("polars scan projected", polars_scan_project)


if __name__ == "__main__":
    main()
