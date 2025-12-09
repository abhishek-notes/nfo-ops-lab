#!/usr/bin/env python3
"""
Strategy benchmark on parquet files (100 files by default).

Strategy:
- Go long when EMA(5) > EMA(21) using the 'price' column (cast to float).
- No shorting.
- Exit when EMA(21) >= EMA(5) or at the final tick of the file.
- Position size: 1 unit. P&L per file is the sum of trade deltas.

Outputs:
- Prints timings and row counts for each approach.
- Writes CSV (strategy_results.csv) with file path and P&L from the PyArrow run.

Approaches benchmarked:
- DuckDB read -> Python EMA logic
- PyArrow dataset read -> Python EMA logic
- Polars read -> Python EMA logic

Usage (from /Users/abhishek/workspace/nfo/data/raw/options):
  python strategy_benchmark.py
  python strategy_benchmark.py --sample 200 --shuffle
"""

from __future__ import annotations

import argparse
import csv
import itertools
import math
import random
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EMA(5/21) long-only strategy benchmark")
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
        help="Number of files to process. 0 or negative means all files.",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle before sampling (materializes file list once).",
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


def ema(prices: Sequence[float], span: int) -> List[float]:
    alpha = 2.0 / (span + 1.0)
    out: List[float] = []
    prev = None
    for p in prices:
        if prev is None:
            prev = p
        else:
            prev = alpha * p + (1 - alpha) * prev
        out.append(prev)
    return out


def compute_pnl(prices: Sequence[float]) -> float:
    if not prices:
        return 0.0
    ema5 = ema(prices, 5)
    ema21 = ema(prices, 21)
    holding = False
    entry = 0.0
    pnl = 0.0
    for price, e5, e21 in zip(prices, ema5, ema21):
        if not holding and e5 > e21:
            holding = True
            entry = price
        elif holding and e5 <= e21:
            pnl += price - entry
            holding = False
    if holding:
        pnl += prices[-1] - entry
    return pnl


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
        sys.exit("polars is required. Install with: pip install polars")
    return pl


def load_duckdb(path: Path):
    duckdb = require_duckdb()
    con = duckdb.connect()
    # Only select needed columns to avoid schema mismatches; union_by_name to tolerate extra columns.
    table = con.execute(
        f"SELECT timestamp, price FROM read_parquet('{path}', union_by_name=True)"
    ).fetch_arrow_table()
    con.close()
    return table


def load_pyarrow(path: Path):
    ds = require_pyarrow_dataset()
    dataset = ds.dataset(str(path), format="parquet")
    return dataset.to_table(columns=["timestamp", "price"], use_threads=True)


def load_polars(path: Path):
    pl = require_polars()
    return pl.read_parquet(path, columns=["timestamp", "price"])


def table_to_prices_arrow(table) -> List[float]:
    import polars as pl

    df = pl.from_arrow(table)
    if "timestamp" not in df.columns or "price" not in df.columns:
        raise ValueError("Required columns missing")
    df = (
        df.select(["timestamp", "price"])
        .drop_nulls(subset=["timestamp", "price"])
        .with_columns(pl.col("price").cast(pl.Float64))
        .sort("timestamp")
    )
    return df["price"].to_list()


def table_to_prices_polars(df) -> List[float]:
    import polars as pl

    if "timestamp" not in df.columns or "price" not in df.columns:
        raise ValueError("Required columns missing")
    df = (
        df.select(["timestamp", "price"])
        .drop_nulls(subset=["timestamp", "price"])
        .with_columns(pl.col("price").cast(pl.Float64))
        .sort("timestamp")
    )
    return df["price"].to_list()


def process_files(
    files: Sequence[Path],
    loader_name: str,
    loader_fn,
    converter_fn,
) -> Tuple[Dict[str, float], float, int]:
    """
    Returns (pnl_by_file, elapsed_seconds, total_rows_processed)
    """
    pnl_by_file: Dict[str, float] = {}
    total_rows = 0
    t0 = time.perf_counter()
    for path in files:
        try:
            table_or_df = loader_fn(path)
            prices = converter_fn(table_or_df)
            total_rows += len(prices)
            pnl = compute_pnl(prices)
            pnl_by_file[str(path)] = pnl
        except Exception as e:
            print(f"[WARN] {loader_name}: skipping {path} due to error: {e}")
    elapsed = time.perf_counter() - t0
    return pnl_by_file, elapsed, total_rows


def compare_results(ref: Dict[str, float], other: Dict[str, float], label: str) -> None:
    for k, v in ref.items():
        if k not in other:
            raise ValueError(f"{label} missing result for {k}")
        if not math.isclose(v, other[k], rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError(f"{label} mismatch on {k}: ref={v}, other={other[k]}")


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir
    if not base_dir.exists():
        sys.exit(f"Base dir does not exist: {base_dir}")

    files = sample_files(base_dir, args.sample, args.shuffle)
    if not files:
        sys.exit("No parquet files found to process.")

    print(f"Processing {len(files)} files (sample={args.sample}, shuffle={args.shuffle})")

    duckdb = require_duckdb()
    ds = require_pyarrow_dataset()
    pl = require_polars()
    del duckdb, ds, pl  # just to assert availability early

    duckdb_res, duckdb_time, duckdb_rows = process_files(
        files, "duckdb", load_duckdb, table_to_prices_arrow
    )
    print(f"DuckDB: {duckdb_rows:,} rows  {duckdb_time:.3f} s  {duckdb_rows/duckdb_time:,.0f} rows/s")

    arrow_res, arrow_time, arrow_rows = process_files(
        files, "pyarrow", load_pyarrow, table_to_prices_arrow
    )
    print(f"PyArrow dataset: {arrow_rows:,} rows  {arrow_time:.3f} s  {arrow_rows/arrow_time:,.0f} rows/s")

    polars_res, polars_time, polars_rows = process_files(
        files, "polars", load_polars, table_to_prices_polars
    )
    print(f"Polars: {polars_rows:,} rows  {polars_time:.3f} s  {polars_rows/polars_time:,.0f} rows/s")

    # Consistency checks
    compare_results(arrow_res, duckdb_res, "DuckDB vs Arrow")
    compare_results(arrow_res, polars_res, "Polars vs Arrow")
    print("All approaches produced matching P&L per file.")

    # Write CSV using Arrow results as canonical
    csv_path = base_dir / "strategy_results.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["file", "pnl"])
        for path_str, pnl in arrow_res.items():
            writer.writerow([path_str, pnl])
    print(f"Wrote per-file P&L to {csv_path}")


if __name__ == "__main__":
    main()
