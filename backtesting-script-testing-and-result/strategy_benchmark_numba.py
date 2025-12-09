#!/usr/bin/env python3
"""
Order-book-aware EMA(5/21) strategy using Polars + Numba and process-level parallelism.

Strategy:
- Go long when EMA5 > EMA21 AND spread_bps <= 5 bps AND volume >= 1.
- Exit on EMA21 >= EMA5 or last tick. No shorts, 1-unit size.

Metrics per file:
- pnl, trades, total_volume, avg_spread, avg_bid_qty, avg_ask_qty, rows

Outputs:
- Prints timing.
- Writes per-file metrics to strategy_results_numba.csv in the base directory.

Usage (from /Users/abhishek/workspace/nfo/data/raw/options):
  python strategy_benchmark_numba.py --sample 100 --workers 16
"""

from __future__ import annotations

import argparse
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import numpy as np
import polars as pl
from numba import njit


@njit
def run_strategy(prices, ema5, ema21, bid0, ask0, volume):
    n = len(prices)
    pnl = 0.0
    trades = 0
    pos = 0
    entry = 0.0
    for i in range(1, n):
        spread_ok = False
        if ask0[i] > 0.0 and bid0[i] > 0.0:
            mid = 0.5 * (ask0[i] + bid0[i])
            if mid > 0.0:
                spread_bps = (ask0[i] - bid0[i]) / mid
                spread_ok = spread_bps <= 0.0005
        vol_ok = volume[i] >= 1.0
        if pos == 0:
            if ema5[i] > ema21[i] and spread_ok and vol_ok:
                pos = 1
                entry = prices[i]
                trades += 1
        else:
            if ema21[i] >= ema5[i] or i == n - 1:
                pnl += prices[i] - entry
                pos = 0
    return pnl, trades


@dataclass
class Metrics:
    file: str
    pnl: float
    trades: int
    total_volume: float
    avg_spread: float
    avg_bid_qty: float
    avg_ask_qty: float
    rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Numba + Polars orderbook strategy benchmark")
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
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Process pool size (default: min(16, CPU cores)).",
    )
    return parser.parse_args()


def sample_files(base_dir: Path, sample: int, shuffle: bool) -> List[Path]:
    iterator: Iterable[Path] = base_dir.glob("*.parquet")
    files = list(iterator)
    if shuffle:
        import random

        random.shuffle(files)
    if sample > 0:
        files = files[:sample]
    return files


def process_one(path: Path) -> Optional[Metrics]:
    required_cols = ["timestamp", "price", "volume", "bp0", "sp0", "bq0", "sq0"]
    try:
        df = pl.read_parquet(path, columns=required_cols, use_pyarrow=True)
    except Exception:
        # Missing columns or unreadable file
        return None

    df = (
        df.with_columns(
            [
                pl.col("price").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
                pl.col("bp0").cast(pl.Float64),
                pl.col("sp0").cast(pl.Float64),
                pl.col("bq0").cast(pl.Float64),
                pl.col("sq0").cast(pl.Float64),
            ]
        )
        .drop_nulls(subset=["timestamp", "price"])
        .sort("timestamp")
    )

    if df.is_empty():
        return None

    df = df.with_columns(
        [
            (pl.col("sp0") - pl.col("bp0")).alias("spread"),
            ((pl.col("sp0") + pl.col("bp0")) / 2.0).alias("mid"),
            pl.col("price").ewm_mean(span=5, adjust=False).alias("ema5"),
            pl.col("price").ewm_mean(span=21, adjust=False).alias("ema21"),
        ]
    )

    prices = df["price"].to_numpy()
    ema5 = df["ema5"].to_numpy()
    ema21 = df["ema21"].to_numpy()
    bid0 = df["bp0"].fill_null(0.0).to_numpy()
    ask0 = df["sp0"].fill_null(0.0).to_numpy()
    volume = df["volume"].fill_null(0.0).to_numpy()

    if len(prices) < 2:
        return None

    pnl, trades = run_strategy(prices, ema5, ema21, bid0, ask0, volume)

    spread_valid = df.filter(pl.col("spread").is_not_null() & pl.col("mid").is_not_null())
    avg_spread = float(spread_valid["spread"].mean()) if spread_valid.height else 0.0
    bid_non_null = df["bq0"].drop_nulls()
    ask_non_null = df["sq0"].drop_nulls()
    avg_bid_qty = float(bid_non_null.mean()) if bid_non_null.len() else 0.0
    avg_ask_qty = float(ask_non_null.mean()) if ask_non_null.len() else 0.0
    total_volume = float(df["volume"].fill_null(0.0).sum())

    return Metrics(
        file=str(path),
        pnl=pnl,
        trades=trades,
        total_volume=total_volume,
        avg_spread=avg_spread,
        avg_bid_qty=avg_bid_qty,
        avg_ask_qty=avg_ask_qty,
        rows=len(prices),
    )


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir
    if not base_dir.exists():
        raise SystemExit(f"Base dir does not exist: {base_dir}")

    files = sample_files(base_dir, args.sample, args.shuffle)
    if not files:
        raise SystemExit("No parquet files found to process.")

    max_workers = args.workers or max(1, min(16, (os.cpu_count() or 4)))
    print(f"Processing {len(files)} files with {max_workers} workers (sample={args.sample}, shuffle={args.shuffle})")

    t0 = time.perf_counter()
    results: List[Metrics] = []
    def run_pool(pool):
        futs = {pool.submit(process_one, p): p for p in files}
        for fut in as_completed(futs):
            res = fut.result()
            if res is not None:
                results.append(res)

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            run_pool(ex)
    except PermissionError:
        print("ProcessPool blocked by sandbox; falling back to ThreadPoolExecutor.")
        results.clear()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            run_pool(ex)
    elapsed = time.perf_counter() - t0

    total_rows = sum(m.rows for m in results)
    rps = total_rows / elapsed if elapsed > 0 else math.inf
    print(f"Processed {len(results)} files")
    print(f"Rows: {total_rows:,}")
    print(f"Elapsed: {elapsed:.3f} s")
    print(f"Throughput: {rps:,.0f} rows/s")

    if results:
        out_path = base_dir / "strategy_results_numba.csv"
        pl.DataFrame(results).write_csv(out_path)
        print(f"Wrote per-file metrics to {out_path}")


if __name__ == "__main__":
    main()
