#!/usr/bin/env python3
"""
Hyper-optimized order-book EMA(5/21) strategy:
- Computes EMA and spread checks inside the Numba loop (no temporary arrays).
- Batches files into chunks to cut process-pool overhead.
- Reads only the essential columns: timestamp, price, volume, bp0, sp0.
- Skips per-file sorting (assumes files are already timestamp-ordered).

Outputs: strategy_results_fastest.csv in the base directory.
"""

from __future__ import annotations

import argparse
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import polars as pl
from numba import njit


@njit(fastmath=True, nogil=True)
def run_strategy_inline(prices, bid0, ask0, volume):
    n = len(prices)
    if n < 2:
        return 0.0, 0, 0

    alpha5 = 2.0 / (5.0 + 1.0)
    alpha21 = 2.0 / (21.0 + 1.0)

    ema5 = prices[0]
    ema21 = prices[0]
    pnl = 0.0
    trades = 0
    pos = 0
    entry = 0.0

    for i in range(1, n):
        price = prices[i]
        ema5 = price * alpha5 + ema5 * (1 - alpha5)
        ema21 = price * alpha21 + ema21 * (1 - alpha21)

        a = ask0[i]
        b = bid0[i]
        spread_ok = False
        if a > 0.0 and b > 0.0:
            mid = 0.5 * (a + b)
            if mid > 0.0 and ((a - b) / mid) <= 0.0005:
                spread_ok = True
        vol_ok = volume[i] >= 1.0

        if pos == 0:
            if (ema5 > ema21) and spread_ok and vol_ok:
                pos = 1
                entry = price
                trades += 1
        else:
            if (ema21 >= ema5) or (i == n - 1):
                pnl += price - entry
                pos = 0

    return pnl, trades, n


@dataclass
class FileResult:
    file: str
    pnl: float
    trades: int
    rows: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Chunked Numba+Polars strategy benchmark")
    p.add_argument("--base-dir", type=Path, default=Path(__file__).resolve().parent, help="Directory with parquet files.")
    p.add_argument("--sample", type=int, default=0, help="Number of files (<=0 means all).")
    p.add_argument("--shuffle", action="store_true", help="Shuffle before sampling.")
    p.add_argument("--workers", type=int, default=None, help="Process pool size (default min(32, CPU cores)).")
    p.add_argument("--chunksize", type=int, default=1000, help="Files per task sent to the pool.")
    return p.parse_args()


def sample_files(base_dir: Path, sample: int, shuffle: bool) -> List[Path]:
    iterator: Iterable[Path] = base_dir.glob("*.parquet")
    files = list(iterator)
    if shuffle:
        import random

        random.shuffle(files)
    if sample > 0:
        files = files[:sample]
    return files


def process_chunk(file_paths: Sequence[Path]) -> List[FileResult]:
    results: List[FileResult] = []
    cols = ["timestamp", "price", "volume", "bp0", "sp0"]

    for path in file_paths:
        try:
            df = pl.read_parquet(path, columns=cols, use_pyarrow=True)
            # Drop rows missing the essentials
            df = df.drop_nulls(subset=["timestamp", "price"])
            if df.height < 2:
                continue

            prices = df["price"].cast(pl.Float64).to_numpy()
            bid0 = df["bp0"].cast(pl.Float64).fill_null(0.0).to_numpy()
            ask0 = df["sp0"].cast(pl.Float64).fill_null(0.0).to_numpy()
            volume = df["volume"].cast(pl.Float64).fill_null(0.0).to_numpy()

            pnl, trades, rows = run_strategy_inline(prices, bid0, ask0, volume)
            results.append(FileResult(str(path), pnl, trades, rows))
        except Exception:
            continue
    return results


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir
    if not base_dir.exists():
        raise SystemExit(f"Base dir does not exist: {base_dir}")

    files = sample_files(base_dir, args.sample, args.shuffle)
    if not files:
        raise SystemExit("No parquet files found.")

    max_workers = args.workers or max(1, min(32, (os.cpu_count() or 4)))
    chunk_size = max(1, args.chunksize)

    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]

    print(f"Processing {len(files)} files in {len(chunks)} chunks of ~{chunk_size} with {max_workers} workers.")
    t0 = time.perf_counter()

    all_results: List[FileResult] = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(process_chunk, chunk) for chunk in chunks]
        for fut in as_completed(futs):
            all_results.extend(fut.result())

    elapsed = time.perf_counter() - t0
    total_rows = sum(r.rows for r in all_results)
    rps = total_rows / elapsed if elapsed > 0 else math.inf

    print(f"Processed {len(all_results)}/{len(files)} files")
    print(f"Rows: {total_rows:,}")
    print(f"Elapsed: {elapsed:.3f} s")
    print(f"Throughput: {rps:,.0f} rows/s")

    if all_results:
        out_path = base_dir / "strategy_results_fastest.csv"
        pl.DataFrame(all_results).write_csv(out_path)
        print(f"Wrote per-file metrics to {out_path}")


if __name__ == "__main__":
    main()
