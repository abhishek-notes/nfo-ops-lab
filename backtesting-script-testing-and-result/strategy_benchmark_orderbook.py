#!/usr/bin/env python3
"""
EMA(5/21) long-only strategy with order-book filters and volume analytics.

Strategy:
- Price series: use 'price' (cast to float); if missing or null on a row, skip that row.
- EMAs computed on price.
- Enter long when EMA5 > EMA21 AND top-of-book spread is available and tight:
    spread_ratio = (ask0 - bid0) / mid <= 0.0005 (5 bps) AND volume >= 1
- Exit when EMA5 <= EMA21 or at final tick.
- No shorting; position size 1 unit.
- P&L per file is sum of trade deltas.

Analytics per file:
- pnl, trades
- total_volume (sum of volume)
- avg_spread (where both bp0/sp0 exist)
- avg_bid_qty (bq0), avg_ask_qty (sq0)
- rows_processed

Outputs:
- Prints timings for DuckDB, PyArrow, Polars readers (same downstream Python logic).
- Writes CSV: strategy_results_orderbook.csv (using PyArrow results as canonical).

Usage (from /Users/abhishek/workspace/nfo/data/raw/options):
  python strategy_benchmark_orderbook.py
  python strategy_benchmark_orderbook.py --sample 200 --shuffle
"""

from __future__ import annotations

import argparse
import csv
import itertools
import math
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EMA(5/21) long-only strategy with order book filters")
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
        help="Thread pool size for per-file processing (default: min(16, CPU cores)).",
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


def ema_list(values: Sequence[float], span: int) -> List[float]:
    alpha = 2.0 / (span + 1.0)
    out: List[float] = []
    prev = None
    for v in values:
        if prev is None:
            prev = v
        else:
            prev = alpha * v + (1 - alpha) * prev
        out.append(prev)
    return out


@dataclass
class Metrics:
    pnl: float
    trades: int
    total_volume: float
    avg_spread: float
    avg_bid_qty: float
    avg_ask_qty: float
    rows: int


def compute_metrics(df: pl.DataFrame) -> Metrics:
    # Ensure required columns exist
    required = {"timestamp", "price"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"Missing required columns: {required - set(df.columns)}")

    # Cast and clean
    cols_to_cast = {
        "price": pl.Float64,
        "volume": pl.Float64,
        "bp0": pl.Float64,
        "sp0": pl.Float64,
        "bq0": pl.Float64,
        "sq0": pl.Float64,
    }
    for c, dt in cols_to_cast.items():
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(dt))

    df = df.drop_nulls(subset=["timestamp", "price"]).sort("timestamp")
    if df.is_empty():
        return Metrics(0.0, 0, 0.0, 0.0, 0.0, 0.0, 0)

    has_book = ("bp0" in df.columns) and ("sp0" in df.columns)
    df = df.with_columns(
        [
            ((pl.col("sp0") - pl.col("bp0"))).alias("spread") if has_book else pl.lit(None, dtype=pl.Float64).alias("spread"),
            (((pl.col("sp0") + pl.col("bp0")) / 2.0)).alias("mid") if has_book else pl.lit(None, dtype=pl.Float64).alias("mid"),
        ]
    )

    alpha5 = 2.0 / (5 + 1.0)
    alpha21 = 2.0 / (21 + 1.0)
    df = df.with_columns(
        [
            pl.col("price").ewm_mean(alpha=alpha5, adjust=False).alias("ema5"),
            pl.col("price").ewm_mean(alpha=alpha21, adjust=False).alias("ema21"),
        ]
    )

    # Precompute aggregates in Polars
    total_volume = df["volume"].fill_null(0.0).sum() if "volume" in df.columns else 0.0
    spread_stats = df.filter(pl.col("spread").is_not_null() & pl.col("mid").is_not_null())
    spread_sum = float(spread_stats["spread"].sum()) if spread_stats.height else 0.0
    spread_cnt = spread_stats.height
    if "bq0" in df.columns:
        bq_non_null = df["bq0"].drop_nulls()
        bid_qty_sum = float(bq_non_null.sum())
        bid_qty_cnt = bq_non_null.len()
    else:
        bid_qty_sum = 0.0
        bid_qty_cnt = 0
    if "sq0" in df.columns:
        sq_non_null = df["sq0"].drop_nulls()
        ask_qty_sum = float(sq_non_null.sum())
        ask_qty_cnt = sq_non_null.len()
    else:
        ask_qty_sum = 0.0
        ask_qty_cnt = 0

    # Entry conditions precomputed to minimize Python overhead
    df = df.with_columns(
        [
            (pl.col("spread") / pl.col("mid") <= 0.0005).fill_null(False).alias("spread_ok"),
            (pl.col("volume").fill_null(0.0) >= 1.0 if "volume" in df.columns else pl.lit(False)).alias("vol_ok"),
            (pl.col("ema5") > pl.col("ema21")).alias("ema_up"),
        ]
    )

    prices = df["price"].to_list()
    ema5 = df["ema5"].to_list()
    ema21 = df["ema21"].to_list()
    spread_ok_list = df["spread_ok"].to_list()
    vol_ok_list = df["vol_ok"].to_list()

    holding = False
    entry = 0.0
    pnl = 0.0
    trades = 0

    for price, e5, e21, spread_ok, vol_ok in zip(prices, ema5, ema21, spread_ok_list, vol_ok_list):
        if not holding and e5 > e21 and spread_ok and vol_ok:
            holding = True
            entry = price
        elif holding and e5 <= e21:
            pnl += price - entry
            trades += 1
            holding = False

    if holding:
        pnl += prices[-1] - entry
        trades += 1

    avg_spread = (spread_sum / spread_cnt) if spread_cnt else 0.0
    avg_bid_qty = (bid_qty_sum / bid_qty_cnt) if bid_qty_cnt else 0.0
    avg_ask_qty = (ask_qty_sum / ask_qty_cnt) if ask_qty_cnt else 0.0

    return Metrics(
        pnl=pnl,
        trades=trades,
        total_volume=total_volume,
        avg_spread=avg_spread,
        avg_bid_qty=avg_bid_qty,
        avg_ask_qty=avg_ask_qty,
        rows=len(prices),
    )


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
    table = con.execute(
        f"""
        SELECT timestamp, price, volume, bp0, sp0, bq0, sq0
        FROM read_parquet('{path}', union_by_name=True)
        """
    ).fetch_arrow_table()
    con.close()
    return table


def load_pyarrow(path: Path):
    ds = require_pyarrow_dataset()
    dataset = ds.dataset(str(path), format="parquet")
    return dataset.to_table(columns=["timestamp", "price", "volume", "bp0", "sp0", "bq0", "sq0"], use_threads=True)


def load_polars(path: Path):
    pl_mod = require_polars()
    return pl_mod.read_parquet(path, columns=["timestamp", "price", "volume", "bp0", "sp0", "bq0", "sq0"])


def to_polars(df_or_table) -> pl.DataFrame:
    if isinstance(df_or_table, pl.DataFrame):
        return df_or_table
    return pl.from_arrow(df_or_table)


def process_files(
    files: Sequence[Path],
    loader_name: str,
    loader_fn,
) -> Tuple[Dict[str, Metrics], float, int]:
    results: Dict[str, Metrics] = {}
    total_rows = 0
    t0 = time.perf_counter()
    for path in files:
        try:
            table_or_df = loader_fn(path)
            df = to_polars(table_or_df)
            metrics = compute_metrics(df)
            total_rows += metrics.rows
            results[str(path)] = metrics
        except Exception as e:
            print(f"[WARN] {loader_name}: skipping {path} due to error: {e}")
    elapsed = time.perf_counter() - t0
    return results, elapsed, total_rows


def process_files_parallel(
    files: Sequence[Path],
    loader_name: str,
    loader_fn,
    max_workers: int,
) -> Tuple[Dict[str, Metrics], float, int]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: Dict[str, Metrics] = {}
    total_rows = 0
    t0 = time.perf_counter()

    def work(path: Path):
        table_or_df = loader_fn(path)
        df = to_polars(table_or_df)
        metrics = compute_metrics(df)
        return path, metrics

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(work, p): p for p in files}
        for fut in as_completed(futures):
            path = futures[fut]
            try:
                path, metrics = fut.result()
                total_rows += metrics.rows
                results[str(path)] = metrics
            except Exception as e:
                print(f"[WARN] {loader_name}: skipping {path} due to error: {e}")

    elapsed = time.perf_counter() - t0
    return results, elapsed, total_rows


def compare_results(ref: Dict[str, Metrics], other: Dict[str, Metrics], label: str) -> None:
    for k, v in ref.items():
        if k not in other:
            raise ValueError(f"{label} missing result for {k}")
        o = other[k]
        fields = ["pnl", "trades", "total_volume", "avg_spread", "avg_bid_qty", "avg_ask_qty", "rows"]
        for f in fields:
            if not math.isclose(getattr(v, f), getattr(o, f), rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError(f"{label} mismatch on {k} field {f}: ref={getattr(v, f)}, other={getattr(o, f)}")


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir
    if not base_dir.exists():
        sys.exit(f"Base dir does not exist: {base_dir}")

    files = sample_files(base_dir, args.sample, args.shuffle)
    if not files:
        sys.exit("No parquet files found to process.")

    print(f"Processing {len(files)} files (sample={args.sample}, shuffle={args.shuffle})")

    require_duckdb()
    require_pyarrow_dataset()
    require_polars()

    max_workers = args.workers or max(1, min(16, (os.cpu_count() or 4)))

    duckdb_res, duckdb_time, duckdb_rows = process_files_parallel(
        files, "duckdb", load_duckdb, max_workers=max_workers
    )
    print(f"DuckDB: {duckdb_rows:,} rows  {duckdb_time:.3f} s  {duckdb_rows/duckdb_time:,.0f} rows/s")

    arrow_res, arrow_time, arrow_rows = process_files_parallel(
        files, "pyarrow", load_pyarrow, max_workers=max_workers
    )
    print(f"PyArrow dataset: {arrow_rows:,} rows  {arrow_time:.3f} s  {arrow_rows/arrow_time:,.0f} rows/s")

    polars_res, polars_time, polars_rows = process_files_parallel(
        files, "polars", load_polars, max_workers=max_workers
    )
    print(f"Polars: {polars_rows:,} rows  {polars_time:.3f} s  {polars_rows/polars_time:,.0f} rows/s")

    compare_results(arrow_res, duckdb_res, "DuckDB vs Arrow")
    compare_results(arrow_res, polars_res, "Polars vs Arrow")
    print("All approaches produced matching metrics per file.")

    csv_path = base_dir / "strategy_results_orderbook.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["file", "pnl", "trades", "total_volume", "avg_spread", "avg_bid_qty", "avg_ask_qty", "rows"])
        for path_str, m in arrow_res.items():
            writer.writerow([path_str, m.pnl, m.trades, m.total_volume, m.avg_spread, m.avg_bid_qty, m.avg_ask_qty, m.rows])
    print(f"Wrote per-file metrics to {csv_path}")


if __name__ == "__main__":
    main()
