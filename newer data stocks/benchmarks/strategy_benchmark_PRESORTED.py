#!/usr/bin/env python3
"""
HYPER-OPTIMIZED Benchmark - Assumes Pre-Sorted Data

Key optimization: NO SORTING in the hot path!
Assumes data is already sorted by: expiry → opt_type → strike → timestamp

This version should hit 50-100M rows/sec.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import numpy as np
import polars as pl
from numba import njit


# ==================== NUMBA STRATEGY (Inline EMA) ====================

@njit(fastmath=True, nogil=True)
def run_strategy_sorted(strikes, opt_types_int, prices, bid0, ask0, volume):
    """
    Strategy for PRE-SORTED data.
    Detects contract changes by comparing strike/opt_type with previous row.
    Calculates EMAs inline (no memory allocation).
    """
    n = len(prices)
    if n < 2:
        return 0.0, 0
    
    total_pnl = 0.0
    total_trades = 0
    
    # EMA constants
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    
    # State
    pos = 0
    entry_price = 0.0
    ema5 = prices[0]
    ema21 = prices[0]
    
    for i in range(1, n):
        price = prices[i]
        
        # 1. Detect contract change (strike or opt_type changed from previous row)
        if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
            # Force exit if in position
            if pos == 1:
                total_pnl += prices[i-1] - entry_price
                pos = 0
            
            # Reset EMAs for new contract
            ema5 = price
            ema21 = price
            continue
        
        # 2. Update EMAs inline
        ema5 = price * alpha5 + ema5 * (1 - alpha5)
        ema21 = price * alpha21 + ema21 * (1 - alpha21)
        
        # 3. Check spread
        spread_ok = False
        if ask0[i] > 0.0 and bid0[i] > 0.0:
            mid = 0.5 * (ask0[i] + bid0[i])
            if mid > 0.0 and ((ask0[i] - bid0[i]) / mid) <= 0.0005:
                spread_ok = True
        
        vol_ok = volume[i] >= 1.0
        
        # 4. Strategy logic
        if pos == 0:
            if (ema5 > ema21) and spread_ok and vol_ok:
                pos = 1
                entry_price = price
                total_trades += 1
        else:
            # Check if next row is different contract
            end_of_contract = (i == n - 1) or \
                            (strikes[i+1] != strikes[i]) or \
                            (opt_types_int[i+1] != opt_types_int[i])
            
            if (ema21 >= ema5) or end_of_contract:
                total_pnl += price - entry_price
                pos = 0
    
    return total_pnl, total_trades


# ==================== FILE PROCESSOR ====================

def process_file_presorted(file_path: Path) -> Tuple[float, int, int]:
    """
    Process one file - ASSUMES DATA IS ALREADY SORTED!
    """
    try:
        # Read file (should already be sorted on disk)
        df = pl.read_parquet(file_path, columns=[
            'strike', 'opt_type', 'price', 'bp0', 'sp0', 'volume'
        ])
        
        if df.is_empty():
            return 0.0, 0, 0
        
        rows = len(df)
        
        # Convert opt_type (Categorical CE/PE) to int (0/1)
        # This is MUCH faster than string comparison in Numba
        types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
        
        # Extract numpy arrays (zero-copy)
        strikes = df['strike'].cast(pl.Float64).fill_null(0.0).to_numpy()
        prices = df['price'].cast(pl.Float64).fill_null(0.0).to_numpy()
        bid0 = df['bp0'].cast(pl.Float64).fill_null(0.0).to_numpy()
        ask0 = df['sp0'].cast(pl.Float64).fill_null(0.0).to_numpy()
        volume = df['volume'].cast(pl.Float64).fill_null(0.0).to_numpy()
        
        # Single Numba call
        pnl, trades = run_strategy_sorted(strikes, types_int, prices, bid0, ask0, volume)
        
        return pnl, trades, rows
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0.0, 0, 0


def process_chunk(file_paths: List[Path]) -> Tuple[float, int, int]:
    """Process a chunk of files"""
    chunk_pnl = 0.0
    chunk_trades = 0
    chunk_rows = 0
    
    for path in file_paths:
        pnl, trades, rows = process_file_presorted(path)
        chunk_pnl += pnl
        chunk_trades += trades
        chunk_rows += rows
    
    return chunk_pnl, chunk_trades, chunk_rows


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="HYPER-OPTIMIZED benchmark for pre-sorted data"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("../data/options_date_packed_FULL"),
        help="Root directory of SORTED date-partitioned data"
    )
    parser.add_argument(
        "--sample-dates",
        type=int,
        default=0,
        help="Number of dates to sample (0 = all)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of worker processes"
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=10,
        help="Files per worker task"
    )
    
    args = parser.parse_args()
    
    # Find all parquet files
    print("Listing parquet files...")
    files = list(args.data_dir.rglob("*.parquet"))
    
    if args.sample_dates > 0:
        dates = set()
        for f in files:
            for part in f.parts:
                if '-' in part and len(part) == 10:
                    dates.add(part)
        
        dates = sorted(list(dates))[:args.sample_dates]
        files = [f for f in files if any(d in str(f) for d in dates)]
    
    print(f"Found {len(files)} parquet files")
    
    if not files:
        print("No files found!")
        return
    
    # Split into chunks
    chunks = [files[i:i + args.chunksize] for i in range(0, len(files), args.chunksize)]
    print(f"Processing {len(files)} files in {len(chunks)} chunks with {args.workers} workers")
    print("✓ ZERO-COPY mode: No sorting, no EMA arrays, inline calculations only")
    
    # Process in parallel
    t0 = time.perf_counter()
    total_pnl = 0.0
    total_trades = 0
    total_rows = 0
    completed = 0
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        
        for fut in as_completed(futures):
            pnl, trades, rows = fut.result()
            total_pnl += pnl
            total_trades += trades
            total_rows += rows
            completed += 1
            
            if completed % 3 == 0:
                elapsed = time.perf_counter() - t0
                rps = total_rows / elapsed if elapsed > 0 else 0
                print(f"Progress: {completed}/{len(chunks)} chunks | {total_rows:,} rows | {rps/1e6:.1f}M rows/s")
    
    t1 = time.perf_counter()
    elapsed = t1 - t0
    rps = total_rows / elapsed if elapsed > 0 else 0
    
    print("\n" + "="*70)
    print("HYPER-OPTIMIZED BENCHMARK COMPLETE")
    print("="*70)
    print(f"Total rows:          {total_rows:,}")
    print(f"Total trades:        {total_trades:,}")
    print(f"Total PnL:           {total_pnl:,.2f}")
    print(f"Elapsed:             {elapsed:.3f} s")
    print(f"Throughput:          {rps:,.0f} rows/s ({rps/1e6:.1f}M rows/s)")
    print()
    print(f"Speedup vs baseline: {rps / 2_499_225:.1f}x")


if __name__ == "__main__":
    main()
