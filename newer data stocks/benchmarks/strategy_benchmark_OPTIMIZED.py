#!/usr/bin/env python3
"""
OPTIMIZED Single-Pass Strategy Benchmark

Uses "contract-aware" Numba that processes all contracts in one pass:
1. Sort once by (strike, expiry, opt_type, timestamp)  
2. Calculate EMAs using Polars over() - vectorized across all contracts
3. Single Numba call per file - resets state on contract changes
4. No Python loops between contracts

Expected: 50-100M rows/sec (20-40x faster than previous version)
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np
import polars as pl
from numba import njit


# ==================== NUMBA STRATEGY (Single-Pass) ====================

@njit(fastmath=True, nogil=True)
def run_strategy_single_pass(contract_ids, prices, ema5, ema21, bid0, ask0, volume):
    """
    Runs backtest on mixed stream of contracts.
    Resets state whenever contract_id changes.
    
    This is the KEY optimization: one Numba call instead of 538.
    """
    n = len(prices)
    if n < 2:
        return 0.0, 0
    
    total_pnl = 0.0
    total_trades = 0
    
    # State variables (reset on contract change)
    current_contract = contract_ids[0]
    pos = 0
    entry_price = 0.0
    
    for i in range(1, n):  # Start at 1 (need previous for EMA)
        # 1. Check for contract change
        if contract_ids[i] != current_contract:
            # Force exit if still in position
            if pos == 1:
                total_pnl += prices[i-1] - entry_price
                pos = 0
            
            # Reset for new contract
            current_contract = contract_ids[i]
            entry_price = 0.0
            continue
        
        # 2. Check spread condition
        spread_ok = False
        a = ask0[i]
        b = bid0[i]
        if a > 0.0 and b > 0.0:
            mid = 0.5 * (a + b)
            if mid > 0.0 and ((a - b) / mid) <= 0.0005:  # 5 bps
                spread_ok = True
        
        vol_ok = volume[i] >= 1.0
        
        # 3. Strategy state machine
        if pos == 0:
            # Entry: EMA5 > EMA21 AND spread OK AND volume OK
            if (ema5[i] > ema21[i]) and spread_ok and vol_ok:
                pos = 1
                entry_price = prices[i]
                total_trades += 1
        else:
            # Exit: EMA21 >= EMA5 OR end of contract
            end_of_contract = (i == n - 1) or (contract_ids[i+1] != current_contract)
            
            if (ema21[i] >= ema5[i]) or end_of_contract:
                total_pnl += prices[i] - entry_price
                pos = 0
    
    return total_pnl, total_trades


# ==================== FILE PROCESSOR ====================

def process_file(file_path: Path) -> Tuple[float, int, int]:
    """
    Process one date/underlying file using single-pass approach.
    
    Returns: (pnl, trades, rows)
    """
    try:
        # 1. Read entire file
        df = pl.read_parquet(file_path)
        
        if df.is_empty():
            return 0.0, 0, 0
        
        # 2. Sort by contract grouping + timestamp
        # This groups all rows of same contract together
        df = df.sort(['strike', 'expiry', 'opt_type', 'timestamp'])
        
        # 3. Create contract ID (hash of strike + expiry + opt_type)
        # This gives each unique contract a unique integer ID
        df = df.with_columns([
            pl.struct(['strike', 'expiry', 'opt_type']).hash().alias('contract_id')
        ])
        
        # 4. Calculate EMAs using Polars over() - VECTORIZED!
        # This calculates EMA for ALL contracts in parallel
        df = df.with_columns([
            pl.col('price').cast(pl.Float64).fill_null(0.0).alias('price'),
            pl.col('volume').cast(pl.Float64).fill_null(0.0).alias('volume'),
            pl.col('bp0').cast(pl.Float64).fill_null(0.0).alias('bp0'),
            pl.col('sp0').cast(pl.Float64).fill_null(0.0).alias('sp0'),
        ]).with_columns([
            # EMA per contract using window function
            pl.col('price').ewm_mean(span=5, adjust=False).over('contract_id').alias('ema5'),
            pl.col('price').ewm_mean(span=21, adjust=False).over('contract_id').alias('ema21'),
        ])
        
        rows = len(df)
        
        # 5. Extract numpy arrays (zero-copy)
        contract_ids = df['contract_id'].to_numpy()
        prices = df['price'].to_numpy()
        ema5 = df['ema5'].to_numpy()
        ema21 = df['ema21'].to_numpy()
        bid0 = df['bp0'].to_numpy()
        ask0 = df['sp0'].to_numpy()
        volume = df['volume'].to_numpy()
        
        # 6. Single Numba call for entire file!
        pnl, trades = run_strategy_single_pass(
            contract_ids, prices, ema5, ema21, bid0, ask0, volume
        )
        
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
        pnl, trades, rows = process_file(path)
        chunk_pnl += pnl
        chunk_trades += trades
        chunk_rows += rows
    
    return chunk_pnl, chunk_trades, chunk_rows


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="Optimized single-pass strategy benchmark"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("../data/options_date_packed_FULL"),
        help="Root directory of date-partitioned data"
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
    print("Using SINGLE-PASS optimization (Polars over() + contract-aware Numba)")
    
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
            
            if completed % 3 == 0:  # Progress every 3 chunks
                elapsed = time.perf_counter() - t0
                rps = total_rows / elapsed if elapsed > 0 else 0
                print(f"Progress: {completed}/{len(chunks)} chunks | {total_rows:,} rows | {rps/1e6:.1f}M rows/s")
    
    t1 = time.perf_counter()
    elapsed = t1 - t0
    rps = total_rows / elapsed if elapsed > 0 else 0
    
    print("\n" + "="*70)
    print("OPTIMIZED BENCHMARK COMPLETE")
    print("="*70)
    print(f"Total rows:          {total_rows:,}")
    print(f"Total trades:        {total_trades:,}")
    print(f"Total PnL:           {total_pnl:,.2f}")
    print(f"Elapsed:             {elapsed:.3f} s")
    print(f"Throughput:          {rps:,.0f} rows/s ({rps/1e6:.1f}M rows/s)")
    print()
    print("Speedup vs previous: {:.1f}x".format(rps / 2_499_225))


if __name__ == "__main__":
    main()
