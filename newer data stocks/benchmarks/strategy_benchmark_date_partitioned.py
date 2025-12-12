#!/usr/bin/env python3
"""
Strategy Benchmark for Date-Partitioned Options Data

Adapts the 100M rows/sec strategy for the new date-partitioned format.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import polars as pl
from numba import njit

# ==================== STRATEGY LOGIC (Numba-compiled) ====================

@njit(fastmath=True, nogil=True)
def run_strategy_inline(prices, bid0, ask0, volume):
    """
    Strategy logic compiled to machine code.
    Calculates EMAs inline to avoid memory allocation.
    
    Entry: EMA5 > EMA21 AND spread ≤ 5bps AND volume ≥ 1
    Exit: EMA21 ≥ EMA5 OR last tick
    """
    n = len(prices)
    if n < 2:
        return 0.0, 0, 0
    
    # EMA constants
    alpha5 = 2.0 / 6.0   # 2 / (span + 1) for span=5
    alpha21 = 2.0 / 22.0  # 2 / (span + 1) for span=21
    
    # Initialize EMAs with first price
    ema5 = prices[0]
    ema21 = prices[0]
    
    pnl = 0.0
    trades = 0
    pos = 0
    entry = 0.0
    
    # Main loop
    for i in range(1, n):
        price = prices[i]
        
        # 1. Update EMAs on the fly (saves RAM)
        ema5 = price * alpha5 + ema5 * (1 - alpha5)
        ema21 = price * alpha21 + ema21 * (1 - alpha21)
        
        # 2. Check spread condition
        spread_ok = False
        a = ask0[i]
        b = bid0[i]
        if a > 0.0 and b > 0.0:
            mid = 0.5 * (a + b)
            if mid > 0.0 and ((a - b) / mid) <= 0.0005:  # 5 bps
                spread_ok = True
        
        vol_ok = volume[i] >= 1.0
        
        # 3. State machine
        if pos == 0:
            # Entry
            if (ema5 > ema21) and spread_ok and vol_ok:
                pos = 1
                entry = price
                trades += 1
        else:
            # Exit
            if (ema21 >= ema5) or (i == n - 1):
                pnl += price - entry
                pos = 0
    
    return pnl, trades, n


# ==================== DATA STRUCTURES ====================

@dataclass
class ContractResult:
    """Result for one contract (strike + expiry + opt_type)"""
    date: str
    underlying: str
    strike: float
    expiry: str
    opt_type: str
    pnl: float
    trades: int
    rows: int


# ==================== WORKER FUNCTION ====================

def process_date_file(file_path: Path) -> List[ContractResult]:
    """
    Process one date partition file.
    Returns one result per unique contract (strike + expiry + opt_type).
    """
    results = []
    
    try:
        # Extract date and underlying from path
        # Path format: options_date_packed_FULL/2025-08-08/BANKNIFTY/part-*.parquet
        parts = file_path.parts
        
        # Find date (YYYY-MM-DD format) and underlying
        date_str = None
        underlying = None
        
        for i, part in enumerate(parts):
            # Date is a directory with format YYYY-MM-DD
            if '-' in part and len(part) == 10:
                try:
                    # Validate it's a date
                    year, month, day = part.split('-')
                    if len(year) == 4 and len(month) == 2 and len(day) == 2:
                        date_str = part
                        # Underlying is usually the next part
                        if i + 1 < len(parts):
                            underlying = parts[i + 1]
                            break
                except:
                    continue
        
        if not date_str or not underlying or underlying.endswith('.parquet'):
            return results
        
        # Read the file
        df = pl.read_parquet(file_path)
        
        if df.is_empty():
            return results
        
        # Group by unique contracts (strike + expiry + opt_type)
        contracts = df.select(['strike', 'expiry', 'opt_type']).unique()
        
        for contract in contracts.iter_rows(named=True):
            strike = contract['strike']
            expiry = contract['expiry']
            opt_type = contract['opt_type']
            
            # Filter to this specific contract
            contract_df = df.filter(
                (pl.col('strike') == strike) &
                (pl.col('expiry') == expiry) &
                (pl.col('opt_type') == opt_type)
            ).sort('timestamp')
            
            if len(contract_df) < 2:
                continue
            
            # Extract arrays for Numba
            prices = contract_df['price'].cast(pl.Float64).to_numpy()
            bid0 = contract_df['bp0'].cast(pl.Float64).fill_null(0.0).to_numpy()
            ask0 = contract_df['sp0'].cast(pl.Float64).fill_null(0.0).to_numpy()
            volume = contract_df['volume'].cast(pl.Float64).fill_null(0.0).to_numpy()
            
            # Run strategy
            pnl, trades, rows = run_strategy_inline(prices, bid0, ask0, volume)
            
            results.append(ContractResult(
                date=date_str,
                underlying=underlying,
                strike=float(strike),
                expiry=str(expiry),
                opt_type=opt_type,
                pnl=pnl,
                trades=trades,
                rows=rows
            ))
    
    except Exception as e:
        # Print errors for debugging
        print(f"Error processing {file_path}: {e}")
    
    return results


def process_chunk(file_paths: List[Path]) -> List[ContractResult]:
    """Process a chunk of files (called by worker process)"""
    all_results = []
    for path in file_paths:
        results = process_date_file(path)
        all_results.extend(results)
    return all_results


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark strategy on date-partitioned data"
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
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("strategy_results_date_partitioned.csv"),
        help="Output CSV file"
    )
    
    args = parser.parse_args()
    
    # Find all parquet files
    print("Listing parquet files...")
    files = list(args.data_dir.rglob("*.parquet"))
    
    if args.sample_dates > 0:
        # Extract unique dates
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
    print(f"Processing {len(files)} files in {len(chunks)} chunks of ~{args.chunksize} with {args.workers} workers")
    
    # Process in parallel
    t0 = time.perf_counter()
    all_results = []
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        
        for fut in as_completed(futures):
            chunk_results = fut.result()
            all_results.extend(chunk_results)
            print(f"Processed {len(all_results)} contracts so far...")
    
    t1 = time.perf_counter()
    elapsed = t1 - t0
    
    # Calculate statistics
    total_rows = sum(r.rows for r in all_results)
    total_trades = sum(r.trades for r in all_results)
    total_pnl = sum(r.pnl for r in all_results)
    
    rps = total_rows / elapsed if elapsed > 0 else 0
    
    print("\n" + "="*60)
    print("BENCHMARK COMPLETE")
    print("="*60)
    print(f"Contracts processed: {len(all_results):,}")
    print(f"Total rows:          {total_rows:,}")
    print(f"Total trades:        {total_trades:,}")
    print(f"Total PnL:           {total_pnl:,.2f}")
    print(f"Elapsed:             {elapsed:.3f} s")
    print(f"Throughput:          {rps:,.0f} rows/s")
    
    # Write results
    if all_results:
        df_results = pl.DataFrame([
            {
                'date': r.date,
                'underlying': r.underlying,
                'strike': r.strike,
                'expiry': r.expiry,
                'opt_type': r.opt_type,
                'pnl': r.pnl,
                'trades': r.trades,
                'rows': r.rows
            }
            for r in all_results
        ])
        
        df_results.write_csv(args.output)
        print(f"\nWrote results to {args.output}")


if __name__ == "__main__":
    main()
