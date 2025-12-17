#!/usr/bin/env python3
"""
FINAL WORKING VERSION: Memory-efficient Numba with correct P&L
Uses the EXACT same logic as slow version but with Numba for speed
"""

from pathlib import Path
from datetime import time
from dataclasses import dataclass
from typing import List
import polars as pl
import numpy as np
from numba import njit
import csv
import gc


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "data").is_dir() and (parent / "strategies").is_dir():
            return parent
    raise RuntimeError("Could not locate project root (expected 'data/' and 'strategies/' directories).")


@dataclass
class Trade:
    entry_date: str
    entry_time: str
    exit_date: str
    exit_time: str
    ce_strike: float
    pe_strike: float
    ce_entry_price: float
    pe_entry_price: float
    ce_exit_price: float
    pe_exit_price: float
    pnl: float
    hold_duration_minutes: int
    exit_reason: str


@njit
def run_strategy_on_expiry_numba(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    entry_hour_1: int, entry_min_1: int,
    entry_hour_2: int, entry_min_2: int,
    profit_target_pct: float,
    stop_loss_mult: float,
    max_hold_min: int
):
    """Process one expiry's data - same logic as slow version"""
    entry_time_1_sec = entry_hour_1 * 3600 + entry_min_1 * 60
    entry_time_2_sec = entry_hour_2 * 3600 + entry_min_2 * 60
    max_hold_sec = max_hold_min * 60
    
    n = len(timestamps_ns)
    max_trades = 1000
    
    # Output arrays
    entry_dates = np.empty(max_trades, dtype=np.int64)
    entry_times = np.empty(max_trades, dtype=np.int64)
    exit_dates = np.empty(max_trades, dtype=np.int64)
    exit_times = np.empty(max_trades, dtype=np.int64)
    ce_strikes = np.empty(max_trades, dtype=np.float64)
    pe_strikes = np.empty(max_trades, dtype=np.float64)
    ce_entry_prices = np.empty(max_trades, dtype=np.float64)
    pe_entry_prices = np.empty(max_trades, dtype=np.float64)
    ce_exit_prices = np.empty(max_trades, dtype=np.float64)
    pe_exit_prices = np.empty(max_trades, dtype=np.float64)
    pnls = np.empty(max_trades, dtype=np.float64)
    hold_mins = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    
    # Process each unique date
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time_sec = times_sec[i]
        
        # Check if this is an entry time
        if current_time_sec != entry_time_1_sec and current_time_sec != entry_time_2_sec:
            i += 1
            continue
        
        # Get all rows at this exact timestamp
        entry_ts_ns = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts_ns:
            i += 1
        block_end = i
        
        # Find ATM strikes
        ce_idx = -1
        pe_idx = -1
        min_ce_dist = 999999.0
        min_pe_dist = 999999.0
        
        for j in range(block_start, block_end):
            abs_dist = abs(distances[j])
            if opt_types[j] == 0:  # CE
                if abs_dist < min_ce_dist:
                    min_ce_dist = abs_dist
                    ce_idx = j
            else:  # PE
                if abs_dist < min_pe_dist:
                    min_pe_dist = abs_dist
                    pe_idx = j
        
        if ce_idx == -1 or pe_idx == -1:
            continue
        
        # Entry
        ce_strike = strikes[ce_idx]
        pe_strike = strikes[pe_idx]
        ce_entry_price = prices[ce_idx]
        pe_entry_price = prices[pe_idx]
        premium_received = ce_entry_price + pe_entry_price
        
        profit_target = premium_received * profit_target_pct
        stop_loss = premium_received * stop_loss_mult
        max_exit_time = current_time_sec + max_hold_sec
        
        # Scan forward
        exit_idx = -1
        ce_exit = 0.0
        pe_exit = 0.0
        exit_reason = 2
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            if times_sec[j] > max_exit_time:
                # Get last valid prices
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == ce_strike and opt_types[k] == 0:
                        ce_exit = prices[k]
                    if strikes[k] == pe_strike and opt_types[k] == 1:
                        pe_exit = prices[k]
                    if ce_exit > 0 and pe_exit > 0:
                        exit_idx = k
                        break
                break
            
            # Get prices at this timestamp
            ts_start = j
            curr_ts = timestamps_ns[j]
            curr_ce = 0.0
            curr_pe = 0.0
            
            while j < n and timestamps_ns[j] == curr_ts:
                if strikes[j] == ce_strike and opt_types[j] == 0:
                    curr_ce = prices[j]
                elif strikes[j] == pe_strike and opt_types[j] == 1:
                    curr_pe = prices[j]
                j += 1
            
            if curr_ce == 0 or curr_pe == 0:
                continue
            
            current_cost = curr_ce + curr_pe
            current_pnl = premium_received - current_cost
            
            if current_pnl >= profit_target:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 0
                break
            
            if current_pnl <= -stop_loss:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 1
                break
        
        if exit_idx != -1 and ce_exit > 0 and pe_exit > 0:
            pnl = premium_received - (ce_exit + pe_exit)
            hold_min = int((times_sec[exit_idx] - current_time_sec) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time_sec
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                ce_strikes[trade_count] = ce_strike
                pe_strikes[trade_count] = pe_strike
                ce_entry_prices[trade_count] = ce_entry_price
                pe_entry_prices[trade_count] = pe_entry_price
                ce_exit_prices[trade_count] = ce_exit
                pe_exit_prices[trade_count] = pe_exit
                pnls[trade_count] = pnl
                hold_mins[trade_count] = hold_min
                exit_reasons[trade_count] = exit_reason
                trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_prices[:trade_count], pe_entry_prices[:trade_count],
        ce_exit_prices[:trade_count], pe_exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count], exit_reasons[:trade_count]
    )


def int_to_date(date_int):
    """Convert date int back to date object"""
    from datetime import date
    # Polars date is days since epoch
    return date.fromordinal(date_int + 719163)  # Adjust for epoch difference


def sec_to_time(sec):
    """Convert seconds to time"""
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def main():
    import time as time_mod
    start = time_mod.time()
    
    print("="*80)
    print("FINAL NUMBA VERSION - Memory Efficient")
    print("="*80)
    
    root = _project_root()
    data_dir = root / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
    results_dir = root / "strategies" / "strategy_results" / "selling" / "strategy_results_numba_final"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    underlying = 'BANKNIFTY'
    all_trades = []
    
    print(f"\nProcessing {underlying}...")
    
    # Process date by date to save memory (like slow version)
    date_dirs = sorted(data_dir.glob("*"))
    processed = 0
    
    for date_dir in date_dirs:
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        underlying_dir = date_dir / underlying
        if not underlying_dir.exists():
            continue
        
        files = sorted(underlying_dir.glob("*.parquet"))
        if not files:
            continue
        
        # Load this date's data
        df = pl.read_parquet(files, columns=[
            'timestamp', 'strike', 'distance_from_spot',
            'opt_type', 'price', 'expiry'
        ]).filter(pl.col('timestamp').dt.year() > 1970)
        
        if df.is_empty():
            continue
        
        # *** KEY FIX: Only use NEAREST/IMMEDIATE expiry ***
        nearest_expiry = df['expiry'].min()
        df = df.filter(pl.col('expiry') == nearest_expiry)
        
        if df.is_empty():
            continue
        
        # Sort by timestamp
        df = df.sort('timestamp')
        
        # Convert to numpy (only nearest expiry data now)
        ts_ns = df['timestamp'].dt.epoch(time_unit='ns').to_numpy()
        dates = df['timestamp'].dt.date().cast(pl.Int32).to_numpy()
        hours = df['timestamp'].dt.hour().cast(pl.Int32).to_numpy()
        mins = df['timestamp'].dt.minute().cast(pl.Int32).to_numpy()
        secs = df['timestamp'].dt.second().cast(pl.Int32).to_numpy()
        times = hours * 3600 + mins * 60 + secs
        strikes = df['strike'].to_numpy()
        dists = df['distance_from_spot'].to_numpy()
        opt_t = (df['opt_type'] == 'PE').cast(pl.Int8).to_numpy()
        prices = df['price'].to_numpy()
            
        # Run Numba on nearest expiry only
        results = run_strategy_on_expiry_numba(
            ts_ns, dates, times, strikes, dists, opt_t, prices,
            9, 20, 13, 0,  # Entry times
            0.5, 2.0, 90   # Params
        )
        
        if len(results[0]) > 0:
            # Unpack and convert to trades
            (entry_dates, entry_times, exit_dates, exit_times,
             ce_strikes, pe_strikes, ce_entry_p, pe_entry_p,
             ce_exit_p, pe_exit_p, pnls, holds, reasons) = results
            
            exit_map = {0: 'profit_target', 1: 'stop_loss', 2: 'time_limit'}
            
            for i in range(len(pnls)):
                trade = Trade(
                    entry_date=str(int_to_date(entry_dates[i])),
                    entry_time=str(sec_to_time(entry_times[i])),
                    exit_date=str(int_to_date(exit_dates[i])),
                    exit_time=str(sec_to_time(exit_times[i])),
                    ce_strike=ce_strikes[i],
                    pe_strike=pe_strikes[i],
                    ce_entry_price=ce_entry_p[i],
                    pe_entry_price=pe_entry_p[i],
                    ce_exit_price=ce_exit_p[i],
                    pe_exit_price=pe_exit_p[i],
                    pnl=pnls[i],
                    hold_duration_minutes=int(holds[i]),
                    exit_reason=exit_map[reasons[i]]
                )
                all_trades.append(trade)
        
        processed += 1
        if processed % 10 == 0:
            print(f"  Processed {processed} dates, {len(all_trades)} trades so far...")
        
        del df
        gc.collect()
    
    # Save
    output_file = results_dir / f"{underlying}_ATM_Straddle_trades.csv"
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'ce_strike', 'pe_strike', 'ce_entry_price', 'pe_entry_price',
            'ce_exit_price', 'pe_exit_price', 'pnl', 'hold_duration_minutes', 'exit_reason'
        ])
        for t in all_trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.ce_strike, t.pe_strike, t.ce_entry_price, t.pe_entry_price,
                t.ce_exit_price, t.pe_exit_price, t.pnl, t.hold_duration_minutes, t.exit_reason
            ])
    
    # Stats
    if all_trades:
        pnls = [t.pnl for t in all_trades]
        wins = sum(1 for p in pnls if p > 0)
        total_pnl = sum(pnls)
        
        print(f"\n✓ COMPLETE in {time_mod.time()-start:.1f}s")
        print(f"  Trades: {len(all_trades)}")
        print(f"  Win Rate: {wins/len(all_trades)*100:.1f}%")
        print(f"  Total P&L: {total_pnl:.2f}")
        print(f"  Avg P&L: {total_pnl/len(all_trades):.2f}")
        print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()
