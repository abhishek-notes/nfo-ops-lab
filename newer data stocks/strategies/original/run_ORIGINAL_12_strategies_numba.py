#!/usr/bin/env python3
"""
ORIGINAL 12 STRATEGIES - Numba Optimized with Nearest-Expiry Filter
These are the original simple strategies, now with Numba optimization
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
import time as time_mod


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
    total_premium_received: float
    total_premium_paid: float
    pnl: float
    hold_duration_minutes: int
    exit_reason: str


@njit
def find_strikes_by_distance(distances: np.ndarray, opt_types: np.ndarray, target_dist: float):
    """Find strikes at specific distance from spot"""
    ce_idx = -1
    pe_idx = -1
    min_ce_diff = 999999.0
    min_pe_diff = 999999.0
    
    for i in range(len(distances)):
        if opt_types[i] == 0:  # CE
            if target_dist == 0:
                diff = abs(distances[i])
            else:
                if distances[i] <= 0:
                    continue
                diff = abs(distances[i] - target_dist)
            
            if diff < min_ce_diff:
                min_ce_diff = diff
                ce_idx = i
        else:  # PE
            if target_dist == 0:
                diff = abs(distances[i])
            else:
                if distances[i] >= 0:
                    continue
                diff = abs(distances[i] + target_dist)
            
            if diff < min_pe_diff:
                min_pe_diff = diff
                pe_idx = i
    
    return ce_idx, pe_idx


@njit
def run_original_strategy_numba(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray,
    entry_times_sec: np.ndarray,
    otm_pct: float,       # 0 for ATM, or percentage for OTM
    hold_until_close: bool  # True = hold until 15:25, False = hold until next day 09:20
):
    """Original strategies: Enter at time, exit at specific time"""
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
    
    trade_count = 0
    
    # Exit time
    if hold_until_close:
        exit_time_sec = 15 * 3600 + 25 * 60  # 15:25
    else:
        exit_time_sec = 9 * 3600 + 20 * 60   # Next day 09:20
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time_sec = times_sec[i]
        
        # Check if entry time
        is_entry = False
        for et in entry_times_sec:
            if current_time_sec == et:
                is_entry = True
                break
        
        if not is_entry:
            i += 1
            continue
        
        # Get timestamp block
        entry_ts_ns = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts_ns:
            i += 1
        block_end = i
        
        # Calculate OTM distance
        if otm_pct > 0:
            spot = spots[block_start]
            target_dist = spot * otm_pct
        else:
            target_dist = 0.0
        
        # Find strikes
        ce_idx, pe_idx = find_strikes_by_distance(
            distances[block_start:block_end],
            opt_types[block_start:block_end],
            target_dist
        )
        
        if ce_idx == -1 or pe_idx == -1:
            continue
        
        ce_idx += block_start
        pe_idx += block_start
        
        # Entry
        ce_strike = strikes[ce_idx]
        pe_strike = strikes[pe_idx]
        ce_entry_price = prices[ce_idx]
        pe_entry_price = prices[pe_idx]
        
        # Find exit
        exit_idx = -1
        ce_exit = 0.0
        pe_exit = 0.0
        
        # Scan forward
        j = block_end
        while j < n:
            # Check if we've reached exit time
            if hold_until_close:
                # Exit same day at 15:25
                if dates_int[j] != current_date:
                    break
                if times_sec[j] >= exit_time_sec:
                    # Get prices at or before exit time
                    for k in range(j, block_start, -1):
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
            else:
                # Exit next day at 09:20
                if dates_int[j] > current_date and times_sec[j] >= exit_time_sec:
                    # Get prices at this exact time
                    ts_start = j
                    curr_ts = timestamps_ns[j]
                    
                    while j < n and timestamps_ns[j] == curr_ts:
                        if strikes[j] == ce_strike and opt_types[j] == 0:
                            ce_exit = prices[j]
                        if strikes[j] == pe_strike and opt_types[j] == 1:
                            pe_exit = prices[j]
                        j += 1
                    
                    if ce_exit > 0 and pe_exit > 0:
                        exit_idx = ts_start
                    break
            
            j += 1
        
        if exit_idx != -1 and ce_exit > 0 and pe_exit > 0:
            pnl = (ce_entry_price + pe_entry_price) - (ce_exit + pe_exit)
            hold_min = int((times_sec[exit_idx] - current_time_sec + (dates_int[exit_idx] - current_date) * 24 * 3600) / 60)
            
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
                trade_count += 1
        
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_prices[:trade_count], pe_entry_prices[:trade_count],
        ce_exit_prices[:trade_count], pe_exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


# ORIGINAL 12 STRATEGY CONFIGURATIONS
ORIGINAL_STRATEGIES = [
    {"name": "ATM_Straddle_0930_close", "entry_times": [time(9, 30)], "otm_pct": 0.0, "hold_until_close": True},
    {"name": "ATM_Straddle_1300_close", "entry_times": [time(13, 0)], "otm_pct": 0.0, "hold_until_close": True},
    {"name": "ATM_Straddle_0930_nextday", "entry_times": [time(9, 30)], "otm_pct": 0.0, "hold_until_close": False},
    {"name": "OTM_Strangle_1pct_0930_close", "entry_times": [time(9, 30)], "otm_pct": 0.01, "hold_until_close": True},
    {"name": "OTM_Strangle_1pct_1300_close", "entry_times": [time(13, 0)], "otm_pct": 0.01, "hold_until_close": True},
    {"name": "OTM_Strangle_1pct_0930_nextday", "entry_times": [time(9, 30)], "otm_pct": 0.01, "hold_until_close": False},
    {"name": "OTM_Strangle_2pct_0930_close", "entry_times": [time(9, 30)], "otm_pct": 0.02, "hold_until_close": True},
    {"name": "OTM_Strangle_2pct_1300_close", "entry_times": [time(13, 0)], "otm_pct": 0.02, "hold_until_close": True},
    {"name": "OTM_Strangle_2pct_0930_nextday", "entry_times": [time(9, 30)], "otm_pct": 0.02, "hold_until_close": False},
    {"name": "OTM_Strangle_05pct_0930_close", "entry_times": [time(9, 30)], "otm_pct": 0.005, "hold_until_close": True},
    {"name": "OTM_Strangle_05pct_1300_close", "entry_times": [time(13, 0)], "otm_pct": 0.005, "hold_until_close": True},
    {"name": "OTM_Strangle_05pct_0930_nextday", "entry_times": [time(9, 30)], "otm_pct": 0.005, "hold_until_close": False},
]


def run_one_original_strategy(data_dir: Path, underlying: str, strategy: dict) -> List[Trade]:
    """Run one original strategy"""
    all_trades = []
    
    date_dirs = sorted(data_dir.glob("*"))
    
    for date_dir in date_dirs:
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        underlying_dir = date_dir / underlying
        if not underlying_dir.exists():
            continue
        
        files = list(underlying_dir.glob("*.parquet"))
        if not files:
            continue
        
        df = pl.read_parquet(files[0], columns=[
            'timestamp', 'strike', 'distance_from_spot',
            'opt_type', 'price', 'expiry', 'spot_price'
        ]).filter(pl.col('timestamp').dt.year() > 1970)
        
        if df.is_empty():
            continue
        
        # Nearest expiry only
        nearest_expiry = df['expiry'].min()
        df = df.filter(pl.col('expiry') == nearest_expiry).sort('timestamp')
        
        if df.is_empty():
            continue
        
        # Convert to numpy
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
        spots = df['spot_price'].to_numpy()
        
        entry_times_sec = np.array([t.hour * 3600 + t.minute * 60 for t in strategy['entry_times']], dtype=np.int64)
        
        results = run_original_strategy_numba(
            ts_ns, dates, times, strikes, dists, opt_t, prices, spots,
            entry_times_sec, strategy['otm_pct'], strategy['hold_until_close']
        )
        
        if len(results[0]) > 0:
            (entry_dates, entry_times, exit_dates, exit_times,
             ce_strikes, pe_strikes, ce_entry_p, pe_entry_p,
             ce_exit_p, pe_exit_p, pnls, holds) = results
            
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
                    total_premium_received=ce_entry_p[i] + pe_entry_p[i],
                    total_premium_paid=ce_exit_p[i] + pe_exit_p[i],
                    pnl=pnls[i],
                    hold_duration_minutes=int(holds[i]),
                    exit_reason='time_limit'
                )
                all_trades.append(trade)
        
        del df
        gc.collect()
    
    return all_trades


def save_original_trades(trades: List[Trade], filename: Path):
    """Save trades to CSV (original format)"""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'ce_strike', 'pe_strike', 'ce_entry_price', 'pe_entry_price',
            'ce_exit_price', 'pe_exit_price', 'total_premium_received',
            'total_premium_paid', 'pnl', 'hold_duration_minutes', 'exit_reason'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.ce_strike, t.pe_strike, t.ce_entry_price, t.pe_entry_price,
                t.ce_exit_price, t.pe_exit_price, t.total_premium_received,
                t.total_premium_paid, t.pnl, t.hold_duration_minutes, t.exit_reason
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("ORIGINAL 12 STRATEGIES - Numba Optimized")
    print("="*80)
    
    data_dir = Path("../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../results/strategy_results_original_optimized")
    results_dir.mkdir(exist_ok=True)
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        for idx, strategy in enumerate(ORIGINAL_STRATEGIES, 1):
            strategy_name = f"{underlying}_{strategy['name']}"
            print(f"\n[{idx}/12] {strategy_name}...")
            
            strat_start = time_mod.time()
            trades = run_one_original_strategy(data_dir, underlying, strategy)
            strat_time = time_mod.time() - strat_start
            
            if trades:
                pnls = [t.pnl for t in trades]
                wins = sum(1 for p in pnls if p > 0)
                total_pnl = sum(pnls)
                
                print(f"  ⚡ {strat_time:.1f}s | Trades: {len(trades)} | Win: {wins/len(trades)*100:.1f}% | P&L: {total_pnl:.2f}")
                
                all_results.append({
                    'strategy': strategy_name,
                    'trades': len(trades),
                    'wins': wins,
                    'pnl': total_pnl,
                    'time_sec': strat_time
                })
            
            output_file = results_dir / f"{strategy_name}_trades.csv"
            save_original_trades(trades, output_file)
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ ALL COMPLETE in {total_time/60:.1f} minutes")
    print(f"{'='*80}")
    
    # Summary
    summary_file = results_dir / "original_strategies_summary.csv"
    with open(summary_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['strategy', 'trades', 'wins', 'win_rate_%', 'total_pnl', 'time_sec'])
        for r in all_results:
            writer.writerow([
                r['strategy'], r['trades'], r['wins'],
                f"{r['wins']/r['trades']*100:.1f}" if r['trades'] > 0 else "0",
                f"{r['pnl']:.2f}", f"{r['time_sec']:.1f}"
            ])
    
    print(f"\nResults: {results_dir}/")


if __name__ == "__main__":
    main()
