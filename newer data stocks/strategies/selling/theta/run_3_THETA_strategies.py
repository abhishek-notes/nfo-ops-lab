#!/usr/bin/env python3
"""
3 THETA STRATEGIES - Numba Optimized Implementation
1. Morning Theta Harvest (09:25-11:30)
2. Afternoon Calm Strangle (12:30-15:15)  
3. Breakout Adaptive Straddle (09:45-15:00)
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
    strategy_name: str


@njit
def find_atm_strikes(distances: np.ndarray, opt_types: np.ndarray):
    """Find ATM strikes (minimum absolute distance)"""
    ce_idx = -1
    pe_idx = -1
    min_ce_dist = 999999.0
    min_pe_dist = 999999.0
    
    for i in range(len(distances)):
        abs_dist = abs(distances[i])
        if opt_types[i] == 0:  # CE
            if abs_dist < min_ce_dist:
                min_ce_dist = abs_dist
                ce_idx = i
        else:  # PE
            if abs_dist < min_pe_dist:
                min_pe_dist = abs_dist
                pe_idx = i
    
    return ce_idx, pe_idx


@njit
def find_otm_strikes(distances: np.ndarray, opt_types: np.ndarray, spots: np.ndarray, otm_pct: float):
    """Find OTM strikes at specific % from spot"""
    target_dist = spots[0] * otm_pct
    ce_idx = -1
    pe_idx = -1
    min_ce_diff = 999999.0
    min_pe_diff = 999999.0
    
    for i in range(len(distances)):
        if opt_types[i] == 0:  # CE
            if distances[i] <= 0:
                continue
            diff = abs(distances[i] - target_dist)
            if diff < min_ce_diff:
                min_ce_diff = diff
                ce_idx = i
        else:  # PE
            if distances[i] >= 0:
                continue
            diff = abs(distances[i] + target_dist)
            if diff < min_pe_diff:
                min_pe_diff = diff
                pe_idx = i
    
    return ce_idx, pe_idx


@njit
def strategy1_morning_theta_harvest(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray
):
    """
    Strategy 1: Morning Theta Harvest
    Enter: 09:25-09:35, ATM straddle
    Exit: 40% decay OR 11:30 OR stop loss (150% of premium)
    """
    entry_start = 9*3600 + 25*60
    entry_end = 9*3600 + 35*60
    exit_time = 11*3600 + 30*60
    
    max_trades = 200
    entry_dates = np.empty(max_trades, dtype=np.int64)
    entry_times = np.empty(max_trades, dtype=np.int64)
    exit_dates = np.empty(max_trades, dtype=np.int64)
    exit_times = np.empty(max_trades, dtype=np.int64)
    ce_strikes = np.empty(max_trades, dtype=np.float64)
    pe_strikes = np.empty(max_trades, dtype=np.float64)
    ce_entry_p = np.empty(max_trades, dtype=np.float64)
    pe_entry_p = np.empty(max_trades, dtype=np.float64)
    ce_exit_p = np.empty(max_trades, dtype=np.float64)
    pe_exit_p = np.empty(max_trades, dtype=np.float64)
    pnls = np.empty(max_trades, dtype=np.float64)
    hold_mins = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    n = len(timestamps_ns)
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time = times_sec[i]
        
        # Entry window check
        if current_time < entry_start or current_time > entry_end:
            i += 1
            continue
        
        # Get timestamp block
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        # Find ATM strikes
        ce_idx, pe_idx = find_atm_strikes(
            distances[block_start:block_end],
            opt_types[block_start:block_end]
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
        entry_credit = ce_entry_price + pe_entry_price
        
        # Targets
        profit_target = entry_credit * 0.6  # 40% decay
        stop_loss = entry_credit * 2.5  # 150% loss
        
        # Scan forward for exit
        exit_idx = -1
        ce_exit = 0.0
        pe_exit = 0.0
        exit_reason = 2  # time_limit
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            if times_sec[j] >= exit_time:
                # Time limit hit
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == ce_strike and opt_types[k] == 0:
                        ce_exit = prices[k]
                    if strikes[k] == pe_strike and opt_types[k] == 1:
                        pe_exit = prices[k]
                    if ce_exit > 0 and pe_exit > 0:
                        exit_idx = k
                        exit_reason = 2
                        break
                break
            
            # Check current prices
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
            
            # Profit target
            if current_cost <= profit_target:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 0
                break
            
            # Stop loss
            if current_cost >= stop_loss:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 1
                break
        
        if exit_idx != -1 and ce_exit > 0 and pe_exit > 0:
            pnl = entry_credit - (ce_exit + pe_exit)
            hold_min = int((times_sec[exit_idx] - current_time) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                ce_strikes[trade_count] = ce_strike
                pe_strikes[trade_count] = pe_strike
                ce_entry_p[trade_count] = ce_entry_price
                pe_entry_p[trade_count] = pe_entry_price
                ce_exit_p[trade_count] = ce_exit
                pe_exit_p[trade_count] = pe_exit
                pnls[trade_count] = pnl
                hold_mins[trade_count] = hold_min
                exit_reasons[trade_count] = exit_reason
                trade_count += 1
        
        # Skip to next day after entry
        while i < n and dates_int[i] == current_date:
            i += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_p[:trade_count], pe_entry_p[:trade_count],
        ce_exit_p[:trade_count], pe_exit_p[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count], exit_reasons[:trade_count]
    )


@njit  
def strategy2_afternoon_calm_strangle(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray
):
    """
    Strategy 2: Afternoon Calm Strangle
    Enter: 13:00-13:30, 1% OTM strangle
    Exit: 50% decay OR 15:10 OR stop loss (200% premium)
    """
    entry_start = 13*3600
    entry_end = 13*3600 + 30*60
    exit_time = 15*3600 + 10*60
    
    max_trades = 200
    entry_dates = np.empty(max_trades, dtype=np.int64)
    entry_times = np.empty(max_trades, dtype=np.int64)
    exit_dates = np.empty(max_trades, dtype=np.int64)
    exit_times = np.empty(max_trades, dtype=np.int64)
    ce_strikes = np.empty(max_trades, dtype=np.float64)
    pe_strikes = np.empty(max_trades, dtype=np.float64)
    ce_entry_p = np.empty(max_trades, dtype=np.float64)
    pe_entry_p = np.empty(max_trades, dtype=np.float64)
    ce_exit_p = np.empty(max_trades, dtype=np.float64)
    pe_exit_p = np.empty(max_trades, dtype=np.float64)
    pnls = np.empty(max_trades, dtype=np.float64)
    hold_mins = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    n = len(timestamps_ns)
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time = times_sec[i]
        
        # Entry window
        if current_time < entry_start or current_time > entry_end:
            i += 1
            continue
        
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        # Find 1% OTM strikes
        ce_idx, pe_idx = find_otm_strikes(
            distances[block_start:block_end],
            opt_types[block_start:block_end],
            spots[block_start:block_end],
            0.01
        )
        
        if ce_idx == -1 or pe_idx == -1:
            continue
        
        ce_idx += block_start
        pe_idx += block_start
        
        ce_strike = strikes[ce_idx]
        pe_strike = strikes[pe_idx]
        ce_entry_price = prices[ce_idx]
        pe_entry_price = prices[pe_idx]
        entry_credit = ce_entry_price + pe_entry_price
        
        profit_target = entry_credit * 0.5  # 50% decay
        stop_loss = entry_credit * 3.0  # 200% loss
        
        exit_idx = -1
        ce_exit = 0.0
        pe_exit = 0.0
        exit_reason = 2
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            if times_sec[j] >= exit_time:
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
            
            if current_cost <= profit_target:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 0
                break
            
            if current_cost >= stop_loss:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 1
                break
        
        if exit_idx != -1 and ce_exit > 0 and pe_exit > 0:
            pnl = entry_credit - (ce_exit + pe_exit)
            hold_min = int((times_sec[exit_idx] - current_time) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                ce_strikes[trade_count] = ce_strike
                pe_strikes[trade_count] = pe_strike
                ce_entry_p[trade_count] = ce_entry_price
                pe_entry_p[trade_count] = pe_entry_price
                ce_exit_p[trade_count] = ce_exit
                pe_exit_p[trade_count] = pe_exit
                pnls[trade_count] = pnl
                hold_mins[trade_count] = hold_min
                exit_reasons[trade_count] = exit_reason
                trade_count += 1
        
        while i < n and dates_int[i] == current_date and times_sec[i] < entry_end:
            i += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_p[:trade_count], pe_entry_p[:trade_count],
        ce_exit_p[:trade_count], pe_exit_p[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count], exit_reasons[:trade_count]
    )


@njit
def strategy3_breakout_adaptive(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray
):
    """
    Strategy 3: Breakout Adaptive Straddle  
    Enter: 09:45-10:15, ATM straddle
    Monitor premium ratio, close loser if >1.8x
    Exit: 50% decay OR 15:00 OR stop loss (200% per leg)
    """
    entry_start = 9*3600 + 45*60
    entry_end = 10*3600 + 15*60
    exit_time = 15*3600
    
    max_trades = 200
    entry_dates = np.empty(max_trades, dtype=np.int64)
    entry_times = np.empty(max_trades, dtype=np.int64)
    exit_dates = np.empty(max_trades, dtype=np.int64)
    exit_times = np.empty(max_trades, dtype=np.int64)
    ce_strikes = np.empty(max_trades, dtype=np.float64)
    pe_strikes = np.empty(max_trades, dtype=np.float64)
    ce_entry_p = np.empty(max_trades, dtype=np.float64)
    pe_entry_p = np.empty(max_trades, dtype=np.float64)
    ce_exit_p = np.empty(max_trades, dtype=np.float64)
    pe_exit_p = np.empty(max_trades, dtype=np.float64)
    pnls = np.empty(max_trades, dtype=np.float64)
    hold_mins = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    n = len(timestamps_ns)
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time = times_sec[i]
        
        if current_time < entry_start or current_time > entry_end:
            i += 1
            continue
        
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        ce_idx, pe_idx = find_atm_strikes(
            distances[block_start:block_end],
            opt_types[block_start:block_end]
        )
        
        if ce_idx == -1 or pe_idx == -1:
            continue
        
        ce_idx += block_start
        pe_idx += block_start
        
        ce_strike = strikes[ce_idx]
        pe_strike = strikes[pe_idx]
        ce_entry_price = prices[ce_idx]
        pe_entry_price = prices[pe_idx]
        entry_credit = ce_entry_price + pe_entry_price
        
        profit_target = entry_credit * 0.5
        stop_loss_per_leg = entry_credit  # 100% of total, or 200% per leg
        
        exit_idx = -1
        ce_exit = 0.0
        pe_exit = 0.0
        exit_reason = 2
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            if times_sec[j] >= exit_time:
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
            
            # Premium ratio check (adaptive logic)
            if curr_ce > 0 and curr_pe > 0:
                ratio = max(curr_ce, curr_pe) / min(curr_ce, curr_pe)
                if ratio > 1.8:
                    # One side losing badly - simplified: just exit both
                    ce_exit = curr_ce
                    pe_exit = curr_pe
                    exit_idx = ts_start
                    exit_reason = 1  # adaptive exit
                    break
            
            if current_cost <= profit_target:
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 0
                break
            
            if current_cost >= (entry_credit + stop_loss_per_leg):
                ce_exit = curr_ce
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 1
                break
        
        if exit_idx != -1 and ce_exit > 0 and pe_exit > 0:
            pnl = entry_credit - (ce_exit + pe_exit)
            hold_min = int((times_sec[exit_idx] - current_time) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                ce_strikes[trade_count] = ce_strike
                pe_strikes[trade_count] = pe_strike
                ce_entry_p[trade_count] = ce_entry_price
                pe_entry_p[trade_count] = pe_entry_price
                ce_exit_p[trade_count] = ce_exit
                pe_exit_p[trade_count] = pe_exit
                pnls[trade_count] = pnl
                hold_mins[trade_count] = hold_min
                exit_reasons[trade_count] = exit_reason
                trade_count += 1
        
        while i < n and dates_int[i] == current_date:
            i += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_p[:trade_count], pe_entry_p[:trade_count],
        ce_exit_p[:trade_count], pe_exit_p[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count], exit_reasons[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def run_strategy(data_dir: Path, underlying: str, strategy_func, strategy_name: str):
    """Run one theta strategy"""
    all_trades = []
    
    date_dirs = sorted(data_dir.glob("*"))
    
    for date_dir in date_dirs:
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        underlying_dir = date_dir / underlying
        if not underlying_dir.exists():
            continue
        
        files = sorted(underlying_dir.glob("*.parquet"))
        if not files:
            continue
        
        df = pl.read_parquet(files, columns=[
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
        
        # Run strategy
        results = strategy_func(ts_ns, dates, times, strikes, dists, opt_t, prices, spots)
        
        if len(results[0]) > 0:
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
                    exit_reason=exit_map[reasons[i]],
                    strategy_name=strategy_name
                )
                all_trades.append(trade)
        
        del df
        gc.collect()
    
    return all_trades


def save_trades(trades: List[Trade], filename: Path):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'ce_strike', 'pe_strike', 'ce_entry_price', 'pe_entry_price',
            'ce_exit_price', 'pe_exit_price', 'pnl', 'hold_duration_minutes', 'exit_reason'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.ce_strike, t.pe_strike, t.ce_entry_price, t.pe_entry_price,
                t.ce_exit_price, t.pe_exit_price, t.pnl, t.hold_duration_minutes, t.exit_reason
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("3 THETA STRATEGIES - Systematic Implementation")
    print("="*80)
    
    root = _project_root()
    data_dir = root / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
    results_dir = root / "strategies" / "strategy_results" / "selling" / "strategy_results_theta"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    strategies = [
        (strategy1_morning_theta_harvest, "THETA_1_Morning_Harvest_0925_1130"),
        (strategy2_afternoon_calm_strangle, "THETA_2_Afternoon_Calm_1300_1510"),
        (strategy3_breakout_adaptive, "THETA_3_Breakout_Adaptive_0945_1500"),
    ]
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        for idx, (strat_func, strat_name) in enumerate(strategies, 1):
            full_name = f"{underlying}_{strat_name}"
            print(f"\n[{idx}/3] {full_name}...")
            
            strat_start = time_mod.time()
            trades = run_strategy(data_dir, underlying, strat_func, full_name)
            strat_time = time_mod.time() - strat_start
            
            if trades:
                pnls = [t.pnl for t in trades]
                wins = sum(1 for p in pnls if p > 0)
                total_pnl = sum(pnls)
                
                print(f"  ⚡ {strat_time:.1f}s | Trades: {len(trades)} | Win: {wins/len(trades)*100:.1f}% | P&L: {total_pnl:.2f}")
                
                all_results.append({
                    'strategy': full_name,
                    'trades': len(trades),
                    'wins': wins,
                    'pnl': total_pnl,
                    'time_sec': strat_time
                })
            
            output_file = results_dir / f"{full_name}_trades.csv"
            save_trades(trades, output_file)
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ ALL 3 STRATEGIES COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    # Summary
    summary_file = results_dir / "theta_strategies_summary.csv"
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
