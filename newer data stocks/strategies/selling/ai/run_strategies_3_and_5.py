#!/usr/bin/env python3
"""
STRATEGIES 3 & 5 - Clean Implementation and Testing
"""

from pathlib import Path
from datetime import time, date
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
    pnl: float
    hold_duration_minutes: int
    exit_reason: str
    strategy_name: str


def int_to_date(date_int):
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def prepare_ema_context(df: pl.DataFrame):
    """Prepare EMA arrays for trend detection"""
    spot_data = df.select(['timestamp', 'spot_price']).unique(subset=['timestamp']).sort('timestamp')
    
    # 5-minute candles for EMA
    spot_5min = spot_data.group_by_dynamic('timestamp', every='5m').agg([
        pl.col('spot_price').first().alias('open'),
        pl.col('spot_price').max().alias('high'),
        pl.col('spot_price').min().alias('low'),
        pl.col('spot_price').last().alias('close'),
    ])
    
    ema_df = spot_5min.with_columns([
        pl.col('close').ewm_mean(span=5).alias('ema5'),
        pl.col('close').ewm_mean(span=21).alias('ema21'),
    ])
    
    ema_times = ema_df.select(
        (pl.col('timestamp').dt.hour() * 3600 + pl.col('timestamp').dt.minute() * 60).alias('sec')
    )['sec'].to_numpy()
    
    ema5_vals = ema_df['ema5'].to_numpy()
    ema21_vals = ema_df['ema21'].to_numpy()
    spot_5min_vals = ema_df['close'].to_numpy()
    
    return {
        'ema_times': ema_times,
        'ema5': ema5_vals,
        'ema21': ema21_vals,
        'spot_5min': spot_5min_vals,
    }


@njit
def find_otm_put(distances: np.ndarray, opt_types: np.ndarray, otm_dist: float):
    """Find OTM Put at specific distance below spot"""
    pe_idx = -1
    min_diff = 999999.0
    
    for i in range(len(distances)):
        if opt_types[i] == 1:  # PE only
            if distances[i] >= 0:  # Skip ITM
                continue
            diff = abs(distances[i] + otm_dist)
            if diff < min_diff:
                min_diff = diff
                pe_idx = i
    
    return pe_idx


@njit
def find_max_oi_strikes(oi_array: np.ndarray, strikes_array: np.ndarray, opt_types: np.ndarray):
    """Find strikes with maximum OI for CE and PE"""
    max_ce_oi = 0.0
    max_pe_oi = 0.0
    ce_strike = 0.0
    pe_strike = 0.0
    ce_idx = -1
    pe_idx = -1
    
    for i in range(len(oi_array)):
        if opt_types[i] == 0:  # CE
            if oi_array[i] > max_ce_oi:
                max_ce_oi = oi_array[i]
                ce_strike = strikes_array[i]
                ce_idx = i
        else:  # PE
            if oi_array[i] > max_pe_oi:
                max_pe_oi = oi_array[i]
                pe_strike = strikes_array[i]
                pe_idx = i
    
    return ce_idx, pe_idx, ce_strike, pe_strike


@njit
def lookup_ema(current_time: int, ema_times: np.ndarray, ema5: np.ndarray, ema21: np.ndarray, spot_5min: np.ndarray):
    """Get EMA values at current time"""
    if len(ema_times) == 0:
        return 0.0, 0.0, 0.0
    
    idx = 0
    min_diff = abs(ema_times[0] - current_time)
    for i in range(1, len(ema_times)):
        diff = abs(ema_times[i] - current_time)
        if diff < min_diff:
            min_diff = diff
            idx = i
    
    return ema5[idx], ema21[idx], spot_5min[idx]


# STRATEGY 3: Trend-Pause (simplified version for testing)
@njit
def strategy3_trend_pause_simple(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray,
    ema_times: np.ndarray,
    ema5: np.ndarray,
    ema21: np.ndarray,
    spot_5min: np.ndarray
):
    """
    Strategy 3: Simplified Trend-Pause - Sell OTM Put in uptrend
    """
    entry_start = 10*3600
    entry_end = 14*3600
    exit_time = 15*3600
    
    max_trades = 100
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
    holds = np.empty(max_trades, dtype=np.int64)
    reasons = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    n = len(timestamps_ns)
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time = times_sec[i]
        
        if current_time < entry_start or current_time > entry_end:
            i += 1
            continue
        
        # Skip if not on 5-minute boundary (reduce noise)
        if current_time % 300 != 0:
            i += 1
            continue
        
        # Check EMA condition
        ema5_val, ema21_val, spot_val = lookup_ema(current_time, ema_times, ema5, ema21, spot_5min)
        
        # Need uptrend: EMA5 > EMA21
        if ema5_val <= ema21_val or ema5_val == 0:
            i += 1
            continue
        
        # Spot within 0.5% of EMA5 (proxim for pullback)
        if abs(spot_val - ema5_val) / ema5_val > 0.005:
            i += 1
            continue
        
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        # Sell OTM Put (100 points below)
        otm_dist = 100.0
        pe_idx = find_otm_put(
            distances[block_start:block_end],
            opt_types[block_start:block_end],
            otm_dist
        )
        
        if pe_idx == -1:
            continue
        
        pe_idx += block_start
        pe_strike = strikes[pe_idx]
        pe_price = prices[pe_idx]
        
        stop_loss = pe_price * 2.0
        profit_target = pe_price * 0.5
        
        pe_exit = 0.0
        exit_idx = -1
        exit_reason = 2
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            # Exit at close
            if times_sec[j] >= exit_time:
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == pe_strike and opt_types[k] == 1:
                        pe_exit = prices[k]
                        exit_idx = k
                        break
                break
            
            # Check current price
            ts_start = j
            curr_ts = timestamps_ns[j]
            curr_pe = 0.0
            
            while j < n and timestamps_ns[j] == curr_ts:
                if strikes[j] == pe_strike and opt_types[j] == 1:
                    curr_pe = prices[j]
                j += 1
            
            if curr_pe == 0:
                continue
            
            if curr_pe <= profit_target:
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 0
                break
            
            if curr_pe >= stop_loss:
                pe_exit = curr_pe
                exit_idx = ts_start
                exit_reason = 1
                break
        
        if exit_idx != -1 and pe_exit > 0:
            pnl = pe_price - pe_exit
            hold_min = int((times_sec[exit_idx] - current_time) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                ce_strikes[trade_count] = 0.0
                pe_strikes[trade_count] = pe_strike
                ce_entry_p[trade_count] = 0.0
                pe_entry_p[trade_count] = pe_price
                ce_exit_p[trade_count] = 0.0
                pe_exit_p[trade_count] = pe_exit
                pnls[trade_count] = pnl
                holds[trade_count] = hold_min
                reasons[trade_count] = exit_reason
                trade_count += 1
        
        # Skip forward 30 minutes
        while i < n and times_sec[i] < current_time + 1800:
            i += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_p[:trade_count], pe_entry_p[:trade_count],
        ce_exit_p[:trade_count], pe_exit_p[:trade_count],
        pnls[:trade_count], holds[:trade_count], reasons[:trade_count]
    )


# STRATEGY 5: Expiry Gamma Surfer (simplified - only weekly expiry, sell ATM on those days at 1:30pm)
@njit
def strategy5_gamma_surfer_simple(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray,
    oi_array: np.ndarray
):
    """
    Strategy 5: Simplified - Sell max OI strikes at 1:30 PM
    (Treating every Thursday as potential expiry)
    """
    entry_time = 13*3600 + 30*60
    exit_time = 15*3600 + 15*60
    
    max_trades = 20
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
    holds = np.empty(max_trades, dtype=np.int64)
    reasons = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    n = len(timestamps_ns)
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time = times_sec[i]
        
        if current_time != entry_time:
            i += 1
            continue
        
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        # Find max OI strikes
        ce_idx, pe_idx, ce_strike_val, pe_strike_val = find_max_oi_strikes(
            oi_array[block_start:block_end],
            strikes[block_start:block_end],
            opt_types[block_start:block_end]
        )
        
        if ce_idx == -1 or pe_idx == -1:
            continue
        
        ce_idx += block_start
        pe_idx += block_start
        
        # Safety check
        current_spot = spots[block_start]
        if current_spot >= ce_strike_val or current_spot <= pe_strike_val:
            continue
        
        ce_price = prices[ce_idx]
        pe_price = prices[pe_idx]
        
        ce_exit = 0.0
        pe_exit = 0.0
        exit_idx = -1
        exit_reason = 2
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            # Strike breach check
            curr_spot = spots[j]
            if curr_spot >= ce_strike_val or curr_spot <= pe_strike_val:
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == ce_strike_val and opt_types[k] == 0:
                        ce_exit = prices[k]
                    if strikes[k] == pe_strike_val and opt_types[k] == 1:
                        pe_exit = prices[k]
                    if ce_exit > 0 and pe_exit > 0:
                        exit_idx = k
                        exit_reason = 1
                        break
                break
            
            if times_sec[j] >= exit_time:
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == ce_strike_val and opt_types[k] == 0:
                        ce_exit = prices[k]
                    if strikes[k] == pe_strike_val and opt_types[k] == 1:
                        pe_exit = prices[k]
                    if ce_exit > 0 and pe_exit > 0:
                        exit_idx = k
                        break
                break
            
            j += 1
        
        if exit_idx != -1 and ce_exit > 0 and pe_exit > 0:
            pnl = (ce_price + pe_price) - (ce_exit + pe_exit)
            hold_min = int((times_sec[exit_idx] - current_time) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                ce_strikes[trade_count] = ce_strike_val
                pe_strikes[trade_count] = pe_strike_val
                ce_entry_p[trade_count] = ce_price
                pe_entry_p[trade_count] = pe_price
                ce_exit_p[trade_count] = ce_exit
                pe_exit_p[trade_count] = pe_exit
                pnls[trade_count] = pnl
                holds[trade_count] = hold_min
                reasons[trade_count] = exit_reason
                trade_count += 1
        
        while i < n and dates_int[i] == current_date:
            i += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_p[:trade_count], pe_entry_p[:trade_count],
        ce_exit_p[:trade_count], pe_exit_p[:trade_count],
        pnls[:trade_count], holds[:trade_count], reasons[:trade_count]
    )


def run_strategy(df: pl.DataFrame, strategy_func, strategy_name: str, use_ema: bool = False):
    """Run one strategy"""
    if df.is_empty():
        return []
    
    # Prepare context
    if use_ema:
        context = prepare_ema_context(df)
    
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
    oi = df['oi'].to_numpy()
    
    # Run strategy
    if use_ema:
        results = strategy_func(
            ts_ns, dates, times, strikes, dists, opt_t, prices, spots,
            context['ema_times'], context['ema5'], context['ema21'], context['spot_5min']
        )
    else:
        results = strategy_func(
            ts_ns, dates, times, strikes, dists, opt_t, prices, spots, oi
        )
    
    if len(results[0]) == 0:
        return []
    
    (entry_dates, entry_times, exit_dates, exit_times,
     ce_strikes, pe_strikes, ce_entry_p, pe_entry_p,
     ce_exit_p, pe_exit_p, pnls, holds, reasons) = results
    
    trades = []
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
        trades.append(trade)
    
    return trades


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
    print("TESTING STRATEGIES 3 & 5")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    
    strategies = [
        (strategy3_trend_pause_simple, "AI_STRAT3_Trend_Pause", True),
        (strategy5_gamma_surfer_simple, "AI_STRAT5_Expiry_Gamma", False),
    ]
    
    for strat_func, strat_name, use_ema in strategies:
        results_dir = Path(f"../results/strategy_results_{strat_name.lower()}")
        results_dir.mkdir(exist_ok=True)
        
        print(f"\n{'='*80}")
        print(f"RUNNING: {strat_name}")
        print(f"{'='*80}")
        
        for underlying in ['BANKNIFTY', 'NIFTY']:
            print(f"\n{underlying}...")
            
            all_trades = []
            date_dirs = sorted(data_dir.glob("*"))
            processed = 0
            
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
                    'opt_type', 'price', 'expiry', 'spot_price', 'oi'
                ]).filter(pl.col('timestamp').dt.year() > 1970)
                
                if df.is_empty():
                    continue
                
                nearest_expiry = df['expiry'].min()
                df = df.filter(pl.col('expiry') == nearest_expiry).sort('timestamp')
                
                if df.is_empty():
                    continue
                
                trades = run_strategy(df, strat_func, strat_name, use_ema)
                all_trades.extend(trades)
                
                processed += 1
                if processed % 20 == 0:
                    print(f"  Processed {processed} dates, {len(all_trades)} trades...")
                
                del df
                gc.collect()
            
            output_file = results_dir / f"{underlying}_{strat_name}_trades.csv"
            save_trades(all_trades, output_file)
            
            if all_trades:
                pnls = [t.pnl for t in all_trades]
                wins = sum(1 for p in pnls if p > 0)
                total_pnl = sum(pnls)
                
                print(f"\n✓ {underlying} COMPLETE")
                print(f"  Trades: {len(all_trades)}")
                print(f"  Win Rate: {wins/len(all_trades)*100:.1f}%")
                print(f"  Total P&L: {total_pnl:.2f} points")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGIES 3 & 5 COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
