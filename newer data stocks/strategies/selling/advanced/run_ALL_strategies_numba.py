#!/usr/bin/env python3
"""
ALL 10 ADVANCED STRATEGIES - Numba Optimized with Nearest-Expiry Filter
Target: <5 minutes for all 20 backtests (10 strategies × 2 underlyings)
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
    pnl: float
    hold_duration_minutes: int
    exit_reason: str


@dataclass
class StrategyConfig:
    name: str
    entry_times: List[time]  # Multiple entry times per day
    profit_target: float
    stop_loss_mult: float
    max_hold_minutes: int
    otm_pct: float = 0.0  # 0 for ATM, >0 for OTM


@njit
def find_atm_or_otm_strikes(distances: np.ndarray, opt_types: np.ndarray, otm_distance: float):
    """Find ATM (otm_distance=0) or OTM strikes"""
    ce_idx = -1
    pe_idx = -1
    min_ce_diff = 999999.0
    min_pe_diff = 999999.0
    
    for i in range(len(distances)):
        if opt_types[i] == 0:  # CE
            if otm_distance == 0:
                # ATM: minimum absolute distance
                diff = abs(distances[i])
            else:
                # OTM: target specific distance on positive side
                if distances[i] <= 0:
                    continue
                diff = abs(distances[i] - otm_distance)
            
            if diff < min_ce_diff:
                min_ce_diff = diff
                ce_idx = i
        else:  # PE
            if otm_distance == 0:
                # ATM: minimum absolute distance
                diff = abs(distances[i])
            else:
                # OTM: target specific distance on negative side
                if distances[i] >= 0:
                    continue
                diff = abs(distances[i] + otm_distance)
            
            if diff < min_pe_diff:
                min_pe_diff = diff
                pe_idx = i
    
    return ce_idx, pe_idx


@njit
def run_strategy_numba(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spot_prices: np.ndarray,
    entry_times_sec: np.ndarray,
    profit_target_pct: float,
    stop_loss_mult: float,
    max_hold_min: int,
    otm_pct: float
):
    """Generic strategy runner with Numba"""
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
        
        # Calculate OTM distance if needed
        if otm_pct > 0:
            spot = spot_prices[block_start]
            otm_dist = spot * otm_pct
        else:
            otm_dist = 0.0
        
        # Find strikes
        ce_idx, pe_idx = find_atm_or_otm_strikes(
            distances[block_start:block_end],
            opt_types[block_start:block_end],
            otm_dist
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
        premium_received = ce_entry_price + pe_entry_price
        
        profit_target = premium_received * profit_target_pct
        stop_loss = premium_received * stop_loss_mult
        max_exit_time = current_time_sec + max_hold_sec
        
        # Scan forward for exit
        exit_idx = -1
        ce_exit = 0.0
        pe_exit = 0.0
        exit_reason = 2
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            if times_sec[j] > max_exit_time:
                # Get last prices
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
            
            # Get current prices
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
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


# ALL 10 STRATEGY CONFIGURATIONS
STRATEGIES = [
    StrategyConfig(
        name="ADV_1_ATM_Straddle_50pct_Quick_90min",
        entry_times=[time(9, 20), time(13, 0)],
        profit_target=0.5, stop_loss_mult=2.0, max_hold_minutes=90, otm_pct=0.0
    ),
    StrategyConfig(
        name="ADV_2_OTM_Strangle_30pct_Quick_30min",
        entry_times=[time(9, 20), time(10, 0), time(11, 0), time(13, 0), time(14, 0)],
        profit_target=0.3, stop_loss_mult=1.5, max_hold_minutes=30, otm_pct=0.01
    ),
    StrategyConfig(
        name="ADV_3_ATM_Straddle_25pct_Ultra_Quick_15min",
        entry_times=[time(9, 20), time(10, 30), time(12, 0), time(13, 30)],
        profit_target=0.25, stop_loss_mult=1.5, max_hold_minutes=15, otm_pct=0.0
    ),
    StrategyConfig(
        name="ADV_4_OTM_Strangle_40pct_Target_60min",
        entry_times=[time(9, 20), time(11, 0), time(13, 0)],
        profit_target=0.4, stop_loss_mult=2.0, max_hold_minutes=60, otm_pct=0.015
    ),
    StrategyConfig(
        name="ADV_5_Tight_OTM_0_5pct_20pct_Target_45min",
        entry_times=[time(9, 20), time(11, 0), time(13, 0), time(14, 0)],
        profit_target=0.2, stop_loss_mult=1.8, max_hold_minutes=45, otm_pct=0.005
    ),
    StrategyConfig(
        name="ADV_6_Wide_OTM_2pct_35pct_Target_120min",
        entry_times=[time(9, 20), time(12, 0)],
        profit_target=0.35, stop_loss_mult=2.5, max_hold_minutes=120, otm_pct=0.02
    ),
    StrategyConfig(
        name="ADV_7_High_Frequency_ATM_Multi_Entry",
        entry_times=[time(9, 20), time(10, 0), time(10, 40), time(11, 20), time(12, 0), time(12, 40), time(13, 20), time(14, 0)],
        profit_target=0.35, stop_loss_mult=1.8, max_hold_minutes=60, otm_pct=0.0
    ),
    StrategyConfig(
        name="ADV_8_Conservative_ATM_60pct_Target",
        entry_times=[time(9, 20), time(13, 0)],
        profit_target=0.6, stop_loss_mult=2.5, max_hold_minutes=120, otm_pct=0.0
    ),
    StrategyConfig(
        name="ADV_9_Aggressive_OTM_15pct_Quick",
        entry_times=[time(9, 30), time(10, 30), time(11, 30), time(12, 30), time(13, 30), time(14, 30)],
        profit_target=0.15, stop_loss_mult=1.2, max_hold_minutes=20, otm_pct=0.01
    ),
    StrategyConfig(
        name="ADV_10_Afternoon_Quick_Strangle",
        entry_times=[time(13, 0), time(13, 30), time(14, 0), time(14, 30)],
        profit_target=0.25, stop_loss_mult=1.5, max_hold_minutes=45, otm_pct=0.01
    ),
]


def run_one_strategy(data_dir: Path, underlying: str, strategy: StrategyConfig) -> List[Trade]:
    """Run one strategy on all dates"""
    all_trades = []
    
    # Process date by date
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
        
        # Load date data
        df = pl.read_parquet(files[0], columns=[
            'timestamp', 'strike', 'distance_from_spot',
            'opt_type', 'price', 'expiry', 'spot_price'
        ]).filter(pl.col('timestamp').dt.year() > 1970)
        
        if df.is_empty():
            continue
        
        # *** NEAREST EXPIRY ONLY ***
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
        
        # Entry times as seconds
        entry_times_sec = np.array([t.hour * 3600 + t.minute * 60 for t in strategy.entry_times], dtype=np.int64)
        
        # Run Numba
        results = run_strategy_numba(
            ts_ns, dates, times, strikes, dists, opt_t, prices, spots,
            entry_times_sec, strategy.profit_target, strategy.stop_loss_mult,
            strategy.max_hold_minutes, strategy.otm_pct
        )
        
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
                    exit_reason=exit_map[reasons[i]]
                )
                all_trades.append(trade)
        
        del df
        gc.collect()
    
    return all_trades


def save_trades(trades: List[Trade], filename: Path):
    """Save trades to CSV"""
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
    print("ALL 10 ADVANCED STRATEGIES - Numba Optimized")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../strategy_results/selling/strategy_results_all_advanced")
    results_dir.mkdir(exist_ok=True)
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        for idx, strategy in enumerate(STRATEGIES, 1):
            strategy_name = f"{underlying}_{strategy.name}"
            print(f"\n[{idx}/10] {strategy_name}...")
            
            strat_start = time_mod.time()
            trades = run_one_strategy(data_dir, underlying, strategy)
            strat_time = time_mod.time() - strat_start
            
            # Stats
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
            
            # Save
            output_file = results_dir / f"{strategy_name}_trades.csv"
            save_trades(trades, output_file)
    
    # Summary
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ ALL COMPLETE in {total_time/60:.1f} minutes")
    print(f"{'='*80}")
    
    # Save summary
    summary_file = results_dir / "all_strategies_summary.csv"
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
