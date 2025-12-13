#!/usr/bin/env python3
"""
STRATEGY 2: Order Book Absorption Scalp
Microstructure-based high-frequency strategy
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


@njit
def find_atm_strikes(distances: np.ndarray, opt_types: np.ndarray):
    ce_idx = -1
    pe_idx = -1
    min_ce_dist = 999999.0
    min_pe_dist = 999999.0
    
    for i in range(len(distances)):
        abs_dist = abs(distances[i])
        if opt_types[i] == 0:
            if abs_dist < min_ce_dist:
                min_ce_dist = abs_dist
                ce_idx = i
        else:
            if abs_dist < min_pe_dist:
                min_pe_dist = abs_dist
                pe_idx = i
    
    return ce_idx, pe_idx


@njit
def strategy2_order_book_absorption(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    spots: np.ndarray,
    bq0_array: np.ndarray,  # Best bid quantity
    sq0_array: np.ndarray   # Best ask quantity
):
    """
    Strategy 2: Order Book Absorption Scalp
    
    Entry:
    - Detect 0.10% spike in underlying (last 60 seconds)
    - Check ATM option orderbook: sq0 > bq0 * 3.0
    - Sell ATM Call (if spike up) or ATM Put (if spike down)
    
    Exit:
    - Max 5 minutes hold
    - 5 points underlying stop loss
    - Target: 5-7 points premium decay
    """
    entry_start = 9*3600 + 45*60  # Start after opening volatility
    entry_end = 14*3600 + 30*60   # Stop before close
    max_hold_seconds = 300  # 5 minutes
    
    max_trades = 500  # High frequency strategy
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
    
    # Track spot price history for spike detection
    spot_history = np.zeros(60)  # Last 60 seconds
    spot_history_idx = 0
    last_check_time = 0
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time = times_sec[i]
        
        if current_time < entry_start or current_time > entry_end:
            i += 1
            continue
        
        # Update spot history every second
        if current_time > last_check_time:
            spot_history[spot_history_idx % 60] = spots[i]
            spot_history_idx += 1
            last_check_time = current_time
        
        # Need at least 60 seconds of history
        if spot_history_idx < 60:
            i += 1
            continue
        
        # Check for spike (current vs 60 seconds ago)
        current_spot = spots[i]
        spot_60s_ago = spot_history[spot_history_idx % 60]
        
        if spot_60s_ago == 0:
            i += 1
            continue
        
        spike_pct = (current_spot - spot_60s_ago) / spot_60s_ago
        
        # Need at least 0.10% move
        if abs(spike_pct) < 0.001:
            i += 1
            continue
        
        # Get current timestamp block
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
        
        # Determine which option to sell based on spike direction
        if spike_pct > 0:
            # Spike UP - check CE orderbook and sell CE
            option_idx = ce_idx
            is_ce = True
        else:
            # Spike DOWN - check PE orderbook and sell PE
            option_idx = pe_idx
            is_ce = False
        
        # Check orderbook absorption signal
        bid_qty = bq0_array[option_idx]
        ask_qty = sq0_array[option_idx]
        
        # Need massive ask wall (absorption)
        if ask_qty <= bid_qty * 3.0:
            continue
        
        # Entry confirmed
        entry_spot = current_spot
        option_strike = strikes[option_idx]
        option_entry_price = prices[option_idx]
        
        # Targets
        profit_target_premium = option_entry_price - 7.0  # 7 points decay
        stop_loss_spot = entry_spot + (5.0 if is_ce else -5.0)  # 5 points against us
        max_exit_time = current_time + max_hold_seconds
        
        # Track position
        option_exit_price = 0.0
        exit_idx = -1
        exit_reason = 2  # time limit
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            # Time limit
            if times_sec[j] >= max_exit_time:
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == option_strike and (opt_types[k] == (0 if is_ce else 1)):
                        option_exit_price = prices[k]
                        exit_idx = k
                        break
                break
            
            # Stop loss - spot breached
            curr_spot = spots[j]
            if is_ce:
                if curr_spot >= stop_loss_spot:
                    # Stop hit
                    for k in range(j-1, block_start, -1):
                        if dates_int[k] != current_date:
                            break
                        if strikes[k] == option_strike and opt_types[k] == 0:
                            option_exit_price = prices[k]
                            exit_idx = k
                            exit_reason = 1
                            break
                    break
            else:
                if curr_spot <= stop_loss_spot:
                    for k in range(j-1, block_start, -1):
                        if dates_int[k] != current_date:
                            break
                        if strikes[k] == option_strike and opt_types[k] == 1:
                            option_exit_price = prices[k]
                            exit_idx = k
                            exit_reason = 1
                            break
                    break
            
            # Check current option price
            ts_start = j
            curr_ts = timestamps_ns[j]
            curr_price = 0.0
            
            while j < n and timestamps_ns[j] == curr_ts:
                if strikes[j] == option_strike and (opt_types[j] == (0 if is_ce else 1)):
                    curr_price = prices[j]
                j += 1
            
            if curr_price == 0:
                continue
            
            # Profit target
            if curr_price <= profit_target_premium:
                option_exit_price = curr_price
                exit_idx = ts_start
                exit_reason = 0
                break
        
        if exit_idx != -1 and option_exit_price > 0:
            pnl = option_entry_price - option_exit_price
            hold_min = int((times_sec[exit_idx] - current_time) / 60)
            
            if trade_count < max_trades:
                entry_dates[trade_count] = current_date
                entry_times[trade_count] = current_time
                exit_dates[trade_count] = dates_int[exit_idx]
                exit_times[trade_count] = times_sec[exit_idx]
                
                if is_ce:
                    ce_strikes[trade_count] = option_strike
                    pe_strikes[trade_count] = 0.0
                    ce_entry_p[trade_count] = option_entry_price
                    pe_entry_p[trade_count] = 0.0
                    ce_exit_p[trade_count] = option_exit_price
                    pe_exit_p[trade_count] = 0.0
                else:
                    ce_strikes[trade_count] = 0.0
                    pe_strikes[trade_count] = option_strike
                    ce_entry_p[trade_count] = 0.0
                    pe_entry_p[trade_count] = option_entry_price
                    ce_exit_p[trade_count] = 0.0
                    pe_exit_p[trade_count] = option_exit_price
                
                pnls[trade_count] = pnl
                holds[trade_count] = hold_min
                reasons[trade_count] = exit_reason
                trade_count += 1
        
        # Don't skip too far - but avoid immediate re-entry (wait 60 seconds)
        while i < n and times_sec[i] < current_time + 60:
            i += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        ce_strikes[:trade_count], pe_strikes[:trade_count],
        ce_entry_p[:trade_count], pe_entry_p[:trade_count],
        ce_exit_p[:trade_count], pe_exit_p[:trade_count],
        pnls[:trade_count], holds[:trade_count], reasons[:trade_count]
    )


def run_strategy(df: pl.DataFrame, strategy_name: str):
    """Run Order Book Absorption strategy"""
    if df.is_empty():
        return []
    
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
    bq0 = df['bq0'].to_numpy()
    sq0 = df['sq0'].to_numpy()
    
    # Run strategy
    results = strategy2_order_book_absorption(
        ts_ns, dates, times, strikes, dists, opt_t, prices, spots, bq0, sq0
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
    print("STRATEGY 2: ORDER BOOK ABSORPTION SCALP")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../strategy_results/selling/strategy_results_ai_strat2")
    results_dir.mkdir(exist_ok=True)
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
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
                'opt_type', 'price', 'expiry', 'spot_price', 'bq0', 'sq0'
            ]).filter(pl.col('timestamp').dt.year() > 1970)
            
            if df.is_empty():
                continue
            
            nearest_expiry = df['expiry'].min()
            df = df.filter(pl.col('expiry') == nearest_expiry).sort('timestamp')
            
            if df.is_empty():
                continue
            
            trades = run_strategy(df, "AI_STRAT2_OB_Absorption")
            all_trades.extend(trades)
            
            processed += 1
            if processed % 10 == 0:
                print(f"  Processed {processed} dates, {len(all_trades)} trades...")
            
            del df
            gc.collect()
        
        output_file = results_dir / f"{underlying}_OB_Absorption_trades.csv"
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
    print(f"✓ STRATEGY 2 COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
