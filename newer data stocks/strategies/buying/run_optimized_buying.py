#!/usr/bin/env python3
"""
ORDER FLOW MOMENTUM BURST - Optimized with Proper ATM Logic

Key Changes:
1. Proper ATM strike selection (NIFTY=50, BANKNIFTY=100)
2. Only trade first OTM strikes (not ATM exactly - slightly OTM for better premium)
3. Wider spread tolerance (1% instead of 0.5%)
4. Looser imbalance threshold (1.2x instead of 1.5x)
5. Longer hold time (15 min instead of 10 min)
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
class BuyTrade:
    """Trade record for option buying"""
    entry_date: str
    entry_time: str
    exit_date: str
    exit_time: str
    strike: float
    opt_type: str
    entry_price: float
    exit_price: float
    pnl: float
    pnl_pct: float
    hold_duration_minutes: int
    exit_reason: str
    max_price: float


@njit
def find_atm_strike(spot_price: float, strikes_arr: np.ndarray, opt_types_arr: np.ndarray, 
                     strike_step: int, start_idx: int, end_idx: int):
    """
    Find proper ATM strike based on exchange rules:
    - NIFTY: Round to nearest 50
    - BANKNIFTY: Round to nearest 100
    
    Returns first OTM strike (slightly out of money for better premium)
    """
    # For CE: first strike >= spot (first OTM call)
    # For PE: first strike <= spot (first OTM put)
    
    ce_strike = -1.0
    pe_strike = -1.0
    ce_idx = -1
    pe_idx = -1
    
    # Round spot to nearest strike_step
    rounded_spot = round(spot_price / strike_step) * strike_step
    
    for i in range(start_idx, end_idx):
        strike = strikes_arr[i]
        opt_type = opt_types_arr[i]
        
        if opt_type == 0:  # CE
            # First OTM call: strike >= spot
            if strike >= rounded_spot and (ce_strike < 0 or strike < ce_strike):
                ce_strike = strike
                ce_idx = i
        else:  # PE
            # First OTM put: strike <= spot
            if strike <= rounded_spot and (pe_strike < 0 or strike > pe_strike):
                pe_strike = strike
                pe_idx = i
    
    return ce_idx, pe_idx, ce_strike, pe_strike


@njit
def strategy_buy_optimized(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    bid_prices: np.ndarray,
    ask_prices: np.ndarray,
    bid_qty: np.ndarray,
    ask_qty: np.ndarray,
    spots: np.ndarray,
    strike_step: int,         # 50 for NIFTY, 100 for BANKNIFTY
    start_time_sec: int,
    end_time_sec: int,
    max_hold_minutes: int,
    eod_exit_time: int
):
    """
    Optimized buying strategy with proper ATM selection
    """
    n = len(prices)
    max_trades = 1000
    
    # Output arrays
    entry_dates = np.empty(max_trades, dtype=np.int64)
    entry_times = np.empty(max_trades, dtype=np.int64)
    exit_dates = np.empty(max_trades, dtype=np.int64)
    exit_times = np.empty(max_trades, dtype=np.int64)
    strike_arr = np.empty(max_trades, dtype=np.float64)
    opt_type_arr = np.empty(max_trades, dtype=np.int8)
    entry_prices = np.empty(max_trades, dtype=np.float64)
    exit_prices = np.empty(max_trades, dtype=np.float64)
    pnls = np.empty(max_trades, dtype=np.float64)
    hold_mins = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    max_prices = np.empty(max_trades, dtype=np.float64)
    
    trade_count = 0
    
    # Strategy state
    in_position = False
    entry_price = 0.0
    entry_idx = 0
    entry_strike = 0.0
    entry_opt_type = 0
    highest_price = 0.0
    
    # EMA state (per timestamp block)
    ema5 = 0.0
    ema21 = 0.0
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    ema_initialized = False
    
    i = 0
    while i < n:
        current_time = times_sec[i]
        current_ts = timestamps_ns[i]
        
        # Force EOD exit at 3:20 PM
        if in_position and current_time >= eod_exit_time:
            if trade_count < max_trades:
                entry_dates[trade_count] = dates_int[entry_idx]
                entry_times[trade_count] = times_sec[entry_idx]
                exit_dates[trade_count] = dates_int[i]
                exit_times[trade_count] = current_time
                strike_arr[trade_count] = entry_strike
                opt_type_arr[trade_count] = entry_opt_type
                entry_prices[trade_count] = entry_price
                exit_prices[trade_count] = prices[i]
                pnls[trade_count] = prices[i] - entry_price
                hold_mins[trade_count] = (current_time - times_sec[entry_idx]) // 60
                exit_reasons[trade_count] = 4  # eod
                max_prices[trade_count] = highest_price
                trade_count += 1
            in_position = False
        
        # Skip outside trading window
        if current_time < start_time_sec or current_time > end_time_sec:
            i += 1
            continue
        
        # Get timestamp block (all strikes at this timestamp)
        block_start = i
        while i < n and timestamps_ns[i] == current_ts:
            i += 1
        block_end = i
        
        # Update EMAs using mid price of block
        if block_end > block_start:
            mid_price = (prices[block_start] + prices[block_end-1]) / 2.0
            if not ema_initialized:
                ema5 = mid_price
                ema21 = mid_price
                ema_initialized = True
            else:
                ema5 = mid_price * alpha5 + ema5 * (1 - alpha5)
                ema21 = mid_price * alpha21 + ema21 * (1 - alpha21)
        
        # === POSITION MANAGEMENT ===
        if in_position:
            # Find current price for our position
            current_price = 0.0
            for j in range(block_start, block_end):
                if strikes[j] == entry_strike and opt_types[j] == entry_opt_type:
                    current_price = prices[j]
                    break
            
            if current_price > 0:
                # Update trailing stop
                if current_price > highest_price:
                    highest_price = current_price
                
                # Exit conditions
                time_held_sec = current_time - times_sec[entry_idx]
                
                time_exit = time_held_sec > (max_hold_minutes * 60)
                trail_exit = current_price < (highest_price * 0.95)
                stop_exit = current_price < (entry_price * 0.90)
                trend_exit = ema5 < ema21
                
                if time_exit or trail_exit or stop_exit or trend_exit:
                    if trade_count < max_trades:
                        entry_dates[trade_count] = dates_int[entry_idx]
                        entry_times[trade_count] = times_sec[entry_idx]
                        exit_dates[trade_count] = dates_int[block_start]
                        exit_times[trade_count] = current_time
                        strike_arr[trade_count] = entry_strike
                        opt_type_arr[trade_count] = entry_opt_type
                        entry_prices[trade_count] = entry_price
                        exit_prices[trade_count] = current_price
                        pnls[trade_count] = current_price - entry_price
                        hold_mins[trade_count] = time_held_sec // 60
                        
                        if time_exit:
                            exit_reasons[trade_count] = 1
                        elif stop_exit:
                            exit_reasons[trade_count] = 2
                        else:
                            exit_reasons[trade_count] = 0
                        
                        max_prices[trade_count] = highest_price
                        trade_count += 1
                    
                    in_position = False
        
        # === ENTRY LOGIC ===
        elif not in_position and ema_initialized:
            # Trend check
            trend_ok = ema5 > ema21
            if not trend_ok:
                continue
            
            # Find ATM strikes
            spot = spots[block_start]
            ce_idx, pe_idx, ce_strike, pe_strike = find_atm_strike(
                spot, strikes, opt_types, strike_step, block_start, block_end
            )
            
            # Try both CE and PE
            for idx, opt_type in [(ce_idx, 0), (pe_idx, 1)]:
                if idx < 0:
                    continue
                
                # Check entry conditions
                if ask_qty[idx] <= 0 or bid_qty[idx] <= 0:
                    continue
                
                # Imbalance (relaxed from 1.5 to 1.2)
                imbalance_ok = (bid_qty[idx] / ask_qty[idx]) > 1.2
                
                # Spread check (relaxed from 0.5% to 1.0%)
                spread_ok = False
                if ask_prices[idx] > 0 and bid_prices[idx] > 0:
                    mid = (ask_prices[idx] + bid_prices[idx]) * 0.5
                    if mid > 0:
                        spread_pct = ((ask_prices[idx] - bid_prices[idx]) / mid) * 100
                        spread_ok = spread_pct < 1.0
                
                if imbalance_ok and spread_ok:
                    # Enter trade
                    in_position = True
                    entry_price = ask_prices[idx]
                    entry_idx = block_start
                    entry_strike = strikes[idx]
                    entry_opt_type = opt_type
                    highest_price = entry_price
                    break  # Take first valid entry
    
    # Force exit any open position
    if in_position and trade_count < max_trades:
        entry_dates[trade_count] = dates_int[entry_idx]
        entry_times[trade_count] = times_sec[entry_idx]
        exit_dates[trade_count] = dates_int[n-1]
        exit_times[trade_count] = times_sec[n-1]
        strike_arr[trade_count] = entry_strike
        opt_type_arr[trade_count] = entry_opt_type
        entry_prices[trade_count] = entry_price
        exit_prices[trade_count] = prices[n-1]
        pnls[trade_count] = prices[n-1] - entry_price
        hold_mins[trade_count] = (times_sec[n-1] - times_sec[entry_idx]) // 60
        exit_reasons[trade_count] = 4
        max_prices[trade_count] = highest_price
        trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        strike_arr[:trade_count], opt_type_arr[:trade_count],
        entry_prices[:trade_count], exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count],
        exit_reasons[:trade_count], max_prices[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def run_optimized_strategy(data_dir: Path, underlying: str) -> List[BuyTrade]:
    """Run optimized buying strategy"""
    all_trades = []
    
    # Strategy parameters
    START_TIME = 12 * 3600
    END_TIME = 14 * 3600 + 30 * 60
    MAX_HOLD_MIN = 15  # Increased from 10
    EOD_EXIT = 15 * 3600 + 20 * 60
    
    # Strike step based on underlying
    STRIKE_STEP = 50 if underlying == "NIFTY" else 100
    
    date_dirs = sorted(data_dir.glob("*"))
    
    print(f"Processing {underlying} (Strike Step: {STRIKE_STEP})...")
    
    for date_dir in date_dirs:
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        underlying_dir = date_dir / underlying
        if not underlying_dir.exists():
            continue
        
        files = list(underlying_dir.glob("*.parquet"))
        if not files:
            continue
        
        try:
            df = pl.read_parquet(files[0], columns=[
                'timestamp', 'strike', 'opt_type', 'price',
                'bp0', 'sp0', 'bq0', 'sq0', 'expiry', 'spot_price'
            ]).filter(pl.col('timestamp').dt.year() > 1970)
            
            if df.is_empty():
                continue
            
            # Nearest expiry only
            nearest_expiry = df['expiry'].min()
            df = df.filter(pl.col('expiry') == nearest_expiry).sort(['timestamp', 'opt_type', 'strike'])
            
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
            opt_t = (df['opt_type'] == 'PE').cast(pl.Int8).to_numpy()
            prices = df['price'].fill_null(0).to_numpy()
            bid_p = df['bp0'].fill_null(0).to_numpy()
            ask_p = df['sp0'].fill_null(0).to_numpy()
            bid_q = df['bq0'].fill_null(0).to_numpy()
            ask_q = df['sq0'].fill_null(0).to_numpy()
            spots = df['spot_price'].fill_null(0).to_numpy()
            
            # Run strategy
            results = strategy_buy_optimized(
                ts_ns, dates, times, strikes, opt_t, prices,
                bid_p, ask_p, bid_q, ask_q, spots, STRIKE_STEP,
                START_TIME, END_TIME, MAX_HOLD_MIN, EOD_EXIT
            )
            
            if len(results[0]) > 0:
                (entry_d, entry_t, exit_d, exit_t, strike_a, opt_a,
                 entry_p, exit_p, pnl_a, hold_a, exit_r, max_p) = results
                
                exit_reason_map = {0: 'trail_stop', 1: 'time_stop', 2: 'stop_loss', 3: 'contract_change', 4: 'eod'}
                
                for i in range(len(pnl_a)):
                    opt_type_str = 'CE' if opt_a[i] == 0 else 'PE'
                    pnl_pct = (pnl_a[i] / entry_p[i] * 100) if entry_p[i] > 0 else 0
                    
                    trade = BuyTrade(
                        entry_date=str(int_to_date(entry_d[i])),
                        entry_time=str(sec_to_time(entry_t[i])),
                        exit_date=str(int_to_date(exit_d[i])),
                        exit_time=str(sec_to_time(exit_t[i])),
                        strike=strike_a[i],
                        opt_type=opt_type_str,
                        entry_price=entry_p[i],
                        exit_price=exit_p[i],
                        pnl=pnl_a[i],
                        pnl_pct=pnl_pct,
                        hold_duration_minutes=int(hold_a[i]),
                        exit_reason=exit_reason_map.get(exit_r[i], 'unknown'),
                        max_price=max_p[i]
                    )
                    all_trades.append(trade)
            
            del df
            gc.collect()
            
        except Exception as e:
            continue
    
    return all_trades


def save_buying_trades(trades: List[BuyTrade], filename: Path):
    """Save buying trades to CSV"""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'strike', 'opt_type', 'entry_price', 'exit_price',
            'pnl', 'pnl_pct', 'hold_duration_minutes', 'exit_reason', 'max_price'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.strike, t.opt_type, t.entry_price, t.exit_price,
                f"{t.pnl:.2f}", f"{t.pnl_pct:.2f}", t.hold_duration_minutes,
                t.exit_reason, f"{t.max_price:.2f}"
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("OPTIMIZED MOMENTUM BURST - Proper ATM Logic")
    print("="*80)
    print("Strategy: EMA trend + Order book imbalance + Proper ATM selection")
    print("NIFTY: 50-point strikes | BANKNIFTY: 100-point strikes")
    print("Time Window: 12:00 PM - 2:30 PM | Max Hold: 15 min | EOD Exit: 3:20 PM")
    print("Spread: <1.0% | Imbalance: >1.2x")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../strategy_results/buying/strategy_results_buying")
    results_dir.mkdir(parents=True, exist_ok=True)
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        strat_start = time_mod.time()
        trades = run_optimized_strategy(data_dir, underlying)
        strat_time = time_mod.time() - strat_start
        
        if trades:
            pnls = [t.pnl for t in trades]
            wins = sum(1 for p in pnls if p > 0)
            total_pnl = sum(pnls)
            avg_pnl = total_pnl / len(trades)
            avg_hold = sum(t.hold_duration_minutes for t in trades) / len(trades)
            
            print(f"\n✓ Completed in {strat_time:.1f}s")
            print(f"  Trades: {len(trades)}")
            print(f"  Wins: {wins} ({wins/len(trades)*100:.1f}%)")
            print(f"  Total P&L: ₹{total_pnl:.2f}")
            print(f"  Avg P&L: ₹{avg_pnl:.2f}")
            print(f"  Avg Hold: {avg_hold:.1f} minutes")
            
            all_results.append({
                'underlying': underlying,
                'trades': len(trades),
                'wins': wins,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'avg_hold_min': avg_hold
            })
            
            output_file = results_dir / f"{underlying}_optimized_trades.csv"
            save_buying_trades(trades, output_file)
            print(f"  Saved: {output_file}")
        else:
            print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGY COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    if all_results:
        summary_file = results_dir / "optimized_summary.csv"
        with open(summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['underlying', 'trades', 'wins', 'win_rate_%', 'total_pnl', 'avg_pnl', 'avg_hold_minutes'])
            for r in all_results:
                writer.writerow([
                    r['underlying'], r['trades'], r['wins'],
                    f"{r['wins']/r['trades']*100:.1f}" if r['trades'] > 0 else "0",
                    f"{r['total_pnl']:.2f}", f"{r['avg_pnl']:.2f}", f"{r['avg_hold_min']:.1f}"
                ])
        print(f"\nSummary: {summary_file}")
    
    print(f"Results directory: {results_dir}/")


if __name__ == "__main__":
    main()
