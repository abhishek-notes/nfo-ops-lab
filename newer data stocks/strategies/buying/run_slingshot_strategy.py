#!/usr/bin/env python3
"""
THE ORDER BOOK SLINGSHOT - Pullback Buying Strategy

Strategy: Buy the pullback in an uptrend, confirmed by order book
- Trend: EMA(5) > EMA(21) - uptrend confirmed
- Pullback: Price < EMA(5) but > EMA(21) - healthy dip
- Entry: Bid Qty > 1.5x Ask Qty - buyers stepping in
- Exit: Price < EMA(21) (trend broken) OR 5-min time stop

Why it works: Buy cheaper on dip, tight stop, delta expansion on resume
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
    """Trade record"""
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
    ema5_at_entry: float
    ema21_at_entry: float


@njit
def find_atm_strike(spot_price: float, strikes_arr: np.ndarray, opt_types_arr: np.ndarray, 
                     strike_step: int, start_idx: int, end_idx: int):
    """Find ATM strike - first OTM"""
    ce_idx = -1
    pe_idx = -1
    
    rounded_spot = round(spot_price / strike_step) * strike_step
    
    for i in range(start_idx, end_idx):
        strike = strikes_arr[i]
        opt_type = opt_types_arr[i]
        
        if opt_type == 0:  # CE
            if strike >= rounded_spot:
                ce_idx = i
                break
        
    for i in range(start_idx, end_idx):
        strike = strikes_arr[i]
        opt_type = opt_types_arr[i]
        
        if opt_type == 1:  # PE
            if strike <= rounded_spot:
                pe_idx = i
                break
    
    return ce_idx, pe_idx


@njit
def strategy_slingshot(
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
    strike_step: int,
    start_time_sec: int,
    end_time_sec: int,
    max_hold_minutes: int,
    eod_exit_time: int
):
    """
    Order Book Slingshot - Pullback buying strategy
    """
    n = len(prices)
    max_trades = 500
    
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
    ema5_entries = np.empty(max_trades, dtype=np.float64)
    ema21_entries = np.empty(max_trades, dtype=np.float64)
    
    trade_count = 0
    
    # Position tracking
    in_position = False
    entry_price = 0.0
    entry_time_sec = 0
    entry_date = 0
    entry_strike = 0.0
    entry_opt_type = 0
    entry_ema5 = 0.0
    entry_ema21 = 0.0
    
    # EMA state
    ema5 = 0.0
    ema21 = 0.0
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    ema_initialized = False
    
    i = 0
    while i < n:
        current_time = times_sec[i]
        current_date = dates_int[i]
        current_ts = timestamps_ns[i]
        
        # Force EOD exit
        if in_position and current_time >= eod_exit_time:
            exit_price = 0.0
            for j in range(i, min(i + 1000, n)):
                if strikes[j] == entry_strike and opt_types[j] == entry_opt_type:
                    exit_price = prices[j]
                    break
            
            if exit_price > 0 and trade_count < max_trades:
                entry_dates[trade_count] = entry_date
                entry_times[trade_count] = entry_time_sec
                exit_dates[trade_count] = current_date
                exit_times[trade_count] = current_time
                strike_arr[trade_count] = entry_strike
                opt_type_arr[trade_count] = entry_opt_type
                entry_prices[trade_count] = entry_price
                exit_prices[trade_count] = exit_price
                pnls[trade_count] = exit_price - entry_price
                hold_mins[trade_count] = (current_time - entry_time_sec) // 60
                exit_reasons[trade_count] = 3  # eod
                ema5_entries[trade_count] = entry_ema5
                ema21_entries[trade_count] = entry_ema21
                trade_count += 1
            
            in_position = False
        
        # Skip outside trading window
        if current_time < start_time_sec or current_time > end_time_sec:
            i += 1
            continue
        
        # Get timestamp block
        block_start = i
        while i < n and timestamps_ns[i] == current_ts:
            i += 1
        block_end = i
        
        # Update EMAs using ATM option price
        if block_end > block_start:
            spot = spots[block_start]
            ce_idx, pe_idx = find_atm_strike(spot, strikes, opt_types, strike_step, block_start, block_end)
            
            # Use CE price for EMA (more liquid usually)
            if ce_idx >= 0 and ce_idx < block_end:
                current_price = prices[ce_idx]
                
                if not ema_initialized:
                    ema5 = current_price
                    ema21 = current_price
                    ema_initialized = True
                else:
                    ema5 = current_price * alpha5 + ema5 * (1 - alpha5)
                    ema21 = current_price * alpha21 + ema21 * (1 - alpha21)
        
        if not ema_initialized:
            continue
        
        # === POSITION MANAGEMENT ===
        if in_position:
            # Find current price
            current_price = 0.0
            for j in range(block_start, block_end):
                if strikes[j] == entry_strike and opt_types[j] == entry_opt_type:
                    current_price = prices[j]
                    break
            
            if current_price > 0:
                time_held_sec = current_time - entry_time_sec
                
                # Exit conditions
                # 1. Trend broken: price < EMA21
                trend_broken = current_price < entry_ema21
                
                # 2. Time stop: 5 minutes
                time_exit = time_held_sec > (max_hold_minutes * 60)
                
                # 3. Hard stop: 20% loss
                stop_exit = current_price < (entry_price * 0.80)
                
                if trend_broken or time_exit or stop_exit:
                    if trade_count < max_trades:
                        entry_dates[trade_count] = entry_date
                        entry_times[trade_count] = entry_time_sec
                        exit_dates[trade_count] = current_date
                        exit_times[trade_count] = current_time
                        strike_arr[trade_count] = entry_strike
                        opt_type_arr[trade_count] = entry_opt_type
                        entry_prices[trade_count] = entry_price
                        exit_prices[trade_count] = current_price
                        pnls[trade_count] = current_price - entry_price
                        hold_mins[trade_count] = time_held_sec // 60
                        
                        if trend_broken:
                            exit_reasons[trade_count] = 0  # trend_broken
                        elif time_exit:
                            exit_reasons[trade_count] = 1  # time_stop
                        else:
                            exit_reasons[trade_count] = 2  # stop_loss
                        
                        ema5_entries[trade_count] = entry_ema5
                        ema21_entries[trade_count] = entry_ema21
                        trade_count += 1
                    
                    in_position = False
        
        # === ENTRY LOGIC ===
        elif not in_position:
            # Check trend: EMA5 > EMA21
            if ema5 <= ema21:
                continue
            
            # Find ATM strikes
            spot = spots[block_start]
            ce_idx, pe_idx = find_atm_strike(spot, strikes, opt_types, strike_step, block_start, block_end)
            
            # Try both CE and PE
            for idx, opt_type in [(ce_idx, 0), (pe_idx, 1)]:
                if idx < 0 or idx >= block_end:
                    continue
                
                price = prices[idx]
                
                # THE PULLBACK CHECK: Price < EMA5 but > EMA21
                # This is the "dip" in the uptrend
                in_pullback = (price < ema5) and (price > ema21)
                
                if not in_pullback:
                    continue
                
                # THE SLINGSHOT: Buyers stepping in (Bid > 1.5x Ask)
                if bid_qty[idx] <= 0 or ask_qty[idx] <= 0:
                    continue
                
                buyers_stepping_in = (bid_qty[idx] / ask_qty[idx]) > 1.5
                
                if not buyers_stepping_in:
                    continue
                
                # Check spread
                if ask_prices[idx] <= 0 or bid_prices[idx] <= 0:
                    continue
                
                mid = (ask_prices[idx] + bid_prices[idx]) * 0.5
                if mid <= 0:
                    continue
                
                spread_pct = ((ask_prices[idx] - bid_prices[idx]) / mid) * 100
                if spread_pct >= 1.5:
                    continue
                
                # ENTER: Buy at ask
                in_position = True
                entry_price = ask_prices[idx]
                entry_time_sec = current_time
                entry_date = current_date
                entry_strike = strikes[idx]
                entry_opt_type = opt_type
                entry_ema5 = ema5
                entry_ema21 = ema21
                break  # Only one position
    
    # Force exit open position
    if in_position and trade_count < max_trades:
        exit_price = 0.0
        for j in range(max(0, n-1000), n):
            if strikes[j] == entry_strike and opt_types[j] == entry_opt_type:
                exit_price = prices[j]
        
        if exit_price > 0:
            entry_dates[trade_count] = entry_date
            entry_times[trade_count] = entry_time_sec
            exit_dates[trade_count] = dates_int[n-1]
            exit_times[trade_count] = times_sec[n-1]
            strike_arr[trade_count] = entry_strike
            opt_type_arr[trade_count] = entry_opt_type
            entry_prices[trade_count] = entry_price
            exit_prices[trade_count] = exit_price
            pnls[trade_count] = exit_price - entry_price
            hold_mins[trade_count] = (times_sec[n-1] - entry_time_sec) // 60
            exit_reasons[trade_count] = 3  # eod
            ema5_entries[trade_count] = entry_ema5
            ema21_entries[trade_count] = entry_ema21
            trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        strike_arr[:trade_count], opt_type_arr[:trade_count],
        entry_prices[:trade_count], exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count],
        exit_reasons[:trade_count], ema5_entries[:trade_count], ema21_entries[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def run_slingshot_strategy(data_dir: Path, underlying: str) -> List[BuyTrade]:
    """Run Order Book Slingshot strategy"""
    all_trades = []
    
    # Strategy parameters
    START_TIME = 9 * 3600 + 30 * 60   # 9:30 AM - earlier for more pullbacks
    END_TIME = 15 * 3600              # 3:00 PM
    MAX_HOLD_MIN = 5                  # 5-minute time stop
    EOD_EXIT = 15 * 3600 + 20 * 60
    
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
            
            nearest_expiry = df['expiry'].min()
            df = df.filter(pl.col('expiry') == nearest_expiry).sort(['timestamp', 'opt_type', 'strike'])
            
            if df.is_empty():
                continue
            
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
            
            results = strategy_slingshot(
                ts_ns, dates, times, strikes, opt_t, prices,
                bid_p, ask_p, bid_q, ask_q, spots, STRIKE_STEP,
                START_TIME, END_TIME, MAX_HOLD_MIN, EOD_EXIT
            )
            
            if len(results[0]) > 0:
                (entry_d, entry_t, exit_d, exit_t, strike_a, opt_a,
                 entry_p, exit_p, pnl_a, hold_a, exit_r, ema5_a, ema21_a) = results
                
                exit_reason_map = {0: 'trend_broken', 1: 'time_stop', 2: 'stop_loss', 3: 'eod'}
                
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
                        ema5_at_entry=ema5_a[i],
                        ema21_at_entry=ema21_a[i]
                    )
                    all_trades.append(trade)
            
            del df
            gc.collect()
            
        except Exception as e:
            continue
    
    return all_trades


def save_trades(trades: List[BuyTrade], filename: Path):
    """Save trades to CSV"""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'strike', 'opt_type', 'entry_price', 'exit_price',
            'pnl', 'pnl_pct', 'hold_duration_minutes', 'exit_reason',
            'ema5_at_entry', 'ema21_at_entry'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.strike, t.opt_type, t.entry_price, t.exit_price,
                f"{t.pnl:.2f}", f"{t.pnl_pct:.2f}", t.hold_duration_minutes,
                t.exit_reason, f"{t.ema5_at_entry:.2f}", f"{t.ema21_at_entry:.2f}"
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("THE ORDER BOOK SLINGSHOT - Pullback Buying Strategy")
    print("="*80)
    print("Strategy Logic:")
    print("  1. Trend: EMA(5) > EMA(21) - confirmed uptrend")
    print("  2. Pullback: Price < EMA(5) but > EMA(21) - healthy dip")
    print("  3. Entry: Bid Qty > 1.5x Ask Qty - buyers stepping in")
    print("  4. Exit: Price < EMA(21) OR 5-min time stop")
    print("")
    print("Time Window: 9:30 AM - 3:00 PM | Max Hold: 5 minutes")
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
        trades = run_slingshot_strategy(data_dir, underlying)
        strat_time = time_mod.time() - strat_start
        
        if trades:
            pnls = [t.pnl for t in trades]
            wins = sum(1 for p in pnls if p > 0)
            total_pnl = sum(pnls)
            avg_pnl = total_pnl / len(trades)
            avg_hold = sum(t.hold_duration_minutes for t in trades) / len(trades)
            
            # Exit reason breakdown
            exit_reasons = {}
            for t in trades:
                exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1
            
            print(f"\n✓ Completed in {strat_time:.1f}s")
            print(f"  Trades: {len(trades)}")
            print(f"  Wins: {wins} ({wins/len(trades)*100:.1f}%)")
            print(f"  Total P&L: ₹{total_pnl:.2f}")
            print(f"  Avg P&L: ₹{avg_pnl:.2f}")
            print(f"  Avg Hold: {avg_hold:.1f} minutes")
            print(f"  Exit Reasons: {exit_reasons}")
            
            all_results.append({
                'underlying': underlying,
                'trades': len(trades),
                'wins': wins,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'avg_hold_min': avg_hold
            })
            
            output_file = results_dir / f"{underlying}_slingshot_trades.csv"
            save_trades(trades, output_file)
            print(f"  Saved: {output_file}")
        else:
            print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGY COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    if all_results:
        summary_file = results_dir / "slingshot_summary.csv"
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
