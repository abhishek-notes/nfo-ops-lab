#!/usr/bin/env python3
"""
THE ABSORPTION BREAKOUT - Microstructure Strategy

Strategy: Detect when a wall of sellers is being absorbed by buyers BEFORE price moves

Logic:
1. Consolidation: Price range tight for last 60 seconds (< 2% movement)
2. The Wall: Ask Quantity (sq0) > 2x recent average (sellers stacked up)
3. The Trigger: sq0 drops >30% rapidly (absorption happening) while price stable
4. Entry: Buy immediately - breakout imminent
5. Exit: 5% profit target OR price drops below entry

Why it works: Entry BEFORE the move (absorption precedes price movement)
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
    sq0_at_entry: int
    sq0_peak: int  # Peak ask qty before absorption


@njit
def find_atm_strike(spot_price: float, strikes_arr: np.ndarray, opt_types_arr: np.ndarray, 
                     strike_step: int, start_idx: int, end_idx: int):
    """Find ATM strike"""
    ce_idx = -1
    pe_idx = -1
    
    rounded_spot = round(spot_price / strike_step) * strike_step
    
    for i in range(start_idx, end_idx):
        strike = strikes_arr[i]
        opt_type = opt_types_arr[i]
        
        if opt_type == 0 and strike >= rounded_spot:
            ce_idx = i
            break
    
    for i in range(start_idx, end_idx):
        strike = strikes_arr[i]
        opt_type = opt_types_arr[i]
        
        if opt_type == 1 and strike <= rounded_spot:
            pe_idx = i
            break
    
    return ce_idx, pe_idx


@njit
def strategy_absorption(
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
    eod_exit_time: int
):
    """
    Absorption Breakout - Microstructure-based entry
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
    sq0_entries = np.empty(max_trades, dtype=np.int64)
    sq0_peaks = np.empty(max_trades, dtype=np.int64)
    
    trade_count = 0
    
    # Position tracking
    in_position = False
    entry_price = 0.0
    entry_time_sec = 0
    entry_date = 0
    entry_strike = 0.0
    entry_opt_type = 0
    entry_sq0 = 0
    sq0_peak = 0
    
    # Price history for consolidation check (last 60 seconds)
    price_history = np.zeros(60, dtype=np.float64)  # Store last 60 price points
    time_history = np.zeros(60, dtype=np.int64)
    price_hist_idx = 0
    price_hist_count = 0
    
    # Ask qty history for wall detection (last 30 observations)
    sq0_history = np.zeros(30, dtype=np.int64)
    sq0_hist_idx = 0
    sq0_hist_count = 0
    sq0_running_peak = 0
    
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
                sq0_entries[trade_count] = entry_sq0
                sq0_peaks[trade_count] = sq0_peak
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
        
        # Find ATM option
        spot = spots[block_start]
        ce_idx, pe_idx = find_atm_strike(spot, strikes, opt_types, strike_step, block_start, block_end)
        
        # Focus on CE for simplicity (can add PE later)
        if ce_idx < 0 or ce_idx >= block_end:
            continue
        
        current_price = prices[ce_idx]
        current_sq0 = ask_qty[ce_idx]
        
        # Update price history for consolidation check
        price_history[price_hist_idx] = current_price
        time_history[price_hist_idx] = current_time
        price_hist_idx = (price_hist_idx + 1) % 60
        if price_hist_count < 60:
            price_hist_count += 1
        
        # Update sq0 history for wall detection
        if current_sq0 > 0:
            sq0_history[sq0_hist_idx] = current_sq0
            sq0_hist_idx = (sq0_hist_idx + 1) % 30
            if sq0_hist_count < 30:
                sq0_hist_count += 1
            
            # Track peak
            if current_sq0 > sq0_running_peak:
                sq0_running_peak = current_sq0
        
        # === POSITION MANAGEMENT ===
        if in_position:
            # Find current price
            exit_price = 0.0
            for j in range(block_start, block_end):
                if strikes[j] == entry_strike and opt_types[j] == entry_opt_type:
                    exit_price = prices[j]
                    break
            
            if exit_price > 0:
                time_held_sec = current_time - entry_time_sec
                
                # Exit conditions
                # 1. Profit target: 5% gain (scalp)
                profit_hit = exit_price >= (entry_price * 1.05)
                
                # 2. Stop loss: Price drops below entry
                stop_hit = exit_price < entry_price
                
                # 3. Time stop: 3 minutes max (quick scalp)
                time_exit = time_held_sec > (3 * 60)
                
                if profit_hit or stop_hit or time_exit:
                    if trade_count < max_trades:
                        entry_dates[trade_count] = entry_date
                        entry_times[trade_count] = entry_time_sec
                        exit_dates[trade_count] = current_date
                        exit_times[trade_count] = current_time
                        strike_arr[trade_count] = entry_strike
                        opt_type_arr[trade_count] = entry_opt_type
                        entry_prices[trade_count] = entry_price
                        exit_prices[trade_count] = exit_price
                        pnls[trade_count] = exit_price - entry_price
                        hold_mins[trade_count] = time_held_sec // 60
                        
                        if profit_hit:
                            exit_reasons[trade_count] = 0  # profit
                        elif time_exit:
                            exit_reasons[trade_count] = 1  # time
                        else:
                            exit_reasons[trade_count] = 2  # stop
                        
                        sq0_entries[trade_count] = entry_sq0
                        sq0_peaks[trade_count] = sq0_peak
                        trade_count += 1
                    
                    in_position = False
        
        # === ENTRY LOGIC ===
        elif not in_position and price_hist_count >= 30 and sq0_hist_count >= 20:
            # 1. CONSOLIDATION CHECK: Price range tight for last 60 seconds
            # Look at prices from last 60 seconds
            recent_prices = price_history[:min(price_hist_count, 60)]
            recent_times = time_history[:min(price_hist_count, 60)]
            
            # Filter for last 60 seconds
            time_cutoff = current_time - 60
            valid_mask = recent_times > time_cutoff
            
            if valid_mask.sum() < 10:  # Need at least 10 observations
                continue
           
            valid_prices = recent_prices[valid_mask]
            
            if len(valid_prices) < 10:
                continue
            
            price_range = valid_prices.max() - valid_prices.min()
            price_mid = (valid_prices.max() + valid_prices.min()) / 2.0
            
            if price_mid == 0:
                continue
            
            range_pct = (price_range / price_mid) * 100
            
            # Consolidation: range < 2%
            is_consolidating = range_pct < 2.0
            
            if not is_consolidating:
                continue
            
            # 2. THE WALL: Current sq0 > 2x recent average
            recent_sq0 = sq0_history[:min(sq0_hist_count, 30)]
            if len(recent_sq0) < 10:
                continue
            
            avg_sq0 = float(recent_sq0.sum()) / float(len(recent_sq0))
            
            if avg_sq0 == 0:
                continue
            
            has_wall = current_sq0 > (avg_sq0 * 2.0)
            
            if not has_wall:
                continue
            
            # 3. THE TRIGGER: sq0 dropping rapidly from peak
            # Check if we had a peak recently and now dropping
            if sq0_running_peak == 0:
                continue
            
            sq0_drop_pct = ((sq0_running_peak - current_sq0) / float(sq0_running_peak)) * 100
            
            # Absorption happening: sq0 dropped >30% from peak
            absorption_detected = sq0_drop_pct > 30.0
            
            if not absorption_detected:
                continue
            
            # 4. Price hasn't moved yet (still in consolidation range)
            price_stable = abs(current_price - price_mid) / price_mid * 100 < 1.0
            
            if not price_stable:
                continue
            
            # Check spread
            if ask_prices[ce_idx] <= 0 or bid_prices[ce_idx] <= 0:
                continue
            
            mid = (ask_prices[ce_idx] + bid_prices[ce_idx]) * 0.5
            if mid <= 0:
                continue
            
            spread_pct = ((ask_prices[ce_idx] - bid_prices[ce_idx]) / mid) * 100
            if spread_pct >= 2.0:  # Tighter spread for scalping
                continue
            
            # ENTER: Buy at ask (aggressive entry)
            in_position = True
            entry_price = ask_prices[ce_idx]
            entry_time_sec = current_time
            entry_date = current_date
            entry_strike = strikes[ce_idx]
            entry_opt_type = 0  # CE only
            entry_sq0 = current_sq0
            sq0_peak = sq0_running_peak
            
            # Reset peak after entry
            sq0_running_peak = current_sq0
    
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
            sq0_entries[trade_count] = entry_sq0
            sq0_peaks[trade_count] = sq0_peak
            trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        strike_arr[:trade_count], opt_type_arr[:trade_count],
        entry_prices[:trade_count], exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count],
        exit_reasons[:trade_count], sq0_entries[:trade_count], sq0_peaks[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def run_absorption_strategy(data_dir: Path, underlying: str) -> List[BuyTrade]:
    """Run Absorption Breakout strategy"""
    all_trades = []
    
    # Strategy parameters
    START_TIME = 10 * 3600      # 10:00 AM - avoid opening volatility
    END_TIME = 14 * 3600 + 30 * 60  # 2:30 PM
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
            
            results = strategy_absorption(
                ts_ns, dates, times, strikes, opt_t, prices,
                bid_p, ask_p, bid_q, ask_q, spots, STRIKE_STEP,
                START_TIME, END_TIME, EOD_EXIT
            )
            
            if len(results[0]) > 0:
                (entry_d, entry_t, exit_d, exit_t, strike_a, opt_a,
                 entry_p, exit_p, pnl_a, hold_a, exit_r, sq0_e, sq0_p) = results
                
                exit_reason_map = {0: 'profit_target', 1: 'time_stop', 2: 'stop_loss', 3: 'eod'}
                
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
                        sq0_at_entry=int(sq0_e[i]),
                        sq0_peak=int(sq0_p[i])
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
            'sq0_at_entry', 'sq0_peak'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.strike, t.opt_type, t.entry_price, t.exit_price,
                f"{t.pnl:.2f}", f"{t.pnl_pct:.2f}", t.hold_duration_minutes,
                t.exit_reason, t.sq0_at_entry, t.sq0_peak
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("THE ABSORPTION BREAKOUT - Microstructure Strategy")
    print("="*80)
    print("Strategy Logic:")
    print("  1. Consolidation: Price range < 2% for last 60 seconds")
    print("  2. The Wall: Ask Qty > 2x recent average (sellers stacked)")
    print("  3. The Trigger: Ask Qty drops >30% from peak (absorption!)")
    print("  4. Entry: Buy before price moves (absorption precedes move)")
    print("  5. Exit: 5% profit target OR stop loss OR 3-min time stop")
    print("")
    print("Time Window: 10:00 AM - 2:30 PM | Max Hold: 3 minutes")
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
        trades = run_absorption_strategy(data_dir, underlying)
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
            
            print(f"\n✓ Completed in{strat_time:.1f}s")
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
            
            output_file = results_dir / f"{underlying}_absorption_trades.csv"
            save_trades(trades, output_file)
            print(f"  Saved: {output_file}")
        else:
            print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGY COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    if all_results:
        summary_file = results_dir / "absorption_summary.csv"
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
