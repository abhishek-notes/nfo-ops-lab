#!/usr/bin/env python3
"""
GAMMA BURST - Expiry Day Special (NIFTY Tuesdays Only)

Strategy: Play OTM options going ITM on expiry day for gamma explosion

Logic:
1. ONLY Tuesday (NIFTY expiry day)
2. Time: After 1:30 PM (final 2 hours)
3. Strike: Spot + 50 (slightly OTM CE)
4. Trigger: Spot breaks High of Day
5. Exit: 30% profit target OR 3% stop OR 3:20 PM exit

Why it works: Near expiry, gamma is highest. 10-point spot move = 50-80% option spike
"""

from pathlib import Path
from datetime import time, datetime
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
    spot_at_entry: float
    high_of_day: float


@njit
def find_otm_strike(spot_price: float, strikes_arr: np.ndarray, opt_types_arr: np.ndarray, 
                     start_idx: int, end_idx: int, target_strike: float):
    """Find specific strike (Spot + 50 for CE)"""
    ce_idx = -1
    
    for i in range(start_idx, end_idx):
        if opt_types_arr[i] == 0 and abs(strikes_arr[i] - target_strike) < 1.0:
            ce_idx = i
            break
    
    return ce_idx


@njit
def strategy_gamma_burst(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    bid_prices: np.ndarray,
    ask_prices: np.ndarray,
    spots: np.ndarray,
    start_time_sec: int,
    eod_exit_time: int
):
    """
    Gamma Burst - Expiry day gamma scalping
    """
    n = len(prices)
    max_trades = 100  # Conservative for expiry day
    
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
    spot_entries = np.empty(max_trades, dtype=np.float64)
    hod_entries = np.empty(max_trades, dtype=np.float64)
    
    trade_count = 0
    
    # Position tracking
    in_position = False
    entry_price = 0.0
    entry_time_sec = 0
    entry_date = 0
    entry_strike = 0.0
    entry_spot = 0.0
    hod_at_entry = 0.0
    
    # Track High of Day (before 1:30 PM)
    high_of_day = 0.0
    hod_established = False
    
    i = 0
    while i < n:
        current_time = times_sec[i]
        current_date = dates_int[i]
        current_ts = timestamps_ns[i]
        
        # Force EOD exit at 3:20 PM
        if in_position and current_time >= eod_exit_time:
            exit_price = 0.0
            # Find exit price
            for j in range(i, min(i + 1000, n)):
                if strikes[j] == entry_strike and opt_types[j] == 0:
                    exit_price = prices[j]
                    break
            
            if exit_price > 0 and trade_count < max_trades:
                entry_dates[trade_count] = entry_date
                entry_times[trade_count] = entry_time_sec
                exit_dates[trade_count] = current_date
                exit_times[trade_count] = current_time
                strike_arr[trade_count] = entry_strike
                opt_type_arr[trade_count] = 0  # CE
                entry_prices[trade_count] = entry_price
                exit_prices[trade_count] = exit_price
                pnls[trade_count] = exit_price - entry_price
                hold_mins[trade_count] = (current_time - entry_time_sec) // 60
                exit_reasons[trade_count] = 3  # EOD
                spot_entries[trade_count] = entry_spot
                hod_entries[trade_count] = hod_at_entry
                trade_count += 1
            
            in_position = False
        
        # Get timestamp block
        block_start = i
        while i < n and timestamps_ns[i] == current_ts:
            i += 1
        block_end = i
        
        current_spot = spots[block_start]
        
        # Establish High of Day (before 1:30 PM)
        if current_time < start_time_sec:
            if current_spot > high_of_day:
                high_of_day = current_spot
            continue
        else:
            if not hod_established:
                hod_established = True
                # print(f"HOD established: {high_of_day}")
        
        # === POSITION MANAGEMENT ===
        if in_position:
            # Find current price for our strike
            exit_price = 0.0
            for j in range(block_start, block_end):
                if strikes[j] == entry_strike and opt_types[j] == 0:
                    exit_price = prices[j]
                    break
            
            if exit_price > 0:
                time_held_sec = current_time - entry_time_sec
                
                # Exit conditions
                # 1. Profit target: 30% (gamma explosion target)
                profit_hit = exit_price >= (entry_price * 1.30)
                
                # 2. Stop loss: 3% (tight stop for expiry day)
                stop_hit = exit_price < (entry_price * 0.97)
                
                # 3. Time stop: 5 minutes max (quick gamma play)
                time_exit = time_held_sec > (5 * 60)
                
                if profit_hit or stop_hit or time_exit:
                    if trade_count < max_trades:
                        entry_dates[trade_count] = entry_date
                        entry_times[trade_count] = entry_time_sec
                        exit_dates[trade_count] = current_date
                        exit_times[trade_count] = current_time
                        strike_arr[trade_count] = entry_strike
                        opt_type_arr[trade_count] = 0  # CE
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
                        
                        spot_entries[trade_count] = entry_spot
                        hod_entries[trade_count] = hod_at_entry
                        trade_count += 1
                    
                    in_position = False
        
        # === ENTRY LOGIC ===
        elif not in_position and hod_established:
            # THE TRIGGER: Spot breaks High of Day
            breakout = current_spot > high_of_day
            
            if not breakout:
                continue
            
            # Calculate target strike: Spot + 50 (slightly OTM)
            target_strike = round((current_spot + 50) / 50) * 50  # Round to nearest 50
            
            # Find the OTM strike
            ce_idx = find_otm_strike(current_spot, strikes, opt_types, block_start, block_end, target_strike)
            
            if ce_idx < 0 or ce_idx >= block_end:
                continue
            
            # Check liquidity
            if ask_prices[ce_idx] <= 0 or bid_prices[ce_idx] <= 0:
                continue
            
            # Check spread (tighter for gamma scalp)
            mid = (ask_prices[ce_idx] + bid_prices[ce_idx]) * 0.5
            if mid <= 0:
                continue
            
            spread_pct = ((ask_prices[ce_idx] - bid_prices[ce_idx]) / mid) * 100
            if spread_pct >= 3.0:  # Allow wider spread on expiry
                continue
            
            # ENTER: Buy at ask
            in_position = True
            entry_price = ask_prices[ce_idx]
            entry_time_sec = current_time
            entry_date = current_date
            entry_strike = strikes[ce_idx]
            entry_spot = current_spot
            hod_at_entry = high_of_day
            
            # Update HOD after breakout
            if current_spot > high_of_day:
                high_of_day = current_spot
    
    # Force exit any open position
    if in_position and trade_count < max_trades:
        exit_price = 0.0
        for j in range(max(0, n-1000), n):
            if strikes[j] == entry_strike and opt_types[j] == 0:
                exit_price = prices[j]
        
        if exit_price > 0:
            entry_dates[trade_count] = entry_date
            entry_times[trade_count] = entry_time_sec
            exit_dates[trade_count] = dates_int[n-1]
            exit_times[trade_count] = times_sec[n-1]
            strike_arr[trade_count] = entry_strike
            opt_type_arr[trade_count] = 0
            entry_prices[trade_count] = entry_price
            exit_prices[trade_count] = exit_price
            pnls[trade_count] = exit_price - entry_price
            hold_mins[trade_count] = (times_sec[n-1] - entry_time_sec) // 60
            exit_reasons[trade_count] = 3  # EOD
            spot_entries[trade_count] = entry_spot
            hod_entries[trade_count] = hod_at_entry
            trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        strike_arr[:trade_count], opt_type_arr[:trade_count],
        entry_prices[:trade_count], exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count],
        exit_reasons[:trade_count], spot_entries[:trade_count], hod_entries[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def is_tuesday(date_str):
    """Check if date is a Tuesday (NIFTY expiry day)"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.weekday() == 1  # 0=Monday, 1=Tuesday
    except:
        return False


def run_gamma_burst_strategy(data_dir: Path) -> List[BuyTrade]:
    """Run Gamma Burst strategy - NIFTY Tuesdays only"""
    all_trades = []
    
    # Strategy parameters
    START_TIME = 13 * 3600 + 30 * 60  # 1:30 PM - wait for HOD to establish
    EOD_EXIT = 15 * 3600 + 20 * 60    # 3:20 PM
    
    date_dirs = sorted(data_dir.glob("*"))
    
    print(f"Processing NIFTY (Tuesdays only - Expiry days)...")
    
    tuesday_count = 0
    
    for date_dir in date_dirs:
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        # CHECK: Is this a Tuesday?
        if not is_tuesday(date_dir.name):
            continue
        
        tuesday_count += 1
        
        underlying_dir = date_dir / "NIFTY"
        if not underlying_dir.exists():
            continue
        
        files = list(underlying_dir.glob("*.parquet"))
        if not files:
            continue
        
        try:
            df = pl.read_parquet(files[0], columns=[
                'timestamp', 'strike', 'opt_type', 'price',
                'bp0', 'sp0', 'expiry', 'spot_price'
            ]).filter(pl.col('timestamp').dt.year() > 1970)
            
            if df.is_empty():
                continue
            
            # Nearest expiry only (should be today's expiry)
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
            spots = df['spot_price'].fill_null(0).to_numpy()
            
            results = strategy_gamma_burst(
                ts_ns, dates, times, strikes, opt_t, prices,
                bid_p, ask_p, spots,
                START_TIME, EOD_EXIT
            )
            
            if len(results[0]) > 0:
                (entry_d, entry_t, exit_d, exit_t, strike_a, opt_a,
                 entry_p, exit_p, pnl_a, hold_a, exit_r, spot_e, hod_e) = results
                
                exit_reason_map = {0: 'profit_target', 1: 'time_stop', 2: 'stop_loss', 3: 'eod'}
                
                for i in range(len(pnl_a)):
                    pnl_pct = (pnl_a[i] / entry_p[i] * 100) if entry_p[i] > 0 else 0
                    
                    trade = BuyTrade(
                        entry_date=str(int_to_date(entry_d[i])),
                        entry_time=str(sec_to_time(entry_t[i])),
                        exit_date=str(int_to_date(exit_d[i])),
                        exit_time=str(sec_to_time(exit_t[i])),
                        strike=strike_a[i],
                        opt_type='CE',
                        entry_price=entry_p[i],
                        exit_price=exit_p[i],
                        pnl=pnl_a[i],
                        pnl_pct=pnl_pct,
                        hold_duration_minutes=int(hold_a[i]),
                        exit_reason=exit_reason_map.get(exit_r[i], 'unknown'),
                        spot_at_entry=spot_e[i],
                        high_of_day=hod_e[i]
                    )
                    all_trades.append(trade)
            
            del df
            gc.collect()
            
        except Exception as e:
            print(f"  Error on {date_dir.name}: {e}")
            continue
    
    print(f"  Processed {tuesday_count} Tuesdays (expiry days)")
    
    return all_trades


def save_trades(trades: List[BuyTrade], filename: Path):
    """Save trades to CSV"""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'strike', 'opt_type', 'entry_price', 'exit_price',
            'pnl', 'pnl_pct', 'hold_duration_minutes', 'exit_reason',
            'spot_at_entry', 'high_of_day'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.strike, t.opt_type, t.entry_price, t.exit_price,
                f"{t.pnl:.2f}", f"{t.pnl_pct:.2f}", t.hold_duration_minutes,
                t.exit_reason, f"{t.spot_at_entry:.2f}", f"{t.high_of_day:.2f}"
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("GAMMA BURST - Expiry Day Special (NIFTY Tuesdays Only)")
    print("="*80)
    print("Strategy Logic:")
    print("  1. ONLY Tuesdays (NIFTY expiry day)")
    print("  2. Time: After 1:30 PM (HOD established)")
    print("  3. Strike: Spot + 50 (slightly OTM CE)")
    print("  4. Trigger: Spot breaks High of Day")
    print("  5. Exit: 30% profit OR 3% stop OR 5-min time OR 3:20 PM")
    print("")
    print("Gamma Play: Near expiry, 10-point spot move = 50-80% option spike")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../strategy_results/buying/strategy_results_buying")
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*80}")
    print(f"PROCESSING NIFTY (Tuesdays Only)")
    print(f"{'='*80}")
    
    strat_start = time_mod.time()
    trades = run_gamma_burst_strategy(data_dir)
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
        
        output_file = results_dir / "NIFTY_gamma_burst_trades.csv"
        save_trades(trades, output_file)
        print(f"  Saved: {output_file}")
        
        # Summary file
        summary_file = results_dir / "gamma_burst_summary.csv"
        with open(summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['underlying', 'trades', 'wins', 'win_rate_%', 'total_pnl', 'avg_pnl', 'avg_hold_minutes'])
            writer.writerow([
                'NIFTY', len(trades), wins,
                f"{wins/len(trades)*100:.1f}" if len(trades) > 0 else "0",
                f"{total_pnl:.2f}", f"{avg_pnl:.2f}", f"{avg_hold:.1f}"
            ])
        print(f"\nSummary: {summary_file}")
    else:
        print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGY COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    print(f"Results directory: {results_dir}/")


if __name__ == "__main__":
    main()
