#!/usr/bin/env python3
"""
VELOCITY & COMPRESSION - High-Frequency Scalping Strategies

Strategy 1: Micro-Burst (Velocity Scalp)
- Trigger: Price moves 5+ points in 3 seconds with volume surge
- Exit: Velocity dies (momentum stops) - "5-second rule"

Strategy 2: Compression Breakout (Pause & Explode) 
- Setup: Range < 10 points in last 60 seconds (compression)
- Entry: Price breaks 60s high with volume
- Exit: Trailing stop (1-2 minute hold)

Combined engine with nanosecond precision for tick-level trading
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
    hold_duration_seconds: int
    exit_reason: str
    strategy_type: str  # 'burst' or 'compression'
    velocity_at_entry: float
    volume_at_entry: int


@njit
def find_atm_strike(spot_price: float, strikes_arr: np.ndarray, opt_types_arr: np.ndarray, 
                     strike_step: int, start_idx: int, end_idx: int):
    """Find ATM strike"""
    ce_idx = -1
    pe_idx = -1
    
    rounded_spot = round(spot_price / strike_step) * strike_step
    
    for i in range(start_idx, end_idx):
        if opt_types_arr[i] == 0 and abs(strikes_arr[i] - rounded_spot) < 1.0:
            ce_idx = i
            break
    
    for i in range(start_idx, end_idx):
        if opt_types_arr[i] == 1 and abs(strikes_arr[i] - rounded_spot) < 1.0:
            pe_idx = i
            break
    
    return ce_idx, pe_idx


@njit
def strategy_velocity_compression(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    ask_prices: np.ndarray,
    quantities: np.ndarray,  # Use qty as volume proxy
    spots: np.ndarray,
    strike_step: int,
    start_time_sec: int,
    end_time_sec: int,
    eod_exit_time: int
):
    """
    Velocity & Compression - High-frequency scalping
    """
    n = len(prices)
    max_trades = 500
    
    # Constants
    VELOCITY_WINDOW_NS = 3_000_000_000  # 3 seconds
    COMPRESSION_WINDOW_NS = 60_000_000_000  # 60 seconds
    MIN_VELOCITY_POINTS = 5.0  # 5 points in 3s
    VOL_MULTIPLIER = 3.0
    
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
    hold_secs = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    strategy_types = np.empty(max_trades, dtype=np.int8)  # 0=burst, 1=compression
    velocities = np.empty(max_trades, dtype=np.float64)
    volumes = np.empty(max_trades, dtype=np.int64)
    
    trade_count = 0
    
    # Position tracking
    in_position = False
    entry_price = 0.0
    entry_ts = 0
    entry_time = 0
    entry_date = 0
    entry_strike = 0.0
    entry_opt_type = 0
    highest_price = 0.0
    entry_velocity = 0.0
    entry_volume = 0
    entry_strategy = 0
    
    # Window indices
    vel_idx = 0
    comp_idx = 0
    
    # Rolling volume stats
    vol_sum = 0.0
    vol_count = 0
    
    # Previous contract tracking
    prev_strike = -1.0
    prev_opt = -1
    
    i = 0
    while i < n:
        current_time = times_sec[i]
        current_date = dates_int[i]
        current_ts = timestamps_ns[i]
        
        # Skip outside trading window
        if current_time < start_time_sec or current_time > end_time_sec:
            i += 1
            continue
        
        # Get strike and opt for this tick
        current_strike = strikes[i]
        current_opt = opt_types[i]
        current_price = prices[i]
        current_qty = quantities[i]
        
        # CONTRACT CHANGE CHECK
        if current_strike != prev_strike or current_opt != prev_opt:
            # Force exit on contract change
            if in_position and trade_count < max_trades:
                entry_dates[trade_count] = entry_date
                entry_times[trade_count] = entry_time
                exit_dates[trade_count] = current_date
                exit_times[trade_count] = current_time
                strike_arr[trade_count] = entry_strike
                opt_type_arr[trade_count] = entry_opt_type
                entry_prices[trade_count] = entry_price
                exit_prices[trade_count] = prices[i-1] if i > 0 else entry_price
                pnls[trade_count] = (prices[i-1] if i > 0 else entry_price) - entry_price
                hold_secs[trade_count] = (current_time - entry_time)
                exit_reasons[trade_count] = 4  # contract_change
                strategy_types[trade_count] = entry_strategy
                velocities[trade_count] = entry_velocity
                volumes[trade_count] = entry_volume
                trade_count += 1
            
            # Reset everything
            in_position = False
            vel_idx = i
            comp_idx = i
            vol_sum = 0.0
            vol_count = 0
            prev_strike = current_strike
            prev_opt = current_opt
        
        # Force EOD exit
        if in_position and current_time >= eod_exit_time:
            if trade_count < max_trades:
                entry_dates[trade_count] = entry_date
                entry_times[trade_count] = entry_time
                exit_dates[trade_count] = current_date
                exit_times[trade_count] = current_time
                strike_arr[trade_count] = entry_strike
                opt_type_arr[trade_count] = entry_opt_type
                entry_prices[trade_count] = entry_price
                exit_prices[trade_count] = current_price
                pnls[trade_count] = current_price - entry_price
                hold_secs[trade_count] = current_time - entry_time
                exit_reasons[trade_count] = 3  # eod
                strategy_types[trade_count] = entry_strategy
                velocities[trade_count] = entry_velocity
                volumes[trade_count] = entry_volume
                trade_count += 1
            
            in_position = False
        
        # UPDATE WINDOWS (slide forward to match time)
        while vel_idx < i and (current_ts - timestamps_ns[vel_idx]) > VELOCITY_WINDOW_NS:
            vel_idx += 1
        
        while comp_idx < i and (current_ts - timestamps_ns[comp_idx]) > COMPRESSION_WINDOW_NS:
            comp_idx += 1
        
        # CALCULATE METRICS
        
        # A. VELOCITY (price change in last 3 seconds)
        velocity = 0.0
        if i > vel_idx and strikes[vel_idx] == current_strike and opt_types[vel_idx] == current_opt:
            velocity = current_price - prices[vel_idx]
        
        # B. COMPRESSION (range in last 60 seconds)
        range_high = -1.0
        range_low = 999999.0
        
        if not in_position:  # Only calculate when looking for entry
            for k in range(comp_idx, i + 1):
                if strikes[k] == current_strike and opt_types[k] == current_opt:
                    if prices[k] > range_high:
                        range_high = prices[k]
                    if prices[k] < range_low:
                        range_low = prices[k]
        
        range_size = range_high - range_low if range_high > 0 else 0.0
        
        # C. ROLLING AVERAGE VOLUME
        if current_qty > 0:
            vol_sum += current_qty
            vol_count += 1
        
        avg_vol = (vol_sum / vol_count) if vol_count > 0 else 1.0
        
        # === POSITION MANAGEMENT ===
        if in_position:
            # Check we're still in same contract
            if current_strike != entry_strike or current_opt != entry_opt_type:
                i += 1
                continue
            
            # Update highest price
            if current_price > highest_price:
                highest_price = current_price
            
            # EXIT CONDITIONS
            time_held = current_time - entry_time
            
            # Scenario 1: Velocity died (for burst entries)
            velocity_died = velocity < 0
            
            # Scenario 2: Trailing stop (tight - 5 points)
            trail_hit = current_price < (highest_price - 5.0)
            
            # Scenario 3: Time stop (60 seconds max for bursts)
            time_out = time_held > 60
            
            # Scenario 4: Profit target (20% for quick scalp)
            profit_hit = current_price >= (entry_price * 1.20)
            
            if velocity_died or trail_hit or time_out or profit_hit:
                if trade_count < max_trades:
                    entry_dates[trade_count] = entry_date
                    entry_times[trade_count] = entry_time
                    exit_dates[trade_count] = current_date
                    exit_times[trade_count] = current_time
                    strike_arr[trade_count] = entry_strike
                    opt_type_arr[trade_count] = entry_opt_type
                    entry_prices[trade_count] = entry_price
                    exit_prices[trade_count] = current_price
                    pnls[trade_count] = current_price - entry_price
                    hold_secs[trade_count] = time_held
                    
                    if profit_hit:
                        exit_reasons[trade_count] = 0  # profit
                    elif velocity_died:
                        exit_reasons[trade_count] = 1  # velocity_died
                    elif time_out:
                        exit_reasons[trade_count] = 2  # time
                    else:
                        exit_reasons[trade_count] = 5  # trail
                    
                    strategy_types[trade_count] = entry_strategy
                    velocities[trade_count] = entry_velocity
                    volumes[trade_count] = entry_volume
                    trade_count += 1
                
                in_position = False
        
        # === ENTRY LOGIC ===
        elif not in_position:
            # SETUP 1: MICRO-BURST
            # Price moved > 5 points in 3s AND Volume is 3x avg
            is_burst = (velocity > MIN_VELOCITY_POINTS) and (current_qty > avg_vol * VOL_MULTIPLIER)
            
            # SETUP 2: COMPRESSION BREAKOUT
            # Range is tight (< 10 points) in last 60s AND we just broke the high
            is_tight = 0.0 < range_size < 10.0
            is_breakout = is_tight and (current_price >= range_high) and (current_price > prices[i-1] if i > 0 else 0) and (current_qty > avg_vol * 2.0)
            
            if is_burst or is_breakout:
                # Check we have valid ask price
                if ask_prices[i] <= 0:
                    i += 1
                    continue
                
                # ENTER
                in_position = True
                entry_price = ask_prices[i]  # Pay the ask
                entry_ts = current_ts
                entry_time = current_time
                entry_date = current_date
                entry_strike = current_strike
                entry_opt_type = current_opt
                highest_price = entry_price
                entry_velocity = velocity
                entry_volume = int(current_qty)
                entry_strategy = 0 if is_burst else 1
                
                # Reset volume stats to adapt to new regime
                vol_sum = current_qty
                vol_count = 1
        
        i += 1
    
    # Force exit any open position
    if in_position and trade_count < max_trades:
        entry_dates[trade_count] = entry_date
        entry_times[trade_count] = entry_time
        exit_dates[trade_count] = dates_int[n-1]
        exit_times[trade_count] = times_sec[n-1]
        strike_arr[trade_count] = entry_strike
        opt_type_arr[trade_count] = entry_opt_type
        entry_prices[trade_count] = entry_price
        exit_prices[trade_count] = prices[n-1]
        pnls[trade_count] = prices[n-1] - entry_price
        hold_secs[trade_count] = times_sec[n-1] - entry_time
        exit_reasons[trade_count] = 3  # eod
        strategy_types[trade_count] = entry_strategy
        velocities[trade_count] = entry_velocity
        volumes[trade_count] = entry_volume
        trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        strike_arr[:trade_count], opt_type_arr[:trade_count],
        entry_prices[:trade_count], exit_prices[:trade_count],
        pnls[:trade_count], hold_secs[:trade_count],
        exit_reasons[:trade_count], strategy_types[:trade_count],
        velocities[:trade_count], volumes[:trade_count]
    )


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


def run_velocity_compression(data_dir: Path, underlying: str) -> List[BuyTrade]:
    """Run Velocity & Compression strategies"""
    all_trades = []
    
    # Strategy parameters
    START_TIME = 10 * 3600  # 10:00 AM
    END_TIME = 15 * 3600    # 3:00 PM
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
                'timestamp', 'strike', 'opt_type', 'price', 'qty',
                'sp0', 'expiry', 'spot_price'
            ]).filter(pl.col('timestamp').dt.year() > 1970)
            
            if df.is_empty():
                continue
            
            nearest_expiry = df['expiry'].min()
            df = df.filter(pl.col('expiry') == nearest_expiry).sort(['timestamp', 'strike', 'opt_type'])
            
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
            ask_p = df['sp0'].fill_null(0).to_numpy()  # Use sp0 as ask
            qtys = df['qty'].fill_null(0).to_numpy()
            spots = df['spot_price'].fill_null(0).to_numpy()
            
            results = strategy_velocity_compression(
                ts_ns, dates, times, strikes, opt_t, prices,
                ask_p, qtys, spots, STRIKE_STEP,
                START_TIME, END_TIME, EOD_EXIT
            )
            
            if len(results[0]) > 0:
                (entry_d, entry_t, exit_d, exit_t, strike_a, opt_a,
                 entry_p, exit_p, pnl_a, hold_a, exit_r, strat_t, vel_a, vol_a) = results
                
                exit_reason_map = {0: 'profit', 1: 'velocity_died', 2: 'time_stop', 3: 'eod', 4: 'contract_change', 5: 'trail_stop'}
                strategy_map = {0: 'burst', 1: 'compression'}
                
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
                        hold_duration_seconds=int(hold_a[i]),
                        exit_reason=exit_reason_map.get(exit_r[i], 'unknown'),
                        strategy_type=strategy_map.get(strat_t[i], 'unknown'),
                        velocity_at_entry=vel_a[i],
                        volume_at_entry=int(vol_a[i])
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
            'pnl', 'pnl_pct', 'hold_duration_seconds', 'exit_reason',
            'strategy_type', 'velocity_at_entry', 'volume_at_entry'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.strike, t.opt_type, t.entry_price, t.exit_price,
                f"{t.pnl:.2f}", f"{t.pnl_pct:.2f}", t.hold_duration_seconds,
                t.exit_reason, t.strategy_type, f"{t.velocity_at_entry:.2f}", t.volume_at_entry
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("VELOCITY & COMPRESSION - High-Frequency Scalping")
    print("="*80)
    print("Strategy 1: Micro-Burst (Velocity Scalp)")
    print("  - Price moves 5+ points in 3 seconds")
    print("  - Volume 3x average")
    print("  - Exit when velocity dies")
    print("")
    print("Strategy 2: Compression Breakout (Pause & Explode)")
    print("  - Range < 10 points in 60 seconds (compression)")
    print("  - Breakout with volume confirmation")
    print("  - Trailing stop exit")
    print("")
    print("Time Window: 10:00 AM - 3:00 PM | Max Hold: 60 seconds")
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
        trades = run_velocity_compression(data_dir, underlying)
        strat_time = time_mod.time() - strat_start
        
        if trades:
            pnls = [t.pnl for t in trades]
            wins = sum(1 for p in pnls if p > 0)
            total_pnl = sum(pnls)
            avg_pnl = total_pnl / len(trades)
            avg_hold = sum(t.hold_duration_seconds for t in trades) / len(trades)
            
            # Breakdown by strategy type
            burst_trades = [t for t in trades if t.strategy_type == 'burst']
            comp_trades = [t for t in trades if t.strategy_type == 'compression']
            
            # Exit reasons
            exit_reasons = {}
            for t in trades:
                exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1
            
            print(f"\n✓ Completed in {strat_time:.1f}s")
            print(f"  Total Trades: {len(trades)}")
            print(f"    - Burst: {len(burst_trades)}")
            print(f"    - Compression: {len(comp_trades)}")
            print(f"  Wins: {wins} ({wins/len(trades)*100:.1f}%)")
            print(f"  Total P&L: ₹{total_pnl:.2f}")
            print(f"  Avg P&L: ₹{avg_pnl:.2f}")
            print(f"  Avg Hold: {avg_hold:.1f} seconds")
            print(f"  Exit Reasons: {exit_reasons}")
            
            all_results.append({
                'underlying': underlying,
                'trades': len(trades),
                'wins': wins,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'avg_hold_sec': avg_hold
            })
            
            output_file = results_dir / f"{underlying}_velocity_compression_trades.csv"
            save_trades(trades, output_file)
            print(f"  Saved: {output_file}")
        else:
            print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGY COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    if all_results:
        summary_file = results_dir / "velocity_compression_summary.csv"
        with open(summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['underlying', 'trades', 'wins', 'win_rate_%', 'total_pnl', 'avg_pnl', 'avg_hold_seconds'])
            for r in all_results:
                writer.writerow([
                    r['underlying'], r['trades'], r['wins'],
                    f"{r['wins']/r['trades']*100:.1f}" if r['trades'] > 0 else "0",
                    f"{r['total_pnl']:.2f}", f"{r['avg_pnl']:.2f}", f"{r['avg_hold_sec']:.1f}"
                ])
        print(f"\nSummary: {summary_file}")
    
    print(f"Results directory: {results_dir}/")


if __name__ == "__main__":
    main()
