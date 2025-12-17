#!/usr/bin/env python3
"""
COMPLETE OPTIMIZED STRATEGIES - All 4 Professional Strategies

1. Momentum Burst 2.0 - With trailing stop (no fixed targets)
2. TTM Squeeze - Bollinger/Keltner compression breakout  
3. Morning ORB + VWAP - Opening range breakout
4. Fixed Absorption - Correct microstructure logic

All use TRAILING STOPS (let winners run) instead of fixed profit targets
Transaction cost: ₹5/trade included
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
    max_price: float
    strategy: str


def int_to_date(date_int):
    from datetime import date
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


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
def calculate_bb_kc(prices: np.ndarray, period: int, idx: int):
    """Calculate Bollinger Bands and Keltner Channels"""
    if idx < period:
        return 0.0, 0.0, 0.0, 0.0
    
    # Calculate SMA
    sma = 0.0
    for i in range(idx - period + 1, idx + 1):
        sma += prices[i]
    sma /= period
    
    # Calculate standard deviation
    variance = 0.0
    for i in range(idx - period + 1, idx + 1):
        diff = prices[i] - sma
        variance += diff * diff
    std = (variance / period) ** 0.5
    
    # Calculate ATR (simplified as average of absolute price changes)
    atr = 0.0
    for i in range(idx - period + 2, idx + 1):
        atr += abs(prices[i] - prices[i-1])
    atr /= (period - 1)
    
    # Bollinger Bands (2 SD)
    bb_upper = sma + 2.0 * std
    bb_lower = sma - 2.0 * std
    
    # Keltner Channels (1.5 ATR)
    kc_upper = sma + 1.5 * atr
    kc_lower = sma - 1.5 * atr
    
    return bb_upper, bb_lower, kc_upper, kc_lower


@njit
def strategy_all_optimized(
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
    transaction_cost: float
):
    """
    All 4 optimized strategies combined with trailing stops
    """
    n = len(prices)
    max_trades = 2000
    
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
    strategy_types = np.empty(max_trades, dtype=np.int8)
    
    trade_count = 0
    
    # Position tracking
    in_position = False
    entry_price = 0.0
    entry_time = 0
    entry_date = 0
    entry_strike = 0.0
    entry_opt_type = 0
    highest_price = 0.0
    entry_strategy = 0
    
    # Price history for indicators
    price_history = np.zeros(1000, dtype=np.float64)
    hist_idx = 0
    
    # EMA state
    ema5 = 0.0
    ema21 = 0.0
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    ema_initialized = False
    
    # ORB state
    orb_high = 0.0
    orb_low = 999999.0
    orb_established = False
    day_start_time = 0
    
    # VWAP state
    vwap_sum_pv = 0.0
    vwap_sum_v = 0.0
    
    # Volume tracking
    vol_sum = 0.0
    vol_count = 0
    
    # Previous contract
    prev_strike = -1.0
    prev_opt = -1
    prev_date = -1
    
    i = 0
    while i < n:
        current_time = times_sec[i]
        current_date = dates_int[i]
        current_ts = timestamps_ns[i]
        current_strike = strikes[i]
        current_opt = opt_types[i]
        current_price = prices[i]
        current_qty = bid_qty[i] + ask_qty[i]
        
        # New day reset
        if current_date != prev_date:
            orb_established = False
            orb_high = 0.0
            orb_low = 999999.0
            day_start_time = current_time
            vwap_sum_pv = 0.0
            vwap_sum_v = 0.0
            prev_date = current_date
        
        # Contract change: exit position
        if (current_strike != prev_strike or current_opt != prev_opt) and prev_strike > 0:
            if in_position:
                exit_price = prices[i-1] if i > 0 else entry_price
                raw_pnl = exit_price - entry_price
                final_pnl = raw_pnl - transaction_cost
                
                if trade_count < max_trades:
                    entry_dates[trade_count] = entry_date
                    entry_times[trade_count] = entry_time
                    exit_dates[trade_count] = current_date
                    exit_times[trade_count] = current_time
                    strike_arr[trade_count] = entry_strike
                    opt_type_arr[trade_count] = entry_opt_type
                    entry_prices[trade_count] = entry_price
                    exit_prices[trade_count] = exit_price
                    pnls[trade_count] = final_pnl
                    hold_mins[trade_count] = (current_time - entry_time) // 60
                    exit_reasons[trade_count] = 4  # contract_change
                    max_prices[trade_count] = highest_price
                    strategy_types[trade_count] = entry_strategy
                    trade_count += 1
                
                in_position = False
                ema_initialized = False
            
            prev_strike = current_strike
            prev_opt = current_opt
        
        # Update price history
        price_history[hist_idx % 1000] = current_price
        hist_idx += 1
        
        # Update VWAP
        if current_qty > 0:
            vwap_sum_pv += current_price * current_qty
            vwap_sum_v += current_qty
            vol_sum += current_qty
            vol_count += 1
        
        vwap = vwap_sum_pv / vwap_sum_v if vwap_sum_v > 0 else current_price
        avg_vol = vol_sum / vol_count if vol_count > 0 else 1.0
        
        # Update EMAs
        if not ema_initialized:
            ema5 = current_price
            ema21 = current_price
            ema_initialized = True
        else:
            ema5 = current_price * alpha5 + ema5 * (1 - alpha5)
            ema21 = current_price * alpha21 + ema21 * (1 - alpha21)
        
        # === ORB ESTABLISHMENT (9:15-9:30 AM) ===
        morning_start = 9 * 3600 + 15 * 60
        morning_end = 9 * 3600 + 30 * 60
        
        if not orb_established and morning_start <= current_time <= morning_end:
            if current_price > orb_high:
                orb_high = current_price
            if current_price < orb_low:
                orb_low = current_price
        elif current_time > morning_end and not orb_established:
            orb_established = True
        
        # === POSITION MANAGEMENT (TRAILING STOP) ===
        if in_position:
            # Update highest
            if current_price > highest_price:
                highest_price = current_price
            
            time_held = current_time - entry_time
            
            # TRAILING STOP (KEY: Let winners run!)
            roi_pct = (highest_price - entry_price) / entry_price if entry_price > 0 else 0
            
            # Initial stop: 10% hard stop
            stop_price = entry_price * 0.90
            
            # After 5% profit: move to breakeven
            if roi_pct >= 0.05:
                stop_price = entry_price + (transaction_cost * 0.5)
            
            # After 10% profit: trail at 15% from peak (loose)
            if roi_pct >= 0.10:
                stop_price = max(stop_price, highest_price * 0.85)
            
            # After 20% profit: trail at 20% from peak (very loose)
            if roi_pct >= 0.20:
                stop_price = max(stop_price, highest_price * 0.80)
            
            # Exit conditions
            trail_hit = current_price < stop_price
            time_exit = time_held > (20 * 60)  # 20 min max
            eod_exit = current_time >= (15 * 3600 + 20 * 60)
            
            if trail_hit or time_exit or eod_exit:
                raw_pnl = current_price - entry_price
                final_pnl = raw_pnl - transaction_cost
                
                if trade_count < max_trades:
                    entry_dates[trade_count] = entry_date
                    entry_times[trade_count] = entry_time
                    exit_dates[trade_count] = current_date
                    exit_times[trade_count] = current_time
                    strike_arr[trade_count] = entry_strike
                    opt_type_arr[trade_count] = entry_opt_type
                    entry_prices[trade_count] = entry_price
                    exit_prices[trade_count] = current_price
                    pnls[trade_count] = final_pnl
                    hold_mins[trade_count] = time_held // 60
                    
                    if eod_exit:
                        exit_reasons[trade_count] = 3
                    elif time_exit:
                        exit_reasons[trade_count] = 1
                    else:
                        exit_reasons[trade_count] = 0  # trail
                    
                    max_prices[trade_count] = highest_price
                    strategy_types[trade_count] = entry_strategy
                    trade_count += 1
                
                in_position = False
        
        # === ENTRY LOGIC (Try all 4 strategies) ===
        elif not in_position and ema_initialized:
            entered = False
            entry_strat = -1
            limit_price = 0.0
            
            # STRATEGY 1: Momentum Burst
            # Time: 10 AM - 2:30 PM, EMA5 > EMA21, Bid > Ask qty
            if 10 * 3600 <= current_time <= 14 * 3600 + 30 * 60:
                if ema5 > ema21:
                    if bid_qty[i] > ask_qty[i] * 1.3 and ask_qty[i] > 0:
                        if ask_prices[i] > 0:
                            entered = True
                            entry_strat = 0  # momentum
                            limit_price = bid_prices[i] + 0.5
            
            # STRATEGY 2: TTM Squeeze  
            # Bollinger inside Keltner + breakout
            if not entered and hist_idx >= 20:
                bb_u, bb_l, kc_u, kc_l = calculate_bb_kc(price_history, 20, (hist_idx - 1) % 1000)
                squeeze_on = (bb_u < kc_u) and (bb_l > kc_l)
                breakout = current_price > kc_u
                
                if squeeze_on and breakout and current_qty > avg_vol * 1.5:
                    if ask_prices[i] > 0:
                        entered = True
                        entry_strat = 1  # squeeze
                        limit_price = bid_prices[i] + 0.5
            
            # STRATEGY 3: Morning ORB + VWAP
            # After 9:30 AM, break ORB high, price > VWAP
            if not entered and orb_established:
                if 9 * 3600 + 30 * 60 <= current_time <= 11 * 3600:
                    orb_breakout = current_price > orb_high
                    above_vwap = current_price > vwap
                    volume_spike = current_qty > avg_vol * 1.5
                    
                    if orb_breakout and above_vwap and volume_spike:
                        if ask_prices[i] > 0:
                            entered = True
                            entry_strat = 2  # orb
                            limit_price = bid_prices[i] + 0.5
            
            # STRATEGY 4: Fixed Absorption
            # Price at ask (buyers aggressive), volume spike
            if not entered:
                if 10 * 3600 <= current_time <= 14 * 3600:
                    price_at_ask = current_price >= ask_prices[i] if ask_prices[i] > 0 else False
                    vol_spike = current_qty > avg_vol * 2.0
                    
                    if price_at_ask and vol_spike:
                        if ask_prices[i] > 0:
                            entered = True
                            entry_strat = 3  # absorption
                            limit_price = bid_prices[i] + 0.5
            
            # ENTER POSITION
            if entered and limit_price > 0:
                in_position = True
                entry_price = limit_price
                entry_time = current_time
                entry_date = current_date
                entry_strike = current_strike
                entry_opt_type = current_opt
                highest_price = entry_price
                entry_strategy = entry_strat
        
        i += 1
    
    # Force exit any open position
    if in_position and trade_count < max_trades:
        raw_pnl = prices[n-1] - entry_price
        final_pnl = raw_pnl - transaction_cost
        
        entry_dates[trade_count] = entry_date
        entry_times[trade_count] = entry_time
        exit_dates[trade_count] = dates_int[n-1]
        exit_times[trade_count] = times_sec[n-1]
        strike_arr[trade_count] = entry_strike
        opt_type_arr[trade_count] = entry_opt_type
        entry_prices[trade_count] = entry_price
        exit_prices[trade_count] = prices[n-1]
        pnls[trade_count] = final_pnl
        hold_mins[trade_count] = (times_sec[n-1] - entry_time) // 60
        exit_reasons[trade_count] = 3  # eod
        max_prices[trade_count] = highest_price
        strategy_types[trade_count] = entry_strategy
        trade_count += 1
    
    return (
        entry_dates[:trade_count], entry_times[:trade_count],
        exit_dates[:trade_count], exit_times[:trade_count],
        strike_arr[:trade_count], opt_type_arr[:trade_count],
        entry_prices[:trade_count], exit_prices[:trade_count],
        pnls[:trade_count], hold_mins[:trade_count],
        exit_reasons[:trade_count], max_prices[:trade_count],
        strategy_types[:trade_count]
    )


def run_all_strategies(data_dir: Path, underlying: str) -> List[BuyTrade]:
    """Run all 4 optimized strategies"""
    all_trades = []
    
    TRANSACTION_COST = 5.0
    STRIKE_STEP = 50 if underlying == "NIFTY" else 100
    
    date_dirs = sorted(data_dir.glob("*"))
    
    print(f"Processing {underlying} (Strike Step: {STRIKE_STEP})...")
    
    for date_dir in date_dirs:
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        underlying_dir = date_dir / underlying
        if not underlying_dir.exists():
            continue
        
        files = sorted(underlying_dir.glob("*.parquet"))
        if not files:
            continue
        
        try:
            df = pl.read_parquet(files, columns=[
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
            
            results = strategy_all_optimized(
                ts_ns, dates, times, strikes, opt_t, prices,
                bid_p, ask_p, bid_q, ask_q, spots, STRIKE_STEP,
                TRANSACTION_COST
            )
            
            if len(results[0]) > 0:
                (entry_d, entry_t, exit_d, exit_t, strike_a, opt_a,
                 entry_p, exit_p, pnl_a, hold_a, exit_r, max_p, strat_t) = results
                
                exit_reason_map = {0: 'trail_stop', 1: 'time', 2: 'stop', 3: 'eod', 4: 'contract_change'}
                strategy_map = {0: 'Momentum', 1: 'TTM_Squeeze', 2: 'ORB_VWAP', 3: 'Absorption'}
                
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
                        max_price=max_p[i],
                        strategy=strategy_map.get(strat_t[i], 'unknown')
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
            'max_price', 'strategy'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.strike, t.opt_type, t.entry_price, t.exit_price,
                f"{t.pnl:.2f}", f"{t.pnl_pct:.2f}", t.hold_duration_minutes,
                t.exit_reason, f"{t.max_price:.2f}", t.strategy
            ])


def main():
    start = time_mod.time()
    
    print("="*80)
    print("ALL OPTIMIZED STRATEGIES - With Trailing Stops")
    print("="*80)
    print("Strategies:")
    print("  1. Momentum Burst - EMA + Order book")
    print("  2. TTM Squeeze - Bollinger/Keltner compression breakout")
    print("  3. Morning ORB + VWAP - Opening range breakout")
    print("  4. Fixed Absorption - Aggressive buyers at ask")
    print("")
    print("Exit Logic: TRAILING STOPS (let winners run!)")
    print("  • 10% initial stop")
    print("  • Breakeven at +5% profit")
    print("  • 15% trail at +10% profit")
    print("  • 20% trail at +20% profit")
    print("")
    print("Transaction Cost: ₹5/trade included")
    print("="*80)
    
    root = _project_root()
    data_dir = root / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
    results_dir = root / "strategies" / "strategy_results" / "buying" / "strategy_results_buying"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        strat_start = time_mod.time()
        trades = run_all_strategies(data_dir, underlying)
        strat_time = time_mod.time() - strat_start
        
        if trades:
            pnls = [t.pnl for t in trades]
            wins = sum(1 for p in pnls if p > 0)
            total_pnl = sum(pnls)
            avg_pnl = total_pnl / len(trades)
            avg_hold = sum(t.hold_duration_minutes for t in trades) / len(trades)
            
            # Breakdown by strategy
            strategy_breakdown = {}
            for t in trades:
                if t.strategy not in strategy_breakdown:
                    strategy_breakdown[t.strategy] = {'count': 0, 'pnl': 0.0}
                strategy_breakdown[t.strategy]['count'] += 1
                strategy_breakdown[t.strategy]['pnl'] += t.pnl
            
            # Exit reasons
            exit_reasons = {}
            for t in trades:
                exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1
            
            print(f"\n✓ Completed in {strat_time:.1f}s")
            print(f"  Total Trades: {len(trades)}")
            print(f"  Wins: {wins} ({wins/len(trades)*100:.1f}%)")
            print(f"  Total P&L (after ₹5 costs): ₹{total_pnl:.2f}")
            print(f"  Avg P&L: ₹{avg_pnl:.2f}")
            print(f"  Avg Hold: {avg_hold:.1f} minutes")
            print(f"\n  Strategy Breakdown:")
            for strat, stats in strategy_breakdown.items():
                print(f"    {strat}: {stats['count']} trades, ₹{stats['pnl']:.2f} P&L")
            print(f"\n  Exit Reasons: {exit_reasons}")
            
            all_results.append({
                'underlying': underlying,
                'trades': len(trades),
                'wins': wins,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'avg_hold_min': avg_hold
            })
            
            output_file = results_dir / f"{underlying}_ALL_optimized_trades.csv"
            save_trades(trades, output_file)
            print(f"  Saved: {output_file}")
        else:
            print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ ALL STRATEGIES COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    if all_results:
        summary_file = results_dir / "ALL_optimized_summary.csv"
        with open(summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['underlying', 'trades', 'wins', 'win_rate_%', 'total_pnl_after_costs', 'avg_pnl', 'avg_hold_minutes'])
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
