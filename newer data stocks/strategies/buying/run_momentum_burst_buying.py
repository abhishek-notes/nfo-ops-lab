#!/usr/bin/env python3
"""
ORDER FLOW MOMENTUM BURST - Option Buying Strategy
Adapted to our data structure and format

Strategy Logic:
- Trade during high-volatility windows (12:00 PM - 02:30 PM)
- Buy when Fast EMA (5) > Slow EMA (21) [Trend]
- Enter when Order Imbalance: Bid Qty > Ask Qty * 1.5 [Microstructure]
- Tight spread < 0.5% [Liquidity check]
- Exit: Trailing stop OR Time stop (theta killer)

Data: BANKNIFTY/NIFTY options from v3 spot enriched
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
    max_price: float  # Track trailing stop


@njit
def strategy_buy_momentum_numba(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray, 
    times_sec: np.ndarray,
    strikes: np.ndarray,
    opt_types: np.ndarray,  # 0=CE, 1=PE
    prices: np.ndarray,
    bid_prices: np.ndarray,  # bp0
    ask_prices: np.ndarray,  # sp0
    bid_qty: np.ndarray,     # bq0
    ask_qty: np.ndarray,     # sq0
    distances: np.ndarray,   # distance_from_spot
    spots: np.ndarray,       # spot_price
    start_time_sec: int,     # 12:00 PM = 43200
    end_time_sec: int,       # 14:30 PM = 52200
    max_hold_minutes: int,   # 10 minutes = 600 seconds
    eod_exit_time: int       # 15:20 (3:20 PM) - force exit before close
):
    """
    Option buying strategy with Numba optimization.
    Reset-aware for contract changes.
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
    exit_reasons = np.empty(max_trades, dtype=np.int8)  # 0=trail, 1=time, 2=stop
    max_prices = np.empty(max_trades, dtype=np.float64)
    
    trade_count = 0
    
    # Strategy state
    in_position = False
    entry_price = 0.0
    entry_idx = 0
    entry_strike = 0.0
    entry_opt_type = 0
    highest_price = 0.0
    
    # EMA state (per contract)
    ema5 = 0.0
    ema21 = 0.0
    alpha5 = 2.0 / 6.0   # α = 2/(N+1)
    alpha21 = 2.0 / 22.0
    
    # Previous contract tracking
    prev_strike = -1.0
    prev_opt_type = -1
    
    i = 0
    while i < n:
        current_strike = strikes[i]
        current_opt = opt_types[i]
        
        # Contract change detection - reset indicators
        if current_strike != prev_strike or current_opt != prev_opt_type:
            # New contract - reset everything
            if in_position:
                # Force exit on contract change
                if trade_count < max_trades:
                    exit_dates[trade_count] = dates_int[i-1]
                    exit_times[trade_count] = times_sec[i-1]
                    strike_arr[trade_count] = entry_strike
                    opt_type_arr[trade_count] = entry_opt_type
                    entry_prices[trade_count] = entry_price
                    exit_prices[trade_count] = prices[i-1]
                    pnl = prices[i-1] - entry_price
                    pnls[trade_count] = pnl
                    hold_mins[trade_count] = (times_sec[i-1] - times_sec[entry_idx]) // 60
                    exit_reasons[trade_count] = 3  # contract_change
                    max_prices[trade_count] = highest_price
                    trade_count += 1
                
                in_position = False
            
            # Reset EMAs
            ema5 = prices[i]
            ema21 = prices[i]
            prev_strike = current_strike
            prev_opt_type = current_opt
            i += 1
            continue
        
        # Update EMAs
        price = prices[i]
        ema5 = price * alpha5 + ema5 * (1 - alpha5)
        ema21 = price * alpha21 + ema21 * (1 - alpha21)
        
        # Time filter (12:00 PM to 2:30 PM for entries)
        current_time = times_sec[i]
        
        # Force EOD exit at 3:20 PM (intraday only)
        if in_position and current_time >= eod_exit_time:
            # Exit position before market close
            if trade_count < max_trades:
                entry_dates[trade_count] = dates_int[entry_idx]
                entry_times[trade_count] = times_sec[entry_idx]
                exit_dates[trade_count] = dates_int[i]
                exit_times[trade_count] = current_time
                strike_arr[trade_count] = entry_strike
                opt_type_arr[trade_count] = entry_opt_type
                entry_prices[trade_count] = entry_price
                exit_prices[trade_count] = price
                pnls[trade_count] = price - entry_price
                hold_mins[trade_count] = (current_time - times_sec[entry_idx]) // 60
                exit_reasons[trade_count] = 4  # eod
                max_prices[trade_count] = highest_price
                trade_count += 1
            in_position = False
        
        if current_time < start_time_sec or current_time > end_time_sec:
            i += 1
            continue
        
        # === POSITION MANAGEMENT ===
        if in_position:
            # Only manage if same strike/type
            if current_strike == entry_strike and current_opt == entry_opt_type:
                # Update trailing stop
                if price > highest_price:
                    highest_price = price
                
                # Exit conditions
                time_held_sec = current_time - times_sec[entry_idx]
                
                # 1. Time stop (theta killer) - 10 minutes
                time_exit = time_held_sec > (max_hold_minutes * 60)
                
                # 2. Trailing stop - 5% from peak
                trail_exit = price < (highest_price * 0.95)
                
                # 3. Hard stop loss - 10%
                stop_exit = price < (entry_price * 0.90)
                
                # 4. Trend reversal
                trend_exit = ema5 < ema21
                
                if time_exit or trail_exit or stop_exit or trend_exit:
                    # Exit position
                    if trade_count < max_trades:
                        entry_dates[trade_count] = dates_int[entry_idx]
                        entry_times[trade_count] = times_sec[entry_idx]
                        exit_dates[trade_count] = dates_int[i]
                        exit_times[trade_count] = current_time
                        strike_arr[trade_count] = entry_strike
                        opt_type_arr[trade_count] = entry_opt_type
                        entry_prices[trade_count] = entry_price
                        exit_prices[trade_count] = price
                        pnl = price - entry_price
                        pnls[trade_count] = pnl
                        hold_mins[trade_count] = time_held_sec // 60
                        
                        # Exit reason
                        if time_exit:
                            exit_reasons[trade_count] = 1
                        elif stop_exit:
                            exit_reasons[trade_count] = 2
                        else:
                            exit_reasons[trade_count] = 0  # trail/trend
                        
                        max_prices[trade_count] = highest_price
                        trade_count += 1
                    
                    in_position = False
        
        # === ENTRY LOGIC ===
        elif not in_position:
            # Check all entry conditions
            
            # 1. Trend check: EMA5 > EMA21
            trend_ok = ema5 > ema21
            
            # 2. Order book imbalance: Bid Qty > Ask Qty * 1.5
            imbalance_ok = False
            if ask_qty[i] > 0 and bid_qty[i] > 0:
                imbalance_ok = (bid_qty[i] / ask_qty[i]) > 1.5
            
            # 3. Spread check: tight spread < 0.5%
            spread_ok = False
            if ask_prices[i] > 0 and bid_prices[i] > 0:
                mid = (ask_prices[i] + bid_prices[i]) * 0.5
                if mid > 0:
                    spread_pct = ((ask_prices[i] - bid_prices[i]) / mid) * 100
                    spread_ok = spread_pct < 0.5
            
            # 4. Only trade ATM options (within 0.3% of spot - very tight ATM)
            moneyness_ok = abs(distances[i]) < (spots[i] * 0.003)
            
            # Entry trigger
            if trend_ok and imbalance_ok and spread_ok and moneyness_ok:
                in_position = True
                entry_price = ask_prices[i]  # Buy at ask (realistic)
                entry_idx = i
                entry_strike = current_strike
                entry_opt_type = current_opt
                highest_price = entry_price
        
        i += 1
    
    # Force exit any open position at end
    if in_position and trade_count < max_trades:
        exit_dates[trade_count] = dates_int[n-1]
        exit_times[trade_count] = times_sec[n-1]
        strike_arr[trade_count] = entry_strike
        opt_type_arr[trade_count] = entry_opt_type
        entry_prices[trade_count] = entry_price
        exit_prices[trade_count] = prices[n-1]
        pnls[trade_count] = prices[n-1] - entry_price
        hold_mins[trade_count] = (times_sec[n-1] - times_sec[entry_idx]) // 60
        exit_reasons[trade_count] = 4  # eod
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


def run_momentum_buying_strategy(data_dir: Path, underlying: str) -> List[BuyTrade]:
    """Run Order Flow Momentum Burst buying strategy"""
    all_trades = []
    
    # Strategy parameters
    START_TIME = 12 * 3600  # 12:00 PM
    END_TIME = 14 * 3600 + 30 * 60  # 2:30 PM
    MAX_HOLD_MIN = 10  # Theta killer time stop
    
    date_dirs = sorted(data_dir.glob("*"))
    
    print(f"Processing {underlying}...")
    
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
            # Load essential columns
            df = pl.read_parquet(files[0], columns=[
                'timestamp', 'strike', 'distance_from_spot', 'opt_type', 
                'price', 'bp0', 'sp0', 'bq0', 'sq0', 'expiry', 'spot_price'
            ]).filter(pl.col('timestamp').dt.year() > 1970)
            
            if df.is_empty():
                continue
            
            # Nearest expiry only
            nearest_expiry = df['expiry'].min()
            df = df.filter(pl.col('expiry') == nearest_expiry).sort(['opt_type', 'strike', 'timestamp'])
            
            if df.is_empty():
                continue
            
            # Convert to numpy - EXACTLY like selling strategies
            ts_ns = df['timestamp'].dt.epoch(time_unit='ns').to_numpy()
            dates = df['timestamp'].dt.date().cast(pl.Int32).to_numpy()
            hours = df['timestamp'].dt.hour().cast(pl.Int32).to_numpy()
            mins = df['timestamp'].dt.minute().cast(pl.Int32).to_numpy()
            secs = df['timestamp'].dt.second().cast(pl.Int32).to_numpy()
            times = hours * 3600 + mins * 60 + secs
            
            strikes = df['strike'].to_numpy()
            dists = df['distance_from_spot'].fill_null(0).to_numpy()
            opt_t = (df['opt_type'] == 'PE').cast(pl.Int8).to_numpy()  # 0=CE, 1=PE
            prices = df['price'].fill_null(0).to_numpy()
            bid_p = df['bp0'].fill_null(0).to_numpy()
            ask_p = df['sp0'].fill_null(0).to_numpy()
            bid_q = df['bq0'].fill_null(0).to_numpy()
            ask_q = df['sq0'].fill_null(0).to_numpy()
            spots = df['spot_price'].fill_null(0).to_numpy()
            
            # Run strategy
            EOD_EXIT = 15 * 3600 + 20 * 60  # 3:20 PM - exit before close
            results = strategy_buy_momentum_numba(
                ts_ns, dates, times, strikes, opt_t, prices,
                bid_p, ask_p, bid_q, ask_q, dists, spots,
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
            # Skip bad dates but don't stop processing
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
    print("ORDER FLOW MOMENTUM BURST - Option Buying Strategy")
    print("="*80)
    print("Strategy: Buy on EMA trend + Order book imbalance")
    print("Time Window: 12:00 PM - 2:30 PM")
    print("Exit: Trailing stop OR 10-minute time stop (theta killer)")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../strategy_results/buying/strategy_results_buying")
    results_dir.mkdir(exist_ok=True)
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        strat_start = time_mod.time()
        trades = run_momentum_buying_strategy(data_dir, underlying)
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
            
            # Save trades
            output_file = results_dir / f"{underlying}_momentum_burst_trades.csv"
            save_buying_trades(trades, output_file)
            print(f"  Saved: {output_file}")
        else:
            print(f"  No trades generated")
    
    total_time = time_mod.time() - start
    print(f"\n{'='*80}")
    print(f"✓ STRATEGY COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")
    
    # Summary
    if all_results:
        summary_file = results_dir / "momentum_burst_summary.csv"
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
