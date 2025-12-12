#!/usr/bin/env python3
"""
CORRECTED: Numba-optimized strategy with nearest expiry and fixed P&L
"""

from pathlib import Path
from datetime import time, date as date_type
from dataclasses import dataclass
from typing import List
import polars as pl
import numpy as np
from numba import njit
import csv
import time as time_module


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
    pnl: float  # Use Numba-calculated P&L directly!
    hold_duration_minutes: int
    exit_reason: str


@njit
def run_scalping_numba(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    prices: np.ndarray,
    entry_times_sec: np.ndarray,
    profit_target_pct: float,
    stop_loss_mult: float,
    max_hold_minutes: int
):
    """Numba-compiled scalping - returns indices and calculated P&L"""
    n = len(timestamps_ns)
    max_trades = 20000
    
    # Output arrays
    entry_indices = np.empty(max_trades, dtype=np.int64)
    exit_indices = np.empty(max_trades, dtype=np.int64)
    pnls = np.empty(max_trades, dtype=np.float64)  # Calculated here!
    hold_minutes = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)
    
    # Also store entry/exit prices for verification
    ce_entry_prices = np.empty(max_trades, dtype=np.float64)
    pe_entry_prices = np.empty(max_trades, dtype=np.float64)
    ce_exit_prices = np.empty(max_trades, dtype=np.float64)
    pe_exit_prices = np.empty(max_trades, dtype=np.float64)
    
    trade_count = 0
    max_hold_sec = max_hold_minutes * 60
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time_sec = times_sec[i]
        
        # Check if EXACTLY matches entry time
        is_entry_time = False
        for et in entry_times_sec:
            if current_time_sec == et:
                is_entry_time = True
                break
        
        if not is_entry_time:
            i += 1
            continue
        
        # Get timestamp block
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        # Find ATM CE and PE  
        ce_idx = -1
        pe_idx = -1
        min_ce_dist = 999999.0
        min_pe_dist = 999999.0
        
        for j in range(block_start, block_end):
            abs_dist = abs(distances[j])
            if opt_types[j] == 0:  # CE
                if abs_dist < min_ce_dist:
                    min_ce_dist = abs_dist
                    ce_idx = j
            else:  # PE
                if abs_dist < min_pe_dist:
                    min_pe_dist = abs_dist
                    pe_idx = j
        
        if ce_idx == -1 or pe_idx == -1:
            continue
        
        # Entry executed
        ce_strike = strikes[ce_idx]
        pe_strike = strikes[pe_idx]
        ce_entry_price = prices[ce_idx]
        pe_entry_price = prices[pe_idx]
        premium_received = ce_entry_price + pe_entry_price
        
        profit_target = premium_received * profit_target_pct
        stop_loss = premium_received * stop_loss_mult
        max_exit_time_sec = current_time_sec + max_hold_sec
        
        # Scan forward for exit
        exit_idx = -1
        exit_reason = 2
        ce_exit_price = 0.0
        pe_exit_price = 0.0
        
        j = block_end
        while j < n:
            if dates_int[j] != current_date:
                break
            
            if times_sec[j] > max_exit_time_sec:
                # Find last prices before timeout
                for k in range(j-1, block_start, -1):
                    if dates_int[k] != current_date:
                        break
                    if strikes[k] == ce_strike and opt_types[k] == 0:
                        ce_exit_price = prices[k]
                    if strikes[k] == pe_strike and opt_types[k] == 1:
                        pe_exit_price = prices[k]
                    if ce_exit_price > 0 and pe_exit_price > 0:
                        exit_idx = k
                        exit_reason = 2
                        break
                break
            
            # Get prices at this timestamp
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
            
            # Calculate P&L
            current_cost = curr_ce + curr_pe
            current_pnl = premium_received - current_cost
            
            # Check profit target
            if current_pnl >= profit_target:
                ce_exit_price = curr_ce
                pe_exit_price = curr_pe
                exit_idx = ts_start
                exit_reason = 0
                break
            
            # Check stop loss
            if current_pnl <= -stop_loss:
                ce_exit_price = curr_ce
                pe_exit_price = curr_pe
                exit_idx = ts_start
                exit_reason = 1
                break
        
        if exit_idx != -1 and ce_exit_price > 0 and pe_exit_price > 0:
            # Calculate final P&L
            pnl = premium_received - (ce_exit_price + pe_exit_price)
            hold_min = int((times_sec[exit_idx] - current_time_sec) / 60)
            
            if trade_count < max_trades:
                entry_indices[trade_count] = ce_idx
                exit_indices[trade_count] = exit_idx
                pnls[trade_count] = pnl  # Store calculated P&L
                hold_minutes[trade_count] = hold_min
                exit_reasons[trade_count] = exit_reason
                ce_entry_prices[trade_count] = ce_entry_price
                pe_entry_prices[trade_count] = pe_entry_price
                ce_exit_prices[trade_count] = ce_exit_price
                pe_exit_prices[trade_count] = pe_exit_price
                trade_count += 1
    
    return (
        entry_indices[:trade_count],
        exit_indices[:trade_count],
        pnls[:trade_count],
        hold_minutes[:trade_count],
        exit_reasons[:trade_count],
        ce_entry_prices[:trade_count],
        pe_entry_prices[:trade_count],
        ce_exit_prices[:trade_count],
        pe_exit_prices[:trade_count]
    )


def load_and_filter_nearest_expiry(data_dir: Path, underlying: str):
    """Load data and filter to nearest expiry for each timestamp"""
    print(f"Loading {underlying} data...")
    
    dfs = []
    for date_dir in sorted(data_dir.glob("*")):
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        underlying_dir = date_dir / underlying
        if underlying_dir.exists():
            files = list(underlying_dir.glob("*.parquet"))
            if files:
                df = pl.scan_parquet(files[0]).select([
                    'timestamp', 'strike', 'distance_from_spot',
                    'opt_type', 'price', 'expiry'
                ]).filter(
                    pl.col('timestamp').dt.year() > 1970
                ).collect()
                dfs.append(df)
    
    all_data = pl.concat(dfs).sort(['timestamp', 'expiry'])
    print(f"  Loaded {len(all_data):,} rows")
    
    # Filter to nearest expiry for each timestamp
    print("  Filtering to nearest expiry per timestamp...")
    filtered = all_data.group_by(['timestamp', 'strike', 'opt_type']).agg([
        pl.col('expiry').min().alias('expiry'),  # Nearest expiry
        pl.col('price').first(),
        pl.col('distance_from_spot').first()
    ])
    
    print(f"  After nearest-expiry filter: {len(filtered):,} rows")
    
    return filtered.sort('timestamp')


def main():
    start_time = time_module.time()
    
    print("=" * 80)
    print("CORRECTED NUMBA STRATEGY (Nearest Expiry + Fixed P&L)")
    print("=" * 80)
    
    data_dir = Path("../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../results/strategy_results_numba_corrected")
    results_dir.mkdir(exist_ok=True)
    
    for underlying in ['BANKNIFTY']:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        # Load with nearest expiry filter
        all_data = load_and_filter_nearest_expiry(data_dir, underlying)
        
        # Convert to NumPy
        timestamps_ns = all_data['timestamp'].dt.epoch(time_unit='ns').to_numpy()
        dates_int = all_data['timestamp'].dt.date().cast(pl.Int32).to_numpy()
        hours = all_data['timestamp'].dt.hour().cast(pl.Int32).to_numpy()
        minutes = all_data['timestamp'].dt.minute().cast(pl.Int32).to_numpy()
        seconds = all_data['timestamp'].dt.second().cast(pl.Int32).to_numpy()
        times_sec = hours * 3600 + minutes * 60 + seconds
        strikes = all_data['strike'].to_numpy()
        distances = all_data['distance_from_spot'].to_numpy()
        opt_types = (all_data['opt_type'] == 'PE').cast(pl.Int8).to_numpy()
        prices = all_data['price'].to_numpy()
        
        # Entry times
        entry_times_sec = np.array([9*3600 + 20*60, 13*3600], dtype=np.int64)
        
        print("\n--- Running: ATM Straddle 50% Quick ---")
        strat_start = time_module.time()
        
        # Run Numba strategy
        (entry_idxs, exit_idxs, pnls, hold_mins, exit_reasons_int,
         ce_entry_prices, pe_entry_prices, ce_exit_prices, pe_exit_prices) = run_scalping_numba(
            timestamps_ns, dates_int, times_sec, strikes, distances,
            opt_types, prices, entry_times_sec,
            profit_target_pct=0.5, stop_loss_mult=2.0, max_hold_minutes=90
        )
        
        strat_time = time_module.time() - strat_start
        
        # Create Trade objects using Numba-calculated values
        trades = []
        exit_reason_map = {0: 'profit_target', 1: 'stop_loss', 2: 'time_limit'}
        
        for i in range(len(pnls)):
            entry_idx = int(entry_idxs[i])
            exit_idx = int(exit_idxs[i])
            
            entry_ts = all_data['timestamp'][entry_idx]
            exit_ts = all_data['timestamp'][exit_idx]
            
            # Get strikes from the data
            entry_snapshot = all_data.filter(pl.col('timestamp') == entry_ts)
            ce_strike = entry_snapshot.filter(pl.col('opt_type') == 'CE')[0]['strike'][0]
            pe_strike = entry_snapshot.filter(pl.col('opt_type') == 'PE')[0]['strike'][0]
            
            trade = Trade(
                entry_date=str(entry_ts.date()),
                entry_time=str(entry_ts.time()),
                exit_date=str(exit_ts.date()),
                exit_time=str(exit_ts.time()),
                ce_strike=ce_strike,
                pe_strike=pe_strike,
                ce_entry_price=ce_entry_prices[i],
                pe_entry_price=pe_entry_prices[i],
                ce_exit_price=ce_exit_prices[i],
                pe_exit_price=pe_exit_prices[i],
                pnl=pnls[i],  # Use Numba-calculated P&L!
                hold_duration_minutes=int(hold_mins[i]),
                exit_reason=exit_reason_map[exit_reasons_int[i]]
            )
            trades.append(trade)
        
        # Calculate metrics
        if trades:
            trade_pnls = [t.pnl for t in trades]
            win_rate = sum(1 for p in trade_pnls if p > 0) / len(trades) * 100
            total_pnl = sum(trade_pnls)
            avg_hold = sum(t.hold_duration_minutes for t in trades) / len(trades)
            
            print(f"  ⚡ Completed in {strat_time:.2f}s")
            print(f"  Trades: {len(trades)}")
            print(f"  Win Rate: {win_rate:.1f}%")
            print(f"  Total P&L: {total_pnl:.2f}")
            print(f"  Avg P&L/trade: {total_pnl/len(trades):.2f}")
            print(f"  Avg Hold: {avg_hold:.1f} min")
        
        # Save
        output_file = results_dir / f"{underlying}_ATM_Straddle_50pct_Quick_90min_trades.csv"
        with open(output_file, 'w', newline='') as f:
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
    
    total_time = time_module.time() - start_time
    print(f"\n{'='*80}")
    print(f"✓ COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
