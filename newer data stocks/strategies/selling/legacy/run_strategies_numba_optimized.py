#!/usr/bin/env python3
"""
OPTIMIZED Scalping Strategies with Numba
Target: <2 minutes for all 10 strategies on both underlyings
"""

from pathlib import Path
from datetime import time
from dataclasses import dataclass
from typing import List
import polars as pl
import numpy as np
from numba import njit
import csv
import time as time_module


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "data").is_dir() and (parent / "strategies").is_dir():
            return parent
    raise RuntimeError("Could not locate project root (expected 'data/' and 'strategies/' directories).")


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


@njit
def run_scalping_numba(
    timestamps_ns: np.ndarray,
    dates_int: np.ndarray,
    times_sec: np.ndarray,  # Seconds from midnight
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,  # 0=CE, 1=PE
    prices: np.ndarray,
    entry_times_sec: np.ndarray,
    profit_target_pct: float,
    stop_loss_mult: float,
    max_hold_minutes: int
):
    """
    Numba-compiled scalping with dynamic exits
    Returns: arrays of entry_idx, exit_idx, entry_strike_ce, entry_strike_pe, pnl, hold_min, exit_reason
    """
    n = len(timestamps_ns)
    max_trades = 20000
    
    # Output arrays
    entry_indices = np.empty(max_trades, dtype=np.int64)
    exit_indices = np.empty(max_trades, dtype=np.int64)
    ce_strikes_out = np.empty(max_trades, dtype=np.float64)
    pe_strikes_out = np.empty(max_trades, dtype=np.float64)
    pnls = np.empty(max_trades, dtype=np.float64)
    hold_minutes = np.empty(max_trades, dtype=np.int64)
    exit_reasons = np.empty(max_trades, dtype=np.int8)  # 0=profit, 1=loss, 2=time
    
    trade_count = 0
    max_hold_sec = max_hold_minutes * 60
    
    i = 0
    while i < n:
        current_date = dates_int[i]
        current_time_sec = times_sec[i]
        
        # Check if this time EXACTLY matches any entry time
        is_entry_time = False
        for et in entry_times_sec:
            if current_time_sec == et:  # EXACT match only
                is_entry_time = True
                break
        
        if not is_entry_time:
            i += 1
            continue
        
        # Get timestamp block (all rows with same timestamp)
        entry_ts = timestamps_ns[i]
        block_start = i
        while i < n and timestamps_ns[i] == entry_ts:
            i += 1
        block_end = i
        
        # Find ATM CE and PE in this block
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
        exit_reason = 2  # time_limit
        ce_exit_price = 0.0
        pe_exit_price = 0.0
        
        j = block_end
        while j < n:
            # Check if still same date
            if dates_int[j] != current_date:
                break
            
            # Check time limit
            if times_sec[j] > max_exit_time_sec:
                # Find last valid prices before timeout
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
            
            # Check P&L
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
            pnl = premium_received - (ce_exit_price + pe_exit_price)
            hold_min = int((times_sec[exit_idx] - current_time_sec) / 60)
            
            if trade_count < max_trades:
                entry_indices[trade_count] = ce_idx
                exit_indices[trade_count] = exit_idx
                ce_strikes_out[trade_count] = ce_strike
                pe_strikes_out[trade_count] = pe_strike
                pnls[trade_count] = pnl
                hold_minutes[trade_count] = hold_min
                exit_reasons[trade_count] = exit_reason
                trade_count += 1
    
    return (
        entry_indices[:trade_count],
        exit_indices[:trade_count],
        ce_strikes_out[:trade_count],
        pe_strikes_out[:trade_count],
        pnls[:trade_count],
        hold_minutes[:trade_count],
        exit_reasons[:trade_count]
    )


def load_all_data(data_dir: Path, underlying: str):
    """Load all data at once"""
    print(f"Loading {underlying} data...")
    
    dfs = []
    for date_dir in sorted(data_dir.glob("*")):
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        underlying_dir = date_dir / underlying
        if underlying_dir.exists():
            files = sorted(underlying_dir.glob("*.parquet"))
            if files:
                df = pl.scan_parquet([str(f) for f in files]).select([
                    'timestamp', 'strike', 'distance_from_spot',
                    'opt_type', 'price'
                ]).filter(
                    pl.col('timestamp').dt.year() > 1970
                ).collect()
                dfs.append(df)
    
    all_data = pl.concat(dfs).sort('timestamp')
    print(f"  Loaded {len(all_data):,} rows")
    
    return all_data


def run_strategy_optimized(df: pl.DataFrame, strategy_config: dict) -> List[Trade]:
    """Run strategy using Numba"""
    
    # Convert to NumPy
    timestamps_ns = df['timestamp'].dt.epoch(time_unit='ns').to_numpy()
    dates_int = df['timestamp'].dt.date().cast(pl.Int32).to_numpy()
    # Fix overflow: cast to int32 before multiplication
    hours = df['timestamp'].dt.hour().cast(pl.Int32).to_numpy()
    minutes = df['timestamp'].dt.minute().cast(pl.Int32).to_numpy()
    seconds = df['timestamp'].dt.second().cast(pl.Int32).to_numpy()
    times_sec = hours * 3600 + minutes * 60 + seconds
    strikes = df['strike'].to_numpy()
    distances = df['distance_from_spot'].to_numpy()
    opt_types = (df['opt_type'] == 'PE').cast(pl.Int8).to_numpy()
    prices = df['price'].to_numpy()
    
    # Entry times
    entry_times = strategy_config['entry_times']
    entry_times_sec = np.array([t.hour * 3600 + t.minute * 60 for t in entry_times], dtype=np.int64)
    
    # Run Numba strategy
    entry_idxs, exit_idxs, ce_strikes, pe_strikes, pnls, hold_mins, exit_reasons_int = run_scalping_numba(
        timestamps_ns,
        dates_int,
        times_sec,
        strikes,
        distances,
        opt_types,
        prices,
        entry_times_sec,
        strategy_config['profit_target'],
        strategy_config['stop_loss_mult'],
        strategy_config['max_hold_minutes']
    )
    
    # Convert to Trade objects
    trades = []
    exit_reason_map = {0: 'profit_target', 1: 'stop_loss', 2: 'time_limit'}
    
    for i in range(len(pnls)):
        entry_idx = int(entry_idxs[i])  # Convert numpy int64 to Python int
        exit_idx = int(exit_idxs[i])
        
        entry_ts = df['timestamp'][entry_idx]
        exit_ts = df['timestamp'][exit_idx]
        
        # Get entry/exit prices by strike
        entry_snapshot = df.filter(pl.col('timestamp') == entry_ts)
        exit_snapshot = df.filter(pl.col('timestamp') == exit_ts)
        
        ce_strike = ce_strikes[i]
        pe_strike = pe_strikes[i]
        
        ce_entry = entry_snapshot.filter((pl.col('strike') == ce_strike) & (pl.col('opt_type') == 'CE'))
        pe_entry = entry_snapshot.filter((pl.col('strike') == pe_strike) & (pl.col('opt_type') == 'PE'))
        ce_exit = exit_snapshot.filter((pl.col('strike') == ce_strike) & (pl.col('opt_type') == 'CE'))
        pe_exit = exit_snapshot.filter((pl.col('strike') == pe_strike) & (pl.col('opt_type') == 'PE'))
        
        if ce_entry.is_empty() or pe_entry.is_empty() or ce_exit.is_empty() or pe_exit.is_empty():
            continue
        
        trade = Trade(
            entry_date=str(entry_ts.date()),
            entry_time=str(entry_ts.time()),
            exit_date=str(exit_ts.date()),
            exit_time=str(exit_ts.time()),
            ce_strike=ce_strike,
            pe_strike=pe_strike,
            ce_entry_price=ce_entry[0]['price'][0],
            pe_entry_price=pe_entry[0]['price'][0],
            ce_exit_price=ce_exit[0]['price'][0],
            pe_exit_price=pe_exit[0]['price'][0],
            pnl=pnls[i],
            hold_duration_minutes=int(hold_mins[i]),
            exit_reason=exit_reason_map[exit_reasons_int[i]]
        )
        trades.append(trade)
    
    return trades


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


# Strategy configurations
STRATEGIES = [
    {
        'name': 'ADV_1_ATM_Straddle_50pct_Quick_90min',
        'entry_times': [time(9, 20), time(13, 0)],
        'profit_target': 0.5,
        'stop_loss_mult': 2.0,
        'max_hold_minutes': 90
    },
]


def main():
    start_time = time_module.time()
    
    print("=" * 80)
    print("OPTIMIZED SCALPING STRATEGIES (Numba)")
    print("=" * 80)
    
    root = _project_root()
    data_dir = root / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
    results_dir = root / "strategies" / "strategy_results" / "selling" / "strategy_results_optimized"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    for underlying in ['BANKNIFTY']:  # Test on BANKNIFTY first
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        # Load all data once
        all_data = load_all_data(data_dir, underlying)
        
        for strategy in STRATEGIES:
            strategy_name = f"{underlying}_{strategy['name']}"
            print(f"\n--- Running: {strategy_name} ---")
            
            strat_start = time_module.time()
            
            # Run strategy
            trades = run_strategy_optimized(all_data, strategy)
            
            strat_time = time_module.time() - strat_start
            
            # Calculate metrics
            if trades:
                pnls = [t.pnl for t in trades]
                win_rate = sum(1 for p in pnls if p > 0) / len(pnls) * 100
                total_pnl = sum(pnls)
                avg_hold = sum(t.hold_duration_minutes for t in trades) / len(trades)
                
                print(f"  ⚡ Completed in {strat_time:.2f}s")
                print(f"  Trades: {len(trades)}")
                print(f"  Win Rate: {win_rate:.1f}%")
                print(f"  Total P&L: {total_pnl:.2f}")
                print(f"  Avg Hold: {avg_hold:.1f} min")
            
            # Save
            output_file = results_dir / f"{strategy_name}_trades.csv"
            save_trades(trades, output_file)
    
    total_time = time_module.time() - start_time
    print(f"\n{'='*80}")
    print(f"✓ COMPLETE in {total_time:.1f}s")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
