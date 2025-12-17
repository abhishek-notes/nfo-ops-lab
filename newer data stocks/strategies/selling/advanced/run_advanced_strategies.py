#!/usr/bin/env python3
"""
Advanced Scalping Strategies with Dynamic Exits
10 strategies with algorithmic advantages
"""

from pathlib import Path
from datetime import time, timedelta
from dataclasses import dataclass
from typing import List, Tuple, Optional
import polars as pl
import numpy as np
from numba import njit
import csv
import gc


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "data").is_dir() and (parent / "strategies").is_dir():
            return parent
    raise RuntimeError("Could not locate project root (expected 'data/' and 'strategies/' directories).")


@dataclass
class Trade:
    """Individual trade record"""
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
    total_premium_received: float
    total_premium_paid: float
    pnl: float
    hold_duration_minutes: int
    exit_reason: str  # NEW: 'profit_target', 'stop_loss', 'time_limit'


@dataclass
class StrategyResult:
    """Results from a single strategy backtest"""
    strategy_name: str
    underlying: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    avg_pnl_per_trade: float
    win_rate: float
    max_profit: float
    max_loss: float
    avg_win: float
    avg_loss: float
    sharpe_ratio: float
    max_drawdown: float
    avg_hold_minutes: float  # NEW


@njit
def find_atm_strike(strikes: np.ndarray, distances: np.ndarray, opt_types: np.ndarray, target_type: int) -> int:
    """Find ATM strike"""
    min_dist = 999999.0
    best_idx = -1
    for i in range(len(strikes)):
        if opt_types[i] != target_type:
            continue
        dist = abs(distances[i])
        if dist < min_dist:
            min_dist = dist
            best_idx = i
    return best_idx


@njit
def find_otm_strike(strikes: np.ndarray, distances: np.ndarray, opt_types: np.ndarray, target_type: int, otm_dist: float) -> int:
    """Find OTM strike"""
    min_diff = 999999.0
    best_idx = -1
    for i in range(len(strikes)):
        if opt_types[i] != target_type:
            continue
        if target_type == 0:  # CE
            if distances[i] <= 0:
                continue
            diff = abs(distances[i] - otm_dist)
        else:  # PE
            if distances[i] >= 0:
                continue
            diff = abs(distances[i] + otm_dist)
        if diff < min_diff:
            min_diff = diff
            best_idx = i
    return best_idx


def calculate_metrics(trades: List[Trade], strategy_name: str, underlying: str) -> StrategyResult:
    """Calculate performance metrics"""
    if not trades:
        return StrategyResult(
            strategy_name=strategy_name, underlying=underlying, total_trades=0,
            winning_trades=0, losing_trades=0, total_pnl=0.0, avg_pnl_per_trade=0.0,
            win_rate=0.0, max_profit=0.0, max_loss=0.0, avg_win=0.0, avg_loss=0.0,
            sharpe_ratio=0.0, max_drawdown=0.0, avg_hold_minutes=0.0
        )
    
    pnls = [t.pnl for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    hold_times = [t.hold_duration_minutes for t in trades]
    
    total_pnl = sum(pnls)
    total_trades = len(trades)
    winning_trades = len(wins)
    losing_trades = len(losses)
    
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
    avg_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
    max_profit = max(pnls) if pnls else 0.0
    max_loss = min(pnls) if pnls else 0.0
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0.0
    
    if len(pnls) > 1:
        std_pnl = np.std(pnls)
        sharpe = (avg_pnl / std_pnl * np.sqrt(250)) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0
    
    cumulative = np.cumsum(pnls)
    running_max = np.maximum.accumulate(cumulative)
    drawdown = running_max - cumulative
    max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0.0
    
    return StrategyResult(
        strategy_name=strategy_name, underlying=underlying, total_trades=total_trades,
        winning_trades=winning_trades, losing_trades=losing_trades, total_pnl=total_pnl,
        avg_pnl_per_trade=avg_pnl, win_rate=win_rate, max_profit=max_profit,
        max_loss=max_loss, avg_win=avg_win, avg_loss=avg_loss,
        sharpe_ratio=sharpe, max_drawdown=max_drawdown, avg_hold_minutes=avg_hold
    )


def save_trades(trades: List[Trade], output_file: Path):
    """Save trades to CSV"""
    if not trades:
        return
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'ce_strike', 'pe_strike', 'ce_entry_price', 'pe_entry_price',
            'ce_exit_price', 'pe_exit_price', 'total_premium_received',
            'total_premium_paid', 'pnl', 'hold_duration_minutes', 'exit_reason'
        ])
        for t in trades:
            writer.writerow([
                t.entry_date, t.entry_time, t.exit_date, t.exit_time,
                t.ce_strike, t.pe_strike, t.ce_entry_price, t.pe_entry_price,
                t.ce_exit_price, t.pe_exit_price, t.total_premium_received,
                t.total_premium_paid, t.pnl, t.hold_duration_minutes, t.exit_reason
            ])


def save_summary(results: List[StrategyResult], output_file: Path):
    """Save summary to CSV"""
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'strategy_name', 'underlying', 'total_trades', 'winning_trades',
            'losing_trades', 'win_rate_%', 'total_pnl', 'avg_pnl_per_trade',
            'max_profit', 'max_loss', 'avg_win', 'avg_loss', 'sharpe_ratio',
            'max_drawdown', 'avg_hold_minutes'
        ])
        for r in results:
            writer.writerow([
                r.strategy_name, r.underlying, r.total_trades, r.winning_trades,
                r.losing_trades, f"{r.win_rate:.2f}", f"{r.total_pnl:.2f}",
                f"{r.avg_pnl_per_trade:.2f}", f"{r.max_profit:.2f}", f"{r.max_loss:.2f}",
                f"{r.avg_win:.2f}", f"{r.avg_loss:.2f}", f"{r.sharpe_ratio:.2f}",
                f"{r.max_drawdown:.2f}", f"{r.avg_hold_minutes:.1f}"
            ])


# ========== STRATEGY 1: ATM Straddle 50% Quick Exit ==========
def strategy_atm_straddle_50pct_quick(
    df: pl.DataFrame,
    entry_times: List[time] = [time(9, 20), time(13, 0)],
    profit_target: float = 0.5,  # 50% of premium
    stop_loss_mult: float = 2.0,  # 2x premium
    max_hold_minutes: int = 90
) -> List[Trade]:
    """Quick scalp ATM straddle with 50% profit target"""
    trades = []
    df = df.filter(
        (pl.col('timestamp').dt.time() >= time(9, 15)) &
        (pl.col('timestamp').dt.time() <= time(15, 30))
    )
    
    expiries = df['expiry'].unique().sort()
    
    for expiry in expiries:
        expiry_df = df.filter(pl.col('expiry') == expiry)
        dates = expiry_df['timestamp'].dt.date().unique().sort()
        
        for date in dates:
            if date == expiry:
                continue
            
            day_df = expiry_df.filter(pl.col('timestamp').dt.date() == date)
            
            # Multiple entries per day
            for entry_time in entry_times:
                entry_df = day_df.filter(pl.col('timestamp').dt.time() >= entry_time).sort('timestamp')
                
                if entry_df.is_empty():
                    continue
                
                entry_timestamp = entry_df[0]['timestamp'][0]
                entry_snapshot = day_df.filter(pl.col('timestamp') == entry_timestamp)
                
                strikes_np = entry_snapshot['strike'].to_numpy()
                distances_np = entry_snapshot['distance_from_spot'].to_numpy()
                opt_types_np = (entry_snapshot['opt_type'] == 'PE').cast(pl.Int32).to_numpy()
                prices_np = entry_snapshot['price'].to_numpy()
                
                ce_idx = find_atm_strike(strikes_np, distances_np, opt_types_np, 0)
                pe_idx = find_atm_strike(strikes_np, distances_np, opt_types_np, 1)
                
                if ce_idx == -1 or pe_idx == -1:
                    continue
                
                ce_strike = strikes_np[ce_idx]
                pe_strike = strikes_np[pe_idx]
                ce_entry_price = prices_np[ce_idx]
                pe_entry_price = prices_np[pe_idx]
                premium_received = ce_entry_price + pe_entry_price
                
                profit_target_level = premium_received * profit_target
                stop_loss_level = premium_received * stop_loss_mult
                max_exit_time = entry_timestamp + timedelta(minutes=max_hold_minutes)
                
                # Track position minute by minute
                exit_timestamp = None
                ce_exit_price = 0.0
                pe_exit_price = 0.0
                exit_reason = 'time_limit'
                
                later_df = day_df.filter(pl.col('timestamp') > entry_timestamp).sort('timestamp')
                unique_timestamps = later_df['timestamp'].unique().sort()
                
                for ts in unique_timestamps:
                    if ts > max_exit_time:
                        # Max time reached
                        snapshot = later_df.filter(pl.col('timestamp') == max_exit_time).sort('timestamp')
                        if snapshot.is_empty():
                            snapshot = later_df.filter(pl.col('timestamp') <= max_exit_time).sort('timestamp')
                            if snapshot.is_empty():
                                break
                            ts = snapshot[-1]['timestamp'][0]
                        else:
                            ts = max_exit_time
                    
                    snapshot = later_df.filter(pl.col('timestamp') == ts)
                    ce_row = snapshot.filter((pl.col('strike') == ce_strike) & (pl.col('opt_type') == 'CE'))
                    pe_row = snapshot.filter((pl.col('strike') == pe_strike) & (pl.col('opt_type') == 'PE'))
                    
                    if ce_row.is_empty() or pe_row.is_empty():
                        continue
                    
                    ce_price = ce_row[0]['price'][0]
                    pe_price = pe_row[0]['price'][0]
                    current_cost = ce_price + pe_price
                    current_pnl = premium_received - current_cost
                    
                    # Check profit target
                    if current_pnl >= profit_target_level:
                        exit_timestamp = ts
                        ce_exit_price = ce_price
                        pe_exit_price = pe_price
                        exit_reason = 'profit_target'
                        break
                    
                    # Check stop loss
                    if current_pnl <= -stop_loss_level:
                        exit_timestamp = ts
                        ce_exit_price = ce_price
                        pe_exit_price = pe_price
                        exit_reason = 'stop_loss'
                        break
                    
                    # Check time limit
                    if ts >= max_exit_time:
                        exit_timestamp = ts
                        ce_exit_price = ce_price
                        pe_exit_price = pe_price
                        exit_reason = 'time_limit'
                        break
                
                if exit_timestamp is None:
                    continue
                
                premium_paid = ce_exit_price + pe_exit_price
                pnl = premium_received - premium_paid
                hold_duration = int((exit_timestamp - entry_timestamp).total_seconds() / 60)
                
                trades.append(Trade(
                    entry_date=str(entry_timestamp.date()),
                    entry_time=str(entry_timestamp.time()),
                    exit_date=str(exit_timestamp.date()),
                    exit_time=str(exit_timestamp.time()),
                    ce_strike=ce_strike,
                    pe_strike=pe_strike,
                    ce_entry_price=ce_entry_price,
                    pe_entry_price=pe_entry_price,
                    ce_exit_price=ce_exit_price,
                    pe_exit_price=pe_exit_price,
                    total_premium_received=premium_received,
                    total_premium_paid=premium_paid,
                    pnl=pnl,
                    hold_duration_minutes=hold_duration,
                    exit_reason=exit_reason
                ))
    
    return trades


# ========== STRATEGY 2: OTM Strangle 30% Quick ==========
def strategy_otm_strangle_30pct_quick(
    df: pl.DataFrame,
    otm_pct: float = 0.01,
    entry_times: List[time] = [time(9, 20), time(10, 0), time(11, 0), time(13, 0), time(14, 0)],
    profit_target: float = 0.3,
    stop_loss_mult: float = 1.5,
    max_hold_minutes: int = 30
) -> List[Trade]:
    """Ultra-quick OTM strangle scalp"""
    trades = []
    df = df.filter(
        (pl.col('timestamp').dt.time() >= time(9, 15)) &
        (pl.col('timestamp').dt.time() <= time(15, 30))
    )
    
    expiries = df['expiry'].unique().sort()
    
    for expiry in expiries:
        expiry_df = df.filter(pl.col('expiry') == expiry)
        dates = expiry_df['timestamp'].dt.date().unique().sort()
        
        for date in dates:
            if date == expiry:
                continue
            
            day_df = expiry_df.filter(pl.col('timestamp').dt.date() == date)
            
            for entry_time in entry_times:
                entry_df = day_df.filter(pl.col('timestamp').dt.time() >= entry_time).sort('timestamp')
                
                if entry_df.is_empty():
                    continue
                
                entry_timestamp = entry_df[0]['timestamp'][0]
                entry_snapshot = day_df.filter(pl.col('timestamp') == entry_timestamp)
                
                spot_price = entry_snapshot['spot_price'][0]
                otm_dist = spot_price * otm_pct
                
                strikes_np = entry_snapshot['strike'].to_numpy()
                distances_np = entry_snapshot['distance_from_spot'].to_numpy()
                opt_types_np = (entry_snapshot['opt_type'] == 'PE').cast(pl.Int32).to_numpy()
                prices_np = entry_snapshot['price'].to_numpy()
                
                ce_idx = find_otm_strike(strikes_np, distances_np, opt_types_np, 0, otm_dist)
                pe_idx = find_otm_strike(strikes_np, distances_np, opt_types_np, 1, otm_dist)
                
                if ce_idx == -1 or pe_idx == -1:
                    continue
                
                ce_strike = strikes_np[ce_idx]
                pe_strike = strikes_np[pe_idx]
                ce_entry_price = prices_np[ce_idx]
                pe_entry_price = prices_np[pe_idx]
                premium_received = ce_entry_price + pe_entry_price
                
                profit_target_level = premium_received * profit_target
                stop_loss_level = premium_received * stop_loss_mult
                max_exit_time = entry_timestamp + timedelta(minutes=max_hold_minutes)
                
                exit_timestamp = None
                ce_exit_price = 0.0
                pe_exit_price = 0.0
                exit_reason = 'time_limit'
                
                later_df = day_df.filter(pl.col('timestamp') > entry_timestamp).sort('timestamp')
                unique_timestamps = later_df['timestamp'].unique().sort()
                
                for ts in unique_timestamps:
                    if ts > max_exit_time:
                        snapshot = later_df.filter(pl.col('timestamp') <= max_exit_time).sort('timestamp')
                        if snapshot.is_empty():
                            break
                        ts = snapshot[-1]['timestamp'][0]
                    
                    snapshot = later_df.filter(pl.col('timestamp') == ts)
                    ce_row = snapshot.filter((pl.col('strike') == ce_strike) & (pl.col('opt_type') == 'CE'))
                    pe_row = snapshot.filter((pl.col('strike') == pe_strike) & (pl.col('opt_type') == 'PE'))
                   
                    if ce_row.is_empty() or pe_row.is_empty():
                        continue
                    
                    ce_price = ce_row[0]['price'][0]
                    pe_price = pe_row[0]['price'][0]
                    current_cost = ce_price + pe_price
                    current_pnl = premium_received - current_cost
                    
                    if current_pnl >= profit_target_level:
                        exit_timestamp = ts
                        ce_exit_price = ce_price
                        pe_exit_price = pe_price
                        exit_reason = 'profit_target'
                        break
                    
                    if current_pnl <= -stop_loss_level:
                        exit_timestamp = ts
                        ce_exit_price = ce_price
                        pe_exit_price = pe_price
                        exit_reason = 'stop_loss'
                        break
                    
                    if ts >= max_exit_time:
                        exit_timestamp = ts
                        ce_exit_price = ce_price
                        pe_exit_price = pe_price
                        exit_reason = 'time_limit'
                        break
                
                if exit_timestamp is None:
                    continue
                
                premium_paid = ce_exit_price + pe_exit_price
                pnl = premium_received - premium_paid
                hold_duration = int((exit_timestamp - entry_timestamp).total_seconds() / 60)
                
                trades.append(Trade(
                    entry_date=str(entry_timestamp.date()),
                    entry_time=str(entry_timestamp.time()),
                    exit_date=str(exit_timestamp.date()),
                    exit_time=str(exit_timestamp.time()),
                    ce_strike=ce_strike,
                    pe_strike=pe_strike,
                    ce_entry_price=ce_entry_price,
                    pe_entry_price=pe_entry_price,
                    ce_exit_price=ce_exit_price,
                    pe_exit_price=pe_exit_price,
                    total_premium_received=premium_received,
                    total_premium_paid=premium_paid,
                    pnl=pnl,
                    hold_duration_minutes=hold_duration,
                    exit_reason=exit_reason
                ))
    
    return trades


# Define strategies with dynamic exits
ADVANCED_STRATEGIES = [
    {
        'name': 'ADV_1_ATM_Straddle_50pct_Quick_90min',
        'func': strategy_atm_straddle_50pct_quick,
        'params': {'profit_target': 0.5, 'stop_loss_mult': 2.0, 'max_hold_minutes': 90}
    },
    {
        'name': 'ADV_2_OTM_Strangle_30pct_Quick_30min',
        'func': strategy_otm_strangle_30pct_quick,
        'params': {'otm_pct': 0.01, 'profit_target': 0.3, 'stop_loss_mult': 1.5, 'max_hold_minutes': 30}
    },
    {
        'name': 'ADV_3_ATM_Straddle_25pct_Ultra_Quick_15min',
        'func': strategy_atm_straddle_50pct_quick,
        'params': {'profit_target': 0.25, 'stop_loss_mult': 1.5, 'max_hold_minutes': 15, 'entry_times': [time(9, 20), time(10, 30), time(12, 0), time(13, 30)]}
    },
    {
        'name': 'ADV_4_OTM_Strangle_40pct_Target_60min',
        'func': strategy_otm_strangle_30pct_quick,
        'params': {'otm_pct': 0.015, 'profit_target': 0.4, 'stop_loss_mult': 2.0, 'max_hold_minutes': 60, 'entry_times': [time(9, 20), time(11, 0), time(13, 0)]}
    },
    {
        'name': 'ADV_5_Tight_OTM_0_5pct_20pct_Target_45min',
        'func': strategy_otm_strangle_30pct_quick,
        'params': {'otm_pct': 0.005, 'profit_target': 0.2, 'stop_loss_mult': 1.8, 'max_hold_minutes': 45, 'entry_times': [time(9, 20), time(11, 0), time(13, 0), time(14, 0)]}
    },
    {
        'name': 'ADV_6_Wide_OTM_2pct_35pct_Target_120min',
        'func': strategy_otm_strangle_30pct_quick,
        'params': {'otm_pct': 0.02, 'profit_target': 0.35, 'stop_loss_mult': 2.5, 'max_hold_minutes': 120, 'entry_times': [time(9, 20), time(12, 0)]}
    },
    {
        'name': 'ADV_7_High_Frequency_ATM_Multi_Entry',
        'func': strategy_atm_straddle_50pct_quick,
        'params': {'profit_target': 0.35, 'stop_loss_mult': 1.8, 'max_hold_minutes': 60, 'entry_times': [time(9, 20), time(10, 0), time(10, 40), time(11, 20), time(12, 0), time(12, 40), time(13, 20), time(14, 0)]}
    },
    {
        'name': 'ADV_8_Conservative_ATM_60pct_Target',
        'func': strategy_atm_straddle_50pct_quick,
        'params': {'profit_target': 0.6, 'stop_loss_mult': 2.5, 'max_hold_minutes': 120, 'entry_times': [time(9, 20), time(13, 0)]}
    },
    {
        'name': 'ADV_9_Aggressive_OTM_15pct_Quick',
        'func': strategy_otm_strangle_30pct_quick,
        'params': {'otm_pct': 0.01, 'profit_target': 0.15, 'stop_loss_mult': 1.2, 'max_hold_minutes': 20, 'entry_times': [time(9, 30), time(10, 30), time(11, 30), time(12, 30), time(13, 30), time(14, 30)]}
    },
    {
        'name': 'ADV_10_Afternoon_Quick_Strangle',
        'func': strategy_otm_strangle_30pct_quick,
        'params': {'otm_pct': 0.01, 'profit_target': 0.25, 'stop_loss_mult': 1.5, 'max_hold_minutes': 45, 'entry_times': [time(13, 0), time(13, 30), time(14, 0), time(14, 30)]}
    },
]


def main():
    print("=" * 80)
    print("ADVANCED SCALPING STRATEGIES - DYNAMIC EXITS")
    print("=" * 80)
    
    root = _project_root()
    data_dir = root / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
    results_dir = root / "strategies" / "strategy_results" / "selling" / "strategy_results_advanced"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    all_results = []
    
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}\nPROCESSING {underlying}\n{'='*80}")
        
        # Find all date files
        date_files = []
        for date_dir in sorted(data_dir.glob("*")):
            if not date_dir.is_dir() or "1970" in date_dir.name:
                continue
            underlying_dir = date_dir / underlying
            if underlying_dir.exists():
                files = sorted(underlying_dir.glob("*.parquet"))
                if files:
                    date_files.append(files)
        
        print(f"Found {len(date_files)} date files")
        
        for strategy in ADVANCED_STRATEGIES:
            strategy_name = f"{underlying}_{strategy['name']}"
            print(f"\n--- {strategy_name} ---")
            
            all_trades = []
            
            for idx, file_paths in enumerate(date_files):
                if idx % 20 == 0:
                    print(f"  Progress: {idx}/{len(date_files)} ({idx/len(date_files)*100:.0f}%)")
                
                df = pl.read_parquet(file_paths)
                df = df.filter(pl.col('timestamp').dt.year() > 1970)

                # Always backtest nearest expiry only (avoid mixing multiple expiries in one file).
                if not df.is_empty():
                    nearest_expiry = df["expiry"].min()
                    df = df.filter(pl.col("expiry") == nearest_expiry)
                
                if not df.is_empty():
                    trades = strategy['func'](df, **strategy['params'])
                    all_trades.extend(trades)
                
                del df
                gc.collect()
            
            result = calculate_metrics(all_trades, strategy_name, underlying)
            all_results.append(result)
            
            print(f"  Trades: {result.total_trades}, Win Rate: {result.win_rate:.1f}%, P&L: {result.total_pnl:.2f}")
            print(f"  Avg Hold: {result.avg_hold_minutes:.1f} min, Sharpe: {result.sharpe_ratio:.2f}")
            
            trades_file = results_dir / f"{strategy_name}_trades.csv"
            save_trades(all_trades, trades_file)
            
            del all_trades
            gc.collect()
    
    summary_file = results_dir / "advanced_strategies_summary.csv"
    save_summary(all_results, summary_file)
    
    print("\n" + "=" * 80)
    print(f"✓ COMPLETE! Results in: {results_dir}")
    print("=" * 80)


if __name__ == "__main__":
    main()
