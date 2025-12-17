#!/usr/bin/env python3
"""
Consolidated Strategy Runner - All 12 Strategies
Single file with everything included
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


@njit
def find_atm_strike(strikes: np.ndarray, distances: np.ndarray, opt_types: np.ndarray, target_type: int) -> int:
    """Find ATM strike index for CE (type=0) or PE (type=1)"""
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
    """Find OTM strike at specific distance from ATM"""
    min_diff = 999999.0
    best_idx = -1
    
    for i in range(len(strikes)):
        if opt_types[i] != target_type:
            continue
        
        # For CE: want positive distance (OTM)
        # For PE: want negative distance (OTM)
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


def calculate_strategy_metrics(trades: List[Trade], strategy_name: str, underlying: str) -> StrategyResult:
    """Calculate performance metrics from trades"""
    if not trades:
        return StrategyResult(
            strategy_name=strategy_name,
            underlying=underlying,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            total_pnl=0.0,
            avg_pnl_per_trade=0.0,
            win_rate=0.0,
            max_profit=0.0,
            max_loss=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0
        )
    
    pnls = [t.pnl for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    
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
    
    # Sharpe ratio
    if len(pnls) > 1:
        std_pnl = np.std(pnls)
        sharpe = (avg_pnl / std_pnl * np.sqrt(250)) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0
    
    # Max drawdown
    cumulative = np.cumsum(pnls)
    running_max = np.maximum.accumulate(cumulative)
    drawdown = running_max - cumulative
    max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0.0
    
    return StrategyResult(
        strategy_name=strategy_name,
        underlying=underlying,
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        total_pnl=total_pnl,
        avg_pnl_per_trade=avg_pnl,
        win_rate=win_rate,
        max_profit=max_profit,
        max_loss=max_loss,
        avg_win=avg_win,
        avg_loss=avg_loss,
        sharpe_ratio=sharpe,
        max_drawdown=max_drawdown
    )


def save_trades_to_csv(trades: List[Trade], output_file: Path):
    """Save trade list to CSV"""
    if not trades:
        return
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'ce_strike', 'pe_strike', 'ce_entry_price', 'pe_entry_price',
            'ce_exit_price', 'pe_exit_price', 'total_premium_received',
            'total_premium_paid', 'pnl', 'hold_duration_minutes'
        ])
        
        for trade in trades:
            writer.writerow([
                trade.entry_date, trade.entry_time, trade.exit_date, trade.exit_time,
                trade.ce_strike, trade.pe_strike, trade.ce_entry_price, trade.pe_entry_price,
                trade.ce_exit_price, trade.pe_exit_price, trade.total_premium_received,
                trade.total_premium_paid, trade.pnl, trade.hold_duration_minutes
            ])


def save_summary_to_csv(results: List[StrategyResult], output_file: Path):
    """Save strategy summary to CSV"""
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'strategy_name', 'underlying', 'total_trades', 'winning_trades',
            'losing_trades', 'win_rate_%', 'total_pnl', 'avg_pnl_per_trade',
            'max_profit', 'max_loss', 'avg_win', 'avg_loss', 'sharpe_ratio', 'max_drawdown'
        ])
        
        for r in results:
            writer.writerow([
                r.strategy_name, r.underlying, r.total_trades, r.winning_trades,
                r.losing_trades, f"{r.win_rate:.2f}", f"{r.total_pnl:.2f}",
                f"{r.avg_pnl_per_trade:.2f}", f"{r.max_profit:.2f}", f"{r.max_loss:.2f}",
                f"{r.avg_win:.2f}", f"{r.avg_loss:.2f}", f"{r.sharpe_ratio:.2f}",
                f"{r.max_drawdown:.2f}"
            ])


# ========== STRATEGY 1: simple ATM Straddle ==========
def strategy_atm_straddle(df: pl.DataFrame, entry_time: time, exit_time: time) -> List[Trade]:
    """Sell ATM straddle at entry, exit at exit time"""
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
            
            # Find exit
            exit_df = day_df.filter(
                (pl.col('timestamp').dt.time() >= exit_time) &
                (pl.col('strike') == ce_strike) &
                (pl.col('opt_type') == 'CE')
            ).sort('timestamp')
            
            if exit_df.is_empty():
                continue
            
            exit_timestamp = exit_df[0]['timestamp'][0]
            exit_snapshot = day_df.filter(pl.col('timestamp') == exit_timestamp)
            
            ce_exit_row = exit_snapshot.filter((pl.col('strike') == ce_strike) & (pl.col('opt_type') == 'CE'))
            pe_exit_row = exit_snapshot.filter((pl.col('strike') == pe_strike) & (pl.col('opt_type') == 'PE'))
            
            if ce_exit_row.is_empty() or pe_exit_row.is_empty():
                continue
            
            ce_exit_price = ce_exit_row[0]['price'][0]
            pe_exit_price = pe_exit_row[0]['price'][0]
            
            premium_received = ce_entry_price + pe_entry_price
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
                hold_duration_minutes=hold_duration
            ))
    
    return trades


# ========== STRATEGY 2: OTM Strangle ==========
def strategy_otm_strangle(df: pl.DataFrame, otm_pct: float, entry_time: time, exit_time: time) -> List[Trade]:
    """Sell OTM strangle"""
    trades = []
    df = df.filter((pl.col('timestamp').dt.time() >= time(9, 15)) & (pl.col('timestamp').dt.time() <= time(15, 30)))
    
    expiries = df['expiry'].unique().sort()
    
    for expiry in expiries:
        expiry_df = df.filter(pl.col('expiry') == expiry)
        dates = expiry_df['timestamp'].dt.date().unique().sort()
        
        for date in dates:
            if date == expiry:
                continue
            
            day_df = expiry_df.filter(pl.col('timestamp').dt.date() == date)
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
            
            exit_df = day_df.filter(
                (pl.col('timestamp').dt.time() >= exit_time) &
                (pl.col('strike') == ce_strike) &
                (pl.col('opt_type') == 'CE')
            ).sort('timestamp')
            
            if exit_df.is_empty():
                continue
            
            exit_timestamp = exit_df[0]['timestamp'][0]
            exit_snapshot = day_df.filter(pl.col('timestamp') == exit_timestamp)
            
            ce_exit_row = exit_snapshot.filter((pl.col('strike') == ce_strike) & (pl.col('opt_type') == 'CE'))
            pe_exit_row = exit_snapshot.filter((pl.col('strike') == pe_strike) & (pl.col('opt_type') == 'PE'))
            
            if ce_exit_row.is_empty() or pe_exit_row.is_empty():
                continue
            
            ce_exit_price = ce_exit_row[0]['price'][0]
            pe_exit_price = pe_exit_row[0]['price'][0]
            
            premium_received = ce_entry_price + pe_entry_price
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
                hold_duration_minutes=hold_duration
            ))
    
    return trades


# Define all 12 strategies
STRATEGIES = [
    {'name': '1_ATM_Straddle_0920_1515', 'func': strategy_atm_straddle, 'params': {'entry_time': time(9, 20), 'exit_time': time(15, 15)}},
    {'name': '2_ATM_Straddle_0920_1400', 'func': strategy_atm_straddle, 'params': {'entry_time': time(9, 20), 'exit_time': time(14, 0)}},
    {'name': '3_ATM_Straddle_0920_1100', 'func': strategy_atm_straddle, 'params': {'entry_time': time(9, 20), 'exit_time': time(11, 0)}},
    {'name': '4_OTM_Strangle_0_5pct', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.005, 'entry_time': time(9, 20), 'exit_time': time(15, 15)}},
    {'name': '5_OTM_Strangle_1pct', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.01, 'entry_time': time(9, 20), 'exit_time': time(15, 15)}},
    {'name': '6_OTM_Strangle_2pct', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.02, 'entry_time': time(9, 20), 'exit_time': time(15, 15)}},
    {'name': '7_OTM_Strangle_3pct', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.03, 'entry_time': time(9, 20), 'exit_time': time(15, 15)}},
    {'name': '8_OTM_Strangle_1pct_Morning', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.01, 'entry_time': time(9, 20), 'exit_time': time(11, 0)}},
    {'name': '9_OTM_Strangle_1pct_Afternoon', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.01, 'entry_time': time(13, 0), 'exit_time': time(15, 15)}},
    {'name': '10_OTM_Strangle_2pct_Morning', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.02, 'entry_time': time(9, 20), 'exit_time': time(11, 0)}},
    {'name': '11_OTM_Strangle_2pct_Early_Exit', 'func': strategy_otm_strangle, 'params': {'otm_pct': 0.02, 'entry_time': time(9, 20), 'exit_time': time(14, 0)}},
    {'name': '12_ATM_Straddle_1300_1515', 'func': strategy_atm_straddle, 'params': {'entry_time': time(13, 0), 'exit_time': time(15, 15)}},
]


def main():
    print("=" * 80)
    print("RUNNING 12 OPTIONS STRATEGIES")
    print("=" * 80)
    
    root = _project_root()
    data_dir = root / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
    results_dir = root / "strategies" / "strategy_results" / "selling" / "strategy_results"
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
        
        for strategy in STRATEGIES:
            strategy_name = f"{underlying}_{strategy['name']}"
            print(f"\n--- {strategy_name} ---")
            
            all_trades = []
            
            # Process date by date
            for idx, file_paths in enumerate(date_files):
                if idx % 10 == 0:
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
            
            result = calculate_strategy_metrics(all_trades, strategy_name, underlying)
            all_results.append(result)
            
            print(f"  Trades: {result.total_trades}, Win Rate: {result.win_rate:.1f}%, P&L: {result.total_pnl:.2f}")
            
            trades_file = results_dir / f"{strategy_name}_trades.csv"
            save_trades_to_csv(all_trades, trades_file)
            
            del all_trades
            gc.collect()
    
    summary_file = results_dir / "all_strategies_summary.csv"
    save_summary_to_csv(all_results, summary_file)
    
    print("\n" + "=" * 80)
    print(f"✓ COMPLETE! Results in: {results_dir}")
    print("=" * 80)


if __name__ == "__main__":
    main()
