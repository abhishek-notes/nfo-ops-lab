#!/usr/bin/env python3
"""
Options Strategy Backtesting Framework
Supports 10+ non-directional strategies on spot-enriched data
"""

from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable
from datetime import time, datetime
import polars as pl
import numpy as np
from numba import njit
import csv


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


def load_enriched_data(
    data_dir: Path,
    underlying: str,
    start_date: str = "2025-08-01",
    end_date: str = "2025-12-01"
) -> pl.DataFrame:
    """
    Load spot-enriched options data for backtesting.
    Excludes 1970 dates (bad data before market open).
    """
    print(f"\nLoading {underlying} data...")
    
    # Find all parquet files for this underlying
    files = []
    for date_dir in sorted(data_dir.glob("*")):
        if not date_dir.is_dir():
            continue
        
        # Skip 1970 dates
        if "1970" in date_dir.name:
            continue
        
        # Check date range
        if date_dir.name < start_date or date_dir.name > end_date:
            continue
        
        underlying_dir = date_dir / underlying
        if underlying_dir.exists():
            parquet_files = list(underlying_dir.glob("*.parquet"))
            files.extend(parquet_files)
    
    print(f"  Found {len(files)} date files")
    
    if not files:
        raise ValueError(f"No files found for {underlying}")
    
    # Read all files
    dfs = []
    for f in files:
        df = pl.read_parquet(f)
        dfs.append(df)
    
    # Combine
    combined = pl.concat(dfs)
    
    # Filter out 1970 dates
    combined = combined.filter(
        pl.col('timestamp').dt.year() > 1970
    )
    
    print(f"  Loaded {len(combined):,} rows")
    print(f"  Date range: {combined['timestamp'].min()} to {combined['timestamp'].max()}")
    print(f"  Columns: {combined.columns}")
    
    return combined


@njit
def find_atm_strike(strikes: np.ndarray, distances: np.ndarray, opt_types: np.ndarray, target_type: int) -> int:
    """
    Find ATM strike index for CE (type=0) or PE (type=1).
    Returns index of strike closest to spot.
    """
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
def find_otm_strike(
    strikes: np.ndarray,
    distances: np.ndarray,
    opt_types: np.ndarray,
    target_type: int,
    otm_pct: float
) -> int:
    """
    Find OTM strike at specific percentage from ATM.
    
    For CE: positive distance (strike > spot)
    For PE: negative distance (strike < spot)
    """
    target_dist = otm_pct  # distance_from_spot is already in price units
    min_diff = 999999.0
    best_idx = -1
    
    for i in range(len(strikes)):
        if opt_types[i] != target_type:
            continue
        
        # For CE: want positive distance (OTM)
        # For PE: want negative distance (OTM)
        if target_type == 0:  # CE
            if distances[i] <= 0:  # Skip ITM/ATM
                continue
            diff = abs(distances[i] - target_dist)
        else:  # PE
            if distances[i] >= 0:  # Skip ITM/ATM
                continue
            diff = abs(distances[i] + target_dist)
        
        if diff < min_diff:
            min_diff = diff
            best_idx = i
    
    return best_idx


def run_atm_straddle_sell(
    df: pl.DataFrame,
    entry_time: time = time(9, 20),
    exit_time: time = time(15, 15),
    strategy_name: str = "ATM Straddle Sell"
) -> List[Trade]:
    """
    Strategy 1: Sell ATM straddle at entry time, exit at exit time.
    """
    trades = []
    
    # Filter to market hours
    df = df.filter(
        (pl.col('timestamp').dt.time() >= time(9, 15)) &
        (pl.col('timestamp').dt.time() <= time(15, 30))
    )
    
    # Get unique expiries
    expiries = df['expiry'].unique().sort()
    
    for expiry in expiries:
        expiry_df = df.filter(pl.col('expiry') == expiry)
        
        # Get unique dates for this expiry
        dates = expiry_df['timestamp'].dt.date().unique().sort()
        
        for date in dates:
            # Skip expiry date (can add this later if needed)
            if date == expiry:
                continue
            
            day_df = expiry_df.filter(pl.col('timestamp').dt.date() == date)
            
            # Find entry tick (first tick at or after entry_time)
            entry_df = day_df.filter(pl.col('timestamp').dt.time() >= entry_time).sort('timestamp')
            
            if entry_df.is_empty():
                continue
            
            entry_tick = entry_df[0]
            entry_timestamp = entry_tick['timestamp'][0]
            
            # Find ATM strikes at entry using unique strikes at this timestamp
            entry_snapshot = day_df.filter(pl.col('timestamp') == entry_timestamp)
            
            # Convert to numpy for Numba
            strikes_np = entry_snapshot['strike'].to_numpy()
            distances_np = entry_snapshot['distance_from_spot'].to_numpy()
            opt_types_np = (entry_snapshot['opt_type'] == 'PE').cast(pl.Int32).to_numpy()
            prices_np = entry_snapshot['price'].to_numpy()
            
            # Find ATM CE and PE
            ce_idx = find_atm_strike(strikes_np, distances_np, opt_types_np, 0)
            pe_idx = find_atm_strike(strikes_np, distances_np, opt_types_np, 1)
            
            if ce_idx == -1 or pe_idx == -1:
                continue
            
            ce_strike = strikes_np[ce_idx]
            pe_strike = strikes_np[pe_idx]
            ce_entry_price = prices_np[ce_idx]
            pe_entry_price = prices_np[pe_idx]
            
            # Find exit tick
            exit_df = day_df.filter(
                (pl.col('timestamp').dt.time() >= exit_time) &
                (pl.col('strike') == ce_strike) &
                (pl.col('opt_type') == 'CE')
            ).sort('timestamp')
            
            if exit_df.is_empty():
                continue
            
            exit_timestamp = exit_df[0]['timestamp'][0]
            
            # Get exit prices for both legs
            exit_snapshot = day_df.filter(pl.col('timestamp') == exit_timestamp)
            
            ce_exit_row = exit_snapshot.filter(
                (pl.col('strike') == ce_strike) &
                (pl.col('opt_type') == 'CE')
            )
            pe_exit_row = exit_snapshot.filter(
                (pl.col('strike') == pe_strike) &
                (pl.col('opt_type') == 'PE')
            )
            
            if ce_exit_row.is_empty() or pe_exit_row.is_empty():
                continue
            
            ce_exit_price = ce_exit_row[0]['price'][0]
            pe_exit_price = pe_exit_row[0]['price'][0]
            
            # Calculate P&L (selling straddle)
            premium_received = ce_entry_price + pe_entry_price
            premium_paid = ce_exit_price + pe_exit_price
            pnl = premium_received - premium_paid
            
            # Calculate hold duration
            hold_duration = int((exit_timestamp - entry_timestamp).total_seconds() / 60)
            
            trade = Trade(
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
            )
            
            trades.append(trade)
    
    return trades


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
    
    # Sharpe ratio (annualized, assuming ~250 trading days)
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
        print(f"  No trades to save for {output_file}")
        return
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'entry_date', 'entry_time', 'exit_date', 'exit_time',
            'ce_strike', 'pe_strike',
            'ce_entry_price', 'pe_entry_price',
            'ce_exit_price', 'pe_exit_price',
            'total_premium_received', 'total_premium_paid',
            'pnl', 'hold_duration_minutes'
        ])
        
        # Data
        for trade in trades:
            writer.writerow([
                trade.entry_date, trade.entry_time, trade.exit_date, trade.exit_time,
                trade.ce_strike, trade.pe_strike,
                trade.ce_entry_price, trade.pe_entry_price,
                trade.ce_exit_price, trade.pe_exit_price,
                trade.total_premium_received, trade.total_premium_paid,
                trade.pnl, trade.hold_duration_minutes
            ])
    
    print(f"  Saved {len(trades)} trades to {output_file}")


def save_summary_to_csv(results: List[StrategyResult], output_file: Path):
    """Save strategy summary to CSV"""
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'strategy_name', 'underlying', 'total_trades',
            'winning_trades', 'losing_trades', 'win_rate_%',
            'total_pnl', 'avg_pnl_per_trade',
            'max_profit', 'max_loss',
            'avg_win', 'avg_loss',
            'sharpe_ratio', 'max_drawdown'
        ])
        
        # Data  
        for r in results:
            writer.writerow([
                r.strategy_name, r.underlying, r.total_trades,
                r.winning_trades, r.losing_trades, f"{r.win_rate:.2f}",
                f"{r.total_pnl:.2f}", f"{r.avg_pnl_per_trade:.2f}",
                f"{r.max_profit:.2f}", f"{r.max_loss:.2f}",
                f"{r.avg_win:.2f}", f"{r.avg_loss:.2f}",
                f"{r.sharpe_ratio:.2f}", f"{r.max_drawdown:.2f}"
            ])
    
    print(f"\n✓ Saved summary to {output_file}")


if __name__ == "__main__":
    # This is just the framework - will be imported by strategy runner
    print("Options Strategy Framework loaded")
