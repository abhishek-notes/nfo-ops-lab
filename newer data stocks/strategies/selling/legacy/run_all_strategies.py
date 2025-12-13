#!/usr/bin/env python3
"""
Memory-Optimized Strategy Runner
Processes one date at a time to avoid OOM issues
"""

from pathlib import Path
from datetime import time, date as date_type
import polars as pl
from strategy_framework import *
import gc


def run_strategy_on_single_date(
    data_file: Path,
    underlying: str,
    strategy_func: Callable,
    strategy_params: dict
) -> List[Trade]:
    """Run strategy on a single date file"""
    
    # Load single date
    df = pl.read_parquet(data_file)
    
    # Filter out 1970 dates
    df = df.filter(pl.col('timestamp').dt.year() > 1970)
    
    if df.is_empty():
        return []
    
    # Run strategy
    trades = strategy_func(df, **strategy_params)
    
    return trades


def run_strategy_batch(
    data_dir: Path,
    underlying: str,
    strategy_name: str,
    strategy_func: Callable,
    strategy_params: dict,
    start_date: str = "2025-08-01",
    end_date: str = "2025-12-01"
) -> List[Trade]:
    """
    Run strategy across multiple dates efficiently.
    Processes one date at a time to avoid memory issues.
    """
    all_trades = []
    
    # Find all date directories
    date_dirs = []
    for date_dir in sorted(data_dir.glob("*")):
        if not date_dir.is_dir():
            continue
        
        # Skip 1970
        if "1970" in date_dir.name:
            continue
        
        # Check range
        if date_dir.name < start_date or date_dir.name > end_date:
            continue
        
        underlying_dir = date_dir / underlying
        if underlying_dir.exists():
            parquet_files = list(underlying_dir.glob("*.parquet"))
            if parquet_files:
                date_dirs.append((date_dir.name, parquet_files[0]))
    
    print(f"  Processing {len(date_dirs)} dates...")
    
    for idx, (date_str, file_path) in enumerate(date_dirs):
        if idx % 10 == 0:
            print(f"    Progress: {idx}/{len(date_dirs)} dates ({idx/len(date_dirs)*100:.0f}%)")
        
        # Process single date
        trades = run_strategy_on_single_date(file_path, underlying, strategy_func, strategy_params)
        all_trades.extend(trades)
        
        # Force garbage collection
        gc.collect()
    
    print(f"  ✓ Completed {len(date_dirs)} dates, found {len(all_trades)} trades")
    
    return all_trades


# Same strategies as before
STRATEGIES = [
    {
        'name': '1_ATM_Straddle_Sell',
        'func': run_atm_straddle_sell,
        'params': {}
    },
    {
        'name': '2_ATM_Straddle_SL_2x',
        'func': run_atm_straddle_with_sl,
        'params': {'stop_loss_multiplier': 2.0}
    },
    {
        'name': '3_ATM_Straddle_PT_50pct',
        'func': run_atm_straddle_profit_target,
        'params': {'profit_target_pct': 0.5}
    },
    {
        'name': '4_OTM_Strangle_1pct',
        'func': run_otm_strangle,
        'params': {'otm_pct': 0.01}
    },
    {
        'name': '5_OTM_Strangle_2pct',
        'func': run_otm_strangle,
        'params': {'otm_pct': 0.02}
    },
    {
        'name': '6_Morning_Straddle_09_20_to_11_00',
        'func': run_atm_straddle_sell,
        'params': {'entry_time': time(9, 20), 'exit_time': time(11, 0)}
    },
    {
        'name': '7_Afternoon_Strangle_13_00_to_15_15',
        'func': run_otm_strangle,
        'params': {'otm_pct': 0.01, 'entry_time': time(13, 0), 'exit_time': time(15, 15)}
    },
    {
        'name': '8_Early_Exit_Straddle_09_20_to_14_00',
        'func': run_atm_straddle_sell,
        'params': {'entry_time': time(9, 20), 'exit_time': time(14, 0)}
    },
    {
        'name': '9_Wide_Strangle_3pct',
        'func': run_otm_strangle,
        'params': {'otm_pct': 0.03}
    },
    {
        'name': '10_Narrow_Strangle_0_5pct',
        'func': run_otm_strangle,
        'params': {'otm_pct': 0.005}
    },
    {
        'name': '11_ATM_Straddle_Tight_SL_1_5x',
        'func': run_atm_straddle_with_sl,
        'params': {'stop_loss_multiplier': 1.5}
    },
    {
        'name': '12_ATM_Straddle_PT_30pct',
        'func': run_atm_straddle_profit_target,
        'params': {'profit_target_pct': 0.3}
    },
]


def main():
    """Run all strategies with memory-efficient processing"""
    
    print("=" * 80)
    print("RUNNING 12 OPTIONS STRATEGIES (MEMORY-OPTIMIZED)")
    print("=" * 80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    results_dir = Path("../strategy_results/selling/strategy_results")
    results_dir.mkdir(exist_ok=True)
    
    underlyings = ['BANKNIFTY', 'NIFTY']
    all_results = []
    
    for underlying in underlyings:
        print(f"\n{'='*80}")
        print(f"PROCESSING {underlying}")
        print(f"{'='*80}")
        
        for strategy in STRATEGIES:
            strategy_name = f"{underlying}_{strategy['name']}"
            print(f"\n--- Running: {strategy_name} ---")
            
            # Run strategy (date-by-date)
            trades = run_strategy_batch(
                data_dir,
                underlying,
                strategy_name,
                strategy['func'],
                strategy['params']
            )
            
            # Calculate metrics
            result = calculate_strategy_metrics(trades, strategy_name, underlying)
            all_results.append(result)
            
            # Print summary
            print(f"  Results:")
            print(f"    Trades: {result.total_trades}")
            print(f"    Win Rate: {result.win_rate:.1f}%")
            print(f"    Total P&L: {result.total_pnl:.2f}")
            print(f"    Avg P&L/Trade: {result.avg_pnl_per_trade:.2f}")
            print(f"    Sharpe: {result.sharpe_ratio:.2f}")
            print(f"    Max DD: {result.max_drawdown:.2f}")
            
            # Save trades to CSV
            trades_file = results_dir / f"{strategy_name}_trades.csv"
            save_trades_to_csv(trades, trades_file)
            
            # Clear memory
            del trades
            gc.collect()
    
    # Save summary
    summary_file = results_dir / "all_strategies_summary.csv"
    save_summary_to_csv(all_results, summary_file)
    
    print("\n" + "=" * 80)
    print("ALL STRATEGIES COMPLETE!")
    print("=" * 80)
    print(f"\nResults saved to: {results_dir}")
    print(f"  - Individual trade CSVs: {len(STRATEGIES) * len(underlyings)} files")
    print(f"  - Summary: {summary_file}")
    print(f"\n✓ Total strategies run: {len(all_results)}")


if __name__ == "__main__":
    main()
