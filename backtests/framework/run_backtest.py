#!/usr/bin/env python3
"""
Main Backtest Runner.

Usage:
    python run_backtest.py --strategy volume_burst --symbol BANKNIFTY --start 2024-08-01 --end 2024-11-30
    python run_backtest.py --strategy all --symbol BANKNIFTY
    python run_backtest.py --list-strategies
"""
from __future__ import annotations
import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any

# Add parent directory to path for package imports
_framework_dir = Path(__file__).parent
_parent_dir = _framework_dir.parent
sys.path.insert(0, str(_parent_dir))

# Now import using the package structure
from framework.config import BacktestConfig
from framework.engines.packed_options import PackedOptionsEngine
from framework.engines.raw_options import RawOptionsEngine
from framework.strategies.volume_burst import VolumeBurstStrategy, VolumeBurstParams
from framework.strategies.atm_seller import ATMSellerStrategy, ATMSellerParams
from framework.strategies.straddle import StraddleStrategy, StraddleParams


# Registry of available strategies
STRATEGIES = {
    "volume_burst": lambda: VolumeBurstStrategy(VolumeBurstParams()),
    "atm_seller": lambda: ATMSellerStrategy(ATMSellerParams()),
    "atm_seller_ce": lambda: ATMSellerStrategy(ATMSellerParams(opt_type="CE")),
    "atm_seller_pe": lambda: ATMSellerStrategy(ATMSellerParams(opt_type="PE")),
    "straddle": lambda: StraddleStrategy(StraddleParams()),
}


def list_strategies() -> None:
    """Print available strategies."""
    print("\nAvailable Strategies:")
    print("-" * 50)
    for name in STRATEGIES:
        strategy = STRATEGIES[name]()
        print(f"  {name:20s} - {strategy.name}")
    print()


def run_single_strategy(strategy_name: str, symbol: str,
                        start_date: date, end_date: date,
                        engine_type: str = "packed",
                        config: BacktestConfig = None) -> Dict[str, Any]:
    """Run a single strategy backtest."""
    if strategy_name not in STRATEGIES:
        print(f"Unknown strategy: {strategy_name}")
        return {"error": f"Unknown strategy: {strategy_name}"}

    config = config or BacktestConfig()
    strategy = STRATEGIES[strategy_name]()

    # Select engine
    if engine_type == "packed":
        engine = PackedOptionsEngine(config)
    else:
        engine = RawOptionsEngine(config)

    print(f"\nRunning {strategy.name} on {symbol}")
    print(f"Period: {start_date} to {end_date}")
    print(f"Engine: {engine_type}")
    print("-" * 50)

    result = engine.run_strategy(strategy, symbol, start_date, end_date)

    print(result.summary())
    print()

    return {
        "strategy": strategy_name,
        "symbol": symbol,
        "trades": len(result.trades),
        "metrics": result.metrics,
    }


def run_all_strategies(symbol: str, start_date: date, end_date: date,
                       engine_type: str = "packed",
                       config: BacktestConfig = None) -> List[Dict[str, Any]]:
    """Run all strategies and compare results."""
    results = []

    print(f"\n{'='*60}")
    print(f"Running ALL strategies on {symbol}")
    print(f"Period: {start_date} to {end_date}")
    print(f"{'='*60}")

    for strategy_name in STRATEGIES:
        try:
            result = run_single_strategy(
                strategy_name, symbol, start_date, end_date,
                engine_type, config
            )
            results.append(result)
        except Exception as e:
            print(f"Error running {strategy_name}: {e}")
            results.append({"strategy": strategy_name, "error": str(e)})

    # Print comparison
    print("\n" + "=" * 60)
    print("STRATEGY COMPARISON")
    print("=" * 60)
    print(f"{'Strategy':<20} {'Trades':<8} {'Win%':<8} {'PnL':<12} {'PF':<8}")
    print("-" * 60)

    for r in results:
        if "error" in r:
            print(f"{r['strategy']:<20} ERROR: {r['error']}")
        else:
            m = r.get("metrics", {})
            print(f"{r['strategy']:<20} {r['trades']:<8} {m.get('win_rate_pct', 0):<8.1f} {m.get('total_pnl', 0):<12.2f} {m.get('profit_factor', 0):<8.2f}")

    print()
    return results


def main():
    parser = argparse.ArgumentParser(description="Run backtests on options data")
    parser.add_argument("--strategy", "-s", type=str, default="all",
                       help="Strategy name or 'all' for all strategies")
    parser.add_argument("--symbol", type=str, default="BANKNIFTY",
                       choices=["BANKNIFTY", "NIFTY"],
                       help="Symbol to backtest")
    parser.add_argument("--start", type=str, default="2024-08-01",
                       help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2024-11-30",
                       help="End date (YYYY-MM-DD)")
    parser.add_argument("--engine", type=str, default="packed",
                       choices=["packed", "raw"],
                       help="Data engine to use")
    parser.add_argument("--list-strategies", action="store_true",
                       help="List available strategies")

    # Risk parameters
    parser.add_argument("--sl-pct", type=float, default=15.0,
                       help="Stop loss percentage")
    parser.add_argument("--tp-pct", type=float, default=15.0,
                       help="Take profit percentage")
    parser.add_argument("--trail-pct", type=float, default=10.0,
                       help="Trailing stop percentage")

    args = parser.parse_args()

    if args.list_strategies:
        list_strategies()
        return

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"Invalid date format: {e}")
        return

    # Create config
    config = BacktestConfig(
        sl_pct=args.sl_pct,
        tp_pct=args.tp_pct,
        trail_pct=args.trail_pct,
    )

    # Run backtests
    if args.strategy.lower() == "all":
        run_all_strategies(args.symbol, start_date, end_date, args.engine, config)
    else:
        run_single_strategy(args.strategy, args.symbol, start_date, end_date,
                           args.engine, config)


if __name__ == "__main__":
    main()
