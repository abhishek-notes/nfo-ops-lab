"""
NFO Options Backtesting Framework
=================================

Optimized backtesting framework for NIFTY/BANKNIFTY options data.

Key optimizations implemented (from docs/optimizations/GUIDE.md):
- Polars over Pandas for 10-100x faster DataFrame operations
- Parquet with ZSTD compression for fast I/O
- Predicate pushdown and column projection
- Dense 1-second grid indexing for O(1) joins
- IST timezone consistency (Asia/Kolkata)
- Rolling window calculations for burst detection
- Vectorized PnL calculations (no Python loops)
- Lazy evaluation where possible

Quick Start:
    # Using the CLI
    python run_backtest.py --strategy volume_burst --symbol BANKNIFTY --start 2024-08-01 --end 2024-11-30
    python run_backtest.py --strategy all --symbol BANKNIFTY
    python run_backtest.py --list-strategies

    # Using the Python API
    from backtests.framework import PackedOptionsEngine, BacktestConfig
    from backtests.framework.strategies import VolumeBurstStrategy

    config = BacktestConfig(sl_pct=15.0, tp_pct=15.0)
    engine = PackedOptionsEngine(config)
    strategy = VolumeBurstStrategy()
    result = engine.run_strategy(strategy, "BANKNIFTY", date(2024, 8, 1), date(2024, 11, 30))
    print(result.summary())

Available Strategies:
    - VolumeBurstStrategy: Detects volume bursts and sells premium
    - ATMSellerStrategy: Sells ATM options at market open
    - StraddleStrategy: Sells ATM straddle (CE + PE) at market open

Available Engines:
    - PackedOptionsEngine: For hierarchical packed parquet format
    - RawOptionsEngine: For flat raw parquet files
"""

from .config import BacktestConfig, STRIKE_STEP, PACKED_OPTIONS_FOLDERS, RAW_OPTIONS_FOLDERS
from .engines.base import BacktestEngine, BacktestResult, Trade
from .engines.packed_options import PackedOptionsEngine
from .engines.raw_options import RawOptionsEngine
from .strategies.base import Strategy, StrategyParams
from .strategies.volume_burst import VolumeBurstStrategy, VolumeBurstParams
from .strategies.atm_seller import ATMSellerStrategy, ATMSellerParams
from .strategies.straddle import StraddleStrategy, StraddleParams

__all__ = [
    # Config
    "BacktestConfig",
    "STRIKE_STEP",
    "PACKED_OPTIONS_FOLDERS",
    "RAW_OPTIONS_FOLDERS",
    # Engines
    "BacktestEngine",
    "BacktestResult",
    "Trade",
    "PackedOptionsEngine",
    "RawOptionsEngine",
    # Strategy base
    "Strategy",
    "StrategyParams",
    # Strategies
    "VolumeBurstStrategy",
    "VolumeBurstParams",
    "ATMSellerStrategy",
    "ATMSellerParams",
    "StraddleStrategy",
    "StraddleParams",
]
