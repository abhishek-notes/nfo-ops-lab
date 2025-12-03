"""
Base Strategy class for backtesting.

All strategies should inherit from this base class and implement
the generate_signals() method.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Any, Optional, TYPE_CHECKING
import polars as pl

if TYPE_CHECKING:
    from ..config import BacktestConfig


@dataclass
class StrategyParams:
    """Base parameters for strategies. Subclass for strategy-specific params."""
    pass


class Strategy(ABC):
    """
    Abstract base class for trading strategies.

    Subclasses must implement:
    - generate_signals(): Returns DataFrame of entry signals

    The engine handles:
    - Data loading and preprocessing
    - Exit logic (SL/TP/trailing/EOD)
    - PnL calculation
    - Results aggregation
    """

    def __init__(self, name: str, params: Optional[StrategyParams] = None):
        self.name = name
        self.params = params or StrategyParams()

    @abstractmethod
    def generate_signals(self, data: pl.DataFrame, symbol: str,
                        trade_date: date, expiry: date,
                        config: "BacktestConfig") -> pl.DataFrame:
        """
        Generate entry signals for a trading day.

        Args:
            data: DataFrame with columns [ts, close, vol_delta, strike, opt_type]
            symbol: BANKNIFTY or NIFTY
            trade_date: Current trading date
            expiry: Expiry date for options
            config: Backtest configuration

        Returns:
            DataFrame with signal columns:
            - ts: Entry timestamp
            - strike: Strike price
            - opt_type: CE or PE
            - side: BUY or SELL
            - entry_price: Entry price

            Return empty DataFrame for no signals.
        """
        pass

    def preprocess(self, data: pl.DataFrame, config: "BacktestConfig") -> pl.DataFrame:
        """
        Optional preprocessing hook. Override for custom preprocessing.

        Default: No transformation.
        """
        return data

    def filter_strikes(self, data: pl.DataFrame, atm_strike: int,
                      n_otm: int = 5, symbol: str = "BANKNIFTY") -> pl.DataFrame:
        """
        Filter to strikes around ATM.

        Optimization: Reduces data to relevant strikes only.
        """
        from ..config import STRIKE_STEP
        step = STRIKE_STEP.get(symbol, 100)

        valid_strikes = set()
        for i in range(-n_otm, n_otm + 1):
            valid_strikes.add(atm_strike + i * step)

        return data.filter(pl.col("strike").is_in(list(valid_strikes)))

    def compute_atm_strike(self, data: pl.DataFrame, symbol: str) -> Optional[int]:
        """
        Estimate ATM strike from option prices at session start.

        Uses the strike where CE and PE prices are closest.
        """
        from ..config import STRIKE_STEP
        step = STRIKE_STEP.get(symbol, 100)

        # Get first few seconds of data
        first_ts = data["ts"].min()
        early_data = data.filter(pl.col("ts") <= first_ts)

        if early_data.is_empty():
            return None

        # Pivot to get CE and PE prices per strike
        strikes = early_data["strike"].unique().to_list()

        min_diff = float("inf")
        atm_strike = None

        for strike in strikes:
            ce_price = early_data.filter(
                (pl.col("strike") == strike) & (pl.col("opt_type") == "CE")
            ).select("close").to_series()

            pe_price = early_data.filter(
                (pl.col("strike") == strike) & (pl.col("opt_type") == "PE")
            ).select("close").to_series()

            if len(ce_price) > 0 and len(pe_price) > 0:
                diff = abs(ce_price[0] - pe_price[0])
                if diff < min_diff:
                    min_diff = diff
                    atm_strike = strike

        return atm_strike

    def empty_signals(self) -> pl.DataFrame:
        """Return empty signals DataFrame with correct schema."""
        return pl.DataFrame({
            "ts": pl.Series([], dtype=pl.Datetime("ns", time_zone="Asia/Kolkata")),
            "strike": pl.Series([], dtype=pl.Int64),
            "opt_type": pl.Series([], dtype=pl.Utf8),
            "side": pl.Series([], dtype=pl.Utf8),
            "entry_price": pl.Series([], dtype=pl.Float64),
        })
