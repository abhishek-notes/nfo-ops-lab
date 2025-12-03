"""
Straddle Strategy.

Sells ATM straddle (both CE and PE at same strike) at market open.
Profits from time decay when market stays range-bound.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING
import polars as pl

from .base import Strategy, StrategyParams

if TYPE_CHECKING:
    from ..config import BacktestConfig


@dataclass
class StraddleParams(StrategyParams):
    """Parameters for straddle strategy."""
    entry_delay_mins: int = 5  # Minutes after market open
    min_combined_premium: float = 100.0  # Minimum combined CE+PE premium


class StraddleStrategy(Strategy):
    """
    Sells ATM straddle at market open.

    Logic:
    1. Wait for entry_delay_mins after market open
    2. Find ATM strike
    3. Sell both CE and PE at ATM strike
    4. Exit on SL/TP/EOD (handled by engine)

    Optimizations:
    - Single ATM detection for both legs
    - Combined premium validation
    """

    def __init__(self, params: StraddleParams = None):
        super().__init__("Straddle", params or StraddleParams())
        self.params: StraddleParams = self.params

    def generate_signals(self, data: pl.DataFrame, symbol: str,
                        trade_date: date, expiry: date,
                        config: "BacktestConfig") -> pl.DataFrame:
        """Generate straddle sell signals."""
        if data is None or data.is_empty():
            return self.empty_signals()

        # Calculate entry time
        entry_time = (
            config.session_start.hour * 3600 +
            config.session_start.minute * 60 +
            self.params.entry_delay_mins * 60
        )

        # Filter to entry window
        entry_data = data.filter(
            pl.col("ts").dt.hour() * 3600 + pl.col("ts").dt.minute() * 60 >= entry_time
        ).head(1000)

        if entry_data.is_empty():
            return self.empty_signals()

        # Find ATM strike
        atm_strike = self.compute_atm_strike(entry_data, symbol)
        if atm_strike is None:
            return self.empty_signals()

        # Get entry data for ATM strike
        atm_data = entry_data.filter(pl.col("strike") == atm_strike)

        if atm_data.is_empty():
            return self.empty_signals()

        # Get CE and PE prices
        ce_data = atm_data.filter(pl.col("opt_type") == "CE").head(1)
        pe_data = atm_data.filter(pl.col("opt_type") == "PE").head(1)

        if ce_data.is_empty() or pe_data.is_empty():
            return self.empty_signals()

        ce_price = ce_data["close"][0]
        pe_price = pe_data["close"][0]

        # Check combined premium
        if ce_price + pe_price < self.params.min_combined_premium:
            return self.empty_signals()

        # Use CE timestamp as entry time for both legs
        entry_ts = ce_data["ts"][0]

        signals = [
            {
                "ts": entry_ts,
                "strike": atm_strike,
                "opt_type": "CE",
                "side": "SELL",
                "entry_price": ce_price,
            },
            {
                "ts": entry_ts,
                "strike": atm_strike,
                "opt_type": "PE",
                "side": "SELL",
                "entry_price": pe_price,
            },
        ]

        return pl.DataFrame(signals)
