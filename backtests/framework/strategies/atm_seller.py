"""
ATM Option Seller Strategy.

Sells ATM options at market open and holds until EOD or SL/TP.
Classic premium decay strategy.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, time, timedelta
from typing import TYPE_CHECKING
import polars as pl

from .base import Strategy, StrategyParams

if TYPE_CHECKING:
    from ..config import BacktestConfig


@dataclass
class ATMSellerParams(StrategyParams):
    """Parameters for ATM seller strategy."""
    entry_delay_mins: int = 5  # Minutes after market open to enter
    opt_type: str = "BOTH"  # CE, PE, or BOTH
    min_premium: float = 50.0  # Minimum premium to sell


class ATMSellerStrategy(Strategy):
    """
    Sells ATM options shortly after market open.

    Logic:
    1. Wait for entry_delay_mins after market open
    2. Find ATM strike (closest to spot)
    3. Sell ATM CE, PE, or both based on params
    4. Exit on SL/TP/EOD (handled by engine)

    Optimizations:
    - Single pass ATM detection
    - Vectorized price filtering
    """

    def __init__(self, params: ATMSellerParams = None):
        super().__init__("ATMSeller", params or ATMSellerParams())
        self.params: ATMSellerParams = self.params

    def generate_signals(self, data: pl.DataFrame, symbol: str,
                        trade_date: date, expiry: date,
                        config: "BacktestConfig") -> pl.DataFrame:
        """Generate ATM sell signals."""
        if data is None or data.is_empty():
            return self.empty_signals()

        # Calculate entry time
        from dateutil import tz
        ist = tz.gettz("Asia/Kolkata")
        entry_time = (
            config.session_start.hour * 3600 +
            config.session_start.minute * 60 +
            self.params.entry_delay_mins * 60
        )

        # Filter to entry window (first 1 minute after entry time)
        entry_start = data["ts"].min()
        if entry_start is None:
            return self.empty_signals()

        # Find data around entry time
        entry_data = data.filter(
            pl.col("ts").dt.hour() * 3600 + pl.col("ts").dt.minute() * 60 >= entry_time
        ).head(1000)  # Limit to first chunk after entry

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

        signals = []

        # Generate signals based on opt_type param
        opt_types = ["CE", "PE"] if self.params.opt_type == "BOTH" else [self.params.opt_type]

        for opt_type in opt_types:
            opt_data = atm_data.filter(pl.col("opt_type") == opt_type)

            if opt_data.is_empty():
                continue

            # Get first row as entry point
            first_row = opt_data.head(1)
            entry_price = first_row["close"][0]

            # Check minimum premium
            if entry_price < self.params.min_premium:
                continue

            signals.append({
                "ts": first_row["ts"][0],
                "strike": atm_strike,
                "opt_type": opt_type,
                "side": "SELL",
                "entry_price": entry_price,
            })

        if not signals:
            return self.empty_signals()

        return pl.DataFrame(signals)
