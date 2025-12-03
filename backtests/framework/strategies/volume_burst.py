"""
Volume Burst Strategy.

Detects sudden volume spikes and trades in the direction of the burst.
Uses rolling window calculations for efficient burst detection.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, time
from typing import TYPE_CHECKING
import polars as pl

from .base import Strategy, StrategyParams

if TYPE_CHECKING:
    from ..config import BacktestConfig


@dataclass
class VolumeBurstParams(StrategyParams):
    """Parameters for volume burst strategy."""
    burst_secs: int = 30  # Window for burst detection
    avg_secs: int = 300  # Window for average calculation (5 min)
    multiplier: float = 1.5  # Burst threshold multiplier
    min_price: float = 10.0  # Minimum option price to trade
    max_signals_per_day: int = 5  # Maximum signals per day
    entry_delay_secs: int = 5  # Seconds to wait after burst detection
    side: str = "SELL"  # Default side (SELL = sell premium on burst)


class VolumeBurstStrategy(Strategy):
    """
    Trades based on volume burst detection.

    Logic:
    1. Calculate rolling volume over burst_secs window
    2. Calculate average volume over avg_secs window
    3. Detect burst when rolling_vol > multiplier * avg_vol
    4. Enter trade (default: sell) on burst detection

    Optimizations:
    - Vectorized rolling calculations using Polars
    - Per-strike/type grouping for independent burst detection
    """

    def __init__(self, params: VolumeBurstParams = None):
        super().__init__("VolumeBurst", params or VolumeBurstParams())
        self.params: VolumeBurstParams = self.params

    def generate_signals(self, data: pl.DataFrame, symbol: str,
                        trade_date: date, expiry: date,
                        config: "BacktestConfig") -> pl.DataFrame:
        """Generate signals based on volume bursts."""
        if data is None or data.is_empty():
            return self.empty_signals()

        # Apply preprocessing with volume burst detection
        data = self._add_burst_columns(data)

        # Filter for burst events
        bursts = data.filter(pl.col("burst"))

        if bursts.is_empty():
            return self.empty_signals()

        # Filter by minimum price
        bursts = bursts.filter(pl.col("close") >= self.params.min_price)

        if bursts.is_empty():
            return self.empty_signals()

        # Get first burst per strike/type combination (limit signals)
        signals = (
            bursts.sort("ts")
            .group_by(["strike", "opt_type"])
            .first()
            .head(self.params.max_signals_per_day)
        )

        # Build signals DataFrame
        result = signals.select([
            pl.col("ts"),
            pl.col("strike"),
            pl.col("opt_type"),
            pl.lit(self.params.side).alias("side"),
            pl.col("close").alias("entry_price"),
        ])

        return result

    def _add_burst_columns(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Add volume burst detection columns.

        Optimization: Uses Polars rolling functions for vectorized computation.
        """
        # Ensure sorted by strike, opt_type, ts for proper rolling
        data = data.sort(["strike", "opt_type", "ts"])

        # Fill null volumes with 0
        data = data.with_columns([
            pl.col("vol_delta").fill_null(0).alias("vol")
        ])

        # Rolling calculations within each strike/type group
        # Note: rolling_sum/rolling_mean use row counts, not time windows
        # For time-based windows, we need to ensure 1-second granularity
        data = data.with_columns([
            pl.col("vol")
              .rolling_sum(window_size=self.params.burst_secs)
              .over(["strike", "opt_type"])
              .alias("vol_burst"),
            (pl.col("vol")
              .rolling_mean(window_size=self.params.avg_secs)
              .over(["strike", "opt_type"]) * self.params.burst_secs)
              .alias("vol_base"),
        ])

        # Detect burst: current volume > multiplier * baseline
        data = data.with_columns([
            (
                (pl.col("vol_burst") > self.params.multiplier * pl.col("vol_base")) &
                (pl.col("vol_base") > 0)  # Avoid division by zero scenarios
            ).alias("burst")
        ])

        return data
