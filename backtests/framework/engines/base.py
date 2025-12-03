"""
Base backtest engine with common functionality.

Implements key optimizations:
- Polars for fast DataFrame operations
- Dense 1-second grid for O(1) timestamp joins
- Vectorized PnL calculations
- IST timezone consistency
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any, TYPE_CHECKING
import polars as pl

if TYPE_CHECKING:
    from ..strategies.base import Strategy
    from ..config import BacktestConfig

IST = "Asia/Kolkata"


@dataclass
class Trade:
    """Single trade record."""
    symbol: str
    expiry: date
    strike: int
    opt_type: str  # CE or PE
    side: str  # BUY or SELL
    entry_ts: datetime
    entry_price: float
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None  # SL, TP, TRAIL, EOD, SIGNAL
    pnl: float = 0.0
    pnl_pct: float = 0.0


@dataclass
class BacktestResult:
    """Results from a backtest run."""
    strategy_name: str
    symbol: str
    start_date: date
    end_date: date
    trades: List[Trade] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dataframe(self) -> pl.DataFrame:
        """Convert trades to Polars DataFrame."""
        if not self.trades:
            return pl.DataFrame()

        return pl.DataFrame([
            {
                "symbol": t.symbol,
                "expiry": t.expiry,
                "strike": t.strike,
                "opt_type": t.opt_type,
                "side": t.side,
                "entry_ts": t.entry_ts,
                "entry_price": t.entry_price,
                "exit_ts": t.exit_ts,
                "exit_price": t.exit_price,
                "exit_reason": t.exit_reason,
                "pnl": t.pnl,
                "pnl_pct": t.pnl_pct,
            }
            for t in self.trades
        ])

    def compute_metrics(self) -> None:
        """Compute performance metrics."""
        if not self.trades:
            self.metrics = {"error": "No trades"}
            return

        df = self.to_dataframe()
        total_pnl = df["pnl"].sum()
        winners = df.filter(pl.col("pnl") > 0)
        losers = df.filter(pl.col("pnl") < 0)

        win_rate = len(winners) / len(df) * 100 if len(df) > 0 else 0
        avg_win = winners["pnl"].mean() if len(winners) > 0 else 0
        avg_loss = losers["pnl"].mean() if len(losers) > 0 else 0

        self.metrics = {
            "total_trades": len(df),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate_pct": round(win_rate, 2),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / len(df), 2) if len(df) > 0 else 0,
            "avg_win": round(avg_win, 2) if avg_win else 0,
            "avg_loss": round(avg_loss, 2) if avg_loss else 0,
            "max_win": round(df["pnl"].max(), 2) if len(df) > 0 else 0,
            "max_loss": round(df["pnl"].min(), 2) if len(df) > 0 else 0,
            "profit_factor": round(abs(winners["pnl"].sum() / losers["pnl"].sum()), 2) if len(losers) > 0 and losers["pnl"].sum() != 0 else float("inf"),
        }

    def summary(self) -> str:
        """Return human-readable summary."""
        if not self.metrics:
            self.compute_metrics()

        lines = [
            f"=== {self.strategy_name} on {self.symbol} ===",
            f"Period: {self.start_date} to {self.end_date}",
            f"Total Trades: {self.metrics.get('total_trades', 0)}",
            f"Win Rate: {self.metrics.get('win_rate_pct', 0)}%",
            f"Total PnL: {self.metrics.get('total_pnl', 0)}",
            f"Avg PnL: {self.metrics.get('avg_pnl', 0)}",
            f"Profit Factor: {self.metrics.get('profit_factor', 0)}",
        ]
        return "\n".join(lines)


class BacktestEngine(ABC):
    """
    Abstract base class for backtest engines.

    Subclasses implement data loading for different formats (packed/raw).
    """

    def __init__(self, config: "BacktestConfig"):
        self.config = config

    @abstractmethod
    def load_data(self, symbol: str, trade_date: date, expiry: date) -> Optional[pl.DataFrame]:
        """
        Load option data for a specific symbol, date, and expiry.

        Returns DataFrame with columns:
        - ts: timestamp (Datetime ns, IST)
        - close: last price
        - vol_delta: volume delta (optional)
        - strike: strike price
        - opt_type: CE or PE
        """
        pass

    @abstractmethod
    def get_available_dates(self, symbol: str) -> List[date]:
        """Get list of available trading dates for a symbol."""
        pass

    @abstractmethod
    def get_expiry_for_date(self, symbol: str, trade_date: date) -> Optional[date]:
        """Get the nearest expiry for a given trading date."""
        pass

    def dense_1s_index(self, start: datetime, end: datetime) -> pl.DataFrame:
        """
        Create dense 1-second timestamp index for O(1) joins.

        Optimization: Pre-generate all seconds in trading session once,
        then join option data to this grid.
        """
        idx = pl.DataFrame({
            "ts": pl.datetime_range(start, end, "1s", time_zone=IST, eager=True)
        })
        return idx.with_columns(pl.col("ts").dt.cast_time_unit("ns"))

    def session_bounds(self, trade_date: date) -> tuple[datetime, datetime]:
        """Get session start and end datetimes for a trading date."""
        from dateutil import tz
        ist = tz.gettz(IST)
        start = datetime.combine(trade_date, self.config.session_start).replace(tzinfo=ist)
        end = datetime.combine(trade_date, self.config.session_end).replace(tzinfo=ist)
        return start, end

    def run_strategy(self, strategy: "Strategy", symbol: str,
                     start_date: date, end_date: date) -> BacktestResult:
        """
        Run a strategy over a date range.

        Args:
            strategy: Strategy instance with generate_signals() method
            symbol: BANKNIFTY or NIFTY
            start_date: Start date for backtest
            end_date: End date for backtest

        Returns:
            BacktestResult with trades and metrics
        """
        result = BacktestResult(
            strategy_name=strategy.name,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

        dates = self.get_available_dates(symbol)
        dates = [d for d in dates if start_date <= d <= end_date]

        if not dates:
            print(f"No data available for {symbol} between {start_date} and {end_date}")
            return result

        for trade_date in sorted(dates):
            expiry = self.get_expiry_for_date(symbol, trade_date)
            if not expiry:
                continue

            df = self.load_data(symbol, trade_date, expiry)
            if df is None or df.is_empty():
                continue

            # Generate signals using strategy
            signals = strategy.generate_signals(df, symbol, trade_date, expiry, self.config)

            # Process signals into trades
            day_trades = self._process_signals(signals, df, symbol, expiry)
            result.trades.extend(day_trades)

        result.compute_metrics()
        return result

    def _process_signals(self, signals: pl.DataFrame, data: pl.DataFrame,
                        symbol: str, expiry: date) -> List[Trade]:
        """
        Process signal DataFrame into Trade objects with PnL.

        Signals DataFrame should have:
        - ts: entry timestamp
        - strike: strike price
        - opt_type: CE or PE
        - side: BUY or SELL
        - sl_price: stop loss price (optional)
        - tp_price: take profit price (optional)
        """
        if signals is None or signals.is_empty():
            return []

        trades = []
        for row in signals.iter_rows(named=True):
            trade = Trade(
                symbol=symbol,
                expiry=expiry,
                strike=row["strike"],
                opt_type=row["opt_type"],
                side=row["side"],
                entry_ts=row["ts"],
                entry_price=row.get("entry_price", 0),
            )

            # Find exit based on SL/TP/EOD
            exit_info = self._find_exit(
                data=data,
                entry_ts=row["ts"],
                strike=row["strike"],
                opt_type=row["opt_type"],
                side=row["side"],
                entry_price=row.get("entry_price", 0),
                sl_pct=self.config.sl_pct,
                tp_pct=self.config.tp_pct,
                trail_pct=self.config.trail_pct,
            )

            if exit_info:
                trade.exit_ts = exit_info["ts"]
                trade.exit_price = exit_info["price"]
                trade.exit_reason = exit_info["reason"]

                # Calculate PnL
                if trade.side == "SELL":
                    trade.pnl = trade.entry_price - trade.exit_price
                else:
                    trade.pnl = trade.exit_price - trade.entry_price

                if trade.entry_price > 0:
                    trade.pnl_pct = (trade.pnl / trade.entry_price) * 100

            trades.append(trade)

        return trades

    def _find_exit(self, data: pl.DataFrame, entry_ts: datetime, strike: int,
                  opt_type: str, side: str, entry_price: float,
                  sl_pct: float, tp_pct: float, trail_pct: float) -> Optional[Dict]:
        """
        Find exit point for a trade using vectorized operations.

        Returns dict with ts, price, reason or None.
        """
        # Filter data for this strike/type after entry
        mask = (
            (pl.col("ts") > entry_ts) &
            (pl.col("strike") == strike) &
            (pl.col("opt_type") == opt_type)
        )
        post_entry = data.filter(mask).sort("ts")

        if post_entry.is_empty():
            return None

        # Calculate SL/TP levels
        if side == "SELL":
            sl_price = entry_price * (1 + sl_pct / 100)
            tp_price = entry_price * (1 - tp_pct / 100)
        else:
            sl_price = entry_price * (1 - sl_pct / 100)
            tp_price = entry_price * (1 + tp_pct / 100)

        # Vectorized SL/TP detection
        if side == "SELL":
            post_entry = post_entry.with_columns([
                (pl.col("close") >= sl_price).alias("hit_sl"),
                (pl.col("close") <= tp_price).alias("hit_tp"),
            ])
        else:
            post_entry = post_entry.with_columns([
                (pl.col("close") <= sl_price).alias("hit_sl"),
                (pl.col("close") >= tp_price).alias("hit_tp"),
            ])

        # Find first SL hit
        sl_rows = post_entry.filter(pl.col("hit_sl"))
        sl_ts = sl_rows["ts"][0] if not sl_rows.is_empty() else None

        # Find first TP hit
        tp_rows = post_entry.filter(pl.col("hit_tp"))
        tp_ts = tp_rows["ts"][0] if not tp_rows.is_empty() else None

        # Determine which came first
        if sl_ts and tp_ts:
            if sl_ts < tp_ts:
                return {"ts": sl_ts, "price": sl_rows["close"][0], "reason": "SL"}
            else:
                return {"ts": tp_ts, "price": tp_rows["close"][0], "reason": "TP"}
        elif sl_ts:
            return {"ts": sl_ts, "price": sl_rows["close"][0], "reason": "SL"}
        elif tp_ts:
            return {"ts": tp_ts, "price": tp_rows["close"][0], "reason": "TP"}

        # EOD exit
        last_row = post_entry.tail(1)
        if not last_row.is_empty():
            return {
                "ts": last_row["ts"][0],
                "price": last_row["close"][0],
                "reason": "EOD"
            }

        return None
