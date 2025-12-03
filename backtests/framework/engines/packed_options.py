"""
Packed Options Backtest Engine.

Loads data from the packed parquet format:
{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet

Optimizations:
- Lazy scanning with predicate pushdown
- Column projection (only load needed columns)
- Parallel file reads when loading multiple strikes
- IST timezone consistency
"""
from __future__ import annotations
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Optional, Set
from functools import lru_cache
import polars as pl

from .base import BacktestEngine, IST
from ..config import BacktestConfig, PACKED_OPTIONS_FOLDERS, SAMPLE_PACKED_OPTIONS, STRIKE_STEP


class PackedOptionsEngine(BacktestEngine):
    """
    Engine for backtesting with packed options data.

    Data format: {SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet
    Columns: timestamp, symbol, opt_type, strike, open, high, low, close, vol_delta, expiry, ...
    """

    def __init__(self, config: BacktestConfig):
        super().__init__(config)
        self._folders = config.get_packed_folders()
        # Add sample folder if main folders are empty
        if not self._folders and SAMPLE_PACKED_OPTIONS.exists():
            self._folders = {"sample": SAMPLE_PACKED_OPTIONS}
        self._index_cache: Dict[str, Dict[date, Set[date]]] = {}  # symbol -> {trade_date -> {expiries}}

    def _build_index(self, symbol: str) -> None:
        """Build index of available dates and expiries for a symbol."""
        if symbol in self._index_cache:
            return

        self._index_cache[symbol] = {}

        for folder_name, folder_path in self._folders.items():
            symbol_path = folder_path / symbol
            if not symbol_path.exists():
                continue

            for month_dir in symbol_path.iterdir():
                if not month_dir.is_dir():
                    continue

                for exp_dir in month_dir.glob("exp=*"):
                    try:
                        expiry_str = exp_dir.name.split("=")[1]
                        expiry = date.fromisoformat(expiry_str)

                        # Find trading dates from parquet files
                        for type_dir in exp_dir.iterdir():
                            if not type_dir.is_dir():
                                continue
                            for pq_file in type_dir.glob("strike=*.parquet"):
                                # Get unique dates from this file
                                try:
                                    df = pl.scan_parquet(str(pq_file)).select("timestamp").collect()
                                    if df.is_empty():
                                        continue
                                    dates = df.select(pl.col("timestamp").dt.date().unique()).to_series().to_list()
                                    for d in dates:
                                        if d not in self._index_cache[symbol]:
                                            self._index_cache[symbol][d] = set()
                                        self._index_cache[symbol][d].add(expiry)
                                except Exception:
                                    continue
                                break  # Only need to check one strike file
                            break  # Only need to check one type
                    except Exception:
                        continue

    def get_available_dates(self, symbol: str) -> List[date]:
        """Get list of available trading dates for a symbol."""
        self._build_index(symbol)
        return sorted(self._index_cache.get(symbol, {}).keys())

    def get_expiry_for_date(self, symbol: str, trade_date: date) -> Optional[date]:
        """Get the nearest expiry for a given trading date."""
        self._build_index(symbol)
        expiries = self._index_cache.get(symbol, {}).get(trade_date, set())
        if not expiries:
            return None
        # Return the nearest expiry that is >= trade_date
        valid = [e for e in expiries if e >= trade_date]
        return min(valid) if valid else max(expiries)

    def _find_strike_files(self, symbol: str, expiry: date, opt_type: str) -> List[Path]:
        """Find all strike parquet files for a given symbol, expiry, and option type."""
        files = []
        yyyymm = f"{expiry.year:04d}{expiry.month:02d}"

        for folder_path in self._folders.values():
            base = folder_path / symbol / yyyymm / f"exp={expiry}" / f"type={opt_type}"
            if base.exists():
                files.extend(base.glob("strike=*.parquet"))

        return files

    def load_data(self, symbol: str, trade_date: date, expiry: date) -> Optional[pl.DataFrame]:
        """
        Load all option data for a specific symbol, date, and expiry.

        Returns DataFrame with columns:
        - ts: timestamp (Datetime ns, IST)
        - close: last price
        - vol_delta: volume delta
        - strike: strike price
        - opt_type: CE or PE
        """
        from dateutil import tz
        ist = tz.gettz(IST)

        start_dt = datetime.combine(trade_date, self.config.session_start).replace(tzinfo=ist)
        end_dt = datetime.combine(trade_date, self.config.session_end).replace(tzinfo=ist)

        dfs = []
        for opt_type in ["CE", "PE"]:
            files = self._find_strike_files(symbol, expiry, opt_type)

            for pq_file in files:
                try:
                    # Extract strike from filename
                    strike_str = pq_file.stem.split("=")[1]
                    strike = int(strike_str)

                    # Load with column projection and predicate pushdown
                    lf = pl.scan_parquet(str(pq_file)).select([
                        "timestamp", "close", "vol_delta"
                    ])

                    # Filter to trading session
                    lf = lf.filter(
                        (pl.col("timestamp") >= start_dt) &
                        (pl.col("timestamp") <= end_dt)
                    )

                    df = lf.collect()
                    if df.is_empty():
                        continue

                    # Add strike and opt_type columns
                    df = df.with_columns([
                        pl.lit(strike).alias("strike"),
                        pl.lit(opt_type).alias("opt_type"),
                    ])

                    # Rename timestamp to ts
                    df = df.rename({"timestamp": "ts"})

                    dfs.append(df)

                except Exception as e:
                    continue

        if not dfs:
            return None

        # Concatenate all DataFrames
        result = pl.concat(dfs, how="vertical_relaxed")

        # Ensure IST timezone and ns precision
        if result["ts"].dtype != pl.Datetime("ns", time_zone=IST):
            result = result.with_columns(
                pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns")
            )

        return result.sort(["ts", "strike", "opt_type"])

    def load_strikes_around_atm(self, symbol: str, trade_date: date, expiry: date,
                                 atm_strike: int, n_strikes: int = 5) -> Optional[pl.DataFrame]:
        """
        Load option data for strikes around ATM.

        Args:
            symbol: BANKNIFTY or NIFTY
            trade_date: Trading date
            expiry: Expiry date
            atm_strike: ATM strike price
            n_strikes: Number of strikes on each side of ATM

        Returns:
            DataFrame with data for selected strikes
        """
        step = STRIKE_STEP.get(symbol, 100)
        strikes = [atm_strike + i * step for i in range(-n_strikes, n_strikes + 1)]

        from dateutil import tz
        ist = tz.gettz(IST)
        start_dt = datetime.combine(trade_date, self.config.session_start).replace(tzinfo=ist)
        end_dt = datetime.combine(trade_date, self.config.session_end).replace(tzinfo=ist)

        dfs = []
        yyyymm = f"{expiry.year:04d}{expiry.month:02d}"

        for opt_type in ["CE", "PE"]:
            for strike in strikes:
                for folder_path in self._folders.values():
                    pq_file = folder_path / symbol / yyyymm / f"exp={expiry}" / f"type={opt_type}" / f"strike={strike}.parquet"

                    if not pq_file.exists():
                        continue

                    try:
                        lf = pl.scan_parquet(str(pq_file)).select([
                            "timestamp", "close", "vol_delta"
                        ]).filter(
                            (pl.col("timestamp") >= start_dt) &
                            (pl.col("timestamp") <= end_dt)
                        )

                        df = lf.collect()
                        if df.is_empty():
                            continue

                        df = df.with_columns([
                            pl.lit(strike).alias("strike"),
                            pl.lit(opt_type).alias("opt_type"),
                        ]).rename({"timestamp": "ts"})

                        dfs.append(df)
                        break  # Found the file, move to next strike
                    except Exception:
                        continue

        if not dfs:
            return None

        result = pl.concat(dfs, how="vertical_relaxed")

        if result["ts"].dtype != pl.Datetime("ns", time_zone=IST):
            result = result.with_columns(
                pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns")
            )

        return result.sort(["ts", "strike", "opt_type"])

    def compute_volume_burst(self, df: pl.DataFrame, burst_secs: int = 30,
                             avg_secs: int = 300, multiplier: float = 1.5) -> pl.DataFrame:
        """
        Add volume burst detection columns using rolling windows.

        Optimization: Uses Polars rolling functions for vectorized computation.

        Args:
            df: DataFrame with ts, close, vol_delta, strike, opt_type
            burst_secs: Window for burst detection (default 30s)
            avg_secs: Window for average calculation (default 300s = 5min)
            multiplier: Burst threshold multiplier (default 1.5)

        Returns:
            DataFrame with added columns: vol_30s, base_30s, burst
        """
        # Group by strike and opt_type for per-contract calculations
        result = df.sort(["strike", "opt_type", "ts"])

        result = result.with_columns([
            pl.col("vol_delta").fill_null(0).alias("vol")
        ])

        # Rolling calculations within each strike/type group
        result = result.with_columns([
            pl.col("vol").rolling_sum(burst_secs).over(["strike", "opt_type"]).alias("vol_burst"),
            (pl.col("vol").rolling_mean(avg_secs).over(["strike", "opt_type"]) * burst_secs).alias("vol_base"),
        ])

        result = result.with_columns([
            (pl.col("vol_burst") > multiplier * pl.col("vol_base")).alias("burst")
        ])

        return result
