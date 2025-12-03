"""
Raw Options Backtest Engine.

Loads data from raw parquet files with naming convention:
{symbol}{YY}{mon}{strike}{type}.parquet
e.g., banknifty25oct50000pe.parquet

Optimizations:
- File indexing with caching
- Lazy scanning with predicate pushdown
- Per-second aggregation
- IST timezone consistency
"""
from __future__ import annotations
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from functools import lru_cache
import polars as pl

from .base import BacktestEngine, IST
from ..config import BacktestConfig, RAW_OPTIONS_FOLDERS, STRIKE_STEP


class RawOptionsEngine(BacktestEngine):
    """
    Engine for backtesting with raw options data.

    Data format: {symbol}{YY}{mon}{strike}{type}.parquet
    Example: banknifty25oct50000pe.parquet

    Columns vary but typically include: timestamp/ts, price, qty, volume, etc.
    """

    # Month abbreviations for parsing filenames
    MONTHS = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    def __init__(self, config: BacktestConfig):
        super().__init__(config)
        self._folders = config.get_raw_folders()
        self._file_index: Dict[str, List[Tuple[Path, int, str, int, int]]] = {}  # symbol -> [(path, strike, opt_type, year, month)]

    def _parse_filename(self, path: Path) -> Optional[Tuple[str, int, str, int, int]]:
        """
        Parse raw option filename to extract metadata.

        Returns: (symbol, strike, opt_type, year, month) or None if parsing fails
        """
        name = path.stem.lower()

        # Match pattern: symbol + YY + mon + strike + type
        # e.g., banknifty25oct50000pe
        pattern = r'^(banknifty|nifty)(\d{2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d+)(ce|pe)$'
        match = re.match(pattern, name)

        if not match:
            return None

        symbol = match.group(1).upper()
        year = 2000 + int(match.group(2))
        month = self.MONTHS[match.group(3)]
        strike = int(match.group(4))
        opt_type = match.group(5).upper()

        return (symbol, strike, opt_type, year, month)

    def _build_index(self, symbol: str) -> None:
        """Build index of available files for a symbol."""
        if symbol in self._file_index:
            return

        self._file_index[symbol] = []

        for folder_name, folder_path in self._folders.items():
            if not folder_path.exists():
                continue

            for pq_file in folder_path.glob("*.parquet"):
                parsed = self._parse_filename(pq_file)
                if parsed is None:
                    continue

                file_symbol, strike, opt_type, year, month = parsed
                if file_symbol != symbol:
                    continue

                self._file_index[symbol].append((pq_file, strike, opt_type, year, month))

    def _get_date_range(self, path: Path) -> Tuple[Optional[date], Optional[date]]:
        """Get min and max dates from a parquet file."""
        try:
            # Try to get timestamp column
            lf = pl.scan_parquet(str(path))
            schema = lf.collect_schema()

            ts_col = None
            if "timestamp" in schema:
                ts_col = "timestamp"
            elif "ts" in schema:
                ts_col = "ts"
            else:
                return (None, None)

            df = lf.select(ts_col).collect()
            if df.is_empty():
                return (None, None)

            # Handle string timestamps
            if df[ts_col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(ts_col).str.strptime(pl.Datetime, strict=False)
                )

            min_ts = df[ts_col].min()
            max_ts = df[ts_col].max()

            min_date = min_ts.date() if min_ts else None
            max_date = max_ts.date() if max_ts else None

            return (min_date, max_date)
        except Exception:
            return (None, None)

    def get_available_dates(self, symbol: str) -> List[date]:
        """Get list of available trading dates for a symbol."""
        self._build_index(symbol)

        dates_set = set()
        for path, strike, opt_type, year, month in self._file_index.get(symbol, []):
            min_date, max_date = self._get_date_range(path)
            if min_date and max_date:
                current = min_date
                while current <= max_date:
                    dates_set.add(current)
                    current = date(current.year, current.month, current.day + 1) if current.day < 28 else date(
                        current.year if current.month < 12 else current.year + 1,
                        current.month + 1 if current.month < 12 else 1,
                        1
                    )

        return sorted(dates_set)

    def get_expiry_for_date(self, symbol: str, trade_date: date) -> Optional[date]:
        """
        Get the nearest expiry for a given trading date.

        For raw data, we infer expiry from the filename (month-end typically).
        """
        self._build_index(symbol)

        # Find files that could contain this trade date
        candidates = []
        for path, strike, opt_type, year, month in self._file_index.get(symbol, []):
            # Check if trade_date falls in this month
            if year == trade_date.year and month >= trade_date.month:
                candidates.append((year, month))
            elif year > trade_date.year:
                candidates.append((year, month))

        if not candidates:
            return None

        # Return the earliest expiry (last Thursday of month, approximated)
        candidates.sort()
        year, month = candidates[0]

        # Approximate last Thursday as day 25-31
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        for day in range(last_day, last_day - 7, -1):
            try:
                d = date(year, month, day)
                if d.weekday() == 3:  # Thursday
                    return d
            except ValueError:
                continue

        return date(year, month, last_day)

    def _find_files_for_trade(self, symbol: str, trade_date: date,
                              expiry: date) -> List[Tuple[Path, int, str]]:
        """Find all relevant files for a trade date."""
        self._build_index(symbol)

        results = []
        for path, strike, opt_type, year, month in self._file_index.get(symbol, []):
            # Check if file covers the expiry month
            if year == expiry.year and month == expiry.month:
                # Verify file contains data for trade_date
                min_date, max_date = self._get_date_range(path)
                if min_date and max_date and min_date <= trade_date <= max_date:
                    results.append((path, strike, opt_type))

        return results

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

        files = self._find_files_for_trade(symbol, trade_date, expiry)
        if not files:
            return None

        dfs = []
        for path, strike, opt_type in files:
            try:
                df = self._load_and_aggregate(path, start_dt, end_dt, strike, opt_type)
                if df is not None and not df.is_empty():
                    dfs.append(df)
            except Exception as e:
                continue

        if not dfs:
            return None

        result = pl.concat(dfs, how="vertical_relaxed")
        return result.sort(["ts", "strike", "opt_type"])

    def _load_and_aggregate(self, path: Path, start_dt: datetime, end_dt: datetime,
                            strike: int, opt_type: str) -> Optional[pl.DataFrame]:
        """
        Load raw file and aggregate to per-second data.

        Optimization: Uses lazy evaluation and per-second groupby aggregation.
        """
        try:
            lf = pl.scan_parquet(str(path))
            schema = lf.collect_schema()

            # Determine timestamp column
            ts_col = "timestamp" if "timestamp" in schema else "ts" if "ts" in schema else None
            if ts_col is None:
                return None

            # Select relevant columns
            cols = [ts_col]
            if "price" in schema:
                cols.append("price")
            if "close" in schema:
                cols.append("close")
            if "qty" in schema:
                cols.append("qty")
            if "volume" in schema:
                cols.append("volume")

            lf = lf.select(cols)

            # Handle string timestamps
            if schema[ts_col] == pl.Utf8:
                lf = lf.with_columns(
                    pl.col(ts_col).str.strptime(pl.Datetime, strict=False).alias(ts_col)
                )

            # Apply timezone and filter
            lf = lf.with_columns(
                pl.col(ts_col).dt.replace_time_zone(IST).dt.cast_time_unit("ns")
            )

            # Collect and filter to session
            df = lf.collect()

            # Filter to trading session
            df = df.filter(
                (pl.col(ts_col) >= start_dt) &
                (pl.col(ts_col) <= end_dt)
            )

            if df.is_empty():
                return None

            # Determine price column
            price_col = "price" if "price" in df.columns else "close" if "close" in df.columns else None
            if price_col is None:
                return None

            # Determine volume column
            vol_col = "qty" if "qty" in df.columns else "volume" if "volume" in df.columns else None

            # Aggregate to per-second
            agg_exprs = [
                pl.col(price_col).last().alias("close"),
            ]
            if vol_col:
                agg_exprs.append(pl.col(vol_col).sum().alias("vol_delta"))

            sec = (
                df.with_columns(pl.col(ts_col).dt.truncate("1s").alias("sec"))
                  .group_by("sec")
                  .agg(agg_exprs)
                  .sort("sec")
            )

            # Rename and add metadata
            sec = sec.rename({"sec": "ts"})
            sec = sec.with_columns([
                pl.lit(strike).alias("strike"),
                pl.lit(opt_type).alias("opt_type"),
            ])

            # Ensure vol_delta exists
            if "vol_delta" not in sec.columns:
                sec = sec.with_columns(pl.lit(0).alias("vol_delta"))

            return sec

        except Exception as e:
            return None

    def load_strike(self, symbol: str, trade_date: date, expiry: date,
                   strike: int, opt_type: str) -> Optional[pl.DataFrame]:
        """Load data for a specific strike and option type."""
        self._build_index(symbol)

        from dateutil import tz
        ist = tz.gettz(IST)
        start_dt = datetime.combine(trade_date, self.config.session_start).replace(tzinfo=ist)
        end_dt = datetime.combine(trade_date, self.config.session_end).replace(tzinfo=ist)

        for path, file_strike, file_opt_type, year, month in self._file_index.get(symbol, []):
            if file_strike == strike and file_opt_type == opt_type:
                if year == expiry.year and month == expiry.month:
                    return self._load_and_aggregate(path, start_dt, end_dt, strike, opt_type)

        return None
