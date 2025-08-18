from __future__ import annotations
import polars as pl
from datetime import time
import pytz

IST = "Asia/Kolkata"

def to_ist(expr: pl.Expr) -> pl.Expr:
    return pl.col(expr).dt.replace_time_zone("UTC").dt.convert_time_zone(IST) if isinstance(expr, pl.Expr) else expr

def ensure_ist(ts: pl.Expr | str) -> pl.Expr:
    e = pl.col(ts) if isinstance(ts, str) else ts
    return e.dt.cast_time_zone(IST) if e.dtype == pl.Datetime else e.dt.replace_time_zone(None).dt.replace_time_zone(IST)

def market_filter(ts_col: str, open_str: str, close_str: str) -> pl.Expr:
    return (pl.col(ts_col).dt.time() >= pl.time(open_str)) & (pl.col(ts_col).dt.time() <= pl.time(close_str))
