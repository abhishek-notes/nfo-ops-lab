from __future__ import annotations
import polars as pl
from typing import Sequence
from .timeutil import IST, market_filter

def seconds_bars_ohlc(df: pl.DataFrame, tz: str = IST) -> pl.DataFrame:
    # group_by_dynamic to 1-second OHLCV
    out = (
        df.group_by_dynamic(index_column="timestamp", every="1s", label="left", closed="left")
          .agg([
              pl.first("open").alias("open"),
              pl.max("high").alias("high"),
              pl.min("low").alias("low"),
              pl.last("close").alias("close"),
              pl.sum("vol_delta").alias("vol"),
          ])
          .sort("timestamp")
    )
    return out

def add_micro_features(bars: pl.DataFrame, windows_s: Sequence[int] = (15, 30), ema_spans_s: Sequence[int] = (15, 30)) -> pl.DataFrame:
    out = bars
    # rolling sums for volume
    for w in windows_s:
        out = out.with_columns(
            pl.col("vol").rolling_sum(window_size=w, min_periods=1).alias(f"vol_{w}s")
        )
    # ratio 15/30 if both exist
    if 15 in windows_s and 30 in windows_s:
        out = out.with_columns((pl.col("vol_15s") / (pl.col("vol_30s") / 2).clip_min(1)).alias("vol_ratio_15_over_30"))
    # 1s return
    out = out.with_columns(((pl.col("close") / pl.col("close").shift(1)) - 1).fill_null(0.0).alias("r1s"))
    # EMAs
    for s in ema_spans_s:
        out = out.with_columns(pl.col("close").ewm_mean(span=s, adjust=False).alias(f"ema_{s}s"))
    return out

def gate_market_hours(df: pl.DataFrame, open_time: str = "09:15:00", close_time: str = "15:30:00") -> pl.DataFrame:
    return df.filter((pl.col("timestamp").dt.time() >= pl.time(open_time)) & (pl.col("timestamp").dt.time() <= pl.time(close_time)))
