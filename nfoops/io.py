from __future__ import annotations
import os, re, math
import polars as pl
from typing import Optional
from .timeutil import IST

# Minimal schema across variants
MIN_COLS = ["timestamp","price","ts","symbol","opt_type","strike","year","month"]

def _read_any(path: str) -> pl.DataFrame:
    # Parquet only (fast path). Some files have 8 cols, some ~50. Let Polars infer, then normalize.
    df = pl.read_parquet(path, use_pyarrow=True)
    # normalize column names
    df = df.rename({c: c.strip() for c in df.columns})
    # Ensure mandatory columns exist
    for c in MIN_COLS:
        if c not in df.columns:
            # fill missing structural columns
            if c in ("ts","symbol","opt_type"): df = df.with_columns(pl.lit(None).alias(c))
            elif c in ("strike","year","month"): df = df.with_columns(pl.lit(None).cast(pl.Int32).alias(c))
            else: df = df.with_columns(pl.lit(None).alias(c))
    return df

def _fix_timestamp(df: pl.DataFrame, tz: str = IST) -> pl.DataFrame:
    # If timestamp is bogus (epoch/1970 or tz-naive), prefer 'ts' column (string) else keep 'timestamp' and cast to IST.
    out = df
    if df["timestamp"].dtype != pl.Datetime:
        out = out.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False, fmt=None).alias("timestamp"))
    # rows where year==1970 or timestamp < 1990: replace from 'ts' string
    bad = (out["timestamp"].dt.year() <= 1971) | (out["timestamp"] < pl.datetime(1990,1,1,0,0))
    if "ts" in out.columns:
        # Handle both string and datetime types for ts column
        if out["ts"].dtype == pl.Utf8:
            # ts is string, parse it
            out = out.with_columns(
                pl.when(bad & pl.col("ts").is_not_null())
                  .then(pl.col("ts").str.strptime(pl.Datetime, strict=False))
                  .otherwise(pl.col("timestamp"))
                  .alias("timestamp")
            )
        else:
            # ts is already datetime, use it directly
            out = out.with_columns(
                pl.when(bad & pl.col("ts").is_not_null())
                  .then(pl.col("ts"))
                  .otherwise(pl.col("timestamp"))
                  .alias("timestamp")
            )
    # make tz-aware IST
    # First check if timestamp has timezone info
    if out["timestamp"].dtype == pl.Datetime:
        # Timestamp is timezone-naive, convert to IST
        out = out.with_columns(
            pl.col("timestamp").dt.replace_time_zone("UTC").dt.convert_time_zone(tz).alias("timestamp")
        )
    else:
        # Timestamp already has timezone, just convert to IST
        out = out.with_columns(
            pl.col("timestamp").dt.convert_time_zone(tz).alias("timestamp")
        )
    return out

def _std_types(df: pl.DataFrame) -> pl.DataFrame:
    casts = {}
    if "symbol" in df.columns: casts["symbol"] = pl.Utf8
    if "opt_type" in df.columns: casts["opt_type"] = pl.Utf8
    if "strike" in df.columns: casts["strike"] = pl.Int32
    for c in ("price","open","high","low","close","avgPrice","qty","volume","volactual"):
        if c in df.columns: casts[c] = pl.Float64
    for c in ("year","month"):
        if c in df.columns: casts[c] = pl.Int32
    return df.with_columns([pl.col(k).cast(v) for k,v in casts.items()])

def read_option_file(path: str) -> pl.DataFrame:
    df = _read_any(path)
    df = _fix_timestamp(df)
    df = _std_types(df)

    # prefer OHLC; if missing, use 'price' replicated
    for c in ("open","high","low","close"):
        if c not in df.columns:
            df = df.with_columns(pl.col("price").alias(c))
    # derive per-tick traded volume as best-effort
    if "volume" in df.columns and df["volume"].null_count() < len(df) * 0.95:
        # many files have cumulative; compute delta within file
        df = df.sort("timestamp").with_columns(
            (pl.col("volume") - pl.col("volume").shift(1)).clip(lower_bound=0).fill_null(0).alias("vol_delta")
        )
    elif "qty" in df.columns:
        df = df.with_columns(pl.col("qty").fill_null(0).cast(pl.Int64).alias("vol_delta"))
    else:
        df = df.with_columns(pl.lit(0).alias("vol_delta"))

    # clean symbol/opt_type
    df = df.with_columns([
        pl.col("symbol").str.to_uppercase().alias("symbol"),
        pl.col("opt_type").str.to_uppercase().alias("opt_type")
    ])

    # drop obvious junk rows (no timestamp or price <= 0)
    df = df.filter(pl.col("timestamp").is_not_null() & (pl.col("close") > 0))

    # minimal selection
    keep = ["timestamp","symbol","opt_type","strike","open","high","low","close","vol_delta","year","month"]
    keep = [c for c in keep if c in df.columns]
    return df.select(keep).sort("timestamp")
