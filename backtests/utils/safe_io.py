#!/usr/bin/env python3
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import polars as pl

IST = "Asia/Kolkata"

def ensure_ist_ns(df: pl.DataFrame, col: str) -> pl.DataFrame:
    if col not in df.columns:
        return df
    # If utf8 or integer, cast to datetime first
    dt = df[col].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, strict=False))
    elif isinstance(dt, pl.datatypes.PolarsDataType) and dt.is_numeric():
        df = df.with_columns(pl.col(col).cast(pl.Datetime("ns"), strict=False))
    # Attach/convert tz and time unit to ns
    return df.with_columns(pl.col(col).dt.replace_time_zone(IST).dt.cast_time_unit("ns"))

def read_spot_day(path: str) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        return pl.DataFrame({"ts": [], "close": []})
    df = pl.read_parquet(str(p))
    # Normalize column names
    if "timestamp" in df.columns:
        df = df.rename({"timestamp": "ts"})
    if "close" not in df.columns and "price" in df.columns:
        df = df.rename({"price": "close"})
    # Ensure required columns exist
    cols = set(df.columns)
    if not {"ts", "close"}.issubset(cols):
        # Try best-effort selection
        return pl.DataFrame({"ts": [], "close": []})
    df = ensure_ist_ns(df, "ts").select(["ts", "close"]).sort("ts")
    return df

def read_option_slice(path: str, start: datetime, end: datetime, columns=("timestamp","close","vol_delta")) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        return pl.DataFrame({c: [] for c in columns})
    # Scan and select available columns
    sel = [c for c in columns if c in pl.scan_parquet(str(p)).schema]
    lf = pl.scan_parquet(str(p)).select(sel)
    if "timestamp" in sel:
        lf = lf.with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        s_l = pl.lit(start).cast(pl.Datetime("ns", time_zone=IST))
        e_l = pl.lit(end).cast(pl.Datetime("ns", time_zone=IST))
        lf = lf.filter((pl.col("timestamp") >= s_l) & (pl.col("timestamp") <= e_l))
    df = lf.collect()
    # Backfill missing expected columns
    if "vol_delta" not in df.columns:
        df = df.with_columns(pl.lit(0, dtype=pl.Int64).alias("vol_delta"))
    if "close" not in df.columns:
        # If no close, create nulls to avoid crashes
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("close"))
    return df

def get_option_close_at(path: str, at: datetime) -> float | None:
    p = Path(path)
    if not p.exists():
        return None
    df = pl.read_parquet(str(p)).select(["timestamp","close"]) if "close" in pl.read_parquet(str(p)).columns else None
    if df is None:
        return None
    df = ensure_ist_ns(df, "timestamp")
    try:
        return float(df.filter(pl.col("timestamp") == pl.lit(at).cast(pl.Datetime("ns", time_zone=IST))).select("close").item())
    except Exception:
        return None

