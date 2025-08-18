from __future__ import annotations
import glob
import polars as pl
from .timeutil import IST

def read_spot_glob(pattern: str) -> pl.DataFrame:
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No spot files found for {pattern}")
    dfs = [pl.read_parquet(f) for f in files]
    df = pl.concat(dfs, how="vertical_relaxed")
    if "timestamp" not in df.columns:
        # sometimes first col name differs
        c0 = df.columns[0]
        df = df.rename({c0: "timestamp"})
    if "price" not in df.columns:
        p = [c for c in df.columns if c.lower()=="price"]
        if p: df = df.rename({p[0]: "price"})
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime).dt.replace_time_zone("UTC").dt.convert_time_zone(IST))
    return df.select(["timestamp","price"]).sort("timestamp")
