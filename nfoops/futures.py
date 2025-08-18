from __future__ import annotations
import polars as pl
from .timeutil import IST

def read_futures_parquet(path: str, symbol: str) -> pl.DataFrame:
    df = pl.read_parquet(path, use_pyarrow=True)
    # normalize
    if "timestamp" not in df.columns:
        # some files may store datetime as index-like
        for c in df.columns:
            if "time" in c.lower():
                df = df.rename({c: "timestamp"}); break
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime).dt.replace_time_zone("UTC").dt.convert_time_zone(IST))
    if "close" not in df.columns and "price" in df.columns:
        df = df.with_columns(pl.col("price").alias("close"))
    return df.filter(pl.col("symbol")==symbol.upper()).select(["timestamp","close"]).rename({"close":"fut_close"}).sort("timestamp")
