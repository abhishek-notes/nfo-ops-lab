#!/usr/bin/env python3
"""ChatGPT's raw vs packed audit"""
import polars as pl, glob, os, re
from datetime import timedelta, time

RAW_DIR = "./data/raw/options"
packed_path = "data/packed/options/BANKNIFTY/201911/exp=2019-11-07/type=CE/strike=30400.parquet"

# --- helpers copied from pack ---
def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    if "timestamp" not in df.columns and "ts" in df.columns:
        df = df.with_columns(pl.col("ts").alias("timestamp"))
    if "timestamp" not in df.columns:
        return df
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt in (pl.Int16, pl.Int32, pl.Int64, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float64, pl.Float32):
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))
    if "ts" in df.columns:
        ts_parsed = pl.col("ts").str.strptime(pl.Datetime, strict=False) if df["ts"].dtype==pl.Utf8 else pl.col("ts").cast(pl.Datetime, strict=False)
        df = df.with_columns(pl.when(pl.col("timestamp").dt.year()<=1971).then(ts_parsed).otherwise(pl.col("timestamp")).alias("timestamp"))
    return df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata").alias("timestamp"))

def ensure_close(df: pl.DataFrame) -> pl.DataFrame:
    for c in ["close","Close","ltp","LTP","price","Price","last","Last","closePrice","ClosePrice","avgPrice","AvgPrice","avg_price"]:
        if c in df.columns:
            return df.with_columns(pl.col(c).cast(pl.Float64, strict=False).alias("close"))
    return df.with_columns(pl.lit(None, dtype=pl.Float64).alias("close"))

# --- load packed & window ---
pf = pl.read_parquet(packed_path)
tmin, tmax = pf["timestamp"].min(), pf["timestamp"].max()
print("Packed rows:", pf.height, "|", tmin, "→", tmax)

# --- find candidate raws for same symbol/strike/type ---
m = re.search(r"/options/(.+?)/\d{6}/exp=\d{4}-\d{2}-\d{2}/type=(CE|PE)/strike=(\d+)\.parquet$", packed_path)
symbol, opt_type, strike = m.groups()
strike = int(strike)

globs = [
    f"{RAW_DIR}/{symbol.lower()}*{strike}{opt_type.lower()}.parquet",
    f"{RAW_DIR}/{symbol.lower()}*{strike:04d}{opt_type.lower()}.parquet",
    f"{RAW_DIR}/{symbol.lower()}*{strike:05d}{opt_type.lower()}.parquet",
]
files=set()
for g in globs: files.update(glob.glob(g))
files=sorted(files)
print("Matching raw files:", len(files))

# --- read/normalize a subset if huge ---
dfs=[]
for fp in files:
    try:
        df = pl.read_parquet(fp)
        df = ensure_close(normalize_timestamp(df))
        dfs.append(df.select(["timestamp","close"]))
    except Exception as e:
        pass
if not dfs:
    print("No readable raw files found.")
    raise SystemExit

raw = pl.concat(dfs, how="vertical_relaxed").filter(pl.col("timestamp").is_not_null())
raw = raw.filter((pl.col("timestamp") >= tmin - timedelta(days=2)) & (pl.col("timestamp") <= tmax + timedelta(days=2)))

raw_rows = raw.height
raw_unique = raw["timestamp"].n_unique()
raw_mkt = raw.filter((pl.col("timestamp").dt.time()>=time(9,15)) & (pl.col("timestamp").dt.time()<=time(15,30)))
raw_unique_mkt = raw_mkt["timestamp"].n_unique()

print("raw_rows:", raw_rows)
print("raw_unique_timestamps:", raw_unique)
print("raw_unique_market_hours:", raw_unique_mkt)
print("packed_rows:", pf.height)