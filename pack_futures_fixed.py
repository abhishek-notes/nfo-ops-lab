#!/usr/bin/env python3
# Pack futures by (symbol, expiry). Handles large futures files.
import os, glob, argparse
from pathlib import Path
from datetime import time
import polars as pl

RAW_DIR = "./data/raw/futures"
OUT_DIR = "./data/packed/futures"
CAL_PATH = "./meta/expiry_calendar.csv"

def load_calendar(path: str) -> pl.DataFrame:
    cal = pl.read_csv(path).rename({"Instrument":"symbol","Final_Expiry":"expiry","Expiry_Type":"kind"})
    cal = (cal.select(
            pl.col("symbol").str.to_uppercase(),
            pl.col("kind").str.to_lowercase(),
            pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"))
          .drop_nulls(["symbol","kind","expiry"])
          .filter(pl.col("kind")=="monthly")
          .select(["symbol","expiry"])
          .unique()
          .sort(["symbol","expiry"])
    )
    return cal

def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    if "timestamp" not in df.columns and "ts" in df.columns:
        df = df.with_columns(pl.col("ts").alias("timestamp"))
    if "timestamp" not in df.columns: return df
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt.is_integer() or dt.is_float():
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))
    if "ts" in df.columns:
        ts_parsed = (pl.col("ts").str.strptime(pl.Datetime, strict=False)
                     if df["ts"].dtype == pl.Utf8 else pl.col("ts").cast(pl.Datetime, strict=False))
        df = df.with_columns(pl.when(pl.col("timestamp").dt.year()<=1971).then(ts_parsed).otherwise(pl.col("timestamp")).alias("timestamp"))
    return df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata").alias("timestamp"))

def ensure_ohlc(df: pl.DataFrame) -> pl.DataFrame:
    srcs = ["close","Close","ltp","LTP","price","Price","last","Last","avgPrice","AvgPrice","avg_price"]
    close_expr = None
    for s in srcs:
        if s in df.columns: close_expr = pl.col(s); break
    if "close" not in df.columns:
        df = df.with_columns((close_expr.cast(pl.Float64, strict=False) if close_expr is not None else pl.lit(None, dtype=pl.Float64)).alias("close"))
    for c in ("open","high","low"):
        if c not in df.columns: df = df.with_columns(pl.col("close").alias(c))
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ("open","high","low","close")])
    df = df.with_columns([
        pl.when((pl.col("open")<=0)|pl.col("open").is_null()).then(pl.col("close")).otherwise(pl.col("open")).alias("open"),
        pl.when((pl.col("high")<=0)|pl.col("high").is_null()).then(pl.max_horizontal("open","close")).otherwise(pl.col("high")).alias("high"),
        pl.when((pl.col("low")<=0)|pl.col("low").is_null()).then(pl.min_horizontal("open","close")).otherwise(pl.col("low")).alias("low"),
    ])
    return df.with_columns([
        pl.min_horizontal("low","open","close").alias("low"),
        pl.max_horizontal("high","open","close").alias("high"),
    ])

def compute_vol_delta(df: pl.DataFrame) -> pl.DataFrame:
    if "volume" in df.columns:
        df = df.with_columns(pl.col("volume").cast(pl.Int64, strict=False).alias("_v"))
        return df.sort("timestamp").with_columns(pl.col("_v").diff().clip(lower_bound=0).fill_null(0).alias("vol_delta")).drop("_v")
    if "qty" in df.columns:
        return df.with_columns(pl.col("qty").cast(pl.Int64, strict=False).fill_null(0).alias("vol_delta"))
    return df.with_columns(pl.lit(0, dtype=pl.Int64).alias("vol_delta"))

def process_futures_file(path: str, cal: pl.DataFrame) -> None:
    """Process large futures file in chunks"""
    # Infer symbol from filename
    basename = os.path.basename(path).lower()
    if "banknifty" in basename:
        symbol = "BANKNIFTY"
    elif "nifty" in basename:
        symbol = "NIFTY"
    else:
        print(f"skip (unknown symbol): {path}")
        return
    
    print(f"Processing {symbol} futures from {path}")
    
    # Read in chunks to handle large files
    chunk_size = 1_000_000
    
    # First pass - get date range
    df_sample = pl.read_parquet(path, n_rows=10000)
    df_sample = normalize_timestamp(df_sample)
    
    if "timestamp" not in df_sample.columns:
        print(f"skip (no timestamp): {path}")
        return
    
    # Process in chunks
    scan = pl.scan_parquet(path)
    
    # Get calendar for this symbol
    cal_s = cal.filter(pl.col("symbol")==symbol).sort("expiry")
    if cal_s.is_empty():
        print(f"no calendar for {symbol}")
        return
    
    # Process data
    df = scan.collect()
    df = normalize_timestamp(df)
    df = ensure_ohlc(df)
    df = compute_vol_delta(df)
    
    df = (df
          .filter(pl.col("timestamp").is_not_null())
          .unique(["timestamp"])
          .filter((pl.col("timestamp").dt.time()>=time(9,15,0)) & (pl.col("timestamp").dt.time()<=time(15,30,0)))
          .with_columns([
              pl.lit(symbol).alias("symbol"),
              pl.col("timestamp").dt.date().alias("trade_date"),
          ])
          .select(["timestamp","symbol","open","high","low","close","vol_delta","trade_date"])
          .sort(["symbol","trade_date","timestamp"])
    )
    
    # Map to expiries
    df = df.join_asof(cal_s, left_on="trade_date", right_on="expiry", strategy="forward")
    df = df.drop_nulls(["expiry"])
    
    if df.height == 0:
        print(f"No data after filtering for {symbol}")
        return
    
    # Group by expiry and write
    for exp in df.select("expiry").unique().to_series():
        g = df.filter(pl.col("expiry")==exp)
        yyyymm = exp.isoformat()[:4] + exp.isoformat()[5:7]
        out_path = Path(OUT_DIR)/symbol/yyyymm/f"exp={exp.isoformat()}"/"ticks.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        g.write_parquet(str(out_path), compression="zstd", compression_level=3, statistics=True)
        print(f"  wrote {out_path} ({g.height} rows)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=RAW_DIR)
    ap.add_argument("--out", default=OUT_DIR)
    ap.add_argument("--cal", default=CAL_PATH)
    args = ap.parse_args()

    cal = load_calendar(args.cal)
    
    # Process the actual futures files
    futures_files = [
        os.path.join(args.raw, "banknifty_futures.parquet"),
        os.path.join(args.raw, "nifty_futures.parquet")
    ]
    
    for ff in futures_files:
        if os.path.exists(ff):
            process_futures_file(ff, cal)
        else:
            print(f"File not found: {ff}")

if __name__ == "__main__":
    main()