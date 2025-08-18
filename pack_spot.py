#!/usr/bin/env python3
# Robust SPOT packer (NIFTY/BANKNIFTY). Writes one parquet per (symbol, trade_date).
import os, glob, argparse
from pathlib import Path
from datetime import time
import polars as pl

RAW_DIR = "./data/raw/spot"
OUT_DIR = "./data/packed/spot"

def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    if "timestamp" not in df.columns and "ts" in df.columns:
        df = df.with_columns(pl.col("ts").alias("timestamp"))
    if "timestamp" not in df.columns:
        return df
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt.is_integer() or dt.is_float():
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))
    if "ts" in df.columns:
        ts_parsed = (pl.col("ts").str.strptime(pl.Datetime, strict=False)
                     if df["ts"].dtype == pl.Utf8 else pl.col("ts").cast(pl.Datetime, strict=False))
        df = df.with_columns(
            pl.when(pl.col("timestamp").dt.year() <= 1971).then(ts_parsed).otherwise(pl.col("timestamp")).alias("timestamp")
        )
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

def infer_symbol(path: str, df: pl.DataFrame) -> str | None:
    base = os.path.basename(path).lower()
    if "banknifty" in base: return "BANKNIFTY"
    if "nifty" in base: return "NIFTY"
    for c in ("symbol","Symbol","instrument","Instrument"):
        if c in df.columns:
            s = df[c][0]
            if s is None: continue
            s = str(s).upper()
            if "BANKNIFTY" in s: return "BANKNIFTY"
            if "NIFTY" in s: return "NIFTY"
    return None

def process_file(path: str) -> pl.DataFrame | None:
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        print(f"read error {path}: {e}"); return None
    sym = infer_symbol(path, df)
    if not sym: print(f"skip (symbol?): {path}"); return None
    df = normalize_timestamp(df)
    if "timestamp" not in df.columns: print(f"skip (no ts): {path}"); return None
    df = ensure_ohlc(df)
    df = compute_vol_delta(df)
    df = (df
          .filter(pl.col("timestamp").is_not_null())
          .unique(["timestamp"])
          .filter((pl.col("timestamp").dt.time()>=time(9,15,0)) & (pl.col("timestamp").dt.time()<=time(15,30,0)))
          .with_columns([
              pl.lit(sym).alias("symbol"),
              pl.col("timestamp").dt.date().alias("trade_date"),
          ])
          .select(["timestamp","symbol","open","high","low","close","vol_delta","trade_date"])
          .sort("timestamp")
    )
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=RAW_DIR)
    ap.add_argument("--out", default=OUT_DIR)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.raw, "*.parquet")))
    if args.limit>0: files = files[:args.limit]
    print(f"SPOT: {len(files)} files")
    by_day: dict[tuple[str, str], list[pl.DataFrame]] = {}

    for i, p in enumerate(files, 1):
        if i%500==0: print(f"... {i}/{len(files)}")
        df = process_file(p)
        if df is None or df.height==0: continue
        for d in df.select("trade_date").unique().to_series():
            g = df.filter(pl.col("trade_date")==d)
            sym = g["symbol"][0]
            key = (sym, d.isoformat())
            by_day.setdefault(key, []).append(g)

    print(f"Writing {len(by_day)} spot partitions...")
    for (sym, d), chunks in by_day.items():
        out = pl.concat(chunks).unique(["timestamp"]).sort("timestamp")
        yyyymm = d[:4]+d[5:7]
        out_path = Path(args.out)/sym/yyyymm/f"date={d}"/"ticks.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.write_parquet(str(out_path), compression="zstd", compression_level=3, statistics=True)
        print(f"  wrote {out_path} ({out.height} rows)")

if __name__ == "__main__":
    main()