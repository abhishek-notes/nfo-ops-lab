#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple/robust NFO options packer with batched writes.

Only change from your previous version:
- Added on-disk flush in batches via --flush-every (writes/merges files periodically).

Everything else is the same:
- Parses symbol/opt_type/strike from messy filenames (handles 4–5 digit strikes)
- Fixes timestamps (uses 'ts' when vendor wrote 1970)
- Makes timestamps IST via replace_time_zone (no shifting)
- Maps each trade_date to NEXT expiry via join_asof(strategy="forward")
- Computes week_index per symbol-month for weekly expiries (internal only)
- Builds clean OHLC (repairs vendor zeros)
- Computes vol_delta from volume/qty
- Writes one parquet per (symbol, expiry, opt_type, strike)
- FINAL SCHEMA (per row):
  timestamp, symbol, opt_type, strike, open, high, low, close, vol_delta,
  expiry, expiry_type, is_monthly, is_weekly
"""

import os, re, glob, argparse
from pathlib import Path
from datetime import date, time
import polars as pl

RAW_DIR = "./data/raw/options"
OUT_DIR = "./data/packed/options"
CAL_PATH = "./meta/expiry_calendar.csv"

# ---------- filename parsing ----------

def parse_filename(path: str):
    """
    Extract (symbol, opt_type, strike) from filenames like:
      banknifty1941128500ce.parquet  -> strike=28500
      banknifty1980127300pe.parquet  -> strike=27300
      nifty23n0917000pe.parquet      -> strike=17000
      nifty20oct13800ce.parquet      -> strike=13800
    Rule: take trailing digit run before CE/PE and use the rightmost 5 (BANKNIFTY)
          or 4/5 (NIFTY) digits as strike.
    """
    base = os.path.basename(path).lower()
    if not base.endswith(".parquet"):
        return None
    stem = base[:-8]  # drop ".parquet"

    # symbol
    if stem.startswith("banknifty"):
        symbol, rest = "BANKNIFTY", stem[len("banknifty"):]
    elif stem.startswith("nifty"):
        symbol, rest = "NIFTY", stem[len("nifty"):]
    else:
        return None

    # opt type
    if rest.endswith("ce"):
        opt_type, core = "CE", rest[:-2]
    elif rest.endswith("pe"):
        opt_type, core = "PE", rest[:-2]
    else:
        return None

    # trailing digits before ce/pe
    m = re.search(r"(\d+)$", core)
    if not m:
        return None
    digits = m.group(1)

    strike = None
    if symbol == "BANKNIFTY":
        # prefer 5 digits (modern BN strikes)
        if len(digits) >= 5:
            v5 = int(digits[-5:])
            if 10000 <= v5 <= 100000:
                strike = v5
        # fallbacks
        if strike is None and len(digits) >= 6:
            v6 = int(digits[-6:])
            if 100000 <= v6 <= 999999:
                strike = v6
        if strike is None and len(digits) >= 4:
            v4 = int(digits[-4:])
            if 1000 <= v4 <= 9999:
                strike = v4
    else:  # NIFTY
        if len(digits) >= 5:
            v5 = int(digits[-5:])
            if 10000 <= v5 <= 50000:
                strike = v5
        if strike is None and len(digits) >= 4:
            v4 = int(digits[-4:])
            if 1000 <= v4 <= 9999:
                strike = v4

    if strike is None:
        return None
    return {"symbol": symbol, "opt_type": opt_type, "strike": int(strike)}

# ---------- calendar ----------

def load_calendar(path: str) -> pl.DataFrame:
    cal = pl.read_csv(path)
    cal = cal.rename({
        "Instrument": "symbol",
        "Final_Expiry": "expiry",
        "Expiry_Type": "kind",
    })
    cal = cal.select(
        pl.col("symbol").str.to_uppercase(),
        pl.col("kind").str.to_lowercase(),
        pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"),
    ).drop_nulls(["symbol","kind","expiry"])

    # week_index for weekly expiries per (symbol, year, month)
    cal = (
        cal
        .with_columns([
            pl.col("expiry").dt.year().alias("_y"),
            pl.col("expiry").dt.month().alias("_m"),
        ])
        .sort(["symbol","_y","_m","expiry"])
        .with_columns([
            pl.when(pl.col("kind") == "weekly")
              .then(pl.col("expiry").rank(method="dense").over(["symbol","_y","_m","kind"]))
              .otherwise(0)
              .cast(pl.Int16)
              .alias("week_index"),
            (pl.col("kind") == "monthly").cast(pl.Int8).alias("is_monthly"),
            (pl.col("kind") == "weekly").cast(pl.Int8).alias("is_weekly"),
        ])
        .select(["symbol","kind","expiry","week_index","is_monthly","is_weekly"])
        .unique()
        .sort(["symbol","expiry"])
    )
    return cal

# ---------- data fixes ----------

def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ensure df['timestamp'] is Datetime (IST). Use 'ts' when vendor left 1970 in 'timestamp'.
    """
    cols = set(df.columns)
    if "timestamp" not in cols and "ts" in cols:
        df = df.with_columns(pl.col("ts").alias("timestamp"))

    if "timestamp" not in df.columns:
        return df  # nothing we can do

    # cast 'timestamp' to Datetime
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt in (pl.Int16, pl.Int32, pl.Int64, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float64, pl.Float32):
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))
    # else already datetime

    # fix 1970s using 'ts' if present
    if "ts" in df.columns:
        if df["ts"].dtype == pl.Utf8:
            ts_parsed = pl.col("ts").str.strptime(pl.Datetime, strict=False)
        else:
            ts_parsed = pl.col("ts").cast(pl.Datetime, strict=False)
        df = df.with_columns(
            pl.when(pl.col("timestamp").dt.year() <= 1971)
              .then(ts_parsed)
              .otherwise(pl.col("timestamp"))
              .alias("timestamp")
        )

    # attach timezone (no shifting)
    df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata").alias("timestamp"))
    return df

def ensure_ohlc(df: pl.DataFrame) -> pl.DataFrame:
    """Guarantee OHLC exists and sanitize vendor zeros."""
    close_sources = [
        "close","Close","ltp","LTP","price","Price","last","Last",
        "closePrice","ClosePrice","avgPrice","AvgPrice","avg_price"
    ]
    close_expr = None
    for c in close_sources:
        if c in df.columns:
            close_expr = pl.col(c)
            break

    if "close" not in df.columns:
        if close_expr is not None:
            df = df.with_columns(close_expr.cast(pl.Float64, strict=False).alias("close"))
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("close"))

    for name in ("open","high","low"):
        if name not in df.columns:
            df = df.with_columns(pl.col("close").alias(name))

    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ("open","high","low","close")])

    # repair zeros/nulls
    df = df.with_columns([
        pl.when((pl.col("open") <= 0) | pl.col("open").is_null()).then(pl.col("close")).otherwise(pl.col("open")).alias("open"),
        pl.when((pl.col("high") <= 0) | pl.col("high").is_null()).then(pl.max_horizontal("open","close")).otherwise(pl.col("high")).alias("high"),
        pl.when((pl.col("low")  <= 0) | pl.col("low").is_null()).then(pl.min_horizontal("open","close")).otherwise(pl.col("low")).alias("low"),
    ])
    # enforce bounds
    df = df.with_columns([
        pl.min_horizontal("low","open","close").alias("low"),
        pl.max_horizontal("high","open","close").alias("high"),
    ])
    return df

def compute_vol_delta(df: pl.DataFrame) -> pl.DataFrame:
    if "volume" in df.columns:
        df = df.with_columns(pl.col("volume").cast(pl.Int64, strict=False).alias("_vol"))
        df = df.sort("timestamp").with_columns(
            pl.col("_vol").diff().clip(lower_bound=0).fill_null(0).alias("vol_delta")
        ).drop("_vol")
    elif "qty" in df.columns:
        df = df.with_columns(pl.col("qty").cast(pl.Int64, strict=False).fill_null(0).alias("vol_delta"))
    else:
        df = df.with_columns(pl.lit(0, dtype=pl.Int64).alias("vol_delta"))
    return df

# ---------- one-file process ----------

def process_file(path: str, cal: pl.DataFrame) -> pl.DataFrame | None:
    meta = parse_filename(path)
    if not meta:
        print(f"Skip (name parse): {path}")
        return None

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None

    df = df.with_columns([
        pl.lit(meta["symbol"]).alias("symbol"),
        pl.lit(meta["opt_type"]).alias("opt_type"),
        pl.lit(meta["strike"]).alias("strike"),
    ])

    df = normalize_timestamp(df)
    if "timestamp" not in df.columns:
        print(f"Error {path}: no timestamp")
        return None

    df = ensure_ohlc(df)
    df = compute_vol_delta(df)

    # keep one row per timestamp
    df = df.filter(pl.col("timestamp").is_not_null()).unique(["timestamp"]).sort("timestamp")

    # market hours only
    df = df.filter(
        (pl.col("timestamp").dt.time() >= time(9, 15, 0)) &
        (pl.col("timestamp").dt.time() <= time(15, 30, 0))
    )

    # add trade_date and map to next expiry
    df = df.with_columns(pl.col("timestamp").dt.date().alias("trade_date"))
    cal_sorted = cal.sort(["symbol","expiry"])
    df_sorted  = df.sort(["symbol","trade_date"])
    dfj = df_sorted.join_asof(
        cal_sorted,
        left_on="trade_date",
        right_on="expiry",
        by="symbol",
        strategy="forward",
    )

    # drop rows that failed to map
    dfj = dfj.filter(pl.col("expiry").is_not_null())

    # rename kind -> expiry_type; drop internal week_index from output
    dfj = dfj.rename({"kind": "expiry_type"})

    # final columns only
    dfj = dfj.select([
        "timestamp","symbol","opt_type","strike",
        "open","high","low","close","vol_delta",
        "expiry","expiry_type","is_monthly","is_weekly",
    ])
    return dfj

# ---------- writer helpers (batched flush) ----------

def _write_partition(out_dir: Path, g: pl.DataFrame):
    """Merge-with-existing and write one (symbol,expiry,opt_type,strike) file."""
    symbol   = g["symbol"][0]
    opt_type = g["opt_type"][0]
    strike   = int(g["strike"][0])
    expiry   = g["expiry"][0]  # python date
    yyyymm   = f"{expiry.year:04d}{expiry.month:02d}"
    exp_str  = expiry.strftime("%Y-%m-%d")

    out_path = out_dir / symbol / yyyymm / f"exp={exp_str}" / f"type={opt_type}" / f"strike={strike}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # final safety: drop any stray columns if they sneak in
    keep = {
        "timestamp","symbol","opt_type","strike",
        "open","high","low","close","vol_delta",
        "expiry","expiry_type","is_monthly","is_weekly",
    }
    if any(c not in keep for c in g.columns):
        g = g.select(sorted(list(keep), key=list(keep).index))

    try:
        if out_path.exists():
            old = pl.read_parquet(str(out_path))
            merged = pl.concat([old, g], how="vertical_relaxed").unique(["timestamp"]).sort("timestamp")
        else:
            merged = g.unique(["timestamp"]).sort("timestamp")
        merged.write_parquet(str(out_path), compression="zstd", compression_level=3, statistics=True)
    except Exception as e:
        print(f"Write error {out_path}: {e}")
        return
    print(f"  wrote {out_path} (+{g.height} rows in batch)")

def _flush_buckets(out_dir: Path, buckets: dict):
    """Flush all in-memory buckets to disk and clear them."""
    if not buckets:
        return
    print(f"Flushing {len(buckets)} partitions to disk...")
    for (symbol, expiry, opt_type, strike), chunks in list(buckets.items()):
        out = pl.concat(chunks, how="vertical_relaxed").unique(["timestamp"]).sort("timestamp")
        _write_partition(out_dir, out)
    buckets.clear()

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="process only N files (smoke test)")
    ap.add_argument("--flush-every", type=int, default=5000, help="flush to disk every K raw files")
    args = ap.parse_args()

    cal = load_calendar(CAL_PATH)

    files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    files.sort()
    if args.limit and args.limit > 0:
        files = files[:args.limit]
    print(f"Found {len(files)} files")

    out_dir = Path(OUT_DIR)
    # group buffers
    buckets: dict[tuple[str, date, str, int], list[pl.DataFrame]] = {}

    for i, path in enumerate(files, 1):
        if i % 500 == 0:
            print(f"... {i}/{len(files)}")

        df = process_file(path, cal)
        if df is None or df.height == 0:
            continue

        # split by expiry -> append to bucket
        for exp in df.select("expiry").unique().to_series():
            g = df.filter(pl.col("expiry") == exp)
            sym = g["symbol"][0]
            opt = g["opt_type"][0]
            k   = int(g["strike"][0])
            key = (sym, exp, opt, k)
            buckets.setdefault(key, []).append(g)

        # periodic flush
        if i % args.flush_every == 0:
            print(f"\nFLUSH @{i} files ...")
            _flush_buckets(out_dir, buckets)

    # final flush
    print(f"\nFinal flush ({len(buckets)} partitions pending)...")
    _flush_buckets(out_dir, buckets)
    print("Done.")

if __name__ == "__main__":
    main()