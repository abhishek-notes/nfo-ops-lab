#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pack Raw Options to Final Format

This script processes raw parquet files and converts them to the
partitioned packed format, 100% consistent with simple_pack.py output.
"""

import os
import glob
import argparse
from pathlib import Path
from datetime import date, time
import polars as pl


def load_calendar(path: str) -> pl.DataFrame:
    """Load and prepare expiry calendar."""
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
    ).drop_nulls(["symbol", "kind", "expiry"])

    # week_index for weekly expiries per (symbol, year, month)
    cal = (
        cal
        .with_columns([
            pl.col("expiry").dt.year().alias("_y"),
            pl.col("expiry").dt.month().alias("_m"),
        ])
        .sort(["symbol", "_y", "_m", "expiry"])
        .with_columns([
            pl.when(pl.col("kind") == "weekly")
              .then(pl.col("expiry").rank(method="dense").over(["symbol", "_y", "_m", "kind"]))
              .otherwise(0)
              .cast(pl.Int16)
              .alias("week_index"),
            (pl.col("kind") == "monthly").cast(pl.Int8).alias("is_monthly"),
            (pl.col("kind") == "weekly").cast(pl.Int8).alias("is_weekly"),
        ])
        .select(["symbol", "kind", "expiry", "week_index", "is_monthly", "is_weekly"])
        .unique()
        .sort(["symbol", "expiry"])
    )
    return cal


def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ensure df['timestamp'] is Datetime (IST). Use 'ts' when vendor left 1970.
    """
    cols = set(df.columns)
    if "timestamp" not in cols and "ts" in cols:
        df = df.with_columns(pl.col("ts").alias("timestamp"))

    if "timestamp" not in df.columns:
        return df

    # Cast timestamp to Datetime
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt in (pl.Int16, pl.Int32, pl.Int64, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float64, pl.Float32):
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))
    # If already Datetime, no conversion needed - just continue to timezone handling

    # Fix 1970s using 'ts' if present
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

    # Attach timezone (no shifting) - CRITICAL: use replace_time_zone not convert
    # Only apply if not already timezone-aware
    ts_dtype = df["timestamp"].dtype
    if hasattr(ts_dtype, 'time_zone') and ts_dtype.time_zone is None:
        df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata").alias("timestamp"))
    elif not hasattr(ts_dtype, 'time_zone'):
        # Not a datetime type yet - something went wrong
        pass
    return df


def ensure_ohlc(df: pl.DataFrame) -> pl.DataFrame:
    """Guarantee OHLC exists and sanitize vendor zeros."""
    close_sources = [
        "close", "Close", "ltp", "LTP", "price", "Price", "last", "Last",
        "closePrice", "ClosePrice", "avgPrice", "AvgPrice", "avg_price"
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

    for name in ("open", "high", "low"):
        if name not in df.columns:
            df = df.with_columns(pl.col("close").alias(name))

    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ("open", "high", "low", "close")])

    # Repair zeros/nulls
    df = df.with_columns([
        pl.when((pl.col("open") <= 0) | pl.col("open").is_null()).then(pl.col("close")).otherwise(pl.col("open")).alias("open"),
        pl.when((pl.col("high") <= 0) | pl.col("high").is_null()).then(pl.max_horizontal("open", "close")).otherwise(pl.col("high")).alias("high"),
        pl.when((pl.col("low") <= 0) | pl.col("low").is_null()).then(pl.min_horizontal("open", "close")).otherwise(pl.col("low")).alias("low"),
    ])
    # Enforce bounds
    df = df.with_columns([
        pl.min_horizontal("low", "open", "close").alias("low"),
        pl.max_horizontal("high", "open", "close").alias("high"),
    ])
    return df


def compute_vol_delta(df: pl.DataFrame) -> pl.DataFrame:
    """Compute volume delta from cumulative volume."""
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


def process_raw_file(path: str, cal: pl.DataFrame) -> pl.DataFrame | None:
    """Process a single raw parquet file and return packed DataFrame."""
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None

    # Get metadata from dataframe columns
    if 'symbol' not in df.columns or 'opt_type' not in df.columns or 'strike' not in df.columns:
        print(f"Skip (missing metadata): {path}")
        return None

    df = normalize_timestamp(df)
    if "timestamp" not in df.columns:
        print(f"Error {path}: no timestamp")
        return None

    df = ensure_ohlc(df)
    df = compute_vol_delta(df)

    # Keep one row per timestamp
    df = df.filter(pl.col("timestamp").is_not_null()).unique(["timestamp"]).sort("timestamp")

    # Market hours only (09:15-15:30 IST)
    df = df.filter(
        (pl.col("timestamp").dt.time() >= time(9, 15, 0)) &
        (pl.col("timestamp").dt.time() <= time(15, 30, 0))
    )

    if df.height == 0:
        return None

    # Add trade_date and map to next expiry
    df = df.with_columns(pl.col("timestamp").dt.date().alias("trade_date"))
    cal_sorted = cal.sort(["symbol", "expiry"])
    df_sorted = df.sort(["symbol", "trade_date"])
    dfj = df_sorted.join_asof(
        cal_sorted,
        left_on="trade_date",
        right_on="expiry",
        by="symbol",
        strategy="forward",
    )

    # Drop rows that failed to map
    dfj = dfj.filter(pl.col("expiry").is_not_null())

    if dfj.height == 0:
        return None

    # Rename kind -> expiry_type
    dfj = dfj.rename({"kind": "expiry_type"})

    # Final columns only
    dfj = dfj.select([
        "timestamp", "symbol", "opt_type", "strike",
        "open", "high", "low", "close", "vol_delta",
        "expiry", "expiry_type", "is_monthly", "is_weekly",
    ])
    return dfj


def write_partition(out_dir: Path, g: pl.DataFrame):
    """Write one parquet file per (symbol, expiry, opt_type, strike)."""
    symbol = g["symbol"][0]
    opt_type = g["opt_type"][0]
    strike = int(g["strike"][0])
    expiry = g["expiry"][0]  # python date
    yyyymm = f"{expiry.year:04d}{expiry.month:02d}"
    exp_str = expiry.strftime("%Y-%m-%d")

    out_path = out_dir / symbol / yyyymm / f"exp={exp_str}" / f"type={opt_type}" / f"strike={strike}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Final safety: keep only expected columns
    keep = {
        "timestamp", "symbol", "opt_type", "strike",
        "open", "high", "low", "close", "vol_delta",
        "expiry", "expiry_type", "is_monthly", "is_weekly",
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
    print(f"  wrote {out_path} ({g.height} rows)")


def flush_buckets(out_dir: Path, buckets: dict):
    """Flush all in-memory buckets to disk and clear them."""
    if not buckets:
        return
    print(f"Flushing {len(buckets)} partitions to disk...")
    for (symbol, expiry, opt_type, strike), chunks in list(buckets.items()):
        out = pl.concat(chunks, how="vertical_relaxed").unique(["timestamp"]).sort("timestamp")
        write_partition(out_dir, out)
    buckets.clear()


def main():
    parser = argparse.ArgumentParser(description='Pack raw parquet files to final format')
    parser.add_argument('--raw-dir', type=str, required=True, help='Directory with raw parquet files')
    parser.add_argument('--out-dir', type=str, required=True, help='Output directory for packed files')
    parser.add_argument('--calendar', type=str, required=True, help='Path to expiry calendar CSV')
    parser.add_argument('--limit', type=int, default=0, help='Max files to process (0=all)')
    parser.add_argument('--flush-every', type=int, default=500, help='Flush to disk every N files')

    args = parser.parse_args()

    print(f"Loading calendar from: {args.calendar}")
    cal = load_calendar(args.calendar)
    print(f"Calendar loaded: {cal.height} expiry records")

    files = glob.glob(os.path.join(args.raw_dir, "*.parquet"))
    files.sort()
    if args.limit and args.limit > 0:
        files = files[:args.limit]
    print(f"Found {len(files)} raw files to process")

    out_path = Path(args.out_dir)
    buckets: dict[tuple[str, date, str, int], list[pl.DataFrame]] = {}

    for i, path in enumerate(files, 1):
        if i % 100 == 0:
            print(f"... {i}/{len(files)}")

        df = process_raw_file(path, cal)
        if df is None or df.height == 0:
            continue

        # Split by expiry -> append to bucket
        for exp in df.select("expiry").unique().to_series():
            g = df.filter(pl.col("expiry") == exp)
            sym = g["symbol"][0]
            opt = g["opt_type"][0]
            k = int(g["strike"][0])
            key = (sym, exp, opt, k)
            buckets.setdefault(key, []).append(g)

        # Periodic flush
        if i % args.flush_every == 0:
            print(f"\nFLUSH @{i} files ...")
            flush_buckets(out_path, buckets)

    # Final flush
    print(f"\nFinal flush ({len(buckets)} partitions pending)...")
    flush_buckets(out_path, buckets)
    print("Done packing.")


if __name__ == '__main__':
    main()
