#!/usr/bin/env python3
"""
Scan raw options parquet files for corruption and produce a detailed report.

Checks per file:
- Readability: parquet can be read
- Row count: zero rows or very low rows (<= 10)
- Time columns: presence of `timestamp` or `ts`
- Normalized timestamp (after using `ts` fallback) non-null ratio
- Timestamp plausibility window (1971..2032, IST attached is OK)

Outputs:
- CSV at analysis_results/corrupted_raw_options.csv with one row per problematic file
- A brief summary printed to stdout

Notes:
- This script only checks raw options under data/raw/options/*.parquet
- Extend similarly for spot/futures if needed
"""

import os
import re
from pathlib import Path
from typing import Optional, Dict, Any, List

import polars as pl

RAW_DIR = Path("data/raw/options")
OUT_DIR = Path("analysis_results")
OUT_CSV = OUT_DIR / "corrupted_raw_options.csv"


def normalize_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    cols = set(df.columns)
    if "timestamp" not in cols and "ts" in cols:
        df = df.with_columns(pl.col("ts").alias("timestamp"))

    if "timestamp" not in df.columns:
        return df

    # Cast 'timestamp' to Datetime
    dt = df["timestamp"].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    elif dt.is_numeric():
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))

    # Use 'ts' to fix 1970/1971 vendor artifacts
    if "ts" in df.columns:
        ts_parsed = (
            pl.col("ts").str.strptime(pl.Datetime, strict=False)
            if df["ts"].dtype == pl.Utf8
            else pl.col("ts").cast(pl.Datetime, strict=False)
        )
        df = df.with_columns(
            pl.when(pl.col("timestamp").dt.year() <= 1971)
            .then(ts_parsed)
            .otherwise(pl.col("timestamp"))
            .alias("timestamp")
        )

    # Attach timezone (no shifting)
    if "timestamp" in df.columns:
        df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata").alias("timestamp"))
    return df


def infer_symbol(path: str) -> Optional[str]:
    base = os.path.basename(path).lower()
    if base.startswith("banknifty"):
        return "BANKNIFTY"
    if base.startswith("nifty"):
        return "NIFTY"
    return None


def infer_hint(path: str) -> str:
    """Return a short hint from filename, e.g., 'banknifty25may' to help regrouping repulls."""
    stem = os.path.basename(path).lower().split(".")[0]
    m = re.match(r"(banknifty|nifty)(\d{2}[a-z]{3})", stem)
    if m:
        return m.group(0)
    return stem[:18]


def check_file(path: str) -> Optional[Dict[str, Any]]:
    symbol = infer_symbol(path) or "?"
    hint = infer_hint(path)
    reasons: List[str] = []

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        return {
            "file": path,
            "symbol": symbol,
            "hint": hint,
            "rows": 0,
            "nonnull_ts": 0,
            "nonnull_ratio": 0.0,
            "tmin": None,
            "tmax": None,
            "reasons": f"read_error: {type(e).__name__}: {e}",
        }

    rows = df.height
    if rows == 0:
        reasons.append("zero_rows")

    has_ts_col = "timestamp" in df.columns
    has_ts_alt = "ts" in df.columns
    if not has_ts_col and not has_ts_alt:
        reasons.append("no_timestamp_columns(timestamp|ts)")

    df2 = normalize_timestamp(df)
    nonnull_ts = df2["timestamp"].is_not_null().sum() if "timestamp" in df2.columns else 0
    ratio = float(nonnull_ts) / rows if rows > 0 else 0.0

    if rows > 0 and nonnull_ts == 0:
        reasons.append("all_timestamps_null")
    elif ratio < 0.5:  # conservative threshold
        reasons.append("low_timestamp_coverage(<50%)")

    tmin = tmax = None
    if nonnull_ts > 0:
        tmin = df2["timestamp"].min()
        tmax = df2["timestamp"].max()
        # Rough plausibility window (packers expect modern dates; 1970 handled earlier)
        yr_min_ok = pl.Series([tmin]).dt.year()[0] if tmin is not None else None
        yr_max_ok = pl.Series([tmax]).dt.year()[0] if tmax is not None else None
        if yr_min_ok is not None and (yr_min_ok < 2010 or yr_min_ok > 2035):
            reasons.append("min_timestamp_out_of_range")
        if yr_max_ok is not None and (yr_max_ok < 2010 or yr_max_ok > 2035):
            reasons.append("max_timestamp_out_of_range")

    # Very low rows → usually placeholders; keep as a separate signal
    if 0 < rows <= 10:
        reasons.append("very_low_rows(<=10)")

    if not reasons:
        return None  # file looks OK

    return {
        "file": path,
        "symbol": symbol,
        "hint": hint,
        "rows": rows,
        "nonnull_ts": int(nonnull_ts),
        "nonnull_ratio": round(ratio, 4),
        "tmin": tmin,
        "tmax": tmax,
        "reasons": ",".join(reasons),
    }


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default=str(RAW_DIR), help="Directory of raw parquet files to scan")
    args = ap.parse_args()

    scan_dir = Path(args.raw_dir)
    print(f"Scanning raw options for corruption…\nSource: {scan_dir}")
    files = sorted(str(p) for p in scan_dir.glob("*.parquet"))
    print(f"Total files: {len(files):,}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    bad_rows: list[dict[str, Any]] = []
    for i, fp in enumerate(files, 1):
        if i % 2000 == 0:
            print(f"… {i:,}/{len(files):,}")
        rec = check_file(fp)
        if rec:
            bad_rows.append(rec)

    if bad_rows:
        df = pl.from_records(bad_rows)
        df = df.select([
            "symbol", "hint", "rows", "nonnull_ts", "nonnull_ratio", "tmin", "tmax", "reasons", "file",
        ]).sort(["symbol", "hint", "file"])  # stable, helpful order
        df.write_csv(OUT_CSV)
        # quick summary
        by_symbol = df.group_by("symbol").len().rename({"len": "files"})
        print("\nSummary by symbol:")
        print(by_symbol)
        by_hint = df.group_by(["symbol", "hint"]).len().sort(["symbol", "len"], descending=[False, True])
        # Only show top 20 groups to keep console readable
        print("\nTop problem groups (symbol, hint):")
        print(by_hint.head(20))
        print(f"\nWritten detailed report: {OUT_CSV}")
    else:
        print("No corrupted files found (per checks).")


if __name__ == "__main__":
    main()
