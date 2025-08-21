#!/usr/bin/env python3
"""
Prewarm per-second options cache for ATM±1 (CE/PE) per day/anchor.

For each trading day:
- Load spot 1s grid (09:15–15:30 IST)
- Compute ATM at configured anchors and derive ATM±1 strikes
- For each (strike, type), read the daily option ticks and aggregate to per-second
- Write to cache path: backtests/cache/seconds/{SYMBOL}/date={YYYY-MM-DD}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet

Reuses logic from atm_volume_ultra to ensure identical semantics.
"""
from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, date, time, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
import polars as pl

import sys, os
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from backtests.atm_volume_ultra import (
    IST, CONFIG, SpotStore, dt_ist, parse_hhmm, ensure_ist,
    option_file_path, option_day_seconds, nearest_strike, load_calendar
)

CACHE_DIR = Path("backtests/cache/seconds")

def cache_file_path(symbol: str, trade_date: date, expiry: date, opt_type: str, strike: int) -> Path:
    return CACHE_DIR / symbol / f"date={trade_date.isoformat()}" / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"

def pick_strikes_for_day(spot_sec: pl.DataFrame, anchors: list[datetime], step: int) -> list[int]:
    ks = set()
    s = spot_sec.lazy()
    for a in anchors:
        v = s.filter(pl.col("ts") == a).select(pl.col("close")).collect()
        if v.height == 0:
            continue
        atm = nearest_strike(float(v.item()), step)
        ks.add(atm - step); ks.add(atm); ks.add(atm + step)
    return sorted(ks)

def process_day(symbol: str, d: date, cal_df: pl.DataFrame, spot_store: SpotStore,
                burst_secs: int, avg_secs: int, step: int) -> dict:
    stats = {"day": d.isoformat(), "written": 0, "skipped": 0}
    t_open = parse_hhmm(CONFIG["session"]["open"]) 
    t_close = parse_hhmm(CONFIG["session"]["close"]) 
    anchors = [parse_hhmm(x) for x in CONFIG["session"]["anchors"]]
    day_open, day_close = dt_ist(d, t_open), dt_ist(d, t_close)

    # map expiry
    exp_row = cal_df.filter((pl.col("symbol")==symbol) & (pl.col("expiry")>=d)).select("expiry").head(1)
    if exp_row.height == 0:
        return stats
    exp = exp_row.item()

    # spot seconds for the day
    spot_sec = spot_store.load_day_seconds(symbol, day_open, day_close)
    if spot_sec.is_empty():
        return stats

    # anchors in bounds
    anchor_dts = [dt_ist(d, a) for a in anchors if day_open <= dt_ist(d, a) < day_close]
    if not anchor_dts:
        return stats
    strikes = pick_strikes_for_day(spot_sec, anchor_dts, step)
    if not strikes:
        return stats

    opts_root = CONFIG["options_root"]
    for k in strikes:
        for opt_type in ("CE", "PE"):
            # cache path
            cpath = cache_file_path(symbol, d, exp, opt_type, k)
            if cpath.exists():
                stats["skipped"] += 1
                continue
            f = option_file_path(opts_root, symbol, exp, opt_type, k)
            if not f.exists():
                continue
            # read and filter daily ticks
            lf = pl.scan_parquet(str(f)).select("timestamp","close","vol_delta")
            if lf.schema["timestamp"] == pl.Utf8:
                lf = lf.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
            lf = lf.with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
            s_l = pl.lit(day_open).cast(pl.Datetime("ns", time_zone=IST))
            e_l = pl.lit(day_close).cast(pl.Datetime("ns", time_zone=IST))
            lf = lf.filter((pl.col("timestamp")>=s_l) & (pl.col("timestamp")<=e_l))
            df = lf.collect()
            if df.is_empty():
                continue
            df = ensure_ist(df, "timestamp")
            sec = option_day_seconds(df, day_open, day_close, burst_secs, avg_secs, CONFIG["signal"]["multiplier"])
            cpath.parent.mkdir(parents=True, exist_ok=True)
            sec.write_parquet(str(cpath))
            stats["written"] += 1
    return stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="ALL", help="BANKNIFTY|NIFTY|ALL")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--max-workers", type=int, default=8)
    args = ap.parse_args()

    symbols = ["BANKNIFTY","NIFTY"] if args.symbol.upper()=="ALL" else [args.symbol.upper()]
    d0 = datetime.fromisoformat(args.start).date()
    d1 = datetime.fromisoformat(args.end).date()
    cal = load_calendar(CONFIG["calendar_csv"])  # columns: symbol, expiry
    spot_glob = CONFIG["spot_glob"]
    burst_secs = CONFIG["signal"]["burst_secs"]
    avg_secs = CONFIG["signal"]["avg_secs"]

    tasks = []
    for symbol in symbols:
        step = CONFIG["strike_step"].get(symbol, 50)
        spot_store = SpotStore(spot_glob)
        d = d0
        while d <= d1:
            tasks.append((symbol, d, cal, spot_store, burst_secs, avg_secs, step))
            d += timedelta(days=1)

    print(f"Prewarming seconds cache for {len(tasks)} day-symbols ...")
    out_stats = []
    with ProcessPoolExecutor(max_workers=args.max_workers) as ex:
        futs = [ex.submit(process_day, *t) for t in tasks]
        for i, fut in enumerate(as_completed(futs), 1):
            st = fut.result()
            out_stats.append(st)
            if i % 100 == 0:
                print(f"... {i}/{len(tasks)} processed")

    # summary
    written = sum(s.get("written",0) for s in out_stats)
    skipped = sum(s.get("skipped",0) for s in out_stats)
    print(f"Done. written={written} skipped={skipped}")

if __name__ == "__main__":
    main()
