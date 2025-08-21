#!/usr/bin/env python3
"""
IV–RV spread proxy scaffold.
Compares intraday realized volatility vs an implied-vol proxy signal; goes long/short gamma accordingly using spot-path proxy.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, time
from pathlib import Path
import polars as pl
from dateutil import tz

IST="Asia/Kolkata"

def dt_ist(d,t):
    return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))

def realized_vol(path: pl.DataFrame, window_s: int=60) -> pl.Series:
    # Simple RV: std of 1s returns over window scaled to window
    s = path.select(pl.col("close")).to_series().cast(pl.Float64)
    r = (s.shift(-1) - s) / s
    return r.rolling_std(window_s, min_periods=10)

def run(symbol: str, start: str, end: str, iv_threshold: float=0.0):
    d0 = datetime.fromisoformat(start).date()
    d1 = datetime.fromisoformat(end).date()
    t_open, t_close = time(9,15), time(15,30)
    d=d0
    trades=[]
    while d<=d1:
        yyyymm=f"{d.year:04d}{d.month:02d}"
        p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
        try:
            spot=pl.read_parquet(p).select(["timestamp","close"]).rename({"timestamp":"ts"})
        except Exception:
            d=d+timedelta(days=1); continue
        spot=spot.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        A=dt_ist(d,time(10,0)); B=dt_ist(d,time(11,0))
        a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
        b_lit=pl.lit(B).cast(pl.Datetime("ns", time_zone=IST))
        path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=b_lit)).select("ts","close")
        if path.is_empty():
            d=d+timedelta(days=1); continue
        rv = realized_vol(path, 60)
        # Proxy IV signal: use options burst baseline already computed elsewhere; here simplified as constant threshold
        # Decision: if RV > threshold, treat as long vol; else short vol
        long_vol = float(pl.Series(rv).drop_nulls().mean() or 0.0) > iv_threshold
        entry= float(path["close"][0]); exit = float(path["close"][-1])
        pnl = abs(exit-entry) if long_vol else -abs(exit-entry)
        trades.append({"date":d.isoformat(),"anchor":"10:00","long_vol":long_vol,"pnl_proxy":pnl})
        d=d+timedelta(days=1)
    if trades:
        out=Path("backtests/results")/f"iv_rv_proxy_{symbol}_{start}_{end}.parquet"
        pl.DataFrame(trades).write_parquet(out); print(f"Wrote {out}")
    else:
        print("No trades")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--iv-threshold", type=float, default=0.0)
    a=ap.parse_args(); run(a.symbol,a.start,a.end,a.iv_threshold)

