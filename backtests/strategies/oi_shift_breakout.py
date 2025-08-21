#!/usr/bin/env python3
"""
OI shift breakout (scaffold, proxy).
Uses options vol_delta as a proxy for OI/flow. Confirms with spot direction; trades breakout.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, time
from pathlib import Path
import polars as pl
from dateutil import tz

IST="Asia/Kolkata"

def dt_ist(d,t): return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))

def run(symbol: str, date: str, expiry: str, strike: int, side: str="with_flow"):
    d=datetime.fromisoformat(date).date(); yyyymm=f"{d.year:04d}{d.month:02d}"; t_open,t_close=time(9,15),time(15,30)
    spot=pl.read_parquet(f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet").rename({"timestamp":"ts"})
    spot=spot.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    A=dt_ist(d,time(10,0)); H=dt_ist(d,time(11,0))
    a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST)); h_lit=pl.lit(H).cast(pl.Datetime("ns", time_zone=IST))
    yyyymm_exp=expiry[:4]+expiry[5:7]
    def load(t):
        p=f"data/packed/options/{symbol}/{yyyymm_exp}/exp={expiry}/type={t}/strike={strike}.parquet"
        if not Path(p).exists(): return None
        return pl.read_parquet(p).select("timestamp","close","vol_delta").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    ce,pe=load("CE"),load("PE")
    if ce is None or pe is None:
        print("Missing legs"); return
    # Flow proxy
    ce_flow = ce.filter((pl.col("timestamp")>=a_lit)&(pl.col("timestamp")<=h_lit)).select(pl.col("vol_delta").sum().alias("f")).item()
    pe_flow = pe.filter((pl.col("timestamp")>=a_lit)&(pl.col("timestamp")<=h_lit)).select(pl.col("vol_delta").sum().alias("f")).item()
    path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=h_lit)).select("ts","close")
    direction = 1 if ce_flow>pe_flow else -1  # CE flow dominance => bullish proxy
    entry=float(path["close"][0]); exit=float(path["close"][-1])
    pnl = (exit-entry)*direction if side=="with_flow" else (entry-exit)*direction
    out=Path("backtests/results")/f"oi_shift_{symbol}_{date}_{expiry}_{strike}.parquet"
    pl.DataFrame([{ "date":date, "expiry":expiry, "strike":strike, "flow_ce":int(ce_flow), "flow_pe":int(pe_flow), "pnl_proxy":float(pnl)}]).write_parquet(out)
    print(f"Wrote {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--date", required=True); ap.add_argument("--expiry", required=True); ap.add_argument("--strike", required=True, type=int)
    ap.add_argument("--side", default="with_flow", choices=["with_flow","against_flow"])
    a=ap.parse_args(); run(a.symbol,a.date,a.expiry,a.strike,a.side)

