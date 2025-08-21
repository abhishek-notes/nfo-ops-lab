#!/usr/bin/env python3
"""
VWAP-like mean reversion using spot average as proxy (since spot vol is absent).
At 13:00, compute day average price so far; if deviation > threshold, fade using options proxy to 14:00.
"""
from __future__ import annotations
import argparse
from datetime import datetime, time, timedelta
from pathlib import Path
import polars as pl
from dateutil import tz

IST="Asia/Kolkata"
def dt_ist(d,t): return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))
def nearest(px,step): return int(round(px/step)*step)

def run(symbol: str, date: str, expiry: str, threshold: float=0.003):
    d=datetime.fromisoformat(date).date(); step=100 if symbol=="BANKNIFTY" else 50
    yyyymm=f"{d.year:04d}{d.month:02d}"; p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
    spot=pl.read_parquet(p).rename({"timestamp":"ts"}).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    A=dt_ist(d,time(13,0)); B=dt_ist(d,time(14,0))
    a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST)); b_lit=pl.lit(B).cast(pl.Datetime("ns", time_zone=IST))
    till=spot.filter((pl.col("ts")>=pl.lit(dt_ist(d,time(9,15))).cast(pl.Datetime("ns", time_zone=IST))) & (pl.col("ts")<=a_lit)).select("close")
    if till.is_empty():
        print("No data till anchor"); return
    avg=float(till["close"].mean()); now=float(spot.filter(pl.col("ts")==a_lit).select("close").item())
    dev=(now-avg)/avg
    # Fade back to average
    side="put" if dev>threshold else ("call" if dev<-threshold else None)
    if side is None:
        print("No signal"); return
    k=nearest(now, step)
    yyyymm_exp=expiry[:4]+expiry[5:7]
    opt_type = "PE" if side=="put" else "CE"
    op=f"data/packed/options/{symbol}/{yyyymm_exp}/exp={expiry}/type={opt_type}/strike={k}.parquet"
    if not Path(op).exists():
        print("No option leg"); return
    df=pl.read_parquet(op).select("timestamp","close").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    entry=float(df.filter(pl.col("timestamp")==a_lit).select("close").item())
    path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=b_lit)).select("ts","close")
    # Mean reversion proxy: expect price move toward avg, option gains accordingly for counter-direction
    target=entry*1.1; stop=entry*0.9; exit_val=entry; reason="hour_end"
    for ts, px in zip(path["ts"].to_list(), path["close"].to_list()):
        # simple linear proxy
        delta=0.5 if side=="call" else -0.5
        val=max(0.05, entry + delta*(float(px)-now))
        if val>=target: exit_val=val; reason="target"; break
        if val<=stop: exit_val=val; reason="stop"; break
    pnl=float(exit_val-entry)
    out=Path("backtests/results")/f"vwap_mr_{symbol}_{date}_{expiry}_{k}.parquet"
    pl.DataFrame([{ "date":date, "expiry":expiry, "strike":k, "side":side, "entry":entry, "exit":exit_val, "pnl":pnl, "dev":float(dev)}]).write_parquet(out)
    print(f"Wrote {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--date", required=True); ap.add_argument("--expiry", required=True)
    ap.add_argument("--threshold", type=float, default=0.003)
    a=ap.parse_args(); run(a.symbol,a.date,a.expiry,a.threshold)

