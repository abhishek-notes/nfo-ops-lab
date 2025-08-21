#!/usr/bin/env python3
"""
Iron Condor Intraday (scaffold).
At 11:00, sell wings at ATM±1 step for current expiry; manage to 15:00 with target/stop using simple proxy.
"""
from __future__ import annotations
import argparse
from datetime import datetime, time
from pathlib import Path
import polars as pl
from dateutil import tz

IST="Asia/Kolkata"
def dt_ist(d,t): return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))
def nearest(px,step): return int(round(px/step)*step)

def run(symbol: str, date: str, expiry: str):
    d=datetime.fromisoformat(date).date(); step=100 if symbol=="BANKNIFTY" else 50
    yyyymm=f"{d.year:04d}{d.month:02d}"; p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
    spot=pl.read_parquet(p).rename({"timestamp":"ts"}).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    A=dt_ist(d,time(11,0)); E=dt_ist(d,time(15,0))
    a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
    s0=float(spot.filter(pl.col("ts")==a_lit).select("close").item())
    k=nearest(s0, step)
    legs=[("CE", k+step), ("PE", k-step)]
    yyyymm_exp=expiry[:4]+expiry[5:7]
    prem=0.0
    for t, ks in legs:
        op=f"data/packed/options/{symbol}/{yyyymm_exp}/exp={expiry}/type={t}/strike={ks}.parquet"
        if not Path(op).exists():
            print("Missing leg", t, ks); return
        df=pl.read_parquet(op).select("timestamp","close").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        prem += float(df.filter(pl.col("timestamp")==a_lit).select("close").item())
    # Short premium (both wings)
    path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=pl.lit(E).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
    target=prem*0.8; stop=prem*1.2
    val=prem; reason="eod"
    entry=float(path["close"][0])
    for ts, px in zip(path["ts"].to_list(), path["close"].to_list()):
        # symmetric decay proxy
        dist=abs(float(px)-entry)
        val_est=max(0.05, prem - 0.4*dist)
        if val_est<=target: val=val_est; reason="target"; break
        if val_est>=stop: val=val_est; reason="stop"; break
    pnl=float(prem - val)
    out=Path("backtests/results")/f"iron_condor_{symbol}_{date}_{expiry}_{k}.parquet"
    pl.DataFrame([{ "date":date, "expiry":expiry, "atm":k, "entry_prem":prem, "exit_prem":val, "pnl":pnl, "exit_reason":reason }]).write_parquet(out)
    print(f"Wrote {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--date", required=True); ap.add_argument("--expiry", required=True)
    a=ap.parse_args(); run(a.symbol,a.date,a.expiry)

