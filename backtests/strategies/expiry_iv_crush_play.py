#!/usr/bin/env python3
"""
Expiry-day IV crush short premium (scaffold).
Sell ATM premium at 10:00 on expiry day; exit before auction or on stop/target.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, time
from pathlib import Path
from dateutil import tz
import polars as pl

IST="Asia/Kolkata"

def dt_ist(d,t): return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))
def nearest(px,step): return int(round(px/step)*step)

def run(symbol: str, trade_date: str, expiry: str):
    d=datetime.fromisoformat(trade_date).date()
    yyyymm=f"{d.year:04d}{d.month:02d}"
    spot=pl.read_parquet(f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet").rename({"timestamp":"ts"})
    spot=spot.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    A=dt_ist(d,time(10,0)); E=dt_ist(d,time(15,15))
    a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
    s_open=float(spot.filter(pl.col("ts")==a_lit).select("close").item())
    step=100 if symbol=="BANKNIFTY" else 50; k=nearest(s_open, step)
    yyyymm_exp=expiry[:4]+expiry[5:7]
    def load(t):
        p=f"data/packed/options/{symbol}/{yyyymm_exp}/exp={expiry}/type={t}/strike={k}.parquet"
        if not Path(p).exists(): return None
        df=pl.read_parquet(p).select("timestamp","close").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        return df
    ce,pe=load("CE"),load("PE")
    if ce is None or pe is None:
        print("Missing legs"); return
    ce0=float(ce.filter(pl.col("timestamp")==a_lit).select("close").item())
    pe0=float(pe.filter(pl.col("timestamp")==a_lit).select("close").item())
    prem0=ce0+pe0
    path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=pl.lit(E).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
    # Sell premium: profit if premium decays; proxy by spot staying near entry
    target=prem0*0.85; stop=prem0*1.15
    val=prem0; exit_ts=path["ts"][-1]; reason="eod"
    entry=float(path["close"][0])
    for ts, px in zip(path["ts"].to_list(), path["close"].to_list()):
        dist=abs(float(px)-entry); val_est=max(0.05, prem0 - dist*0.5)
        if val_est<=target: val=val_est; exit_ts=ts; reason="target"; break
        if val_est>=stop: val=val_est; exit_ts=ts; reason="stop"; break
    pnl = (prem0 - val)  # short premium
    out=Path("backtests/results")/f"expiry_iv_crush_{symbol}_{trade_date}_{expiry}.parquet"
    pl.DataFrame([{ "date":trade_date, "expiry":expiry, "strike":k, "entry_prem":prem0, "exit_prem":val, "pnl":float(pnl), "exit_reason":reason }]).write_parquet(out)
    print(f"Wrote {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--date", required=True); ap.add_argument("--expiry", required=True)
    a=ap.parse_args(); run(a.symbol,a.date,a.expiry)

