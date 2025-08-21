#!/usr/bin/env python3
"""
Short straddle premium decay (scaffold, improved entry/exit handling).
Sells ATM straddle at anchor; exits on decay/stop or hour end. Uses spot-path proxy and tolerant timestamp matching.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, time
from pathlib import Path
import polars as pl
from dateutil import tz

IST="Asia/Kolkata"
def dt_ist(d,t): return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))
def nearest(px,step): return int(round(px/step)*step)

def run(symbol: str, start: str, end: str, expiry: str, anchor="11:00", target_frac: float = 0.9, stop_frac: float = 1.1, trail_pts: float = 0.0):
    d0=datetime.fromisoformat(start).date(); d1=datetime.fromisoformat(end).date()
    step=100 if symbol=="BANKNIFTY" else 50
    t_open, t_close = time(9,15), time(15,30)
    A_time = datetime.strptime(anchor, "%H:%M").time()
    d=d0; trades=[]
    while d<=d1:
        yyyymm=f"{d.year:04d}{d.month:02d}"; p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
        try:
            spot=pl.read_parquet(p).rename({"timestamp":"ts"}).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        except Exception:
            d=d+timedelta(days=1); continue
        A=dt_ist(d,A_time); H=dt_ist(d,t_close)
        a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
        try:
            s0=float(spot.filter(pl.col("ts")==a_lit).select("close").item())
        except Exception:
            d=d+timedelta(days=1); continue
        k=nearest(s0, step); yyyymm_exp=expiry[:4]+expiry[5:7]
        def load(t):
            p=f"data/packed/options/{symbol}/{yyyymm_exp}/exp={expiry}/type={t}/strike={k}.parquet"
            if not Path(p).exists(): return None
            return pl.read_parquet(p).select("timestamp","close").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        ce,pe=load("CE"),load("PE")
        if ce is None or pe is None:
            d=d+timedelta(days=1); continue
        # tolerant fetch: exact second or nearest within ±2s; else last known before anchor
        def near(df):
            exact=df.filter(pl.col("timestamp")==a_lit).select("close")
            if exact.height:
                return float(exact.item())
            s=A - timedelta(seconds=2); e=A + timedelta(seconds=2)
            s_l=pl.lit(s).cast(pl.Datetime("ns", time_zone=IST)); e_l=pl.lit(e).cast(pl.Datetime("ns", time_zone=IST))
            cand=(df.filter((pl.col("timestamp")>=s_l)&(pl.col("timestamp")<=e_l))
                    .with_columns((pl.col("timestamp").cast(pl.Int64) - a_lit.cast(pl.Int64)).abs().alias("dt"))
                    .sort("dt"))
            if cand.height:
                return float(cand.select("close").head(1).item())
            # fallback: last known before anchor
            prev=df.sort("timestamp").filter(pl.col("timestamp")<=a_lit).tail(1)
            return float(prev.select("close").item()) if prev.height else None
        ce0=near(ce); pe0=near(pe)
        if ce0 is None or pe0 is None:
            d=d+timedelta(days=1); continue
        prem0=ce0+pe0
        path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=pl.lit(H).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
        target=prem0*target_frac; stop=prem0*stop_frac
        val=prem0; exit_ts=path["ts"][-1]; reason="eod"; entry=float(path["close"][0])
        best=val; hit_target=False
        last_est = prem0
        for ts, px in zip(path["ts"].to_list(), path["close"].to_list()):
            # simple proxy: straddle value decays with distance and time; add mild time decay 0.02 per min
            try:
                minutes = max(0.0, (ts - A).total_seconds() / 60.0)
            except Exception:
                minutes = 0.0
            decay = 0.02 * minutes
            val_est=max(0.05, prem0 - abs(float(px)-entry)*0.5 - decay)
            if val_est<=target:
                val=val_est; exit_ts=ts; reason="target"; hit_target=True
                best=val;  # best is lowest value for short premium
                if trail_pts<=0:
                    break
            last_est = val_est
            if hit_target and trail_pts>0:
                if val_est<best: best=val_est
                if val_est>=best+trail_pts:
                    val=val_est; exit_ts=ts; reason="trail_stop"; break
            if val_est>=stop:
                val=val_est; exit_ts=ts; reason="stop"; break
        # if we reached end without exits, use last estimated value
        if reason=="eod":
            val = last_est
        pnl=float(prem0 - val)
        trades.append({"date":d.isoformat(),"anchor":anchor,"expiry":expiry,"strike":k,"entry":prem0,"exit":val,"pnl":pnl,"exit_reason":reason})
        d=d+timedelta(days=1)
    if trades:
        out=Path("backtests/results")/f"short_straddle_{symbol}_{start}_{end}_{expiry}.parquet"
        pl.DataFrame(trades).write_parquet(out); print(f"Wrote {out}")
    else:
        print("No trades")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True); ap.add_argument("--expiry", required=True)
    ap.add_argument("--anchor", default="11:00")
    ap.add_argument("--target-frac", type=float, default=0.9)
    ap.add_argument("--stop-frac", type=float, default=1.1)
    ap.add_argument("--trail-pts", type=float, default=0.0)
    a=ap.parse_args(); run(a.symbol,a.start,a.end,a.expiry,a.anchor, a.target_frac, a.stop_frac, a.trail_pts)
