#!/usr/bin/env python3
"""
Opening Range Breakout (ORB) via options proxy (scaffold).
Defines ORB 09:15–09:30. If breakout occurs in the next hour, enter long call/put at break time using ATM strike.
PnL simulated via spot-path delta proxy until 11:00 or stop/target.
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

def load_spot(symbol: str, d) -> pl.DataFrame:
    yyyymm=f"{d.year:04d}{d.month:02d}"
    p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
    return pl.read_parquet(p).rename({"timestamp":"ts"}).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))

def load_opt_close(symbol: str, expiry: str, strike: int, when: datetime, opt_type: str) -> float | None:
    yyyymm=expiry[:4]+expiry[5:7]
    p=Path(f"data/packed/options/{symbol}/{yyyymm}/exp={expiry}/type={opt_type}/strike={strike}.parquet")
    if not p.exists():
        return None
    df=pl.read_parquet(p).select("timestamp","close").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    try:
        return float(df.filter(pl.col("timestamp")==pl.lit(when).cast(pl.Datetime("ns", time_zone=IST))).select("close").item())
    except Exception:
        return None

def price_proxy(entry_opt: float, entry_spot: float, spot_now: float, side: str) -> float:
    # Long option directional proxy: assume delta ~ 0.5
    sign = 1 if side=="call" else -1
    return max(0.05, entry_opt + 0.5 * sign * (spot_now - entry_spot))

def run(symbol: str, date: str, expiry: str):
    d=datetime.fromisoformat(date).date(); step=100 if symbol=="BANKNIFTY" else 50
    spot=load_spot(symbol, d)
    o_start, o_end = dt_ist(d,time(9,15)), dt_ist(d,time(9,30))
    s=spot.filter((pl.col("ts")>=pl.lit(o_start).cast(pl.Datetime("ns", time_zone=IST))) & (pl.col("ts")<=pl.lit(o_end).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
    if s.is_empty():
        print("No OR window data"); return
    or_high=float(s["close"].max()); or_low=float(s["close"].min())
    watch_start, watch_end = o_end, dt_ist(d,time(11,0))
    watch=spot.filter((pl.col("ts")>=pl.lit(watch_start).cast(pl.Datetime("ns", time_zone=IST))) & (pl.col("ts")<=pl.lit(watch_end).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
    if watch.is_empty():
        print("No watch window data"); return
    entry=None; side=None
    for ts, px in zip(watch["ts"].to_list(), watch["close"].to_list()):
        x=float(px)
        if x>or_high:
            entry=(ts, x); side="call"; break
        if x<or_low:
            entry=(ts, x); side="put"; break
    if not entry:
        print("No breakout"); return
    entry_ts, entry_spot = entry
    k=nearest(entry_spot, step)
    opt_entry = load_opt_close(symbol, expiry, k, entry_ts, "CE" if side=="call" else "PE")
    if opt_entry is None:
        print("No option entry price"); return
    target = opt_entry * 1.2; stop = opt_entry * 0.8
    out_ts, out_val, reason = watch["ts"][-1], opt_entry, "hour_end"
    for ts, px in zip(watch["ts"].to_list(), watch["close"].to_list()):
        val = price_proxy(opt_entry, entry_spot, float(px), side)
        if val>=target: out_ts, out_val, reason = ts, val, "target"; break
        if val<=stop: out_ts, out_val, reason = ts, val, "stop"; break
    pnl=float(out_val - opt_entry)
    Path("backtests/results").mkdir(parents=True, exist_ok=True)
    out=Path("backtests/results")/f"orb_{symbol}_{date}_{expiry}_{k}.parquet"
    pl.DataFrame([{ "date":date, "expiry":expiry, "strike":k, "side":side, "entry":float(opt_entry), "exit":float(out_val), "pnl":pnl, "exit_reason":reason }]).write_parquet(out)
    print(f"Wrote {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--date", required=True); ap.add_argument("--expiry", required=True)
    a=ap.parse_args(); run(a.symbol,a.date,a.expiry)

