#!/usr/bin/env python3
"""
Calendar Spread Weekly Proxy (scaffold).
On a trade date, take ATM front-week vs next-week premiums at 11:00 and hold 1 hour.
Direction is a parameter (front short / back long) to reflect term-structure stance.
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

def load_calendar(path="meta/expiry_calendar.csv") -> pl.DataFrame:
    cal=pl.read_csv(path).rename({"Instrument":"symbol","Final_Expiry":"expiry","Expiry_Type":"kind"})
    return cal.select(pl.col("symbol").str.to_uppercase(), pl.col("kind").str.to_lowercase(), pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry")).drop_nulls().sort(["symbol","expiry"])

def next_two_weeklies(symbol: str, trade_date, cal: pl.DataFrame):
    c=cal.filter((pl.col("symbol")==symbol)&(pl.col("kind")=="weekly")&(pl.col("expiry")>=trade_date)).select("expiry")
    if c.height<2: return None, None
    return c[0,0], c[1,0]

def run(symbol: str, date: str, stance: str="front_short_back_long"):
    d=datetime.fromisoformat(date).date(); step=100 if symbol=="BANKNIFTY" else 50
    cal=load_calendar()
    e1, e2 = next_two_weeklies(symbol, d, cal)
    if not e1 or not e2:
        print("No two weekly expiries"); return
    yyyymm=f"{d.year:04d}{d.month:02d}"; p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
    spot=pl.read_parquet(p).rename({"timestamp":"ts"}).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    A=dt_ist(d,time(11,0)); H=dt_ist(d,time(12,0))
    a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
    s0=float(spot.filter(pl.col("ts")==a_lit).select("close").item()); k=nearest(s0, step)
    def leg(expiry, opt_type):
        y=f"{expiry.year:04d}{expiry.month:02d}"; op=f"data/packed/options/{symbol}/{y}/exp={expiry}/type={opt_type}/strike={k}.parquet"
        if not Path(op).exists(): return None
        df=pl.read_parquet(op).select("timestamp","close").with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        return float(df.filter(pl.col("timestamp")==a_lit).select("close").item())
    # Use straddle proxies per expiry (sum of CE+PE)
    def straddle(expiry):
        ce=leg(expiry, "CE"); pe=leg(expiry, "PE")
        if ce is None or pe is None: return None
        return ce+pe
    front=straddle(e1); back=straddle(e2)
    if front is None or back is None: print("Missing legs"); return
    prem0 = back - front if stance=="front_short_back_long" else front - back
    # Proxy PnL over 1h: assume spread changes with spot distance modestly
    path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=pl.lit(H).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
    exit_prem=prem0
    for ts, px in zip(path["ts"].to_list(), path["close"].to_list()):
        dist = abs(float(px)-s0)
        # front decays faster; spread widens slightly with time and dist
        delta_spread = 0.1 * (dist/ max(1.0, s0)) * (1 if stance=="front_short_back_long" else -1)
        exit_prem = prem0 + delta_spread
    pnl=float(exit_prem - prem0)
    out=Path("backtests/results")/f"calendar_proxy_{symbol}_{date}_{e1}_{e2}_{k}.parquet"
    pl.DataFrame([{ "date":date, "front":e1.isoformat(), "back":e2.isoformat(), "atm":k, "stance":stance, "entry":prem0, "exit":exit_prem, "pnl":pnl }]).write_parquet(out)
    print(f"Wrote {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--date", required=True); ap.add_argument("--stance", default="front_short_back_long", choices=["front_short_back_long","front_long_back_short"])
    a=ap.parse_args(); run(a.symbol,a.date,a.stance)

