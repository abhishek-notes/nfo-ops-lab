#!/usr/bin/env python3
"""
Dispersion proxy (index vs basket) scaffold.
Proxy approach: Compare realized vol of index vs average realized vol of selected basket; take opposing vol stances.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, time
from pathlib import Path
import polars as pl
from dateutil import tz

IST="Asia/Kolkata"
def dt_ist(d,t): return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))

def rv(series: pl.Series, win=60):
    s=series.cast(pl.Float64); r=(s.shift(-1)-s)/s
    return r.rolling_std(win, min_periods=10)

def run(symbol: str, start: str, end: str):
    # Index is symbol; basket proxy: shifting strikes around ATM across anchors (simplified: use spot as proxy for components)
    d0=datetime.fromisoformat(start).date(); d1=datetime.fromisoformat(end).date()
    tA=time(10,0); d=d0; rows=[]
    while d<=d1:
        yyyymm=f"{d.year:04d}{d.month:02d}"; p=f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
        try:
            spot=pl.read_parquet(p).rename({"timestamp":"ts"}).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        except Exception:
            d=d+timedelta(days=1); continue
        A=dt_ist(d,tA); B=dt_ist(d,time(11,0))
        a_lit=pl.lit(A).cast(pl.Datetime("ns", time_zone=IST)); b_lit=pl.lit(B).cast(pl.Datetime("ns", time_zone=IST))
        path=spot.filter((pl.col("ts")>=a_lit)&(pl.col("ts")<=b_lit)).select("ts","close")
        if path.height<120: d=d+timedelta(days=1); continue
        rvi = rv(path["close"], 60).drop_nulls().mean()
        # Basket proxy: use earlier + later 10-min RV windows as pseudo-components
        path2=spot.filter((pl.col("ts")>=pl.lit(dt_ist(d,time(11,0))).cast(pl.Datetime("ns", time_zone=IST)) ) & (pl.col("ts")<=pl.lit(dt_ist(d,time(12,0))).cast(pl.Datetime("ns", time_zone=IST)))).select("close")
        rvb = rv(path2["close"],60).drop_nulls().mean()
        stance = "short_index_long_basket" if (rvi or 0) > (rvb or 0) else "long_index_short_basket"
        rows.append({"date":d.isoformat(),"rv_index":float(rvi or 0.0),"rv_basket":float(rvb or 0.0),"stance":stance})
        d=d+timedelta(days=1)
    if rows:
        out=Path("backtests/results")/f"dispersion_proxy_{symbol}_{start}_{end}.parquet"; pl.DataFrame(rows).write_parquet(out); print(f"Wrote {out}")
    else:
        print("No rows")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    a=ap.parse_args(); run(a.symbol,a.start,a.end)

