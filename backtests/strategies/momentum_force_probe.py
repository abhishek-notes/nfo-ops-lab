#!/usr/bin/env python3
from __future__ import annotations
import argparse
from datetime import datetime, date, time, timedelta
from pathlib import Path
import polars as pl
from dateutil import tz

IST='Asia/Kolkata'
SPOT_ROOT=Path('data/packed/spot')
OUT_DIR=Path('backtests/results')

def dt_ist(d,t):
    return datetime.combine(d,t).replace(tzinfo=tz.gettz(IST))

def dense_spot(symbol,d):
    yyyymm=f"{d.year:04d}{d.month:02d}"
    p=SPOT_ROOT/symbol/yyyymm/f"date={d}"/'ticks.parquet'
    df=pl.read_parquet(str(p)).select(['timestamp','close']).rename({'timestamp':'ts'})
    df=df.with_columns(pl.col('ts').dt.replace_time_zone(IST).dt.cast_time_unit('ns'))
    sec=(df.with_columns(pl.col('ts').dt.truncate('1s').alias('ts')).group_by('ts').agg(pl.col('close').last().alias('close')).sort('ts'))
    t0,t1=dt_ist(d,time(9,15)),dt_ist(d,time(15,30))
    idx=pl.DataFrame({'ts': pl.datetime_range(t0,t1,'1s',time_zone=IST,eager=True)})
    sec=idx.join(sec,on='ts',how='left').with_columns(pl.col('close').fill_null(strategy='forward'))
    return sec

def run(symbol,start,end,short_secs=3,price_thresh=2.0,hold_secs=10,max_per_day=50):
    d0=datetime.fromisoformat(start).date(); d1=datetime.fromisoformat(end).date()
    out=[]
    d=d0
    while d<=d1:
        try:
            spot=dense_spot(symbol,d)
        except Exception:
            d+=timedelta(days=1); continue
        s_ts=spot['ts']; s_close=spot['close']
        lag=s_close.shift(short_secs); pdiff=s_close-lag
        up=(pdiff>=price_thresh); down=(pdiff<=-price_thresh)
        taken=0
        i=0
        while i<spot.height and taken<max_per_day:
            u = bool(up[i]) if up[i] is not None else False
            dn = bool(down[i]) if down[i] is not None else False
            if not (u or dn):
                i+=1; continue
            entry_ts=s_ts[i]; entry_spot=float(s_close[i])
            j=min(i+hold_secs, spot.height-1)
            exit_ts=s_ts[j]; exit_spot=float(s_close[j])
            # proxy pnl: 0.5*spot_move for buy on up, sell on down
            if u:
                pnl=0.5*(exit_spot-entry_spot); side='buy_up'
            else:
                pnl=0.5*(entry_spot-exit_spot); side='sell_down'
            out.append({'symbol':symbol,'date':d.isoformat(),'entry_ts':entry_ts.isoformat(),'exit_ts':exit_ts.isoformat(),'side':side,'entry_spot':entry_spot,'exit_spot':exit_spot,'pnl':float(pnl)})
            taken+=1
            i+=short_secs
        d+=timedelta(days=1)
    if out:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        outp=OUT_DIR/f"momentum_force_{symbol}_{start}_{end}.parquet"
        pl.DataFrame(out).write_parquet(outp)
        print(f"Wrote {outp}")
    else:
        print('No trades')

if __name__=='__main__':
    ap=argparse.ArgumentParser()
    ap.add_argument('--symbol', required=True, choices=['BANKNIFTY','NIFTY'])
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--short-secs', type=int, default=3)
    ap.add_argument('--price-thresh', type=float, default=2.0)
    ap.add_argument('--hold-secs', type=int, default=10)
    ap.add_argument('--max-per-day', type=int, default=50)
    a=ap.parse_args()
    run(a.symbol,a.start,a.end,a.short_secs,a.price_thresh,a.hold_secs,a.max_per_day)
