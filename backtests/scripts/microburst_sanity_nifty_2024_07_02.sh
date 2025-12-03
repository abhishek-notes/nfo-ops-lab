#!/usr/bin/env bash
set -euo pipefail

# Sanity check microburst candidates for a single day (NIFTY 2024-07-02).
# Usage:
#   bash backtests/scripts/microburst_sanity_nifty_2024_07_02.sh

if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}
$PY - << 'PY'
import polars as pl
from datetime import date, datetime, time
from dateutil import tz
IST='Asia/Kolkata'
def dt_ist(d,t):
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))

sym='NIFTY'; d=date(2024,7,2)
spot=pl.read_parquet(f'data/packed/spot/{sym}/{d.year:04d}{d.month:02d}/date={d}/ticks.parquet')\
        .select(['timestamp','close']).rename({'timestamp':'ts'})\
        .with_columns(pl.col('ts').dt.replace_time_zone(IST).dt.cast_time_unit('ns'))
sec=(spot.with_columns(pl.col('ts').dt.truncate('1s').alias('ts')).group_by('ts').agg(pl.col('close').last().alias('close')).sort('ts'))
s_ts=sec['ts']; s_px=sec['close']
lag=s_px.shift(3); pdiff=s_px-lag; pdiff1=pdiff.shift(1)
accel_drop=(pdiff.abs()<=pdiff1.abs())
sign_change=((pdiff*pdiff1)<=0).fill_null(False)
base=pl.DataFrame({'ts':s_ts,'px':s_px,'pd':pdiff,'ad':accel_drop,'sg':sign_change})
within=(pl.col('ts')>=pl.lit(dt_ist(d,time(9,20))).cast(pl.Datetime('ns', time_zone=IST))) & (pl.col('ts')<=pl.lit(dt_ist(d,time(15,15))).cast(pl.Datetime('ns', time_zone=IST)))
for th in (0.9,1.2,1.5):
    cond_up=(pl.col('pd')>=th) & (pl.col('ad')|pl.col('sg'))
    cond_dn=(pl.col('pd')<=-th) & (pl.col('ad')|pl.col('sg'))
    cands=(base.with_columns(pl.when(cond_up).then(pl.lit('up')).when(cond_dn).then(pl.lit('down')).otherwise(pl.lit(None)).alias('dir'))\
              .filter(within & pl.col('dir').is_not_null()))
    print(f'th={th}: candidates={cands.height}')
PY
