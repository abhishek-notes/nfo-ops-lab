#!/usr/bin/env python3
"""
Gamma Scalp (EOD) — exits only on target/stop/trail or end-of-day.

Differences from baseline:
- No hour-based exit; holds until 15:30 IST.
- Exit at EOD uses last computed proxy premium, not the entry premium.
- Optional debug dump for a single trade shows per-second spot, proxy value, and triggers.
"""
from __future__ import annotations
from datetime import datetime, timedelta, time
from pathlib import Path
import argparse
import polars as pl
from dateutil import tz
from backtests.utils.raw_opts import load_option_seconds_raw

IST = "Asia/Kolkata"

def parse_date(s: str):
    return datetime.fromisoformat(s).date()

def dt_ist(d, t):
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))

def nearest_strike(px: float, step: int) -> int:
    return int(round(px/step)*step)

def load_spot_day(symbol: str, d) -> pl.DataFrame:
    yyyymm = f"{d.year:04d}{d.month:02d}"
    p = f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
    df = pl.read_parquet(p).select(["timestamp","close"]).rename({"timestamp":"ts"})
    df = df.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    # 1s ffill
    sec = (df.with_columns(pl.col("ts").dt.truncate("1s").alias("ts"))
             .group_by("ts").agg(pl.col("close").last().alias("close"))
             .sort("ts"))
    t0, t1 = dt_ist(d, time(9,15)), dt_ist(d, time(15,30))
    idx = pl.DataFrame({"ts": pl.datetime_range(t0, t1, "1s", time_zone=IST, eager=True)})
    sec = idx.join(sec, on="ts", how="left").with_columns(pl.col("close").fill_null(strategy="forward"))
    return sec

def load_opt_series_raw_day(symbol: str, expiry: str, strike: int, opt_type: str, day: datetime) -> pl.DataFrame:
    # Load per-second CE/PE from raw options using actual trade price and qty
    d = day.date()
    exp = datetime.fromisoformat(expiry).date()
    sec = load_option_seconds_raw(symbol, d, exp, strike, opt_type)
    return sec if sec is not None else pl.DataFrame({"ts": [], "close": [], "vol": []})

def nearest_at(df: pl.DataFrame, ts_col: str, target: datetime, window_secs: int = 2) -> float | None:
    if df.is_empty():
        return None
    t_lit = pl.lit(target).cast(pl.Datetime("ns", time_zone=IST))
    exact = df.filter(pl.col(ts_col) == t_lit)
    if exact.height:
        try:
            return float(exact.select("close").item())
        except Exception:
            pass
    # ±window search
    start = target - timedelta(seconds=window_secs)
    end = target + timedelta(seconds=window_secs)
    s_l = pl.lit(start).cast(pl.Datetime("ns", time_zone=IST))
    e_l = pl.lit(end).cast(pl.Datetime("ns", time_zone=IST))
    cand = df.filter((pl.col(ts_col) >= s_l) & (pl.col(ts_col) <= e_l))
    if cand.is_empty():
        # fallback: last known before target
        prev = df.sort(ts_col).filter(pl.col(ts_col) <= t_lit).tail(1)
        return float(prev.select("close").item()) if prev.height else None
    cand = cand.with_columns((pl.col(ts_col).cast(pl.Int64) - t_lit.cast(pl.Int64)).abs().alias("dt"))
    return float(cand.sort("dt").select("close").head(1).item())

def option_price_proxy(entry_opt: float, entry_spot: float, spot_now: float, delta: float, side: str) -> float:
    sign = 1.0  # long straddle value increases with |spot move|
    # using magnitude delta ~ 1.0 for sum of legs approximation
    return max(0.05, float(entry_opt + sign * abs(spot_now - entry_spot)))

def run(symbol: str, start: str, end: str, expiry: str, anchors: str = "10:00,11:00,12:00,13:00,14:00,15:00",
        target_frac: float = 1.05, stop_frac: float = 0.90, trail_pts: float = 0.0,
        debug_sample: str | None = None):
    t_open, t_close = time(9,15), time(15,30)
    anchor_ts = [datetime.strptime(x.strip(), "%H:%M").time() for x in anchors.split(",")]
    d0, d1 = parse_date(start), parse_date(end)
    opts_root = "data/packed/options"
    step = 100 if symbol=="BANKNIFTY" else 50
    d = d0
    trades = []
    while d <= d1:
        day_open, day_close = dt_ist(d, t_open), dt_ist(d, t_close)
        try:
            spot = load_spot_day(symbol, d)
        except Exception:
            d = d + timedelta(days=1); continue
        for a in anchor_ts:
            A = dt_ist(d, a)
            if not (day_open <= A < day_close):
                continue
            a_lit = pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
            try:
                spot_open = float(spot.filter(pl.col("ts") == a_lit).select("close").item())
            except Exception:
                continue
            k_atm = nearest_strike(spot_open, step)
            # EOD window
            end_ts = day_close
            ce = load_opt_series_raw_day(symbol, expiry, k_atm, "CE", A)
            pe = load_opt_series_raw_day(symbol, expiry, k_atm, "PE", A)
            if ce.is_empty() or pe.is_empty():
                continue
            # Build straddle per-second from raw CE/PE prices
            strad = (
                ce.rename({"ts":"ts_ce","close":"ce"}).join(
                    pe.rename({"ts":"ts_pe","close":"pe"}), left_on="ts_ce", right_on="ts_pe", how="inner"
                )
            )
            if strad.is_empty():
                continue
            strad = strad.select([pl.col("ts_ce").alias("ts"), (pl.col("ce") + pl.col("pe")).alias("straddle")]).sort("ts")
            # Entry from nearest straddle at A
            str0 = nearest_at(strad.rename({"ts":"timestamp","straddle":"close"}), "timestamp", A, window_secs=2)
            if str0 is None:
                continue
            prem0 = float(str0)
            path = strad.filter((pl.col("ts")>=a_lit) & (pl.col("ts")<=pl.lit(end_ts).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","straddle").sort("ts")
            target = prem0 * target_frac
            stop   = prem0 * stop_frac
            exit_ts, exit_val, exit_reason = path["ts"][-1], prem0, "eod"
            best = prem0
            dbg_rows = []
            for ts, sv in zip(path["ts"].to_list(), path["straddle"].to_list()):
                val = float(sv)
                # persist last value for EOD fallback
                exit_ts, exit_val = ts, val
                if trail_pts and val > best:
                    best = val
                if val >= target:
                    exit_ts, exit_val, exit_reason = ts, val, "target"; break
                if val <= stop:
                    exit_ts, exit_val, exit_reason = ts, val, "stop"; break
                if trail_pts and (best - val) >= trail_pts:
                    exit_ts, exit_val, exit_reason = ts, val, "trail_stop"; break
                if debug_sample:
                    dbg_rows.append({"ts": ts, "straddle": float(val), "best": float(best)})
            trades.append({
                "date": d.isoformat(), "anchor": a.isoformat(), "expiry": expiry,
                "strike": k_atm, "entry_prem": prem0, "exit_prem": float(exit_val),
                "pnl": float(exit_val-prem0), "exit_reason": exit_reason,
            })
            # optional debug dump for one sample (first matching)
            if debug_sample and f"{d.isoformat()} {a.isoformat()}" == debug_sample and dbg_rows:
                outd = Path("backtests/results")/f"gamma_eod_debug_{symbol}_{d}_{a.isoformat().replace(':','')}_{expiry}.csv"
                pl.DataFrame(dbg_rows).write_csv(outd)
        d = d + timedelta(days=1)
    if trades:
        out = Path("backtests/results")/f"gamma_scalp_eod_{symbol}_{start}_{end}_{expiry}.parquet"
        pl.DataFrame(trades).write_parquet(out)
        print(f"Wrote {out}")
    else:
        print("No trades.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--expiry", required=True)
    ap.add_argument("--anchors", default="10:00,11:00,12:00,13:00,14:00,15:00")
    ap.add_argument("--target-frac", type=float, default=1.05)
    ap.add_argument("--stop-frac", type=float, default=0.90)
    ap.add_argument("--trail-pts", type=float, default=0.0)
    ap.add_argument("--debug-sample", default=None)
    a = ap.parse_args()
    run(a.symbol, a.start, a.end, a.expiry, a.anchors, a.target_frac, a.stop_frac, a.trail_pts, a.debug_sample)
