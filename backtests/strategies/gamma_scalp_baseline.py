#!/usr/bin/env python3
"""
Gamma scalp baseline scaffold.
Buys ATM straddle at anchor, delta-hedges along spot path, exits at hour end or stop/target.

Data dependencies:
- Spot (packed, per-day ticks): data/packed/spot/{SYMBOL}/{YYYYMM}/date=YYYY-MM-DD/ticks.parquet
- Options (packed, per-expiry/strike): data/packed/options/{SYMBOL}/{YYYYMM}/exp=YYYY-MM-DD/type={CE,PE}/strike=K.parquet

Note: This is a scaffold to illustrate wiring and evaluation; pricing uses spot delta-proxy.
"""
from __future__ import annotations
from datetime import datetime, timedelta, time
from pathlib import Path
import argparse
import polars as pl

IST = "Asia/Kolkata"

def parse_date(s: str):
    return datetime.fromisoformat(s).date()

def dt_ist(d, t):
    from dateutil import tz
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))

def dense_1s_index(start: datetime, end: datetime) -> pl.DataFrame:
    return pl.DataFrame({"ts": pl.datetime_range(start, end, "1s", time_zone=IST, eager=True)}).with_columns(pl.col("ts").dt.cast_time_unit("ns"))

def nearest_strike(px: float, step: int) -> int:
    return int(round(px/step)*step)

def load_spot_day(symbol: str, d) -> pl.DataFrame:
    yyyymm = f"{d.year:04d}{d.month:02d}"
    p = f"data/packed/spot/{symbol}/{yyyymm}/date={d}/ticks.parquet"
    df = pl.read_parquet(p).select(["timestamp","close"]).rename({"timestamp":"ts"})
    return df

def load_opt_series(root: str, symbol: str, expiry: str, strike: int, opt_type: str,
                    start: datetime, end: datetime) -> pl.DataFrame:
    yyyymm = expiry[:4]+expiry[5:7]
    p = Path(root)/symbol/yyyymm/f"exp={expiry}"/f"type={opt_type}"/f"strike={strike}.parquet"
    if not p.exists():
        return pl.DataFrame({"timestamp":[],"close":[],"vol_delta":[]})
    lf = pl.scan_parquet(str(p)).select("timestamp","close","vol_delta")
    lf = lf.with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    s_l = pl.lit(start).cast(pl.Datetime("ns", time_zone=IST))
    e_l = pl.lit(end).cast(pl.Datetime("ns", time_zone=IST))
    return lf.filter((pl.col("timestamp")>=s_l)&(pl.col("timestamp")<=e_l)).collect()

def nearest_at(df: pl.DataFrame, ts_col: str, target: datetime, window_secs: int = 2) -> float | None:
    """
    Return value of 'close' at exact target if present; otherwise the nearest within ±window_secs.
    Assumes df contains ts_col and 'close' with tz IST and ns unit.
    """
    if df.is_empty():
        return None
    t_lit = pl.lit(target).cast(pl.Datetime("ns", time_zone=IST))
    exact = df.filter(pl.col(ts_col) == t_lit)
    if exact.height:
        try:
            return float(exact.select("close").item())
        except Exception:
            pass
    # tolerant search within window
    start = target - timedelta(seconds=window_secs)
    end = target + timedelta(seconds=window_secs)
    s_l = pl.lit(start).cast(pl.Datetime("ns", time_zone=IST))
    e_l = pl.lit(end).cast(pl.Datetime("ns", time_zone=IST))
    cand = df.filter((pl.col(ts_col) >= s_l) & (pl.col(ts_col) <= e_l))
    if cand.is_empty():
        return None
    # choose nearest by absolute difference
    cand = cand.with_columns((pl.col(ts_col).cast(pl.Int64) - t_lit.cast(pl.Int64)).abs().alias("dt"))
    try:
        return float(cand.sort("dt").select("close").head(1).item())
    except Exception:
        return None

def option_price_proxy(entry_opt_px: float, entry_spot: float, spot_now: float, delta: float) -> float:
    # Long straddle; value approx sum of legs; use magnitude delta ~ 1.0 as coarse proxy
    return max(0.05, float(entry_opt_px + delta * (abs(spot_now-entry_spot))))

def run(symbol: str, start: str, end: str, expiry: str, anchors: str = "10:00,11:00,12:00,13:00,14:00,15:00"):
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
            spot = load_spot_day(symbol, d).with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        except Exception:
            d = d + timedelta(days=1); continue
        for a in anchor_ts:
            A = dt_ist(d, a)
            if not (day_open <= A < day_close):
                continue
            a_lit = pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))
            spot_open = nearest_at(spot, "ts", A, window_secs=2)
            if spot_open is None:
                continue
            k_atm = nearest_strike(spot_open, step)
            hour_end = min(A + timedelta(hours=1), day_close)
            # Load option closes at anchor for CE/PE
            ce = load_opt_series(opts_root, symbol, expiry, k_atm, "CE", A, hour_end)
            pe = load_opt_series(opts_root, symbol, expiry, k_atm, "PE", A, hour_end)
            if ce.is_empty() or pe.is_empty():
                continue
            ce0 = nearest_at(ce, "timestamp", A, window_secs=2)
            pe0 = nearest_at(pe, "timestamp", A, window_secs=2)
            if ce0 is None or pe0 is None:
                continue
            prem0 = ce0 + pe0
            # Spot path over window
            path = spot.filter((pl.col("ts")>=a_lit) & (pl.col("ts")<=pl.lit(hour_end).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
            entry_spot = float(path["close"][0])
            # Simple scalp: mark option proxy over path (delta ~ 1.0 for straddle magnitude change)
            target = prem0 * 1.05
            stop   = prem0 * 0.90
            exit_ts, exit_val, exit_reason = path["ts"][-1], prem0, "hour_end"
            for ts, px in zip(path["ts"].to_list(), path["close"].to_list()):
                val = option_price_proxy(prem0, entry_spot, float(px), 1.0)
                if val >= target:
                    exit_ts, exit_val, exit_reason = ts, val, "target"; break
                if val <= stop:
                    exit_ts, exit_val, exit_reason = ts, val, "stop"; break
            trades.append({
                "date": d.isoformat(), "anchor": a.isoformat(), "expiry": expiry,
                "strike": k_atm, "entry_prem": prem0, "exit_prem": exit_val,
                "pnl": float(exit_val-prem0), "exit_reason": exit_reason,
            })
        d = d + timedelta(days=1)
    if trades:
        out = Path("backtests/results")/f"gamma_scalp_{symbol}_{start}_{end}_{expiry}.parquet"
        pl.DataFrame(trades).write_parquet(out)
        print(f"Wrote {out}")
    else:
        print("No trades.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--expiry", required=True, help="YYYY-MM-DD (weekly/monthly)")
    args = ap.parse_args()
    run(args.symbol, args.start, args.end, args.expiry)
