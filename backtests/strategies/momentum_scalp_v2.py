#!/usr/bin/env python3
from __future__ import annotations
import argparse
from datetime import datetime, date, time, timedelta
from pathlib import Path
import polars as pl
from dateutil import tz

IST = "Asia/Kolkata"
SPOT_ROOT = Path("data/packed/spot")
OPT_ROOT = Path("data/packed/options")
OUT_DIR = Path("backtests/results")

# -------------------- helpers --------------------

def dt_ist(d: date, t: time) -> datetime:
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))

def dense_spot_1s(symbol: str, d: date) -> pl.DataFrame:
    yyyymm = f"{d.year:04d}{d.month:02d}"
    p = SPOT_ROOT/symbol/yyyymm/f"date={d}"/"ticks.parquet"
    df = pl.read_parquet(str(p)).select(["timestamp","close"]).rename({"timestamp":"ts"})
    df = df.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    sec = (
        df.with_columns(pl.col("ts").dt.truncate("1s").alias("ts"))
          .group_by("ts").agg(pl.col("close").last().alias("close"))
          .sort("ts")
    )
    t0, t1 = dt_ist(d, time(9,15)), dt_ist(d, time(15,30))
    idx = pl.DataFrame({"ts": pl.datetime_range(t0, t1, "1s", time_zone=IST, eager=True)})
    sec = idx.join(sec, on="ts", how="left").with_columns(pl.col("close").fill_null(strategy="forward"))
    # backfill first if needed
    if sec["close"].is_null().any():
        fc = float(df["close"].drop_nulls().head(1)[0])
        sec = sec.with_columns(pl.when(pl.col("close").is_null()).then(pl.lit(fc)).otherwise(pl.col("close")).alias("close"))
    return sec

def nearest_strike(px: float, step: int) -> int:
    return int(round(px/step)*step)

# list available strikes and choose nearest that exists in both CE & PE for this expiry

def choose_nearest_strike(symbol: str, d: date, k_atm: int) -> tuple[int, date] | None:
    # scan next 4 weeks for an expiry folder that has CE and PE common strikes
    for off in range(0, 28, 7):
        cand = d + timedelta(days=off)
        y = f"{cand.year:04d}{cand.month:02d}"
        base = OPT_ROOT/symbol/y/f"exp={cand:%Y-%m-%d}"
        ce_dir = base/"type=CE"; pe_dir = base/"type=PE"
        if not (ce_dir.exists() and pe_dir.exists()):
            continue
        ces = [int(p.name.split('=')[1].split('.')[0]) for p in ce_dir.glob('strike=*.parquet')]
        pes = [int(p.name.split('=')[1].split('.')[0]) for p in pe_dir.glob('strike=*.parquet')]
        common = set(ces).intersection(pes)
        if not common:
            continue
        k_use = min(common, key=lambda k: abs(k - k_atm))
        return k_use, cand
    return None

def load_seconds(symbol: str, d: date, exp: date, strike: int, opt_type: str, t0: datetime, t1: datetime) -> pl.DataFrame | None:
    y = f"{exp.year:04d}{exp.month:02d}"
    p = OPT_ROOT/symbol/y/f"exp={exp:%Y-%m-%d}"/f"type={opt_type}"/f"strike={strike}.parquet"
    if not p.exists():
        return None
    lf = pl.scan_parquet(str(p)).select("timestamp","close","vol_delta")
    lf = lf.with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    s_l = pl.lit(t0).cast(pl.Datetime("ns", time_zone=IST)); e_l = pl.lit(t1).cast(pl.Datetime("ns", time_zone=IST))
    df = lf.filter((pl.col("timestamp")>=s_l)&(pl.col("timestamp")<=e_l)).collect()
    if df.is_empty():
        return None
    sec = (
        df.with_columns(pl.col("timestamp").dt.truncate("1s").alias("ts"))
          .group_by("ts").agg([pl.col("vol_delta").sum().alias("vol"), pl.col("close").last().alias("close")])
          .sort("ts")
    )
    return sec

# simple option price proxy via delta

def option_price_proxy(entry_opt: float, entry_spot: float, spot_now: float, delta: float, side: str) -> float:
    sign = -1 if side == "sell" else 1
    est = entry_opt + sign * delta * (spot_now - entry_spot)
    return max(0.05, float(est))

# -------------------- main runner --------------------

def run(symbol: str, start: str, end: str,
        short_secs: int = 3, base_secs: int = 20,
        price_thresh: float = 3.0, vol_mult: float = 1.05,
        mode: str = "buy", target_pts: float = 2.0, stop_pts: float = 1.0,
        trail_pts: float = 0.5, drop_confirm_secs: int = 2,
        allow_price_only: bool = False,
        force_first: bool = False):
    d0 = datetime.fromisoformat(start).date(); d1 = datetime.fromisoformat(end).date()
    step = 100 if symbol=="BANKNIFTY" else 50
    out_rows = []
    d = d0
    while d <= d1:
        try:
            spot = dense_spot_1s(symbol, d)
        except Exception:
            d += timedelta(days=1); continue
        s_ts = spot["ts"]; s_close = spot["close"]
        lag = s_close.shift(short_secs)
        pdiff = s_close - lag
        up = (pdiff >= price_thresh)
        down = (pdiff <= -price_thresh)
        candidates = [i for i,(u,dn) in enumerate(zip(up.to_list(), down.to_list())) if u or dn]
        if not candidates:
            d += timedelta(days=1); continue
        t0 = dt_ist(d, time(9,15)); t1 = dt_ist(d, time(15,30))
        i = 0
        while i < len(candidates):
            idx = candidates[i]
            ts = s_ts[idx]; px = float(s_close[idx])
            direction = "up" if up[idx] else ("down" if down[idx] else None)
            if direction is None:
                i += 1; continue
            k_atm = nearest_strike(px, step)
            chosen = choose_nearest_strike(symbol, d, k_atm)
            if not chosen:
                i += 1; continue
            k_use, exp = chosen
            ce_sec = load_seconds(symbol, d, exp, k_use, "CE", t0, t1)
            pe_sec = load_seconds(symbol, d, exp, k_use, "PE", t0, t1)
            if ce_sec is None or pe_sec is None:
                i += 1; continue
            # align CE+PE vol
            vol = (ce_sec.select(["ts","vol"]).rename({"vol":"vce"})
                   .join(pe_sec.select(["ts","vol"]).rename({"vol":"vpe"}), on="ts", how="inner")
                   .with_columns((pl.col("vce")+pl.col("vpe")).alias("vol"))
                   .select(["ts","vol"]).sort("ts"))
            vol = spot.select("ts").join(vol, on="ts", how="left").with_columns(pl.col("vol").fill_null(0))
            vol_ws = vol.with_columns([
                pl.col("vol").rolling_sum(short_secs).fill_null(0).alias("vol_ws"),
                (pl.col("vol").rolling_mean(base_secs).fill_null(0)*short_secs).alias("vol_base")
            ]).select(["ts","vol_ws","vol_base"]).with_row_count("rn")
            pm_series = pl.Series('pm', [1 if (x is True) else 0 for x in ((up if direction=="up" else down).to_list())])
            sig = pl.DataFrame({"ts": s_ts, "pm": pm_series}).with_row_count("rn")
            sig = sig.join(vol_ws, on="rn", how="inner")
            if vol_mult <= 0.0 or allow_price_only:
                sig = sig.filter((pl.col("rn")>=idx)&(pl.col("pm")==1))
            else:
                sig = sig.filter((pl.col("rn")>=idx)&(pl.col("pm")==1)&(pl.col("vol_ws")>vol_mult*pl.col("vol_base")))
            if sig.is_empty():
                if (vol_mult <= 0.0 or allow_price_only or force_first):
                    entry_ts = s_ts[idx]
                else:
                    i += 1; continue
            entry_ts = sig["ts"][0]
            # build path from entry to end-of-day
            path = spot.filter(pl.col("ts")>=pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))).select(["ts","close"])
            if path.is_empty():
                i += 1; continue
            # entry option price
            leg = ce_sec if direction=="up" else pe_sec
            try:
                entry_opt = float(leg.filter(pl.col("ts")==pl.lit(entry_ts)).select("close").item())
            except Exception:
                entry_opt = float(leg.sort("ts").filter(pl.col("ts")<=pl.lit(entry_ts)).tail(1).select("close").item())
            entry_spot = float(path["close"][0])
            side = ("sell" if mode=="sell_opposite" else "buy"); delta=0.5
            tgt = entry_opt + (target_pts if side=="buy" else -target_pts)
            stp = entry_opt - (stop_pts if side=="buy" else -stop_pts)
            best = entry_opt; exit_ts = path["ts"][-1]; exit_opt = entry_opt; exit_reason = "eod"
            drop_cnt = 0
            for ts2, px2 in zip(path["ts"].to_list(), path["close"].to_list()):
                val = option_price_proxy(entry_opt, entry_spot, float(px2), delta, side)
                # momentum drop on price only
                j = int((ts2.timestamp() - entry_ts.timestamp())) if hasattr(ts2, 'timestamp') else 0
                # simple direction check over short window
                if direction=="up":
                    ok = (float(px2) - entry_spot) >= 0
                else:
                    ok = (float(px2) - entry_spot) <= 0
                if not ok:
                    drop_cnt += 1
                    if drop_cnt >= drop_confirm_secs:
                        exit_ts, exit_opt, exit_reason = ts2, val, "mom_drop"; break
                else:
                    drop_cnt = 0
                # trailing
                if trail_pts>0:
                    if side=="sell":
                        if val < best: best = val
                        if val >= best + trail_pts:
                            exit_ts, exit_opt, exit_reason = ts2, val, "trail"; break
                    else:
                        if val > best: best = val
                        if val <= best - trail_pts:
                            exit_ts, exit_opt, exit_reason = ts2, val, "trail"; break
                # hard targets/stops
                if side=="sell":
                    if val <= tgt: exit_ts, exit_opt, exit_reason = ts2, val, "target"; break
                    if val >= stp: exit_ts, exit_opt, exit_reason = ts2, val, "stop"; break
                else:
                    if val >= tgt: exit_ts, exit_opt, exit_reason = ts2, val, "target"; break
                    if val <= stp: exit_ts, exit_opt, exit_reason = ts2, val, "stop"; break
            pnl = (exit_opt - entry_opt) if side=="buy" else (entry_opt - exit_opt)
            out_rows.append({
                "symbol": symbol,
                "date": d.isoformat(),
                "entry_ts": entry_ts.isoformat(),
                "exit_ts": exit_ts.isoformat(),
                "direction": direction,
                "mode": mode,
                "strike": int(k_use),
                "entry_opt": float(entry_opt),
                "exit_opt": float(exit_opt),
                "pnl": float(pnl),
                "exit_reason": exit_reason,
            })
            i += 1
        d += timedelta(days=1)
    if out_rows:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        out = OUT_DIR/f"momentum_scalp_v2_{symbol}_{start}_{end}_{mode}.parquet"
        pl.DataFrame(out_rows).write_parquet(out)
        print(f"Wrote {out}")
    else:
        print("No trades")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--mode", default="buy", choices=["buy","sell_opposite"])
    ap.add_argument("--short-secs", type=int, default=3)
    ap.add_argument("--base-secs", type=int, default=20)
    ap.add_argument("--price-thresh", type=float, default=3.0)
    ap.add_argument("--vol-mult", type=float, default=1.05)
    ap.add_argument("--target-pts", type=float, default=2.0)
    ap.add_argument("--stop-pts", type=float, default=1.0)
    ap.add_argument("--trail-pts", type=float, default=0.5)
    ap.add_argument("--drop-confirm-secs", type=int, default=2)
    ap.add_argument("--allow-price-only", action='store_true', default=False)
    ap.add_argument("--force-first", action='store_true', default=False)
    a = ap.parse_args()
    run(a.symbol, a.start, a.end, a.short_secs, a.base_secs, a.price_thresh, a.vol_mult, a.mode, a.target_pts, a.stop_pts, a.trail_pts, a.drop_confirm_secs, a.allow_price_only, getattr(a,'force_first', False))
