#!/usr/bin/env python3
"""
Microburst Mean-Reversion Seller (MMS)

Deterministic, second-level scalper that fades 3s price bursts when deceleration
and options flow confirmation occur. Sells single-leg (CE for up-burst, PE for down-burst)
with tight target/stop and short time-stop. Expiry/strike discovery pulls from filesystem
to support both weekly and monthly layouts.

Outputs a parquet with one row per trade under backtests/results/.
"""
from __future__ import annotations
from datetime import datetime, date, time, timedelta
from pathlib import Path
import argparse
import polars as pl
from dateutil import tz
from backtests.utils.raw_opts import load_option_seconds_raw

IST = "Asia/Kolkata"
SPOT_ROOT = Path("data/packed/spot")
OPT_ROOT = Path("data/packed/options")


def dt_ist(d: date, t: time) -> datetime:
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))


def nearest_strike(px: float, step: int) -> int:
    return int(round(px / step) * step)


def dense_spot_1s(symbol: str, d: date) -> pl.DataFrame:
    yyyymm = f"{d.year:04d}{d.month:02d}"
    p = SPOT_ROOT / symbol / yyyymm / f"date={d}" / "ticks.parquet"
    df = pl.read_parquet(str(p)).select(["timestamp", "close"]).rename({"timestamp": "ts"})
    df = df.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    sec = (
        df.with_columns(pl.col("ts").dt.truncate("1s").alias("ts"))
        .group_by("ts")
        .agg(pl.col("close").last().alias("close"))
        .sort("ts")
    )
    t0, t1 = dt_ist(d, time(9, 15)), dt_ist(d, time(15, 30))
    idx = pl.DataFrame({"ts": pl.datetime_range(t0, t1, "1s", time_zone=IST, eager=True)})
    sec = idx.join(sec, on="ts", how="left").with_columns(pl.col("close").fill_null(strategy="forward"))
    # backfill
    if sec["close"].is_null().any():
        fc = float(df["close"].drop_nulls().head(1)[0])
        sec = sec.with_columns(pl.when(pl.col("close").is_null()).then(pl.lit(fc)).otherwise(pl.col("close")).alias("close"))
    return sec


def list_expiries_fs(symbol: str, around: date) -> list[date]:
    # Search current and next month for exp=* directories
    months = [f"{around.year:04d}{around.month:02d}"]
    nmo = (around.replace(day=1) + timedelta(days=40)).replace(day=1)
    months.append(f"{nmo.year:04d}{nmo.month:02d}")
    exps = []
    for yyyymm in months:
        base = OPT_ROOT / symbol / yyyymm
        if not base.exists():
            continue
        for exp_dir in base.glob("exp=*"):
            try:
                exps.append(datetime.fromisoformat(exp_dir.name.split("=")[1]).date())
            except Exception:
                pass
    return sorted(set(exps))


def nearest_future_expiry(symbol: str, d: date) -> date | None:
    exps = list_expiries_fs(symbol, d)
    for e in exps:
        if e >= d:
            return e
    return exps[-1] if exps else None


def list_common_strikes(symbol: str, expiry: date) -> list[int]:
    yyyymm = f"{expiry.year:04d}{expiry.month:02d}"
    base = OPT_ROOT / symbol / yyyymm / f"exp={expiry:%Y-%m-%d}"
    ce_dir = base / "type=CE"
    pe_dir = base / "type=PE"
    ces = [int(p.name.split("=")[1].split(".")[0]) for p in ce_dir.glob("strike=*.parquet")] if ce_dir.exists() else []
    pes = [int(p.name.split("=")[1].split(".")[0]) for p in pe_dir.glob("strike=*.parquet")] if pe_dir.exists() else []
    return sorted(list(set(ces).intersection(pes)))


def load_option_seconds(symbol: str, expiry: date, strike: int, opt_type: str, d: date) -> pl.DataFrame | None:
    # Prefer raw options per-second series using trade price and qty
    sec = load_option_seconds_raw(symbol, d, expiry, strike, opt_type)
    return sec


def option_price_at(sec: pl.DataFrame, ts: datetime) -> float | None:
    try:
        return float(sec.filter(pl.col("ts") == pl.lit(ts).cast(pl.Datetime("ns", time_zone=IST))).select("close").item())
    except Exception:
        prev = sec.sort("ts").filter(pl.col("ts") <= pl.lit(ts).cast(pl.Datetime("ns", time_zone=IST))).tail(1)
        if prev.height:
            return float(prev.select("close").item())
        return None


def run(
    symbol: str,
    start: str,
    end: str,
    short_secs: int = 3,
    base_secs: int = 30,
    price_thresh: float | None = None,
    vol_mult: float = 1.15,
    target_pts: float = 2.0,
    stop_pts: float = 2.0,
    trail_pts: float = 1.0,
    time_stop_secs: int = 90,
    cooldown_secs: int = 60,
):
    d0 = datetime.fromisoformat(start).date()
    d1 = datetime.fromisoformat(end).date()
    step = 100 if symbol == "BANKNIFTY" else 50
    if price_thresh is None:
        price_thresh = 3.0 if symbol == "BANKNIFTY" else 1.2
    out_rows = []
    d = d0
    total_candidates = 0
    total_trades = 0
    while d <= d1:
        # trading window
        win_start, win_end = time(9, 20), time(15, 15)
        t0 = dt_ist(d, win_start)
        t1 = dt_ist(d, win_end)
        try:
            spot = dense_spot_1s(symbol, d)
        except Exception:
            d = d + timedelta(days=1)
            continue

        # compute short momentum
        s_ts = spot["ts"]
        s_px = spot["close"]
        lag = s_px.shift(short_secs)
        pdiff = s_px - lag
        pdiff1 = pdiff.shift(1)
        accel_drop = (pdiff.abs() <= pdiff1.abs())
        sign_change = ((pdiff * pdiff1) <= 0).fill_null(False)
        # Build candidate set vectorized to avoid tz comparison quirks
        t0_l = pl.lit(t0).cast(pl.Datetime("ns", time_zone=IST))
        t1_l = pl.lit(t1).cast(pl.Datetime("ns", time_zone=IST))
        base_df = pl.DataFrame({
            "ts": s_ts,
            "px": s_px,
            "pd": pdiff,
            "ad": accel_drop,
            "sg": sign_change,
        })
        cond_up = (pl.col("pd") >= price_thresh) & (pl.col("ad") | pl.col("sg"))
        cond_dn = (pl.col("pd") <= -price_thresh) & (pl.col("ad") | pl.col("sg"))
        within = (pl.col("ts") >= t0_l) & (pl.col("ts") <= t1_l)
        cands = (
            base_df
            .with_columns([
                pl.when(cond_up).then(pl.lit("up")).when(cond_dn).then(pl.lit("down")).otherwise(pl.lit(None)).alias("dir")
            ])
            .filter(within & pl.col("dir").is_not_null())
            .select(["ts","px","dir"]).sort("ts")
        )
        # cooldown tracker
        last_exit = None

        # iterate candidates
        for ts, px_now, direction in zip(cands["ts"].to_list(), cands["px"].to_list(), cands["dir"].to_list()):
            # cooldown
            if last_exit is not None:
                if (ts - last_exit).total_seconds() < cooldown_secs:
                    continue
            total_candidates += 1
            # expiry and strike
            exp = nearest_future_expiry(symbol, d)
            if not exp:
                continue
            k_atm = nearest_strike(px_now, step)
            strikes = list_common_strikes(symbol, exp)
            if not strikes:
                continue
            band = step * 2
            ks = min([k for k in strikes if abs(k - k_atm) <= band] or strikes, key=lambda x: abs(x - k_atm))
            # load seconds for CE/PE
            ce_sec = load_option_seconds(symbol, exp, ks, "CE", d)
            pe_sec = load_option_seconds(symbol, exp, ks, "PE", d)
            if ce_sec is None or pe_sec is None:
                continue
            vol = (
                ce_sec.select(["ts", "vol"]).rename({"vol": "vce"})
                .join(pe_sec.select(["ts", "vol"]).rename({"vol": "vpe"}), on="ts", how="inner")
                .with_columns((pl.col("vce") + pl.col("vpe")).alias("vol"))
                .select(["ts", "vol"]).sort("ts")
            )
            # align to spot and compute rolling vol
            v_al = spot.select("ts").join(vol, on="ts", how="left").with_columns(pl.col("vol").fill_null(0))
            vol_ws = v_al.with_columns([
                pl.col("vol").rolling_sum(short_secs).fill_null(0).alias("vol_ws"),
                (pl.col("vol").rolling_mean(base_secs).fill_null(0) * short_secs).alias("vol_base"),
            ])
            vrow = vol_ws.filter(pl.col("ts") == pl.lit(ts).cast(pl.Datetime("ns", time_zone=IST)))
            try:
                vws = float(vrow.select("vol_ws").item())
                vbase = float(vrow.select("vol_base").item())
            except Exception:
                vws, vbase = 0.0, 0.0
            if vol_mult > 0.0 and not (vws > vol_mult * vbase and vbase > 0):
                continue
            # enter
            opt_type = "CE" if direction == "up" else "PE"
            sec_leg = ce_sec if opt_type == "CE" else pe_sec
            entry_ts = ts
            entry_opt = option_price_at(sec_leg, entry_ts)
            if entry_opt is None:
                continue
            # targets
            tgt = entry_opt - target_pts
            stp = entry_opt + stop_pts
            best = entry_opt
            exit_ts = entry_ts
            exit_opt = entry_opt
            # EOD-only exit policy (no time-based cut)
            exit_reason = "eod"
            hard_end = dt_ist(d, time(15, 30))
            time_limit = hard_end
            # Use option per-second price directly (from raw)
            op_path = sec_leg.filter(
                (pl.col("ts") >= pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST)))
                & (pl.col("ts") <= pl.lit(time_limit).cast(pl.Datetime("ns", time_zone=IST)))
            ).select(["ts", "close"]).sort("ts")
            for ts2, opv in zip(op_path["ts"].to_list(), op_path["close"].to_list()):
                val = float(opv)
                if val < best:
                    best = val
                if val <= tgt:
                    exit_ts, exit_opt, exit_reason = ts2, val, "target"
                    continue
                if val >= stp:
                    exit_ts, exit_opt, exit_reason = ts2, val, "stop"
                    break
                if best < entry_opt and trail_pts > 0:
                    trail_stop = best + trail_pts
                    if val >= trail_stop:
                        exit_ts, exit_opt, exit_reason = ts2, val, "trail_stop"
                        break
                exit_ts, exit_opt = ts2, val
            pnl = float(entry_opt - exit_opt)
            out_rows.append(
                {
                    "symbol": symbol,
                    "date": d.isoformat(),
                    "entry_ts": entry_ts.isoformat(),
                    "exit_ts": exit_ts.isoformat(),
                    "opt_type": opt_type,
                    "expiry": exp.isoformat(),
                    "strike": int(ks),
                    "entry_opt": float(entry_opt),
                    "exit_opt": float(exit_opt),
                    "pnl": float(pnl),
                    "exit_reason": exit_reason,
                }
            )
            last_exit = exit_ts
            total_trades += 1
        d = d + timedelta(days=1)
    if out_rows:
        out = Path("backtests/results") / f"microburst_seller_{symbol}_{start}_{end}.parquet"
        pl.DataFrame(out_rows).write_parquet(out)
        print(f"Wrote {out} | candidates={total_candidates} trades={total_trades}")
    else:
        print(f"No trades | candidates={total_candidates} trades={total_trades}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY", "NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--short-secs", type=int, default=3)
    ap.add_argument("--base-secs", type=int, default=30)
    ap.add_argument("--price-thresh", type=float, default=None)
    ap.add_argument("--vol-mult", type=float, default=1.15)
    ap.add_argument("--target-pts", type=float, default=2.0)
    ap.add_argument("--stop-pts", type=float, default=2.0)
    ap.add_argument("--trail-pts", type=float, default=1.0)
    ap.add_argument("--time-stop-secs", type=int, default=90)
    ap.add_argument("--cooldown-secs", type=int, default=60)
    a = ap.parse_args()
    run(a.symbol, a.start, a.end, a.short_secs, a.base_secs, a.price_thresh, a.vol_mult, a.target_pts, a.stop_pts, a.trail_pts, a.time_stop_secs, a.cooldown_secs)
