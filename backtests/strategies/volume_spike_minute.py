#!/usr/bin/env python3
"""
Volume Spike (Minute) Strategy

Rule:
- Compute per-minute volume for ATM CE+PE at the next weekly expiry.
- Signal when the current minute's volume > mult * average of the previous N minutes.
- Direction via spot 1-minute change: up -> buy CE, down -> buy PE; flat -> skip.
- Risk: target/stop in option points; trail after target by trail_pts.

Inputs:
- symbol: BANKNIFTY|NIFTY
- start, end: YYYY-MM-DD inclusive
- mult: volume spike multiple (default 4.0)
- lookback_min: number of previous minutes to average (default 5)
- target_pts: option points target (default 5.0)
- stop_pts: option points stop (default 3.0)
- trail_pts: trail distance after target (default 1.0)

Outputs:
- backtests/results/volume_spike_minute_{symbol}_{start}_{end}.parquet

Notes:
- Uses packed options/spot. Timestamps aligned to Asia/Kolkata, ns.
- Uses per-second aggregation for option close; per-minute volume sums from vol_delta.
"""
from __future__ import annotations
from datetime import datetime, date, time, timedelta
from pathlib import Path
import argparse
import polars as pl
from dateutil import tz

IST = "Asia/Kolkata"
OPTIONS_ROOT = Path("data/packed/options")
SPOT_ROOT = Path("data/packed/spot")


def dt_ist(d: date, t: time) -> datetime:
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))


def parse_date(s: str) -> date:
    return datetime.fromisoformat(s).date()


def nearest_strike(px: float, step: int) -> int:
    return int(round(px / step) * step)


def load_spot_1s(symbol: str, d: date) -> pl.DataFrame:
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
    # backfill first
    if sec["close"].is_null().any():
        fc = float(df["close"].drop_nulls().head(1)[0])
        sec = sec.with_columns(
            pl.when(pl.col("close").is_null()).then(pl.lit(fc)).otherwise(pl.col("close")).alias("close")
        )
    return sec


def find_expiry_and_strike(symbol: str, d: date, k_atm: int) -> tuple[date | None, int | None]:
    # Choose the nearest available weekly expiry directory within the next 4 weeks
    for off in range(0, 28, 7):
        cand = d + timedelta(days=off)
        y = f"{cand.year:04d}{cand.month:02d}"
        base = OPTIONS_ROOT / symbol / y / f"exp={cand:%Y-%m-%d}"
        if not base.exists():
            continue
        try:
            ce_dir = base / "type=CE"
            pe_dir = base / "type=PE"
            ces = [int(p.name.split("=")[1].split(".")[0]) for p in ce_dir.glob("strike=*.parquet")] if ce_dir.exists() else []
            pes = [int(p.name.split("=")[1].split(".")[0]) for p in pe_dir.glob("strike=*.parquet")] if pe_dir.exists() else []
            common = set(ces).intersection(pes)
            if not common:
                continue
            k_use = min(common, key=lambda k: abs(k - k_atm))
            return cand, k_use
        except Exception:
            continue
    return None, None


def list_common_strikes(symbol: str, expiry: date) -> list[int]:
    """List strikes present in both CE and PE for the expiry."""
    y = f"{expiry.year:04d}{expiry.month:02d}"
    base = OPTIONS_ROOT / symbol / y / f"exp={expiry:%Y-%m-%d}"
    ce_dir = base / "type=CE"
    pe_dir = base / "type=PE"
    ces = [int(p.name.split("=")[1].split(".")[0]) for p in ce_dir.glob("strike=*.parquet")] if ce_dir.exists() else []
    pes = [int(p.name.split("=")[1].split(".")[0]) for p in pe_dir.glob("strike=*.parquet")] if pe_dir.exists() else []
    return sorted(list(set(ces).intersection(pes)))


def load_option_seconds(symbol: str, expiry: date, strike: int, opt_type: str, d: date) -> pl.DataFrame | None:
    y = f"{expiry.year:04d}{expiry.month:02d}"
    f = OPTIONS_ROOT / symbol / y / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"
    if not f.exists():
        return None
    lf = pl.scan_parquet(str(f)).select("timestamp", "close", "vol_delta")
    # normalize ts
    if lf.schema.get("timestamp") == pl.Utf8:
        lf = lf.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    lf = lf.with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    t0, t1 = dt_ist(d, time(9, 15)), dt_ist(d, time(15, 30))
    s_l = pl.lit(t0).cast(pl.Datetime("ns", time_zone=IST))
    e_l = pl.lit(t1).cast(pl.Datetime("ns", time_zone=IST))
    df = lf.filter((pl.col("timestamp") >= s_l) & (pl.col("timestamp") <= e_l)).collect()
    if df.is_empty():
        return None
    sec = (
        df.with_columns(pl.col("timestamp").dt.truncate("1s").alias("ts"))
        .group_by("ts")
        .agg([
            pl.col("close").last().alias("close"),
            pl.col("vol_delta").sum().alias("vol"),
        ])
        .sort("ts")
    )
    return sec


def minute_volume_spike(vol_ce: pl.DataFrame, vol_pe: pl.DataFrame, lookback_min: int, mult: float) -> pl.DataFrame:
    vol = (
        vol_ce.select(["ts", "vol"]).rename({"vol": "vce"})
        .join(vol_pe.select(["ts", "vol"]).rename({"vol": "vpe"}), on="ts", how="inner")
        .with_columns((pl.col("vce") + pl.col("vpe")).alias("vol"))
        .select(["ts", "vol"]).sort("ts")
    )
    # per-minute aggregate
    min_vol = (
        vol.with_columns(pl.col("ts").dt.truncate("1m").alias("m"))
        .group_by("m").agg(pl.col("vol").sum().alias("vol_m"))
        .sort("m")
    )
    # rolling average of previous N minutes; exclude current using shift
    avg_prev = min_vol["vol_m"].rolling_mean(lookback_min).shift(1)
    sig = min_vol.with_columns(avg_prev.alias("avg_prev"))
    sig = sig.with_columns((pl.col("vol_m") > pl.lit(mult) * pl.col("avg_prev")).fill_null(False).alias("spike"))
    return sig


def run(
    symbol: str,
    start: str,
    end: str,
    mult: float = 4.0,
    lookback_min: int = 5,
    target_pts: float = 5.0,
    stop_pts: float = 3.0,
    trail_pts: float = 1.0,
):
    d0, d1 = parse_date(start), parse_date(end)
    step = 100 if symbol == "BANKNIFTY" else 50
    out_rows = []
    d = d0
    while d <= d1:
        # spot
        try:
            spot = load_spot_1s(symbol, d)
        except Exception:
            d = d + timedelta(days=1)
            continue
        s_ts = spot["ts"]
        s_close = spot["close"]
        # 1-minute price change for direction
        lag1m = s_close.shift(60)
        px_diff = s_close - lag1m
        # choose ATM using spot at each second when needed; signal will align to minutes
        # We'll compute ATM at each minute boundary when spike occurs
        # Session bounds
        t_close = dt_ist(d, time(15, 30))
        # iterate minute by minute
        # derive unique minutes from spot
        minutes = (
            spot.select(pl.col("ts").dt.truncate("1m").alias("m")).unique().sort("m")["m"].to_list()
        )
        # start from (lookback_min + 1)-th minute
        for m in minutes:
            # Build direction based on last 60s spot change at m
            try:
                idx = s_ts.to_list().index(m)  # exact minute boundary exists in index from dense range
            except ValueError:
                continue
            if idx < 60:
                continue
            dir_up = bool(px_diff[idx] is not None and px_diff[idx] >= 0.0)
            dir_down = bool(px_diff[idx] is not None and px_diff[idx] < 0.0)
            if not (dir_up or dir_down):
                continue
            # Compute ATM strike at this time
            k_atm = nearest_strike(float(s_close[idx]), step)
            # Find expiry and nearest available strike
            exp, k_use = find_expiry_and_strike(symbol, d, k_atm)
            if not exp or not k_use:
                continue
            # Try ATM and nearby strikes (±2 steps)
            chosen = None
            strikes = list_common_strikes(symbol, exp)
            if not strikes:
                continue
            band = step * 2
            cand_strikes = [k for k in strikes if abs(k - k_use) <= band]
            if not cand_strikes:
                cand_strikes = [k_use]
            ce_sec = pe_sec = sig = None
            for ks in sorted(cand_strikes, key=lambda x: abs(x - k_use)):
                ce_sec = load_option_seconds(symbol, exp, ks, "CE", d)
                pe_sec = load_option_seconds(symbol, exp, ks, "PE", d)
                if ce_sec is None or pe_sec is None:
                    continue
                sig = minute_volume_spike(ce_sec, pe_sec, lookback_min, mult)
                row = sig.filter(pl.col("m") == pl.lit(m)).collect()
                if not row.is_empty() and bool(row.select("spike").item()):
                    chosen = ks
                    break
            if not chosen or ce_sec is None or pe_sec is None or sig is None:
                continue
            # Entry at first second of next minute if available, else at m
            m_next = m + timedelta(minutes=1)
            entry_ts = m_next if m_next <= t_close else m
            # Entry option type and price
            opt_type = "CE" if dir_up else "PE"
            sec_leg = ce_sec if opt_type == "CE" else pe_sec
            # nearest second price
            try:
                entry_opt = float(
                    sec_leg.filter(pl.col("ts") == pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))).select("close").item()
                )
            except Exception:
                # fallback: last known before entry
                try:
                    entry_opt = float(
                        sec_leg.sort("ts").filter(pl.col("ts") <= pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))).tail(1).select("close").item()
                    )
                except Exception:
                    continue
            # Build path from entry to close
            path = (
                sec_leg.filter(
                    (pl.col("ts") >= pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST)))
                    & (pl.col("ts") <= pl.lit(t_close).cast(pl.Datetime("ns", time_zone=IST)))
                )
                .select(["ts", "close"]).sort("ts")
            )
            if path.is_empty():
                continue
            # Risk management
            tgt = entry_opt + target_pts
            stp = entry_opt - stop_pts
            best = entry_opt
            exit_ts = path["ts"][ -1 ]
            exit_opt = float(path["close"][ -1 ])
            exit_reason = "session_end"
            crossed_target = False
            for ts2, px2 in zip(path["ts"].to_list(), path["close"].to_list()):
                val = float(px2)
                if not crossed_target and val >= tgt:
                    crossed_target = True
                    best = val
                # trailing after target
                if crossed_target and trail_pts > 0:
                    if val > best:
                        best = val
                    trail_stop = best - trail_pts
                    if val <= trail_stop:
                        exit_ts, exit_opt, exit_reason = ts2, val, "trail_stop"
                        break
                # hard target/stop
                if val >= tgt:
                    exit_ts, exit_opt, exit_reason = ts2, val, "target"
                    # allow trailing only if not immediate; else treat as target
                    break
                if val <= stp:
                    exit_ts, exit_opt, exit_reason = ts2, val, "stop"
                    break
            pnl = float(exit_opt - entry_opt)
            out_rows.append(
                {
                    "symbol": symbol,
                    "date": d.isoformat(),
                    "minute": m.isoformat(),
                    "entry_ts": entry_ts.isoformat(),
                    "exit_ts": exit_ts.isoformat(),
                    "expiry": exp.isoformat(),
                    "strike": int(chosen),
                    "opt_type": opt_type,
                    "entry_opt": float(entry_opt),
                    "exit_opt": float(exit_opt),
                    "pnl": pnl,
                    "exit_reason": exit_reason,
                }
            )
        d = d + timedelta(days=1)
    if out_rows:
        out = Path("backtests/results") / f"volume_spike_minute_{symbol}_{start}_{end}.parquet"
        pl.DataFrame(out_rows).write_parquet(out)
        print(f"Wrote {out}")
    else:
        print("No trades")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY", "NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--mult", type=float, default=4.0)
    ap.add_argument("--lookback-min", type=int, default=5)
    ap.add_argument("--target-pts", type=float, default=5.0)
    ap.add_argument("--stop-pts", type=float, default=3.0)
    ap.add_argument("--trail-pts", type=float, default=1.0)
    args = ap.parse_args()
    run(args.symbol, args.start, args.end, args.mult, args.lookback_min, args.target_pts, args.stop_pts, args.trail_pts)
