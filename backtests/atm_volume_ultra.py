#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATM ±1 strikes, hourly routing by SPOT, per-day preloading (ultra fast).

Key ideas for speed
-------------------
1) Preload SPOT 1s grid once per DAY (09:15..15:30 IST).
2) From those anchors, compute the union of candidate strikes for the DAY (ATM ±1 per anchor).
3) For each candidate (strike, CE/PE), read ONE parquet slice for the DAY only,
   aggregate to 1s once, compute rolling burst once, then reuse for all anchors.
4) Map trade_date -> next expiry in-memory (O(1)).
5) PnL via delta-proxy (no option ticks needed intrabar).

Inputs (packed options layout)
------------------------------
data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet
  columns: timestamp, close, vol_delta, expiry, symbol, opt_type, strike

Spot parquet (glob via CLI or config)
-------------------------------------
timestamp, close   (any other cols ignored)

Outputs
-------
backtests/results/trades_{SYMBOL}_{START}_{END}.parquet
backtests/results/summary_{SYMBOL}_{START}_{END}.parquet
"""

from __future__ import annotations
import os, glob, math, argparse
from pathlib import Path
from datetime import datetime, date, time, timedelta
from functools import lru_cache

import polars as pl
from dateutil import tz

# -------- config (minimal; no external YAML to keep it copy-pasteable) --------

IST = "Asia/Kolkata"
CACHE_DIR = Path("backtests/cache/seconds")
CONFIG = {
    "options_root": "./data/packed/options",
    "spot_glob": "./data/packed/spot/{symbol}/**/date=*/ticks.parquet",
    "calendar_csv": "./meta/expiry_calendar.csv",
    "session": {"open": "09:15:00", "close": "15:30:00", "anchors": ["10:00","11:00","12:00","13:00","14:00","15:00"]},
    "strike_step": {"BANKNIFTY": 100, "NIFTY": 50},
    "signal": {"burst_secs": 30, "avg_secs": 300, "multiplier": 1.5},
    "delta": {"ATM": 0.50, "NEAR": 0.40},
    "risk": {"side": "sell", "target_pct": 0.15, "stop_pct": 0.15, "trail_pct": 0.10},
    "pnl_mode": "delta_proxy",
}

# -------------------------------- utils --------------------------------------

def parse_hhmm(s: str) -> time:
    s = s.strip()
    parts = s.split(":")
    if len(parts) == 2:
        hh, mm = int(parts[0]), int(parts[1]); ss = 0
    elif len(parts) >= 3:
        hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
    else:
        raise ValueError(f"Bad time string: {s}")
    return time(hh, mm, ss)

def dt_ist(d: date, t: time) -> datetime:
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))

def ensure_ist(df: pl.DataFrame, col="timestamp") -> pl.DataFrame:
    dt = df[col].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, strict=False))
    elif not isinstance(dt, pl.Datetime):
        df = df.with_columns(pl.col(col).cast(pl.Datetime("ns"), strict=False))
    return df.with_columns(pl.col(col).dt.replace_time_zone(IST)).sort(col)

def dense_1s_index(start: datetime, end: datetime) -> pl.DataFrame:
    idx = pl.DataFrame({"ts": pl.datetime_range(start, end, "1s", time_zone=IST, eager=True)})
    # ensure ns time unit to match downstream
    return idx.with_columns(pl.col("ts").dt.cast_time_unit("ns"))

def nearest_strike(px: float, step: int) -> int:
    return int(round(px / step) * step)

def pick_strikes_for_day(spot_sec: pl.DataFrame, anchors: list[datetime], step: int) -> list[int]:
    # spot_sec: columns ts, close
    ks = set()
    s = spot_sec.lazy()
    for a in anchors:
        # exact second exists since we generated a dense 1s grid
        a_lit = pl.lit(a).cast(pl.Datetime("ns", time_zone=IST))
        v = s.filter(pl.col("ts") == a_lit).select(pl.col("close")).collect()
        if v.height == 0:
            continue
        atm = nearest_strike(float(v.item()), step)
        ks.add(atm - step); ks.add(atm); ks.add(atm + step)
    out = sorted(ks)
    return out

def option_file_path(root: str, symbol: str, expiry: date, opt_type: str, strike: int) -> Path:
    yyyymm = f"{expiry.year:04d}{expiry.month:02d}"
    return Path(root) / symbol / yyyymm / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"

def cache_file_path(symbol: str, trade_date: date, expiry: date, opt_type: str, strike: int) -> Path:
    return CACHE_DIR / symbol / f"date={trade_date.isoformat()}" / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"

# ------------------------------ calendar -------------------------------------

@lru_cache(maxsize=8)
def load_calendar(calendar_csv: str) -> pl.DataFrame:
    return (
        pl.read_csv(calendar_csv)
        .rename({"Instrument":"symbol","Final_Expiry":"expiry"})
        .select([
            pl.col("symbol").str.to_uppercase(),
            pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"),
        ])
        .drop_nulls()
        .unique()
        .sort(["symbol","expiry"])
    )

def next_expiry(symbol: str, d: date, cal: pl.DataFrame) -> date | None:
    x = cal.filter((pl.col("symbol")==symbol) & (pl.col("expiry") >= d)).select("expiry").head(1)
    return x.item() if x.height else None

# ------------------------------- loaders -------------------------------------

class SpotStore:
    def __init__(self, glob_pattern: str):
        self.glob_pattern = glob_pattern

    def list_files(self, symbol: str) -> list[str]:
        return sorted(glob.glob(self.glob_pattern.format(symbol=symbol)))

    def load_day_seconds(self, symbol: str, start_dt: datetime, end_dt: datetime) -> pl.DataFrame:
        files = self.list_files(symbol)
        if not files:
            return pl.DataFrame({"ts": [], "close": []})
        # Read all files as dataframe first
        df = pl.read_parquet(files, columns=["timestamp", "close"])
        # Parse timestamp if needed
        if df["timestamp"].dtype == pl.Utf8:
            df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
        # Ensure IST timezone
        df = ensure_ist(df, "timestamp")
        # Filter for the day with literals cast to the SAME tz and unit
        start_lit = pl.lit(start_dt).cast(pl.Datetime("ns", time_zone=IST))
        end_lit = pl.lit(end_dt).cast(pl.Datetime("ns", time_zone=IST))
        df = df.filter((pl.col("timestamp") >= start_lit) & (pl.col("timestamp") <= end_lit))
        if df.is_empty():
            return pl.DataFrame({"ts": [], "close": []})
        df = ensure_ist(df, "timestamp")
        # squash to per-second last close, then dense 1s join + ffill
        g = df.with_columns(pl.col("timestamp").dt.truncate("1s").alias("ts")).group_by("ts").agg(
            pl.col("close").last().alias("close")
        )
        idx = dense_1s_index(start_dt, end_dt)
        sec = (
            idx.join(g, on="ts", how="left")
               .with_columns(pl.col("close").fill_null(strategy="forward"))
        )
        # backfill the very first gap if needed
        if sec["close"].is_null().any():
            first = float(df["close"][0])
            sec = sec.with_columns(
                pl.when(pl.col("close").is_null()).then(pl.lit(first)).otherwise(pl.col("close")).alias("close")
            )
        return sec

def option_day_seconds(df: pl.DataFrame, start_dt: datetime, end_dt: datetime,
                       burst_secs: int, avg_secs: int, multiplier: float) -> pl.DataFrame:
    """
    df: timestamp, vol_delta, close (IST)
    returns per-second frame with: ts, vol, close, vol_30s, base_30s, burst
    """
    if df.is_empty():
        idx = dense_1s_index(start_dt, end_dt)
        return idx.with_columns([
            pl.lit(0).alias("vol"),
            pl.lit(None).alias("close"),
            pl.lit(0).alias("vol_30s"),
            pl.lit(0).alias("base_30s"),
            pl.lit(False).alias("burst"),
        ])

    # per-second aggregation
    sec = (
        df.with_columns(pl.col("timestamp").dt.truncate("1s").alias("ts"))
          .group_by("ts")
          .agg([
              pl.col("vol_delta").sum().alias("vol"),
              pl.col("close").last().alias("close"),
          ])
          .sort("ts")
    )
    # dense join + ffill close, fill vol zeros
    idx = dense_1s_index(start_dt, end_dt)
    sec = (
        idx.join(sec, on="ts", how="left")
           .with_columns([
               pl.col("vol").fill_null(0).alias("vol"),
               pl.col("close").fill_null(strategy="forward"),
           ])
    )
    # if first few seconds are null close, backfill with first valid
    if sec["close"].is_null().any():
        # Get first non-null close as a fallback
        first_non_null_close = df["close"].drop_nulls()
        first_close = float(first_non_null_close[0]) if len(first_non_null_close) > 0 else 0.0
        sec = sec.with_columns(
            pl.when(pl.col("close").is_null()).then(pl.lit(first_close)).otherwise(pl.col("close")).alias("close")
        )

    # rolling features (windows in rows because it's per-second)
    sec = sec.with_columns([
        pl.col("vol").rolling_sum(burst_secs).fill_null(0).alias("vol_30s"),
        (pl.col("vol").rolling_mean(avg_secs).fill_null(0) * burst_secs).alias("base_30s"),
    ]).with_columns([
        (pl.col("vol_30s") > multiplier * pl.col("base_30s")).alias("burst")
    ])
    return sec

# ---------------------------- strategy bits ----------------------------------

def entry_filter_trend(side: str, opt_type: str, spot_now: float, hour_open: float) -> bool:
    # side "sell": sell CE only if spot <= hour open (weak/up-failing) ; sell PE only if spot >= hour open (weak/down-failing)
    if side == "sell":
        return (opt_type == "CE" and spot_now <= hour_open) or (opt_type == "PE" and spot_now >= hour_open)
    # side "long": classic trend follow
    return (opt_type == "CE" and spot_now >= hour_open) or (opt_type == "PE" and spot_now <= hour_open)

def delta_for(opt_type: str, rel: str) -> float:
    base = CONFIG["delta"]["ATM"] if rel == "ATM" else CONFIG["delta"]["NEAR"]
    return base  # magnitude; side sign handled later

def option_price_proxy(entry_opt_px: float, entry_spot: float, spot_now: float, delta: float, side: str) -> float:
    sign = -1 if side == "sell" else 1
    est = entry_opt_px + sign * delta * (spot_now - entry_spot)
    return max(0.05, float(est))

# ------------------------------- main loop -----------------------------------

def run(symbol: str, start: str, end: str, spot_glob: str | None = None,
        burst_mult: float | None = None):

    opts_root = CONFIG["options_root"]
    spot_glob = spot_glob or CONFIG["spot_glob"]
    cal = load_calendar(CONFIG["calendar_csv"])

    t_open  = parse_hhmm(CONFIG["session"]["open"])
    t_close = parse_hhmm(CONFIG["session"]["close"])
    anchors = [parse_hhmm(x) for x in CONFIG["session"]["anchors"]]
    burst_secs = CONFIG["signal"]["burst_secs"]
    avg_secs   = CONFIG["signal"]["avg_secs"]
    multiplier = burst_mult if burst_mult is not None else CONFIG["signal"]["multiplier"]
    step = CONFIG["strike_step"].get(symbol, 50)

    side       = CONFIG["risk"]["side"]
    target_pct = CONFIG["risk"]["target_pct"]
    stop_pct   = CONFIG["risk"]["stop_pct"]
    trail_pct  = CONFIG["risk"]["trail_pct"]

    out_dir = Path("backtests/results")
    out_dir.mkdir(parents=True, exist_ok=True)

    d0 = datetime.fromisoformat(start).date()
    d1 = datetime.fromisoformat(end).date()

    spot_store = SpotStore(spot_glob)
    trades = []
    
    print(f"Starting backtest for {symbol} from {d0} to {d1}")
    print(f"Spot glob pattern: {spot_glob}")
    print(f"Options root: {opts_root}")
    print(f"Volume burst multiplier: {multiplier}")

    d = d0
    days_processed = 0
    while d <= d1:
        # trading day bounds (IST)
        day_open = dt_ist(d, t_open)
        day_close = dt_ist(d, t_close)

        # 1) map expiry once
        exp = next_expiry(symbol, d, cal)
        if exp is None:
            print(f"Skipping {d}: No expiry found")
            d += timedelta(days=1)
            continue

        # 2) SPOT 1s grid for the day
        spot_sec = spot_store.load_day_seconds(symbol, day_open, day_close)
        if spot_sec.is_empty():
            print(f"Skipping {d}: No spot data")
            d += timedelta(days=1)
            continue
        
        print(f"\nProcessing {d}: expiry={exp}")

        # 3) candidate strikes (union across anchors)
        anchor_dts = [dt_ist(d, a) for a in anchors if day_open <= dt_ist(d, a) < day_close]
        strikes = pick_strikes_for_day(spot_sec, anchor_dts, step)
        if not strikes:
            print(f"  No strikes found for anchors")
            d += timedelta(days=1)
            continue
        print(f"  Candidate strikes: {strikes}")

        # 4) preload options seconds per candidate (CE & PE)
        per_second = {}     # (strike, opt_type) -> df(ts, vol, close, vol_30s, base_30s, burst)
        entry_px_at = {}    # (strike, opt_type, anchor_ts) -> option close at anchor (from per-second close)
        for k in strikes:
            for opt_type in ("CE", "PE"):
                f = option_file_path(opts_root, symbol, exp, opt_type, k)
                if not f.exists():
                    continue
                # Try cache
                cpath = cache_file_path(symbol, d, exp, opt_type, k)
                if cpath.exists():
                    try:
                        sec = pl.read_parquet(str(cpath))
                        if "ts" in sec.columns:
                            sec = sec.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
                        per_second[(k, opt_type)] = sec
                        if "close" in sec.columns:
                            for a in anchor_dts:
                                a_lit = pl.lit(a).cast(pl.Datetime("ns", time_zone=IST))
                                try:
                                    entry_px_at[(k, opt_type, a)] = float(sec.filter(pl.col("ts")==a_lit).select("close").item())
                                except Exception:
                                    pass
                        continue
                    except Exception:
                        pass
                lf = pl.scan_parquet(str(f)).select("timestamp","close","vol_delta")
                # cast/standardize timestamp early for reliable filtering
                if lf.schema["timestamp"] == pl.Utf8:
                    lf = lf.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
                # ensure common tz/unit on the lazy column
                lf = lf.with_columns(
                    pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns")
                )
                # compare against literals cast to same tz/unit
                start_lit = pl.lit(day_open).cast(pl.Datetime("ns", time_zone=IST))
                end_lit = pl.lit(day_close).cast(pl.Datetime("ns", time_zone=IST))
                lf = lf.filter((pl.col("timestamp") >= start_lit) & (pl.col("timestamp") <= end_lit))
                df = lf.collect()
                if df.is_empty():
                    continue
                df = ensure_ist(df, "timestamp")
                sec = option_day_seconds(df, day_open, day_close, burst_secs, avg_secs, multiplier)
                # Write cache for faster future runs
                try:
                    cpath.parent.mkdir(parents=True, exist_ok=True)
                    sec.write_parquet(str(cpath))
                except Exception:
                    pass
                per_second[(k, opt_type)] = sec
                # cache entry close for all anchors quickly by joining exact ts
                # (dense 1s grid guarantees exact match)
                if "close" in sec.columns:
                    for a in anchor_dts:
                        a_lit = pl.lit(a).cast(pl.Datetime("ns", time_zone=IST))
                        entry_px_at[(k, opt_type, a)] = float(
                            sec.filter(pl.col("ts") == a_lit).select("close").item()
                        )

        if not per_second:
            print(f"  No options data loaded")
            d += timedelta(days=1)
            continue
        print(f"  Loaded {len(per_second)} option series")

        # 5) iterate anchors -> pick earliest burst among candidates, apply filters, simulate
        for a in anchor_dts:
            # hour slice bounds
            hour_end = min(a + timedelta(hours=1), day_close)

            # spot open/now
            a_lit = pl.lit(a).cast(pl.Datetime("ns", time_zone=IST))
            spot_hour_open = float(spot_sec.filter(pl.col("ts") == a_lit).select("close").item())
            # find first burst across all candidates after anchor
            first_hit = None  # (ts, strike, opt_type)
            for (k, t), sec in per_second.items():
                a_lit = pl.lit(a).cast(pl.Datetime("ns", time_zone=IST))
                hour_end_lit = pl.lit(hour_end).cast(pl.Datetime("ns", time_zone=IST))
                hit = (
                    sec.filter((pl.col("ts") >= a_lit) & (pl.col("ts") <= hour_end_lit) & (pl.col("burst") == True))
                       .select("ts")
                       .head(1)
                )
                if hit.height:
                    ts_hit = hit["ts"][0]
                    if (first_hit is None) or (ts_hit < first_hit[0]):
                        first_hit = (ts_hit, k, t)
            if first_hit is None:
                continue
            print(f"    Found burst at {first_hit[0]} for strike {first_hit[1]} {first_hit[2]}")

            entry_ts, k_sel, t_sel = first_hit
            # trend filter at entry
            entry_ts_lit = pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))
            spot_now = float(spot_sec.filter(pl.col("ts") == entry_ts_lit).select("close").item())
            if not entry_filter_trend(side, t_sel, spot_now, spot_hour_open):
                continue

            # simulate using spot path (delta-proxy)
            hour_end_lit = pl.lit(hour_end).cast(pl.Datetime("ns", time_zone=IST))
            spot_path = spot_sec.filter((pl.col("ts") >= entry_ts_lit) & (pl.col("ts") <= hour_end_lit)).select("ts","close")
            if spot_path.is_empty():
                continue

            rel = "ATM" if k_sel == nearest_strike(spot_hour_open, step) else "NEAR"
            delta = float(delta_for(t_sel, rel))
            opt_entry = float(entry_px_at.get((k_sel, t_sel, a), 0.0) or max(1.0, 0.1 * (spot_hour_open/step)))

            entry_spot = float(spot_path["close"][0])
            target_opt = opt_entry * (1 - target_pct) if side=="sell" else opt_entry * (1 + target_pct)
            stop_opt   = opt_entry * (1 + stop_pct)   if side=="sell" else opt_entry * (1 - stop_pct)
            best_for_trail = opt_entry
            exit_reason = "hour_end"
            exit_ts = spot_path["ts"][-1]
            exit_opt = opt_entry

            for ts, px in zip(spot_path["ts"].to_list(), spot_path["close"].to_list()):
                opt_now = option_price_proxy(opt_entry, entry_spot, float(px), delta, side)

                # trailing
                if trail_pct and trail_pct > 0:
                    if side == "sell":
                        if opt_now < best_for_trail:
                            best_for_trail = opt_now
                        trail_stop = best_for_trail * (1 + trail_pct)
                        if opt_now >= trail_stop:
                            exit_reason, exit_ts, exit_opt = "trail_stop", ts, opt_now
                            break
                    else:
                        if opt_now > best_for_trail:
                            best_for_trail = opt_now
                        trail_stop = best_for_trail * (1 - trail_pct)
                        if opt_now <= trail_stop:
                            exit_reason, exit_ts, exit_opt = "trail_stop", ts, opt_now
                            break

                # hard target/stop
                if side == "sell":
                    if opt_now <= target_opt:
                        exit_reason, exit_ts, exit_opt = "target", ts, opt_now
                        break
                    if opt_now >= stop_opt:
                        exit_reason, exit_ts, exit_opt = "stop", ts, opt_now
                        break
                else:
                    if opt_now >= target_opt:
                        exit_reason, exit_ts, exit_opt = "target", ts, opt_now
                        break
                    if opt_now <= stop_opt:
                        exit_reason, exit_ts, exit_opt = "stop", ts, opt_now
                        break

            pnl_pts = (opt_entry - exit_opt) if side=="sell" else (exit_opt - opt_entry)
            trades.append({
                "symbol": symbol,
                "trade_date": d.isoformat(),
                "anchor": a.time().isoformat(),
                "expiry": exp.isoformat(),
                "opt_type": t_sel,
                "strike": int(k_sel),
                "side": side,
                "entry_ts": entry_ts.isoformat(),
                "exit_ts": exit_ts.isoformat(),
                "entry_spot": float(entry_spot),
                "exit_spot": float(spot_path["close"][-1]),
                "delta_used": float(delta),
                "entry_opt": float(opt_entry),
                "exit_opt": float(exit_opt),
                "pnl_pts": float(pnl_pts),
                "exit_reason": exit_reason,
            })

        # next day
        days_processed += 1
        if days_processed % 10 == 0:
            print(f"\nProcessed {days_processed} days, {len(trades)} trades so far")
        d += timedelta(days=1)

    if not trades:
        print(f"\nNo trades generated in the requested window.")
        print(f"Total days processed: {days_processed}")
        return

    trades_df = pl.DataFrame(trades).with_columns(pl.col("pnl_pts").cum_sum().alias("pnl_cum"))
    summary = (
        trades_df
        .with_columns(pl.col("trade_date").str.strptime(pl.Date))
        .group_by("trade_date")
        .agg([
            pl.count().alias("n_trades"),
            pl.col("pnl_pts").sum().alias("pnl_pts"),
            pl.col("pnl_pts").mean().alias("avg_pnl"),
            pl.col("pnl_pts").std(ddof=1).alias("std_pnl"),
        ])
        .sort("trade_date")
    )

    tag = f"{symbol}_{start}_{end}"
    out_dir = Path("backtests/results")
    trades_path  = out_dir / f"trades_{tag}.parquet"
    summary_path = out_dir / f"summary_{tag}.parquet"
    trades_df.write_parquet(trades_path)
    summary.write_parquet(summary_path)
    print(f"Wrote:\n  {trades_path}\n  {summary_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    ap.add_argument("--spot-glob", default=None, help='Override (default: ./data/spot/{symbol}/*.parquet)')
    ap.add_argument("--multiplier", type=float, default=None, help='Volume burst multiplier (default 1.5)')
    args = ap.parse_args()
    run(args.symbol, args.start, args.end, spot_glob=args.spot_glob, burst_mult=args.multiplier)
