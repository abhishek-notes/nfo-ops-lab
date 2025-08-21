#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATM + ±1 strikes, hourly routing, volume-burst signal, ultra-fast Polars.
FIXED VERSION - Resolves timezone issues while maintaining original logic

What it does
------------
• At each hourly anchor (10:00..15:00 IST), read SPOT, choose ATM strike (rounded to step),
  also track ±1 strikes (both CE & PE).
• For each chosen option series, build per-second volumes from `vol_delta`,
  compute a 30s burst vs a 5m baseline, and trigger a trade on first burst
  passing a trend filter against spot.
• P&L modes:
   - delta_proxy (default): proxy option price by spot move × delta (robust, fast)
   - option_close: use option 'close' series (use only if your option OHLC is tick-valid)
• Writes trades + summary parquet.
"""

from __future__ import annotations
import os, sys, math, argparse, glob
from pathlib import Path
from datetime import datetime, date, time, timedelta
from functools import lru_cache
import yaml
import polars as pl
from dateutil import tz
from tqdm import tqdm

# -------------------- defaults & config --------------------

ROOT = Path(__file__).resolve().parents[1]  # repo root
CFG_PATH = ROOT / "backtests" / "config.yaml"
DEFAULTS = {
    "raw_options_dir": "./data/packed/options",
    "spot_glob": "./data/packed/spot/{symbol}/**/*.parquet",  # Fixed path
    "calendar_csv": "./meta/expiry_calendar.csv",
    "cache_dir": "./cache/seconds",
    "session": {"open": "09:15:00", "close": "15:30:00", "anchors": ["10:00","11:00","12:00","13:00","14:00","15:00"]},
    "strike_step": {"BANKNIFTY": 100, "NIFTY": 50},
    "delta": {"ATM": 0.50, "NEAR": 0.40},
    "signal": {"burst_secs": 30, "avg_secs": 300, "multiplier": 1.5},
    "risk": {"side": "sell", "target_pct": 0.15, "stop_pct": 0.15, "trail_pct": 0.10, "max_hold_secs": 3600},
    "pnl_mode": "delta_proxy"
}

def load_cfg() -> dict:
    if CFG_PATH.exists():
        with open(CFG_PATH, "r") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}
    # deep merge defaults
    def deep(d,u):
        for k,v in u.items():
            if isinstance(v, dict):
                d[k] = deep(d.get(k,{}), v)
            else:
                d.setdefault(k, v)
        return d
    return deep(cfg, DEFAULTS.copy())

CFG = load_cfg()
IST = "Asia/Kolkata"
IST_TZ = tz.gettz(IST)

# -------------------- calendar --------------------

@lru_cache(maxsize=8)
def load_calendar(calendar_csv: str) -> pl.DataFrame:
    cal = (
        pl.read_csv(calendar_csv)
        .rename({"Instrument": "symbol", "Final_Expiry": "expiry"})
        .select([
            pl.col("symbol").str.to_uppercase(),
            pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"),
        ])
        .drop_nulls()
        .unique()
        .sort(["symbol", "expiry"])
    )
    return cal

def next_expiry_for(symbol: str, trade_dt: date, cal: pl.DataFrame) -> date | None:
    x = cal.filter((pl.col("symbol")==symbol) & (pl.col("expiry") >= trade_dt)).select("expiry").head(1)
    return x.item() if x.height else None

# -------------------- filesystem helpers --------------------

def option_file_path(options_root: str, symbol: str, expiry: date, opt_type: str, strike: int) -> Path:
    yyyymm = f"{expiry.year:04d}{expiry.month:02d}"
    return Path(options_root) / symbol / yyyymm / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"

def list_spot_files(spot_glob: str, symbol: str) -> list[str]:
    patt = spot_glob.format(symbol=symbol)
    files = sorted(glob.glob(patt, recursive=True))
    return files

# -------------------- data transforms --------------------

def ensure_dt_ist(df: pl.DataFrame, col: str="timestamp") -> pl.DataFrame:
    dt = df[col].dtype
    if dt == pl.Utf8:
        df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, strict=False))
    elif isinstance(dt, pl.Datetime):
        pass
    else:
        df = df.with_columns(pl.col(col).cast(pl.Datetime("ns"), strict=False))
    return df.with_columns(pl.col(col).dt.replace_time_zone(IST))

def to_seconds_frame(df: pl.DataFrame, value_col: str, start: datetime, end: datetime) -> pl.DataFrame:
    """
    Returns a dense 1s grid from [start,end] with `value_col` summed per second (missing=0).
    """
    if df.is_empty():
        idx = pl.DataFrame({"ts": pl.datetime_range(start, end, "1s", time_zone=IST, eager=True)})
        return idx.with_columns(pl.lit(0).alias("val"))
    g = (
        df.with_columns(pl.col("timestamp").dt.truncate("1s").dt.cast_time_unit("us").alias("ts"))
          .group_by("ts")
          .agg(pl.col(value_col).sum().alias("val"))
          .sort("ts")
    )
    # dense index
    idx = pl.DataFrame({"ts": pl.datetime_range(start, end, "1s", time_zone=IST, eager=True)})
    out = idx.join(g, on="ts", how="left").with_columns(pl.col("val").fill_null(0))
    return out

def seconds_signal(sec_df: pl.DataFrame, burst_secs: int, avg_secs: int, multiplier: float) -> pl.DataFrame:
    """
    Inputs: sec_df(ts, val). Outputs: ts, vol, vol_30s, base_30s, burst(bool)
    """
    return (
        sec_df
        .with_columns([
            pl.col("val").rolling_sum(burst_secs).alias("vol_30s"),
            pl.col("val").rolling_mean(avg_secs).fill_null(0).alias("_avg_sec"),
        ])
        .with_columns([
            (pl.col("_avg_sec") * burst_secs).alias("base_30s"),
            (pl.col("vol_30s") > multiplier * pl.col("_avg_sec") * burst_secs).alias("burst"),
        ])
        .drop("_avg_sec")
        .rename({"val":"vol"})
    )

# -------------------- spot loader --------------------

class SpotStore:
    def __init__(self, spot_glob: str, symbol: str):
        self.spot_glob = spot_glob
        self.symbol = symbol
        self.files = list_spot_files(spot_glob, symbol=symbol)
        if not self.files:
            print(f"[WARN] No spot files matched: {spot_glob.format(symbol=symbol)}", file=sys.stderr)

    @lru_cache(maxsize=128)
    def load_range(self, start: datetime, end: datetime) -> pl.DataFrame:
        if not self.files:
            return pl.DataFrame({"timestamp": [], "close": []})
        
        # Ensure timezone
        if not start.tzinfo:
            start = start.replace(tzinfo=IST_TZ)
        if not end.tzinfo:
            end = end.replace(tzinfo=IST_TZ)
            
        # Read all files and concatenate
        dfs = []
        for f in self.files:
            if Path(f).exists():
                df = pl.read_parquet(f).select("timestamp","close")
                dfs.append(df)
        
        if not dfs:
            return pl.DataFrame({"timestamp": [], "close": []})
            
        df = pl.concat(dfs)
        df = ensure_dt_ist(df, "timestamp").sort("timestamp")
        
        # Filter using with_columns to avoid lazy evaluation issues
        df = df.with_columns([
            pl.lit(start).dt.replace_time_zone(IST).alias("_start"),
            pl.lit(end).dt.replace_time_zone(IST).alias("_end")
        ])
        df = df.filter((pl.col("timestamp") >= pl.col("_start")) & 
                      (pl.col("timestamp") <= pl.col("_end")))
        df = df.drop(["_start", "_end"])
        
        if df.is_empty():
            return df
            
        # make 1s grid with last-close ffill
        start_sec = start.replace(microsecond=0)
        end_sec = end.replace(microsecond=0)
        idx = pl.DataFrame({"ts": pl.datetime_range(start_sec, end_sec, "1s", time_zone=IST, eager=True)})
        g = df.with_columns(pl.col("timestamp").dt.truncate("1s").dt.cast_time_unit("us").alias("ts")).group_by("ts").agg(pl.col("close").last().alias("close"))
        out = idx.join(g, on="ts", how="left").with_columns(pl.col("close").fill_null(strategy="forward"))
        
        # backfill first if needed
        if out["close"].null_count() > 0:
            first_val = out["close"].drop_nulls().first() if out["close"].drop_nulls().len() > 0 else 0.0
            out = out.with_columns(pl.col("close").fill_null(first_val))
            
        return out.rename({"ts":"timestamp"})

# -------------------- option per-second cache --------------------

class SecondsCache:
    def __init__(self, cache_dir: str):
        self.root = Path(cache_dir)

    def _path(self, symbol: str, expiry: date, opt_type: str, strike: int) -> Path:
        return self.root / symbol / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"

    def has(self, symbol: str, expiry: date, opt_type: str, strike: int) -> bool:
        return self._path(symbol, expiry, opt_type, strike).exists()

    def load(self, symbol: str, expiry: date, opt_type: str, strike: int) -> pl.DataFrame:
        return pl.read_parquet(self._path(symbol, expiry, opt_type, strike))

    def save(self, symbol: str, expiry: date, opt_type: str, strike: int, df: pl.DataFrame):
        p = self._path(symbol, expiry, opt_type, strike)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(p, compression="zstd", compression_level=3, statistics=True)

# -------------------- strategy engine --------------------

def nearest_strike(price: float, step: int) -> int:
    return int(round(price / step) * step)

def pick_strikes(spot_px: float, step: int) -> list[int]:
    atm = nearest_strike(spot_px, step)
    return [atm - step, atm, atm + step]

def entry_filter_trend(side: str, opt_type: str, spot_now: float, hour_open: float) -> bool:
    if side == "sell":
        # sell CE only if below open; sell PE only if above open
        if opt_type == "CE":
            return spot_now <= hour_open
        else:
            return spot_now >= hour_open
    else:
        # long calls in uptrend; long puts in downtrend
        if opt_type == "CE":
            return spot_now >= hour_open
        else:
            return spot_now <= hour_open

def delta_for(opt_type: str, rel: str="ATM") -> float:
    d = CFG["delta"]["ATM"] if rel=="ATM" else CFG["delta"]["NEAR"]
    return d if opt_type=="CE" else d  # magnitude only; direction handled by trade side

def option_price_proxy(mode: str, entry_opt_px: float, entry_spot: float, spot_now: float, delta: float, side: str) -> float:
    if mode == "option_close":
        # in that mode, the caller should pass the actual option close_now instead.
        return math.nan
    # delta_proxy
    sign = -1 if side=="sell" else 1
    est = entry_opt_px + sign * delta * (spot_now - entry_spot)
    return max(0.05, est)  # avoid negatives

def backtest_hour(
    symbol: str,
    trade_dt: date,
    hour_open_ts: datetime,
    hour_end_ts: datetime,
    spot_store: SpotStore,
    cal: pl.DataFrame,
    cache: SecondsCache,
    options_root: str,
    step: int,
    burst_secs: int,
    avg_secs: int,
    mult: float,
    side: str,
    target_pct: float,
    stop_pct: float,
    trail_pct: float,
    pnl_mode: str,
) -> list[dict]:
    trades: list[dict] = []
    # 1) load spot for this hour (plus a minute lookback)
    spot = spot_store.load_range(hour_open_ts - timedelta(seconds=60), hour_end_ts)
    if spot.is_empty():
        return trades
        
    # Find hour open price - use with_columns to handle timezone
    spot = spot.with_columns(pl.lit(hour_open_ts).dt.replace_time_zone(IST).alias("_target"))
    hour_open_spot = spot.filter(pl.col("timestamp") == pl.col("_target"))
    if hour_open_spot.height > 0:
        hour_open_px = float(hour_open_spot["close"][0])
    else:
        # Get closest price
        hour_open_px = float(spot["close"][0])
    spot = spot.drop("_target")
    
    # ATM selection from spot at anchor
    spot_at_anchor = hour_open_px
    strikes = pick_strikes(spot_at_anchor, step)

    # 2) expiry for this trade date
    exp = next_expiry_for(symbol, trade_dt, cal)
    if not exp:
        return trades

    # 3) For each (strike, type) build seconds signal (from cache or build)
    opt_defs = [(k, "CE") for k in strikes] + [(k, "PE") for k in strikes]
    seconds_map: dict[tuple[int,str], pl.DataFrame] = {}
    price_snap: dict[tuple[int,str], float] = {}  # option close at entry ts (if available)

    for strike, opt_type in opt_defs:
        fpath = option_file_path(options_root, symbol, exp, opt_type, strike)
        if not fpath.exists():
            continue
        if cache.has(symbol, exp, opt_type, strike):
            sec = cache.load(symbol, exp, opt_type, strike)
        else:
            # read only this hour range
            df = pl.read_parquet(str(fpath)).select("timestamp","vol_delta","close")
            if df.is_empty():
                continue
                
            df = ensure_dt_ist(df, "timestamp").sort("timestamp")
            
            # Filter to hour range
            df = df.with_columns([
                pl.lit(hour_open_ts - timedelta(seconds=60)).dt.replace_time_zone(IST).alias("_start"),
                pl.lit(hour_end_ts).dt.replace_time_zone(IST).alias("_end")
            ])
            df = df.filter((pl.col("timestamp") >= pl.col("_start")) & 
                          (pl.col("timestamp") <= pl.col("_end")))
            df = df.drop(["_start", "_end"])
            
            if df.is_empty():
                continue
                
            sec = to_seconds_frame(
                df.select("timestamp","vol_delta").rename({"vol_delta":"val"}),
                value_col="val",
                start=hour_open_ts - timedelta(seconds=60),
                end=hour_end_ts
            )
            
            # also stash the option close at anchor for entry reference
            df_temp = df.with_columns(pl.lit(hour_open_ts).dt.replace_time_zone(IST).alias("_target"))
            entry_opt_df = df_temp.filter(pl.col("timestamp") == pl.col("_target"))
            if entry_opt_df.height > 0:
                entry_px = float(entry_opt_df["close"][0])
            else:
                entry_px = float(df["close"][0]) if df.height > 0 else 0.0
            
            price_snap[(strike,opt_type)] = entry_px
            cache.save(symbol, exp, opt_type, strike, sec)
            
        if (strike,opt_type) not in price_snap:
            price_snap[(strike,opt_type)] = 0.0
            
        # build burst signal
        seconds_map[(strike,opt_type)] = seconds_signal(sec, burst_secs, avg_secs, mult)

    if not seconds_map:
        return trades

    # 4) merge a quick view to find first trigger across all candidates within the hour
    rows = []
    for (k,t), df in seconds_map.items():
        # focus within the hour [hour_open_ts, hour_end_ts]
        df = df.with_columns([
            pl.lit(hour_open_ts).dt.replace_time_zone(IST).alias("_start"),
            pl.lit(hour_end_ts).dt.replace_time_zone(IST).alias("_end")
        ])
        dff = df.filter((pl.col("ts") >= pl.col("_start")) & (pl.col("ts") <= pl.col("_end")))
        if dff.is_empty():
            continue
            
        # find first burst True
        hit = dff.filter(pl.col("burst") == True).head(1)
        if hit.height:
            ts_hit = hit["ts"][0]
            rows.append((ts_hit, k, t))
            
    if not rows:
        return trades

    # earliest burst wins
    rows.sort(key=lambda x: x[0])
    entry_ts, k_sel, t_sel = rows[0]

    # 5) trend filter at entry
    spot_temp = spot.with_columns(pl.lit(entry_ts).dt.replace_time_zone(IST).alias("_entry"))
    spot_at_entry = spot_temp.filter(pl.col("timestamp") == pl.col("_entry"))
    if spot_at_entry.height > 0:
        spot_now = float(spot_at_entry["close"][0])
    else:
        spot_now = float(spot["close"][-1])
        
    if not entry_filter_trend(side, t_sel, spot_now, hour_open_px):
        return trades

    # 6) simulate trade until hit target/stop/trail or hour end
    trail_on = trail_pct and trail_pct > 0
    
    # Get spot path from entry to hour end
    spot_path = spot.with_columns([
        pl.lit(entry_ts).dt.replace_time_zone(IST).alias("_entry"),
        pl.lit(hour_end_ts).dt.replace_time_zone(IST).alias("_end")
    ])
    spot_path = spot_path.filter((pl.col("timestamp") >= pl.col("_entry")) & 
                                (pl.col("timestamp") <= pl.col("_end")))
    spot_path = spot_path.drop(["_entry", "_end"]).select("timestamp","close")
    
    if spot_path.is_empty():
        return trades

    # entry refs
    entry_spot = float(spot_path["close"][0])
    entry_opt_close = price_snap[(k_sel,t_sel)]
    rel = "ATM" if k_sel == nearest_strike(spot_at_anchor, step) else "NEAR"
    delta = float(delta_for(t_sel, rel=rel))

    opt_entry = float(entry_opt_close if entry_opt_close else max(1.0, 0.1 * (entry_spot/step)))

    target_opt = opt_entry * (1 - target_pct) if side=="sell" else opt_entry * (1 + target_pct)
    stop_opt   = opt_entry * (1 + stop_pct)   if side=="sell" else opt_entry * (1 - stop_pct)
    best_for_trail = opt_entry  # best achieved in favorable direction

    exit_reason = "hour_end"
    exit_ts = spot_path["timestamp"][-1]
    exit_opt = opt_entry

    for ts, px in zip(spot_path["timestamp"].to_list(), spot_path["close"].to_list()):
        opt_now = option_price_proxy("delta_proxy", opt_entry, entry_spot, float(px), delta, side)

        # update trailing
        if trail_on:
            if side == "sell":
                best_for_trail = min(best_for_trail, opt_now)
                trail_stop = best_for_trail * (1 + trail_pct)
                if opt_now >= trail_stop:
                    exit_reason = "trail_stop"
                    exit_ts = ts
                    exit_opt = opt_now
                    break
            else:
                best_for_trail = max(best_for_trail, opt_now)
                trail_stop = best_for_trail * (1 - trail_pct)
                if opt_now <= trail_stop:
                    exit_reason = "trail_stop"
                    exit_ts = ts
                    exit_opt = opt_now
                    break

        # hard stops / targets
        if side == "sell":
            if opt_now <= target_opt:
                exit_reason = "target"
                exit_ts = ts
                exit_opt = opt_now
                break
            if opt_now >= stop_opt:
                exit_reason = "stop"
                exit_ts = ts
                exit_opt = opt_now
                break
        else:
            if opt_now >= target_opt:
                exit_reason = "target"
                exit_ts = ts
                exit_opt = opt_now
                break
            if opt_now <= stop_opt:
                exit_reason = "stop"
                exit_ts = ts
                exit_opt = opt_now
                break

    # 7) finalize P&L in option points
    pnl_pts = (opt_entry - exit_opt) if side=="sell" else (exit_opt - opt_entry)

    trade = {
        "symbol": symbol,
        "trade_date": trade_dt.isoformat(),
        "anchor": hour_open_ts.time().isoformat(),
        "expiry": exp.isoformat(),
        "opt_type": t_sel,
        "strike": int(k_sel),
        "side": side,
        "entry_ts": entry_ts.isoformat(),
        "exit_ts": exit_ts.isoformat(),
        "entry_spot": float(entry_spot),
        "exit_spot": float(spot_path["close"][-1]),
        "delta_used": delta,
        "entry_opt": float(opt_entry),
        "exit_opt": float(exit_opt),
        "pnl_pts": float(pnl_pts),
        "exit_reason": exit_reason,
    }
    trades.append(trade)
    return trades

# -------------------- driver --------------------

def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)

def parse_hhmm(s: str) -> time:
    parts = s.split(":")
    hh, mm = int(parts[0]), int(parts[1])
    return time(hh, mm, 0)

def main(args):
    cfg = CFG
    options_root = cfg["raw_options_dir"]
    cal = load_calendar(cfg["calendar_csv"])
    step = cfg["strike_step"].get(args.symbol, cfg["strike_step"]["NIFTY"])
    burst_secs = cfg["signal"]["burst_secs"]
    avg_secs = cfg["signal"]["avg_secs"]
    mult = args.multiplier if args.multiplier else cfg["signal"]["multiplier"]
    side = cfg["risk"]["side"]
    target_pct = cfg["risk"]["target_pct"]
    stop_pct = cfg["risk"]["stop_pct"]
    trail_pct = cfg["risk"]["trail_pct"]
    pnl_mode = cfg["pnl_mode"]

    # session/anchors
    t_open = parse_hhmm(cfg["session"]["open"])
    t_close = parse_hhmm(cfg["session"]["close"])
    anchors = [parse_hhmm(x) for x in cfg["session"]["anchors"]]

    out_dir = ROOT / "backtests" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    cache = SecondsCache(cfg["cache_dir"])
    spot_store = SpotStore(args.spot_glob or cfg["spot_glob"], args.symbol)

    trades_all: list[pl.DataFrame] = []

    start_d = datetime.fromisoformat(args.start).date()
    end_d   = datetime.fromisoformat(args.end).date()

    pbar = tqdm(list(daterange(start_d, end_d)), desc="Days")
    for d in pbar:
        # day-level bounds
        o = datetime.combine(d, t_open).replace(tzinfo=IST_TZ)
        c = datetime.combine(d, t_close).replace(tzinfo=IST_TZ)
        for hh in anchors:
            anchor = datetime.combine(d, hh).replace(tzinfo=IST_TZ)
            if anchor < o or anchor >= c:
                continue
            hour_end = min(anchor + timedelta(hours=1), c)
            trades = backtest_hour(
                symbol=args.symbol,
                trade_dt=d,
                hour_open_ts=anchor,
                hour_end_ts=hour_end,
                spot_store=spot_store,
                cal=cal,
                cache=cache,
                options_root=options_root,
                step=step,
                burst_secs=burst_secs,
                avg_secs=avg_secs,
                mult=mult,
                side=side,
                target_pct=target_pct,
                stop_pct=stop_pct,
                trail_pct=trail_pct,
                pnl_mode=pnl_mode,
            )
            if trades:
                trades_all.append(pl.DataFrame(trades))

    if not trades_all:
        print("No trades generated in the requested window.")
        return

    trades_df = pl.concat(trades_all).with_columns([
        pl.col("pnl_pts").cum_sum().alias("pnl_cum"),
    ])

    # summary per-day
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

    tag = f"{args.symbol}_{args.start}_{args.end}"
    trades_path  = out_dir / f"trades_{tag}.parquet"
    summary_path = out_dir / f"summary_{tag}.parquet"
    trades_df.write_parquet(trades_path)
    summary.write_parquet(summary_path)
    print(f"Wrote:\n  {trades_path}\n  {summary_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--multiplier", type=float, default=None, help="volume burst multiplier override")
    ap.add_argument("--spot-glob", type=str, default=None, help="override spot glob, e.g. ./data/packed/spot/{symbol}/**/*.parquet")
    ap.add_argument("--workers", type=int, default=0, help="(reserved) parallelism not needed for 1s steps")
    args = ap.parse_args()
    main(args)