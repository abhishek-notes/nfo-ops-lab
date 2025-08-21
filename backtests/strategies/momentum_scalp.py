#!/usr/bin/env python3
"""
Momentum scalp based on short-window price momentum + option volume burst.

Signal (per day):
- Price momentum: |spot[t] - spot[t-ws]| >= price_thresh (points) in last `ws` seconds.
- Volume burst: (ATM CE+PE vol_delta sum over last `ws`) > mult * (avg per-sec vol over last `base` seconds * ws)

Action:
- If price up + vol burst -> direction="up"; else if price down + vol burst -> direction="down".
- Enter long option in direction (call for up, put for down) OR sell opposite if mode="sell_opposite".
- Use option delta proxy for PnL; target/stop/trailing in option points.

Data:
- Spot: packed day ticks; dense 1s grid via truncate + ffill.
- Option per-second volume: uses cache if present (backtests/cache/seconds), otherwise computes ATM CE/PE seconds for the day.
"""
from __future__ import annotations
from datetime import datetime, date, time, timedelta
from pathlib import Path
import argparse
import polars as pl
from dateutil import tz

IST = "Asia/Kolkata"
CACHE_DIR = Path("backtests/cache/seconds")
OPTIONS_ROOT = "data/packed/options"
SPOT_ROOT = "data/packed/spot"

def dt_ist(d: date, t: time) -> datetime:
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))

def nearest_strike(px: float, step: int) -> int:
    return int(round(px/step)*step)

def dense_spot_1s(symbol: str, d: date) -> pl.DataFrame:
    yyyymm=f"{d.year:04d}{d.month:02d}"
    p=Path(SPOT_ROOT)/symbol/yyyymm/f"date={d}"/"ticks.parquet"
    df=pl.read_parquet(str(p)).select(["timestamp","close"]).rename({"timestamp":"ts"})
    df = df.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    # truncate to 1s and forward fill over dense index
    sec = (df.with_columns(pl.col("ts").dt.truncate("1s").alias("ts"))
             .group_by("ts").agg(pl.col("close").last().alias("close"))
             .sort("ts"))
    t0 = dt_ist(d, time(9,15)); t1 = dt_ist(d, time(15,30))
    idx = pl.DataFrame({"ts": pl.datetime_range(t0, t1, "1s", time_zone=IST, eager=True)})
    sec = idx.join(sec, on="ts", how="left").with_columns(pl.col("close").fill_null(strategy="forward"))
    # backfill very first if needed
    if sec["close"].is_null().any():
        fc = float(df["close"].drop_nulls().head(1)[0])
        sec = sec.with_columns(pl.when(pl.col("close").is_null()).then(pl.lit(fc)).otherwise(pl.col("close")).alias("close"))
    return sec

def cache_path(symbol: str, d: date, expiry: date, opt_type: str, strike: int) -> Path:
    return CACHE_DIR / symbol / f"date={d.isoformat()}" / f"exp={expiry:%Y-%m-%d}" / f"type={opt_type}" / f"strike={strike}.parquet"

def option_file_path(symbol: str, expiry: date, opt_type: str, strike: int) -> Path:
    y=f"{expiry.year:04d}{expiry.month:02d}"
    return Path(OPTIONS_ROOT)/symbol/y/f"exp={expiry:%Y-%m-%d}"/f"type={opt_type}"/f"strike={strike}.parquet"

def load_or_build_seconds(symbol: str, d: date, expiry: date, strike: int, opt_type: str,
                          t0: datetime, t1: datetime) -> pl.DataFrame | None:
    cp = cache_path(symbol, d, expiry, opt_type, strike)
    if cp.exists():
        try:
            sec = pl.read_parquet(str(cp))
            return sec.with_columns(pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
        except Exception:
            pass
    f = option_file_path(symbol, expiry, opt_type, strike)
    if not f.exists():
        return None
    lf = pl.scan_parquet(str(f)).select("timestamp","close","vol_delta")
    if lf.schema["timestamp"] == pl.Utf8:
        lf = lf.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
    lf = lf.with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    s_l = pl.lit(t0).cast(pl.Datetime("ns", time_zone=IST)); e_l = pl.lit(t1).cast(pl.Datetime("ns", time_zone=IST))
    df = lf.filter((pl.col("timestamp")>=s_l)&(pl.col("timestamp")<=e_l)).collect()
    if df.is_empty():
        return None
    # per-second aggregate
    sec = (df.with_columns(pl.col("timestamp").dt.truncate("1s").alias("ts"))
             .group_by("ts").agg([
                 pl.col("vol_delta").sum().alias("vol"),
                 pl.col("close").last().alias("close"),
             ])
             .sort("ts"))
    # write cache
    try:
        cp.parent.mkdir(parents=True, exist_ok=True)
        sec.write_parquet(str(cp))
    except Exception:
        pass
    return sec

def option_price_proxy(entry_opt: float, entry_spot: float, spot_now: float, delta: float, side: str) -> float:
    sign = -1 if side == "sell" else 1
    est = entry_opt + sign * delta * (spot_now - entry_spot)
    return max(0.05, float(est))

def run(symbol: str, start: str, end: str,
        short_secs: int = 5, base_secs: int = 60,
        price_thresh: float = 10.0, vol_mult: float = 1.5,
        mode: str = "buy", target_pts: float = 3.0, stop_pts: float = 2.0,
        trail_pts: float = 1.0, drop_confirm_secs: int = 3):
    d0 = datetime.fromisoformat(start).date(); d1 = datetime.fromisoformat(end).date()
    step = 100 if symbol=="BANKNIFTY" else 50
    t0_time, t1_time = time(9,15), time(15,30)
    out_rows = []
    d = d0
    while d <= d1:
        try:
            spot = dense_spot_1s(symbol, d)
        except Exception:
            d = d + timedelta(days=1); continue
        t0 = dt_ist(d, t0_time); t1 = dt_ist(d, t1_time)
        # current ATM strike series per second
        # approximate ATM using spot at each second (rounded), but we will use fixed per entry to query CE/PE seconds
        # compute price momentum
        s_close = spot["close"]
        s_ts = spot["ts"]
        # rolling previous value at t - short_secs
        lag = s_close.shift(short_secs)
        price_diff = s_close - lag
        price_up = (price_diff >= price_thresh)
        price_down = (price_diff <= -price_thresh)
        # For volume: load ATM CE+PE seconds for the day using strike from the moment of entry
        # Precompute a simple per-second avg baseline by using CE/PE summed series derived at the entry strike
        # Iterate sparse candidates where price_up/down is true
        idxs = [i for i, (pu, pd) in enumerate(zip(price_up.to_list(), price_down.to_list())) if pu or pd]
        if not idxs:
            d = d + timedelta(days=1); continue
        i = 0
        while i < len(idxs):
            idx = idxs[i]
            ts = s_ts[idx]
            px = float(s_close[idx])
            direction = "up" if price_up[idx] else ("down" if price_down[idx] else None)
            if direction is None:
                i += 1; continue
            k_atm = nearest_strike(px, step)
            # robust: pick nearest available strike (CE & PE both exist) at the nearest weekly expiry within 4 weeks
            chosen = None
            exp = None
            for off in range(0, 28, 7):
                cand = (d + timedelta(days=off))
                y = f"{cand.year:04d}{cand.month:02d}"
                base = Path(OPTIONS_ROOT)/symbol/y/f"exp={cand:%Y-%m-%d}"
                if not base.exists():
                    continue
                try:
                    ce_dir = base/"type=CE"
                    pe_dir = base/"type=PE"
                    ces = []
                    pes = []
                    if ce_dir.exists():
                        ces = [int(p.name.split('=')[1].split('.')[0]) for p in ce_dir.glob('strike=*.parquet')]
                    if pe_dir.exists():
                        pes = [int(p.name.split('=')[1].split('.')[0]) for p in pe_dir.glob('strike=*.parquet')]
                    common = set(ces).intersection(pes)
                except Exception:
                    common = set()
                if not common:
                    continue
                k_use = min(common, key=lambda k: abs(k - k_atm))
                chosen = k_use
                exp = cand
                break
            if not exp:
                i += 1; continue
            k_use = chosen if chosen is not None else k_atm
            ce_sec = load_or_build_seconds(symbol, d, exp, k_use, "CE", t0, t1)
            pe_sec = load_or_build_seconds(symbol, d, exp, k_use, "PE", t0, t1)
            if ce_sec is None or pe_sec is None:
                i += 1; continue
            # join vol series by ts
            vol = (ce_sec.select(["ts","vol"]).rename({"vol":"vce"})
                    .join(pe_sec.select(["ts","vol"]).rename({"vol":"vpe"}), on="ts", how="inner")
                    .with_columns((pl.col("vce") + pl.col("vpe")).alias("vol"))
                    .select(["ts","vol"]).sort("ts"))
            # align to spot
            vol = spot.select("ts").join(vol, on="ts", how="left").with_columns(pl.col("vol").fill_null(0))
            # rolling vol signals
            vol_ws = vol.with_columns([
                pl.col("vol").rolling_sum(short_secs).fill_null(0).alias("vol_ws"),
                (pl.col("vol").rolling_mean(base_secs).fill_null(0) * short_secs).alias("vol_base")
            ])
            # find first ts where both conditions are true at/after idx
            pm_src = price_up if direction=="up" else price_down
            pm = pl.Series('pm', [1 if (x is True) else 0 for x in pm_src.to_list()])
            sig = pl.DataFrame({"ts": s_ts, "pm": pm, "vol_ws": vol_ws["vol_ws"], "vol_base": vol_ws["vol_base"]}).with_row_count('rn')
            sig = sig.filter((pl.col('rn') >= idx) & (pl.col('pm') == 1)) if vol_mult <= 0.0 else sig.filter((pl.col('rn') >= idx) & (pl.col('pm') == 1) & (pl.col('vol_ws') > vol_mult * pl.col('vol_base')))
            if sig.is_empty():
                i += 1; continue
            entry_ts = sig["ts"][0]
            # simulate to +60m or end of session
            day_end = dt_ist(d, time(15,30))
            path = spot.filter((pl.col("ts") >= pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))) & (pl.col("ts") <= pl.lit(day_end).cast(pl.Datetime("ns", time_zone=IST)))).select("ts","close")
            if path.is_empty():
                i += 1; continue
            # entry option price from CE/PE series at entry_ts (nearest exact from series)
            opt_type = "CE" if direction=="up" else "PE"
            sec_leg = ce_sec if opt_type=="CE" else pe_sec
            try:
                entry_opt = float(sec_leg.filter(pl.col("ts")==pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))).select("close").item())
            except Exception:
                # if missing exact second, take last known
                entry_opt = float(sec_leg.sort("ts").filter(pl.col("ts") <= pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))).tail(1).select("close").item())
            entry_spot = float(path["close"][0])
            side = ("sell" if mode=="sell_opposite" else "buy")
            delta = 0.5
            # targets
            tgt = entry_opt + (target_pts if side=="buy" else -target_pts)
            stp = entry_opt - (stop_pts if side=="buy" else -stop_pts)
            best = entry_opt
            exit_ts = path["ts"][-1]
            exit_opt = entry_opt
            exit_reason = "hour_end"
            # build momentum drop detector over path window
            # recompute pm series for direction within window
            # derive pm_dir: 1 if price momentum aligns with direction over short_secs
            # We evaluate drop when pm_dir==0 for drop_confirm_secs consecutively OR vol falls below baseline
            # Prepare helper series aligned to path
            base_df = pl.DataFrame({"ts": s_ts, "price": s_close})
            base_df = base_df.filter((pl.col("ts") >= pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))) & (pl.col("ts") <= pl.lit(day_end).cast(pl.Datetime("ns", time_zone=IST))))
            lagp = base_df["price"].shift(short_secs)
            pdiff = base_df["price"] - lagp
            pm_dir = (pdiff >= price_thresh) if direction=="up" else (pdiff <= -price_thresh)
            pm_dir = pm_dir.fill_null(False)
            # rolling drop counter
            drop_count = (~pm_dir).cast(pl.Int8).rolling_sum(drop_confirm_secs).fill_null(0)
            mom = pl.DataFrame({"ts": base_df["ts"], "drop": drop_count})
            # align vol condition too
            vol_sig = vol.with_columns([
                pl.col("vol").rolling_sum(short_secs).fill_null(0).alias("vol_ws"),
                (pl.col("vol").rolling_mean(base_secs).fill_null(0) * short_secs).alias("vol_base")
            ]).select(["ts","vol_ws","vol_base"]) 
            mom = mom.join(vol_sig, on="ts", how="left").with_columns(pl.col("vol_ws").fill_null(0), pl.col("vol_base").fill_null(0))
            for ts2, px2 in zip(path["ts"].to_list(), path["close"].to_list()):
                val = option_price_proxy(entry_opt, entry_spot, float(px2), delta, side)
                # exit on momentum drop
                try:
                    row = mom.filter(pl.col("ts")==pl.lit(ts2).cast(pl.Datetime("ns", time_zone=IST))).select(["drop","vol_ws","vol_base"]).row(0)
                    drop_now, vws, vbase = int(row[0]), float(row[1]), float(row[2])
                except Exception:
                    drop_now, vws, vbase = 0, 0.0, 0.0
                if drop_now >= drop_confirm_secs or (vws <= vol_mult * vbase and vbase>0):
                    exit_ts, exit_opt, exit_reason = ts2, val, "momentum_drop"; break
                # trailing
                if trail_pts and trail_pts > 0:
                    if side == "sell":
                        if val < best: best = val
                        trail_stop = best + trail_pts
                        if val >= trail_stop:
                            exit_ts, exit_opt, exit_reason = ts2, val, "trail_stop"; break
                    else:
                        if val > best: best = val
                        trail_stop = best - trail_pts
                        if val <= trail_stop:
                            exit_ts, exit_opt, exit_reason = ts2, val, "trail_stop"; break
                # hard target/stop
                if side == "sell":
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
                "opt_type": opt_type,
                "entry_opt": float(entry_opt),
                "exit_opt": float(exit_opt),
                "pnl": float(pnl),
                "exit_reason": exit_reason,
            })
            # skip ahead to avoid multiple overlapping entries within same minute
            i += 1
        d = d + timedelta(days=1)
    if out_rows:
        out = Path("backtests/results")/f"momentum_scalp_{symbol}_{start}_{end}_{mode}.parquet"
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
    ap.add_argument("--short-secs", type=int, default=5)
    ap.add_argument("--base-secs", type=int, default=60)
    ap.add_argument("--price-thresh", type=float, default=10.0)
    ap.add_argument("--vol-mult", type=float, default=1.5)
    ap.add_argument("--target-pts", type=float, default=3.0)
    ap.add_argument("--stop-pts", type=float, default=2.0)
    ap.add_argument("--trail-pts", type=float, default=1.0)
    a = ap.parse_args()
    run(a.symbol, a.start, a.end, a.short_secs, a.base_secs, a.price_thresh, a.vol_mult, a.mode, a.target_pts, a.stop_pts, a.trail_pts)
