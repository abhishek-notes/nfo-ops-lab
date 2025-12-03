#!/usr/bin/env python3
from __future__ import annotations
import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time
from pathlib import Path
import polars as pl

from backtests.engine.intraday import load_day_cache
from backtests.utils.ladder import IST, anchors_from_strings, strike_step_for, spot_path_for_day


@dataclass(frozen=True)
class Params:
    mult: float = 1.5
    burst_secs: int = 30
    base_secs: int = 300
    target_pts: float = 5.0
    stop_pts: float = 3.0
    trail_trigger_pts: float = 5.0
    trail_step_pts: float = 1.0
    slippage_pts: float = 0.5


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Intraday Volume Burst (option vol-driven) using wide cache")
    ap.add_argument("--symbol", default="NIFTY")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--cache-dir", default="cache/seconds")
    ap.add_argument("--mult", type=float, default=1.5)
    ap.add_argument("--burst-secs", type=int, default=30)
    ap.add_argument("--base-secs", type=int, default=300)
    ap.add_argument("--target-pts", type=float, default=5.0)
    ap.add_argument("--stop-pts", type=float, default=3.0)
    ap.add_argument("--trail-trigger-pts", type=float, default=5.0)
    ap.add_argument("--trail-step-pts", type=float, default=1.0)
    ap.add_argument("--slippage-pts", type=float, default=0.5)
    return ap.parse_args()


def daterange(s: date, e: date):
    d = s
    while d <= e:
        yield d
        d += timedelta(days=1)


def run_day(cache_dir: str, symbol: str, day: date, params: Params) -> pl.DataFrame:
    cache = load_day_cache(cache_dir, symbol, day)
    schema = {
        "symbol": pl.Utf8,
        "trade_date": pl.Date,
        "expiry": pl.Date,
        "entry_ts": pl.Datetime("ns", time_zone=IST),
        "exit_ts": pl.Datetime("ns", time_zone=IST),
        "opt_type": pl.Utf8,
        "strike": pl.Int64,
        "side": pl.Utf8,
        "entry_opt": pl.Float64,
        "exit_opt": pl.Float64,
        "pnl_pts": pl.Float64,
        "exit_reason": pl.Utf8,
    }
    if cache is None:
        return pl.DataFrame(schema=schema)

    # Load spot day for trend filter
    spot_p = spot_path_for_day(symbol, day)
    if not spot_p.exists():
        return pl.DataFrame(schema=schema)
    spot = pl.read_parquet(str(spot_p))
    if "timestamp" in spot.columns:
        spot = spot.rename({"timestamp": "ts"})
    spot = spot.select(["ts", "close"]).with_columns(
        pl.col("ts").dt.replace_time_zone(IST).dt.cast_time_unit("ns")
    ).sort("ts")

    # Determine ATM strike at 10:00 anchor
    anchors = anchors_from_strings(day, ["10:00"])  # first anchor only for ATM
    anchor_ts = anchors[0]
    # last spot close at/before anchor
    s_anchor = pl.DataFrame({"t": [anchor_ts]}).with_columns(
        pl.col("t").dt.replace_time_zone(IST).dt.cast_time_unit("ns")
    ).join_asof(spot.rename({"ts": "ts_s"}), left_on="t", right_on="ts_s", strategy="backward")
    if s_anchor.is_empty() or s_anchor[0, "close"] is None:
        return pl.DataFrame(schema=schema)
    step = strike_step_for(symbol)
    atm_guess = int(round(s_anchor[0, "close"] / step) * step)
    # Find nearest available strike in cache columns
    cols = cache.df.columns
    strikes = sorted({int(c.split("_")[-1]) for c in cols if c.startswith("close_CE_")})
    if not strikes:
        return pl.DataFrame(schema=schema)
    atm = min(strikes, key=lambda k: abs(k - atm_guess))

    # Build per-second vol sums and baseline for ATM CE/PE
    df = cache.df.sort("ts")
    ce_vol = df.select(["ts", f"vol_CE_{atm}"]).rename({f"vol_CE_{atm}": "vol"})
    pe_vol = df.select(["ts", f"vol_PE_{atm}"]).rename({f"vol_PE_{atm}": "vol"})
    def burst_frame(vol_df: pl.DataFrame) -> pl.DataFrame:
        return (
            vol_df
            .with_columns([
                pl.col("vol").fill_null(0).alias("vol"),
                pl.col("vol").rolling_sum(window_size=params.burst_secs, min_periods=params.burst_secs).alias("vol_30s"),
                (pl.col("vol").rolling_mean(window_size=params.base_secs, min_periods=params.base_secs) * params.burst_secs).alias("base_30s"),
            ])
            .with_columns((pl.col("vol_30s") > params.mult * pl.col("base_30s")).alias("burst"))
        )
    ce_b = burst_frame(ce_vol)
    pe_b = burst_frame(pe_vol)

    # Compute simple 60s spot momentum for direction at second resolution
    spot_sec = spot.with_columns(pl.col("ts").dt.truncate("1s").alias("sec")).group_by("sec").agg(pl.col("close").last()).rename({"sec": "ts", "close": "sclose"}).sort("ts")
    spot_mom = spot_sec.with_columns(
        (pl.col("sclose") - pl.col("sclose").shift(params.burst_secs)).alias("mom")
    ).select(["ts", "mom"]).with_columns(pl.col("mom").fill_null(0))

    # Merge frames
    merged = df.select(["ts"]).join(ce_b.select(["ts", "vol_30s", "base_30s", "burst"]).rename({"vol_30s": "ce_30s", "base_30s": "ce_base", "burst": "ce_burst"}), on="ts", how="left") \
        .join(pe_b.select(["ts", "vol_30s", "base_30s", "burst"]).rename({"vol_30s": "pe_30s", "base_30s": "pe_base", "burst": "pe_burst"}), on="ts", how="left") \
        .join(spot_mom, on="ts", how="left") \
        .with_columns([
            pl.col("ce_burst").fill_null(False),
            pl.col("pe_burst").fill_null(False),
            pl.col("mom").fill_null(0),
        ])

    # Choose side based on spot momentum at trigger second; first qualified across CE/PE
    trigger_row = None
    for row in merged.iter_rows(named=True):
        if not row["ce_burst"] and not row["pe_burst"]:
            continue
        mom = float(row["mom"]) if row["mom"] is not None else 0.0
        side = None
        opt_type = None
        if mom >= 0 and row["ce_burst"]:
            side = "buy"; opt_type = "CE"
        elif mom < 0 and row["pe_burst"]:
            side = "buy"; opt_type = "PE"
        else:
            # if mismatch (e.g., mom up but only pe bursts), skip
            continue
        trigger_row = (row["ts"], side, opt_type)
        break

    if trigger_row is None:
        return pl.DataFrame(schema=schema)

    t0 = trigger_row[0]
    entry_ts = (t0 + timedelta(seconds=1)).astimezone()  # will be normalized
    entry_ts_pl = pl.lit(entry_ts).cast(pl.Datetime("ns", time_zone=IST))
    price_col = f"close_{trigger_row[2]}_{atm}"
    series = df.select(["ts", price_col]).rename({price_col: "px"})
    # Entry price
    entry_df = series.filter(pl.col("ts") == entry_ts_pl)
    if entry_df.is_empty() or entry_df[0, "px"] is None:
        return pl.DataFrame(schema=schema)
    entry_price = float(entry_df[0, "px"]) + params.slippage_pts  # buy

    # Simulate forward
    hist = series.filter(pl.col("ts") >= entry_ts_pl).sort("ts")
    peak = entry_price
    trail_active = False
    trail_stop = None
    exit_ts = None
    exit_price = None
    exit_reason = None
    for r in hist.iter_rows(named=True):
        px = float(r["px"]) if r["px"] is not None else peak
        # Target -> enable trailing
        if not trail_active and (px - entry_price) >= params.trail_trigger_pts:
            trail_active = True
            peak = max(peak, px)
            trail_stop = peak - params.trail_step_pts
        if trail_active:
            if px > peak:
                peak = px
                trail_stop = peak - params.trail_step_pts
            if px <= trail_stop:
                exit_ts = r["ts"]
                exit_price = px - params.slippage_pts
                exit_reason = "trailing"
                break
        # Hard stop
        if (entry_price - px) >= params.stop_pts:
            exit_ts = r["ts"]
            exit_price = px - params.slippage_pts
            exit_reason = "stop"
            break
        # Hard target (if no trailing configured)
        if params.trail_trigger_pts <= 0 and (px - entry_price) >= params.target_pts:
            exit_ts = r["ts"]
            exit_price = px - params.slippage_pts
            exit_reason = "target"
            break
    # EOD exit
    if exit_ts is None:
        last = hist.tail(1)
        if not last.is_empty():
            exit_ts = last[0, "ts"]
            exit_price = float(last[0, "px"]) - params.slippage_pts
            exit_reason = "eod"

    if exit_ts is None or exit_price is None:
        return pl.DataFrame(schema=schema)

    pnl = (exit_price - entry_price)
    return pl.DataFrame({
        "symbol": [symbol.upper()],
        "trade_date": [day],
        "expiry": [cache.expiry],
        "entry_ts": [entry_ts],
        "exit_ts": [exit_ts],
        "opt_type": [trigger_row[2]],
        "strike": [atm],
        "side": [trigger_row[1]],
        "entry_opt": [entry_price],
        "exit_opt": [exit_price],
        "pnl_pts": [pnl],
        "exit_reason": [exit_reason],
    })


def main() -> None:
    args = parse_args()
    s = date.fromisoformat(args.start)
    e = date.fromisoformat(args.end)
    params = Params(
        mult=args.mult,
        burst_secs=args.burst_secs,
        base_secs=args.base_secs,
        target_pts=args.target_pts,
        stop_pts=args.stop_pts,
        trail_trigger_pts=args.trail_trigger_pts,
        trail_step_pts=args.trail_step_pts,
        slippage_pts=args.slippage_pts,
    )
    trades: list[pl.DataFrame] = []
    for d in daterange(s, e):
        trades.append(run_day(args.cache_dir, args.symbol, d, params))
    out = pl.concat(trades) if trades else pl.DataFrame()
    out_dir = Path("backtests/results")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"volume_burst_intraday_{args.symbol}_{args.start}_{args.end}.parquet"
    out.write_parquet(out_path)
    print(f"Wrote {out.height} trades to {out_path}")


if __name__ == "__main__":
    pl.Config.set_tbl_rows(10)
    main()
