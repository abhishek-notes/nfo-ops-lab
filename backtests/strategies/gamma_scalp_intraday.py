#!/usr/bin/env python3
from __future__ import annotations
import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import polars as pl

from backtests.engine.intraday import load_day_cache


@dataclass(frozen=True)
class Params:
    band_pts: float = 30.0
    target_pts: float = 8.0
    stop_pts: float = 5.0
    trail_trigger_pts: float = 8.0
    trail_step_pts: float = 2.0
    slippage_pts: float = 0.5


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Intraday Gamma Scalp (skeleton) using wide cache")
    ap.add_argument("--symbol", default="NIFTY")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--cache-dir", default="cache/seconds")
    ap.add_argument("--band-pts", type=float, default=30.0)
    ap.add_argument("--target-pts", type=float, default=8.0)
    ap.add_argument("--stop-pts", type=float, default=5.0)
    ap.add_argument("--trail-trigger-pts", type=float, default=8.0)
    ap.add_argument("--trail-step-pts", type=float, default=2.0)
    ap.add_argument("--slippage-pts", type=float, default=0.5)
    return ap.parse_args()


def daterange(s: date, e: date):
    d = s
    while d <= e:
        yield d
        d += timedelta(days=1)


def run_day(cache_dir: str, symbol: str, day: date, params: Params) -> pl.DataFrame:
    cache = load_day_cache(cache_dir, symbol, day)
    if cache is None:
        return pl.DataFrame(schema={
            "symbol": pl.Utf8,
            "trade_date": pl.Date,
            "expiry": pl.Date,
            "entry_ts": pl.Datetime("ns", time_zone="Asia/Kolkata"),
            "exit_ts": pl.Datetime("ns", time_zone="Asia/Kolkata"),
            "opt_type": pl.Utf8,
            "strike": pl.Int64,
            "side": pl.Utf8,
            "entry_opt": pl.Float64,
            "exit_opt": pl.Float64,
            "pnl_pts": pl.Float64,
            "exit_reason": pl.Utf8,
        })
    # TODO: implement gamma scalp logic using spot path or synthetic from options grid
    return pl.DataFrame({
        "symbol": [],
        "trade_date": [],
        "expiry": [],
        "entry_ts": [],
        "exit_ts": [],
        "opt_type": [],
        "strike": [],
        "side": [],
        "entry_opt": [],
        "exit_opt": [],
        "pnl_pts": [],
        "exit_reason": [],
    })


def main() -> None:
    args = parse_args()
    s = date.fromisoformat(args.start)
    e = date.fromisoformat(args.end)
    params = Params(
        band_pts=args.band_pts,
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
    out_path = out_dir / f"gamma_scalp_intraday_{args.symbol}_{args.start}_{args.end}.parquet"
    out.write_parquet(out_path)
    print(f"Wrote {out.height} trades to {out_path}")


if __name__ == "__main__":
    pl.Config.set_tbl_rows(10)
    main()

