#!/usr/bin/env python3
"""
Run a suite of strategies over a date range with robust logging and skip-on-error.

Features
- Strategy registry: maps names → callables with parameter builders.
- Calendar mapping: trade_date → next expiry (weekly/monthly) from meta/expiry_calendar.csv
- Spot/ATM utilities: compute ATM strike at anchor per day.
- Parallel execution with process pool; per-task logs; master status CSV.
- Error handling: Catch/log exceptions; continue.

Usage
  python3 backtests/run_all_strategies.py --symbol BANKNIFTY --start 2024-07-01 --end 2024-08-15 \
      --max-workers 4 --include atm_volume_ultra,gamma_scalp,iv_rv,expiry_crush,short_straddle,dispersion,oi_shift

Results
- Strategy-specific outputs under backtests/results by called modules.
- Master status at backtests/results/run_all_status_{tag}.parquet
- Logs under backtests/logs/
"""
from __future__ import annotations
import argparse, os, traceback, sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from pathlib import Path
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Imports of strategy modules
from backtests import atm_volume_ultra
from backtests.strategies import (
    gamma_scalp_baseline as strat_gamma,
    iv_rv_spread_proxy as strat_ivrv,
    expiry_iv_crush_play as strat_crush,
    short_straddle_premium_decay as strat_short,
    dispersion_proxy_index_vs_basket as strat_disp,
    oi_shift_breakout as strat_oi,
    opening_range_breakout_options as strat_orb,
    vwap_mean_reversion_options as strat_vwap,
    iron_condor_intraday as strat_condor,
    calendar_spread_weekly_proxy as strat_cal,
    momentum_scalp as strat_momo,
    volume_spike_minute as strat_vspike,
)

IST = "Asia/Kolkata"
CAL_PATH = "meta/expiry_calendar.csv"
SPOT_DIR = "data/packed/spot"


def ensure_dirs():
    (ROOT/"backtests"/"logs").mkdir(parents=True, exist_ok=True)
    (ROOT/"backtests"/"results").mkdir(parents=True, exist_ok=True)


def schema_audit(symbol: str, d0: date, d1: date) -> Path:
    """Lightweight audit of packed schemas across time to catch older format drift.
    Writes a log under backtests/logs/ and returns its path.
    """
    logp = ROOT/"backtests"/"logs"/f"schema_audit_{symbol}_{d0}_{d1}.log"
    lines = []
    try:
        # Sample: packed manifest for options (20 random rows over range)
        man = ROOT/"meta"/"packed_manifest.csv"
        if man.exists():
            m = pl.read_csv(str(man))
            m = m.filter(pl.col("symbol")==symbol)
            sample = m.head(20)
            for row in sample.iter_rows(named=True):
                p = Path(row["path"]).resolve()
                try:
                    lf = pl.scan_parquet(str(p))
                    schema = lf.schema
                    ts_dtype = schema.get("timestamp")
                    has_vol = "vol_delta" in schema
                    lines.append(f"[OPTION] {p} timestamp={ts_dtype} vol_delta={has_vol}")
                except Exception as e:
                    lines.append(f"[OPTION] {p} ERROR {e}")
        # Sample: spot first day of each quarter
        dd = d0
        seen = set()
        while dd <= d1:
            yyyymm = f"{dd.year:04d}{dd.month:02d}"
            sp = ROOT/ SPOT_DIR / symbol / yyyymm
            if sp.exists():
                for child in sorted(sp.glob("date=*/ticks.parquet"))[:1]:
                    try:
                        lf = pl.scan_parquet(str(child))
                        schema = lf.schema
                        ts_dtype = schema.get("timestamp") or schema.get("ts")
                        cols = list(schema.keys())
                        lines.append(f"[SPOT] {child} ts={ts_dtype} cols={cols}")
                    except Exception as e:
                        lines.append(f"[SPOT] {child} ERROR {e}")
            # advance by ~90 days
            dd = dd + timedelta(days=90)
    except Exception as e:
        lines.append(f"[AUDIT ERROR] {e}")
    with open(logp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines)+"\n")
    return logp


def parse_date(s: str) -> date:
    return datetime.fromisoformat(s).date()


def load_calendar(path: str) -> pl.DataFrame:
    cal = pl.read_csv(path).rename({"Instrument":"symbol","Final_Expiry":"expiry","Expiry_Type":"kind"})
    return (
        cal.select([
            pl.col("symbol").str.to_uppercase(),
            pl.col("kind").str.to_lowercase(),
            pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"),
        ])
        .drop_nulls(["symbol","kind","expiry"]).unique().sort(["symbol","expiry"]) )


def next_expiry_for(symbol: str, d: date, cal: pl.DataFrame) -> date | None:
    x = cal.filter((pl.col("symbol")==symbol) & (pl.col("expiry")>=d)).select("expiry").head(1)
    return x.item() if x.height else None


def dt_ist(d: date, t: time) -> datetime:
    from dateutil import tz
    return datetime.combine(d, t).replace(tzinfo=tz.gettz(IST))


def spot_at(symbol: str, d: date, hh: int, mm: int=0) -> float | None:
    yyyymm = f"{d.year:04d}{d.month:02d}"
    p = ROOT/ SPOT_DIR / symbol / yyyymm / f"date={d}" / "ticks.parquet"
    if not p.exists():
        return None
    df = pl.read_parquet(str(p)).select(["timestamp","close"])\
         .with_columns(pl.col("timestamp").dt.replace_time_zone(IST).dt.cast_time_unit("ns"))\
         .rename({"timestamp":"ts"})
    A = dt_ist(d, time(hh,mm))
    try:
        return float(df.filter(pl.col("ts") == pl.lit(A).cast(pl.Datetime("ns", time_zone=IST))).select("close").item())
    except Exception:
        return None


def nearest_strike(px: float, step: int) -> int:
    return int(round(px/step)*step)


@dataclass
class Task:
    name: str
    params: dict


def build_tasks(symbol: str, d0: date, d1: date, cal: pl.DataFrame, include: set[str]) -> list[Task]:
    tasks: list[Task] = []

    # 1) ATM volume burst (runs whole range once)
    if "atm_volume_ultra" in include:
        tasks.append(Task("atm_volume_ultra", dict(symbol=symbol, start=d0.isoformat(), end=d1.isoformat(), kwargs={})))

    # Discover expiries in the window
    d = d0
    expiries: set[str] = set()
    trading_days: list[date] = []
    while d <= d1:
        trading_days.append(d)
        e = next_expiry_for(symbol, d, cal)
        if e:
            expiries.add(e.isoformat())
        d += timedelta(days=1)

    # 2) Gamma scalp baseline per expiry
    if "gamma_scalp" in include:
        for exp in sorted(expiries):
            tasks.append(Task("gamma_scalp", dict(symbol=symbol, start=d0.isoformat(), end=d1.isoformat(), expiry=exp)))

    # 3) IV-RV spread proxy over full range
    if "iv_rv" in include:
        tasks.append(Task("iv_rv", dict(symbol=symbol, start=d0.isoformat(), end=d1.isoformat())))

    # 4) Expiry crush per expiry date (trade_date == expiry)
    if "expiry_crush" in include:
        for exp in sorted(expiries):
            tasks.append(Task("expiry_crush", dict(symbol=symbol, date=exp, expiry=exp)))

    # 5) Short straddle per expiry
    if "short_straddle" in include:
        for exp in sorted(expiries):
            tasks.append(Task("short_straddle", dict(symbol=symbol, start=d0.isoformat(), end=d1.isoformat(), expiry=exp, anchor="11:00")))

    # 6) Dispersion proxy over full range
    if "dispersion" in include:
        tasks.append(Task("dispersion", dict(symbol=symbol, start=d0.isoformat(), end=d1.isoformat())))

    # 7) OI shift breakout per trading day using ATM at 10:00
    if "oi_shift" in include:
        step = 100 if symbol=="BANKNIFTY" else 50
        for d in trading_days:
            px = spot_at(symbol, d, 10, 0)
            if px is None:
                continue
            k = nearest_strike(px, step)
            exp = next_expiry_for(symbol, d, cal)
            if not exp:
                continue
            tasks.append(Task("oi_shift", dict(symbol=symbol, date=d.isoformat(), expiry=exp.isoformat(), strike=k, side="with_flow")))

    # 8) Opening range breakout per trading day
    if "orb" in include:
        for d in trading_days:
            exp = next_expiry_for(symbol, d, cal)
            if not exp:
                continue
            tasks.append(Task("orb", dict(symbol=symbol, date=d.isoformat(), expiry=exp.isoformat())))

    # 9) VWAP-like mean reversion per trading day (13:00→14:00)
    if "vwap_mr" in include:
        for d in trading_days:
            exp = next_expiry_for(symbol, d, cal)
            if not exp:
                continue
            tasks.append(Task("vwap_mr", dict(symbol=symbol, date=d.isoformat(), expiry=exp.isoformat(), threshold=0.003)))

    # 10) Iron condor per trading day (11:00→15:00)
    if "iron_condor" in include:
        for d in trading_days:
            exp = next_expiry_for(symbol, d, cal)
            if not exp:
                continue
            tasks.append(Task("iron_condor", dict(symbol=symbol, date=d.isoformat(), expiry=exp.isoformat())))

    # 11) Calendar spread weekly proxy per trading day
    if "calendar_proxy" in include:
        for d in trading_days:
            tasks.append(Task("calendar_proxy", dict(symbol=symbol, date=d.isoformat(), stance="front_short_back_long")))

    # 12) Momentum scalp per trading day (both buy and sell_opposite)
    if "momentum_scalp" in include:
        for d in trading_days:
            for mode in ("buy","sell_opposite"):
                params = dict(
                    symbol=symbol,
                    start=d.isoformat(),
                    end=d.isoformat(),
                    mode=mode,
                    short_secs=3,
                    base_secs=20,
                    price_thresh=3 if symbol=="BANKNIFTY" else 1.5,
                    vol_mult=1.05,
                    target_pts=2.0,
                    stop_pts=1.0,
                    trail_pts=0.5,
                    drop_confirm_secs=2,
                )
                tasks.append(Task("momentum_scalp", params))

    # 13) Volume spike (minute) per trading day
    if "vol_spike_minute" in include:
        for d in trading_days:
            params = dict(
                symbol=symbol,
                start=d.isoformat(),
                end=d.isoformat(),
                mult=4.0,
                lookback_min=5,
                target_pts=5.0,
                stop_pts=3.0,
                trail_pts=1.0,
            )
            tasks.append(Task("vol_spike_minute", params))

    return tasks


def run_task(t: Task) -> dict:
    name = t.name
    params = t.params
    started = datetime.now()
    ok = False
    err = None
    try:
        if name == "atm_volume_ultra":
            atm_volume_ultra.run(params["symbol"], params["start"], params["end"], spot_glob=None, burst_mult=None)
        elif name == "gamma_scalp":
            strat_gamma.run(params["symbol"], params["start"], params["end"], params["expiry"])
        elif name == "iv_rv":
            strat_ivrv.run(params["symbol"], params["start"], params["end"], iv_threshold=0.0)
        elif name == "expiry_crush":
            strat_crush.run(params["symbol"], params["date"], params["expiry"])
        elif name == "short_straddle":
            strat_short.run(params["symbol"], params["start"], params["end"], params["expiry"], anchor=params.get("anchor","11:00"))
        elif name == "dispersion":
            strat_disp.run(params["symbol"], params["start"], params["end"])
        elif name == "oi_shift":
            strat_oi.run(params["symbol"], params["date"], params["expiry"], params["strike"], side=params.get("side","with_flow"))
        elif name == "orb":
            strat_orb.run(params["symbol"], params["date"], params["expiry"])
        elif name == "vwap_mr":
            strat_vwap.run(params["symbol"], params["date"], params["expiry"], threshold=params.get("threshold",0.003))
        elif name == "iron_condor":
            strat_condor.run(params["symbol"], params["date"], params["expiry"])
        elif name == "calendar_proxy":
            strat_cal.run(params["symbol"], params["date"], stance=params.get("stance","front_short_back_long"))
        elif name == "momentum_scalp":
            strat_momo.run(
                params["symbol"], params["start"], params["end"],
                short_secs=int(params.get("short_secs",3)),
                base_secs=int(params.get("base_secs",30)),
                price_thresh=float(params.get("price_thresh", 5 if params.get("symbol")=="BANKNIFTY" else 3)),
                vol_mult=float(params.get("vol_mult",1.2)),
                mode=params.get("mode","buy"),
                target_pts=float(params.get("target_pts",2.0)),
                stop_pts=float(params.get("stop_pts",1.0)),
                trail_pts=float(params.get("trail_pts",0.5)),
                drop_confirm_secs=int(params.get("drop_confirm_secs",3)),
            )
        elif name == "vol_spike_minute":
            strat_vspike.run(
                params["symbol"], params["start"], params["end"],
                mult=float(params.get("mult",4.0)),
                lookback_min=int(params.get("lookback_min",5)),
                target_pts=float(params.get("target_pts",5.0)),
                stop_pts=float(params.get("stop_pts",3.0)),
                trail_pts=float(params.get("trail_pts",1.0)),
            )
        else:
            raise ValueError(f"Unknown strategy: {name}")
        ok = True
    except Exception as e:
        err = f"{e}\n{traceback.format_exc()}"
        # Write per-task log
        logp = ROOT/"backtests"/"logs"/f"{name}_{started:%Y%m%d_%H%M%S}.log"
        with open(logp, "w", encoding="utf-8") as f:
            f.write(f"Task: {name}\nParams: {params}\nError: {err}\n")
    finished = datetime.now()
    return {
        "strategy": name,
        "params": str(params),
        "ok": ok,
        "error": err or "",
        "started": started.isoformat(),
        "finished": finished.isoformat(),
        "duration_s": (finished-started).total_seconds(),
    }


def main():
    ensure_dirs()
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--max-workers", type=int, default=2)
    ap.add_argument("--include", default="atm_volume_ultra,gamma_scalp,iv_rv,expiry_crush,short_straddle,dispersion,oi_shift,orb,vwap_mr,iron_condor,calendar_proxy",
                    help="Comma-separated list of strategies")
    args = ap.parse_args()

    d0, d1 = parse_date(args.start), parse_date(args.end)
    cal = load_calendar(CAL_PATH)
    include = set([x.strip() for x in args.include.split(",") if x.strip()])
    audit_log = schema_audit(args.symbol, d0, d1)
    print(f"Wrote schema audit: {audit_log}")
    tasks = build_tasks(args.symbol, d0, d1, cal, include)
    print(f"Scheduled {len(tasks)} tasks: {[t.name for t in tasks[:10]]}{'...' if len(tasks)>10 else ''}")

    results = []
    with ProcessPoolExecutor(max_workers=args.max_workers) as ex:
        futs = [ex.submit(run_task, t) for t in tasks]
        for fut in as_completed(futs):
            res = fut.result()
            results.append(res)
            status = "OK" if res["ok"] else "FAIL"
            print(f"[{status}] {res['strategy']} in {res['duration_s']:.1f}s")

    tag = f"{args.symbol}_{args.start}_{args.end}"
    out = ROOT/"backtests"/"results"/f"run_all_status_{tag}.parquet"
    pl.DataFrame(results).write_parquet(out)
    print(f"Wrote status: {out}")


if __name__ == "__main__":
    main()
