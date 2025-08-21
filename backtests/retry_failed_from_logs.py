#!/usr/bin/env python3
from __future__ import annotations
import ast
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

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
)

def run_one(name: str, params: dict):
    if name == "atm_volume_ultra":
        atm_volume_ultra.run(params["symbol"], params["start"], params["end"], spot_glob=None, burst_mult=None)
    elif name == "gamma_scalp":
        strat_gamma.run(params["symbol"], params["start"], params["end"], params["expiry"])
    elif name == "iv_rv":
        strat_ivrv.run(params["symbol"], params["start"], params["end"], iv_threshold=float(params.get("iv_threshold", 0.0)))
    elif name == "expiry_crush":
        strat_crush.run(params["symbol"], params["date"], params["expiry"])
    elif name == "short_straddle":
        strat_short.run(params["symbol"], params["start"], params["end"], params["expiry"], anchor=params.get("anchor","11:00"))
    elif name == "dispersion":
        strat_disp.run(params["symbol"], params["start"], params["end"])
    elif name == "oi_shift":
        strat_oi.run(params["symbol"], params["date"], params["expiry"], int(params["strike"]), side=params.get("side","with_flow"))
    elif name == "orb":
        strat_orb.run(params["symbol"], params["date"], params["expiry"])
    elif name == "vwap_mr":
        strat_vwap.run(params["symbol"], params["date"], params["expiry"], threshold=float(params.get("threshold",0.003)))
    elif name == "iron_condor":
        strat_condor.run(params["symbol"], params["date"], params["expiry"])
    elif name == "calendar_proxy":
        strat_cal.run(params["symbol"], params["date"], stance=params.get("stance","front_short_back_long"))
    else:
        raise ValueError(f"Unknown strategy: {name}")

def parse_log(path: Path):
    name = None
    params = None
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("Task:"):
                name = line.split(":",1)[1].strip()
            if line.startswith("Params:"):
                payload = line.split(":",1)[1].strip()
                try:
                    params = ast.literal_eval(payload)
                except Exception:
                    # Try JSON-like fallbacks
                    params = None
    return name, params

def main():
    if len(sys.argv) < 2:
        print("Usage: retry_failed_from_logs.py <logfile>")
        sys.exit(2)
    logf = Path(sys.argv[1])
    name, params = parse_log(logf)
    if not name or not params:
        print(f"Could not parse task from {logf}")
        sys.exit(1)
    try:
        run_one(name, params)
        # mark success
        dest = logf.parent/"retried"/f"{logf.stem}_retried_{datetime.now():%Y%m%d_%H%M%S}.log"
        dest.parent.mkdir(parents=True, exist_ok=True)
        logf.rename(dest)
        print(f"Retried OK: {name} {params}")
    except Exception as e:
        print(f"Retry failed for {name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

