# NFOpsLab — Internal Wiki for Strategy Development and Execution

This wiki is my working playbook for navigating the repo, running strategies, adding new ones, and iterating quickly with local data. It consolidates directory layout, runtime requirements, strategy APIs, and example commands.

## Fast Start

- Python: 3.11+ with `polars`, `python-dateutil`. Install deps:
  - Repo root: `pip install -r requirements.txt`
  - Backtests: `pip install -r backtests/requirements.txt`
- Data: Packed parquet exists under `data/packed/{spot,options,futures}`. Use `docs/PROJECT_WIKI.md` for schemas.
- One-shot run for a short window:
  - `python3 backtests/run_all_strategies.py --symbol BANKNIFTY --start 2023-11-15 --end 2023-11-21 --max-workers 2 --include vol_spike_minute`
  - Outputs parquet to `backtests/results/`.

## Directory Essentials

- `data/packed/spot/{SYMBOL}/{YYYYMM}/date=YYYY-MM-DD/ticks.parquet`: per-day 1s ticks with `timestamp, close` (IST, ns).
- `data/packed/options/{SYMBOL}/{YYYYMM}/exp=YYYY-MM-DD/type={CE|PE}/strike={K}.parquet`: per-expiry strike option ticks with `timestamp, close, vol_delta`.
- `meta/expiry_calendar.csv`: weekly+monthly expiries; used by packers and strategy scheduling.
- `backtests/`: strategy modules, runner, cache, logs, results.

## Strategy Runner

- Entry: `python3 backtests/run_all_strategies.py --symbol SYMBOL --start YYYY-MM-DD --end YYYY-MM-DD [--include name1,name2] [--max-workers N]`
- Built-in names (partial): `atm_volume_ultra, gamma_scalp, iv_rv, expiry_crush, short_straddle, dispersion, oi_shift, orb, vwap_mr, iron_condor, calendar_proxy, momentum_scalp, vol_spike_minute`.
- Scheduling:
  - Per-range runs (e.g., `atm_volume_ultra`, `iv_rv`, `dispersion`) run once across the window.
  - Per-day runs generate a task per trading day and, where needed, per-expiry.
- Logging: schema audit written to `backtests/logs/`; per-task failures to `backtests/logs/<name>_*.log`.
- Status: consolidated parquet at `backtests/results/run_all_status_{SYMBOL}_{START}_{END}.parquet`.

## Strategy Module Pattern

- Location: `backtests/strategies/<name>.py`
- Required `run(...)` function with string dates and concrete params.
- CLI block (`if __name__ == "__main__":`) to allow direct execution.
- I/O:
  - Load spot via `data/packed/spot` and align timestamps to `datetime[ns, Asia/Kolkata]`.
  - Load options via `data/packed/options` and aggregate per-second as needed (`truncate('1s')`).
  - Results: write to `backtests/results/<name>_{symbol}_{start}_{end}*.parquet`.

## New Strategy: Volume Spike (Minute)

- File: `backtests/strategies/volume_spike_minute.py`
- Rule:
  - Current minute’s ATM CE+PE volume > `mult` × average of previous `lookback_min` minutes (default mult=4.0, lookback=5).
  - Direction by spot 1-minute delta: up -> buy CE; down -> buy PE.
  - Risk: `target_pts=5.0`, `stop_pts=3.0`, trail after target by `trail_pts=1.0`.
- Entry/Exit specifics:
  - Entry at first second after the spike minute (or at the minute boundary if near close).
  - Uses actual option per-second `close` series (aggregated from ticks). PnL in option points.
- Run directly:
  - `python3 backtests/strategies/volume_spike_minute.py --symbol BANKNIFTY --start 2023-11-15 --end 2023-11-21 --mult 4.0 --lookback-min 5 --target-pts 5 --stop-pts 3 --trail-pts 1`
- Run via orchestrator:
  - `python3 backtests/run_all_strategies.py --symbol BANKNIFTY --start 2023-11-15 --end 2023-11-21 --include vol_spike_minute`

## Data Handling Notes

- Timezone: All timestamps cast to `datetime[ns, Asia/Kolkata]`. Use `.dt.replace_time_zone(IST).dt.cast_time_unit('ns')` when loading.
- Options quality: Vendor options OHLC can be session-wide; rely on `close` and derived `vol_delta`.
- Spot authority: Spot path used for direction/anchors; option pricing uses per-second closes or proxy if needed.

## Analysis and Iteration

- Outputs contain one row per trade: `date, entry_ts, exit_ts, expiry, strike, opt_type, entry_opt, exit_opt, pnl, exit_reason`.
- Quick sanity:
  - `python -c "import polars as pl; import sys; print(pl.read_parquet(sys.argv[1]).head())" backtests/results/volume_spike_minute_BANKNIFTY_2023-11-15_2023-11-21.parquet`
- Iterate parameters:
  - Spike `mult`: 3.0–6.0
  - Lookback: 3–10 minutes
  - Target/stop/trail: tighten/loosen based on outcome distribution.

## Common Pitfalls

- Missing data day: skip handled; ensure `data/packed/spot` and matching `data/packed/options` exist for the date.
- Exact-second gaps: loaders use nearest previous close if the exact second is missing.
- Expiry mapping: module finds nearest available weekly expiry dir within +4 weeks and the nearest strike with both CE/PE present.

