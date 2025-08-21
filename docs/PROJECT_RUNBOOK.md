# NFOpsLab Runbook

This runbook captures the full context: goals, environment, data layout, processes, strategies, orchestration, optimizations, and your Mac specs. It is the “single source” to resume work later without losing context.

## Goals
- Normalize NSE options/spot/futures ticks to partitioned Parquet; produce bars; run multi-year backtests quickly.
- Implement a suite of options strategies (vol-based, spreads, intraday) with robust I/O and timezone handling.
- Optimize to run multi-year sweeps within minutes where feasible; cache per-second computations to speed iterations.

## Environment
- Machine: MacBook Pro M3 Max, 36 GB RAM, 1 TB storage (~100 GB free)
- Python: 3.12
- Libraries: Polars (Rust backend), PyArrow, etc.
- Data: under `data/packed/{options,spot,futures}`; metadata in `meta/`

## Data Layout
- Options: `data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`
- Spot: `data/packed/spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet`
- Futures: `data/packed/futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet`
- Calendar: `meta/expiry_calendar.csv`
- Packed manifest: `meta/packed_manifest.csv`

## Packing and Bars (summary)
- simple_pack.py (options): normalizes timestamp to IST; repairs OHLC; computes vol_delta; maps to next expiry.
- pack_spot.py (spot): infers symbol; repairs OHLC; vol_delta=0; per-day parquet.
- pack_futures.py (futures): maps trade_date→monthly expiry; repairs OHLC; computes vol_delta.
- Bars: spot/futures 1m bars; options bars skipped due to vendor OHLC anomaly.

## Backtest Orchestration
- Main runner: `backtests/run_all_strategies.py`
  - Strategies: `atm_volume_ultra`, `gamma_scalp`, `iv_rv`, `expiry_crush`, `short_straddle`, `dispersion`, `oi_shift`, `orb`, `vwap_mr`, `iron_condor`, `calendar_proxy`.
  - Args: `--symbol`, `--start`, `--end`, `--max-workers`, `--include`
  - Outputs: strategy results in `backtests/results/`, master status parquet, per-task logs in `backtests/logs/`.
  - Monitoring: `backtests/monitor_run.sh`; auto-retry loop: `backtests/monitor_loop.sh` + `backtests/retry_failed_from_logs.py`.

## Timezone/Schema Safety
- All timestamps cast to `datetime[ns, Asia/Kolkata]` before filters/joins.
- Older files (2019–2021) may have μs timestamps; safe readers normalize.
- Options missing `vol_delta` backfilled to 0; spot `vol_delta` always 0.

## Caching (per-second option series)
- Location: `backtests/cache/seconds/{SYMBOL}/date={YYYY-MM-DD}/exp={...}/type={CE|PE}/strike={K}.parquet`
- Implemented in `backtests/atm_volume_ultra.py` for heavy per-second aggregation. Speeds up reruns greatly.

## Strategies Implemented (scaffolds)
- gamma_scalp_baseline.py (tolerant anchor ±2s), iv_rv_spread_proxy.py, expiry_iv_crush_play.py, short_straddle_premium_decay.py, dispersion_proxy_index_vs_basket.py, oi_shift_breakout.py, opening_range_breakout_options.py, vwap_mean_reversion_options.py, iron_condor_intraday.py. Core fast strategy: atm_volume_ultra.py.

## Optimizations Used
- Polars scan + predicate pushdown; selective column projection.
- ProcessPoolExecutor parallelism; skip-on-error; per-task logs with retries.
- Tz/unit normalization; dense 1s index; anchor caching / tolerant nearest.
- Per-second cache for options (atm_volume_ultra) to avoid recomputing.

## Typical Commands
- Full run (BANKNIFTY):
  - `nohup python3 backtests/run_all_strategies.py --symbol BANKNIFTY --start 2019-01-01 --end 2025-07-31 --max-workers 8 > backtests/logs/run_all_BANKNIFTY_2019_2025.out 2>&1 &`
  - Monitor: `./backtests/monitor_loop.sh backtests/logs/run_all_BANKNIFTY_2019_2025.out backtests/logs/run_all.pid 360 60`
- NIFTY run: same with `--symbol NIFTY`

## Machine Preferences
- You’ve approved using up to 8+ workers (M3 Max 14 cores, 36 GB RAM) and to use caching to accelerate repeated strategy runs with different parameters.

## Results and Metrics
- See `docs/strategies/CATALOG.md` (28 families, 74 variants) and `docs/optimizations/GUIDE.md`.
- Master status parquets capture per-strategy runtimes and task ok rates.

## Next Iterations
- Expand strategy catalog with more coded variants; add disk caching hooks for other heavy series if needed; refine vectorization in hotspots while maintaining correctness.
