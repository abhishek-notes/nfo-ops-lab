#!/usr/bin/env markdown

# Session Context and Handoff (NFOpsLab)

This document consolidates all relevant context from the prior pasted conversation (codex‑chat‑resume and follow‑ups) and this session. It is designed so a new session can resume instantly without losing any information.

## Environment
- Machine: MacBook Pro M3 Max, 14 CPU cores, 36 GB RAM, 1 TB storage (~100 GB free)
- Python: 3.12; Polars: 1.32.3 (Apple Silicon SIMD wheel); PyArrow: 21.0.0
- CLI: Codex CLI; sandbox: danger‑full‑access; approvals: on‑request
- Timezone for data processing: IST; all timestamps normalized to `datetime[ns, Asia/Kolkata]`

## Data Layout
- Packed (Parquet)
  - Spot: `data/packed/spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet`
  - Options: `data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`
  - Futures: `data/packed/futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet`
- Meta
  - Calendar: `meta/expiry_calendar.csv`
  - Packed manifest: `meta/packed_manifest.csv`
- Outputs & Logs
  - Results: `backtests/results/*`
  - Logs: `backtests/logs/*`
  - Cache: `backtests/cache/seconds/{SYMBOL}/date=YYYY-MM-DD/exp=YYYY-MM-DD/type={CE|PE}/strike={K}.parquet`

## Original Orchestrator & Strategies
- Orchestrator: `backtests/run_all_strategies.py`
  - Parallel (ProcessPool), skip‑on‑error, per‑task logs, master status parquet `run_all_status_{SYMBOL}_{START}_{END}.parquet`
  - Uses `meta/expiry_calendar.csv` for expiries; computes ATM anchors; writes per‑strategy outputs
- Strategies (wired):
  - `backtests/atm_volume_ultra.py` (tz/unit fixes; dense 1s; CE/PE seconds cache; burst logic)
  - `backtests/strategies/gamma_scalp_baseline.py` (±2s tolerant anchors)
  - `backtests/strategies/iv_rv_spread_proxy.py`
  - `backtests/strategies/expiry_iv_crush_play.py`
  - `backtests/strategies/short_straddle_premium_decay.py`
  - `backtests/strategies/dispersion_proxy_index_vs_basket.py`
  - `backtests/strategies/oi_shift_breakout.py`
  - `backtests/strategies/opening_range_breakout_options.py`
  - `backtests/strategies/vwap_mean_reversion_options.py`
  - `backtests/strategies/calendar_spread_weekly_proxy.py`
- Monitoring & Tools:
  - `backtests/monitor_run.sh`, `backtests/monitor_loop.sh`, `backtests/retry_failed_from_logs.py`

## New Files (non‑breaking; created this session)
- `backtests/strategies/momentum_scalp_v2.py`
  - Nearest‑available CE+PE strike selection at expiry (not only ATM±1)
  - Price‑only mode (`--vol-mult 0` or `--allow-price-only`) to debug entry gating
  - (Planned) `--force-first` fallback for probes
  - Output: `backtests/results/momentum_scalp_v2_{SYMBOL}_{START}_{END}_{MODE}.parquet`
- `backtests/strategies/momentum_sanity_probe.py`
  - Spot‑only sanity; ensures write pipeline works; output: `backtests/results/momentum_sanity_{SYMBOL}_{START}_{END}.parquet`
- `backtests/strategies/momentum_force_probe.py`
  - Minimal spot momentum writer to force a few entries for validation; output: `backtests/results/momentum_force_{SYMBOL}_{START}_{END}.parquet`
- Fees postprocessor: `backtests/postprocess_fees.py`
  - Aggregates per strategy/year; applies India F&O fees (no brokerage); outputs `backtests/results/strategy_summary_{SYMBOL}.csv`

## Completed Full‑Suite Runs (prior)
- BANKNIFTY & NIFTY: 2019‑01‑01 → 2025‑07‑31 with 12 workers; status Parquets present
- Totals (2019–2025):
  - BANKNIFTY: gamma_scalp +235,905; iv_rv +162,041; iron_condor +56,997; expiry_iv_crush +17,321; oi_shift −12,951; orb −24,010; vwap_mr −114; atm_volume_ultra −22,396
  - NIFTY: gamma_scalp +58,511; iv_rv +51,525; iron_condor +13,329; expiry_iv_crush +3,810; oi_shift −3,721; orb −4,589; vwap_mr −206; atm_volume_ultra −9,530
- Year‑by‑year highlights:
  - BANKNIFTY gamma: 2019 +22k; 2020 +56k; 2024 +55.6k; 2025 YTD +10.7k
  - NIFTY gamma: 2024 +18.8k; 2025 YTD +14.6k
  - BANKNIFTY iv_rv: 2019 +10k; 2020 +30k; 2024 +33.2k; 2025 YTD +19.1k

## 6‑Month Momentum Sweeps (this session)
- Ran via orchestrator for both symbols; Start/End stamped in logs; status parquets exist:
  - `backtests/results/run_all_status_BANKNIFTY_2025-02-01_2025-07-31.parquet`
  - `backtests/results/run_all_status_NIFTY_2025-02-01_2025-07-31.parquet`
- Momentum trade parquets from these sweeps: none yet — cause: entry seconds gated by exact ATM/±1 availability at expiry

## Momentum Workstream (Status)
- Fixed: pm (price momentum) Series alignment at 1s grid
- Remaining blocker: entry seconds built only for ATM/±1 — missing files at exact second often collapse entries
- v2 adds nearest‑available CE+PE strike selection at expiry (non‑breaking)
- Debug proof of write: `backtests/results/momentum_scalp_v2_BANKNIFTY_2020-03-12_2020-03-12_buy.parquet` (1 row, pnl 2.0)

## Fees & Taxes (India F&O; no brokerage)
- Fees: STT 0.10% (sell), Exchange 0.035% (turnover), IPFT 0.0005%, SEBI 0.0001%, Stamp 0.003% (buy), GST 18% (over txn fees)
- Lot sizes for rupee conversions: BANKNIFTY=30; NIFTY=75
- Postprocess:
  - `python3 backtests/postprocess_fees.py --symbol BANKNIFTY --lot-size 30`
  - `python3 backtests/postprocess_fees.py --symbol NIFTY --lot-size 75`
  - Output: `backtests/results/strategy_summary_{SYMBOL}.csv`

## Optimizations Applied
- Polars scan + predicate pushdown; column projection; tz IST/ns normalization
- Per‑second cache (ATM CE/PE seconds) under `backtests/cache/seconds`
- Parallel runs: 12 workers; skip‑on‑error, per‑task logs, master status parquet
- Gamma anchors: nearest within ±2 seconds on 1s grid (improved ok_rate; negligible bias)

## Known Issues / Next Steps (Minimal Touch)
1) Momentum entries (v2 file only; original files untouched):
   - Finalize nearest‑available CE+PE strike selection at expiry (list CE/PE strikes, intersect, choose nearest to ATM)
   - Add `--force-first` (debug) fallback to guarantee a probe entry on volatile days
   - Run probe (e.g., BANKNIFTY 2020‑03‑12), then last‑6M sweeps for both symbols/modes:
     - BANKNIFTY: short=3, base=20, price_thresh=3, vol_mult=1.05, drop_confirm=2, targets/stop/trail=2/1/0.5
     - NIFTY: short=3, base=20, price_thresh=1.5, vol_mult=1.05, drop_confirm=2, targets/stop/trail=2/1/0.5
2) Visuals update (after momentum trades exist):
   - `backtests/results/strategy_summary_{BANKNIFTY|NIFTY}.csv` (fee‑adjusted)
   - `docs/results/RESULTS_SUMMARY.md` (rolled‑up tables, last‑1Y slice)

## How To Resume
1) Momentum probe (v2; non‑breaking):
```
python3 backtests/strategies/momentum_scalp_v2.py \
  --symbol BANKNIFTY --start 2020-03-12 --end 2020-03-12 \
  --mode buy --short-secs 3 --base-secs 10 --price-thresh 0.2 \
  --vol-mult 0.0 --allow-price-only --force-first
```
2) Momentum 6‑month sweeps (v2):
```
nohup python3 backtests/run_all_strategies.py \
  --symbol BANKNIFTY --start 2024-11-01 --end 2025-04-30 \
  --include momentum_scalp --max-workers 12 \
  > backtests/logs/momentum_BANKNIFTY_recent6m.out 2>&1 &

nohup python3 backtests/run_all_strategies.py \
  --symbol NIFTY --start 2025-02-01 --end 2025-04-30 \
  --include momentum_scalp --max-workers 12 \
  > backtests/logs/momentum_NIFTY_recent6m.out 2>&1 &
```
3) Fees & Visuals after momentum writes:
```
python3 backtests/postprocess_fees.py --symbol BANKNIFTY --lot-size 30
python3 backtests/postprocess_fees.py --symbol NIFTY --lot-size 75
```
Check: `backtests/results/strategy_summary_{BANKNIFTY|NIFTY}.csv` and `docs/results/RESULTS_SUMMARY.md`.

## Quick Path Checks
- Status parquets (6M momentum):
  - `backtests/results/run_all_status_BANKNIFTY_2025-02-01_2025-07-31.parquet`
  - `backtests/results/run_all_status_NIFTY_2025-02-01_2025-07-31.parquet`
- Logs (Start/End stamps):
  - `backtests/logs/momentum_BANKNIFTY_recent6m.out`
  - `backtests/logs/momentum_NIFTY_recent6m.out`
- Debug proof of write:
  - `backtests/results/momentum_scalp_v2_BANKNIFTY_2020-03-12_2020-03-12_buy.parquet`

## File Catalog (Code, Data, Results, Logs)

### Code: Orchestrator & Utilities
- `backtests/run_all_strategies.py`: Main orchestrator.
  - Discovers expiries from `meta/expiry_calendar.csv`.
  - Schedules per‑strategy tasks, runs in ProcessPool, writes master status Parquet.
  - Graceful skip‑on‑error; per‑task logs in `backtests/logs/`.
- `backtests/monitor_run.sh`: One‑shot PID/log tail snapshot (health, recent outputs, log tail).
- `backtests/monitor_loop.sh`: Periodic monitors + auto‑retry loop.
- `backtests/retry_failed_from_logs.py`: Parses per‑task logs and re‑executes failed tasks.
- `backtests/postprocess_fees.py`: Fee‑aware aggregation to CSV by strategy/year.
- (Original utils in strategies) `safe_io` style patterns (tz/ns casting, schema alignment) embedded in strategy files.

### Code: Strategies (original, wired)
- `backtests/atm_volume_ultra.py`: ATM±1 CE/PE seconds cache; volume burst detector; writes `trades_*.parquet`, `summary_*.parquet`.
- `backtests/strategies/gamma_scalp_baseline.py`: Delta‑proxy PnL; anchors with ±2s tolerance; per‑expiry.
- `backtests/strategies/iv_rv_spread_proxy.py`: IV vs RV stance proxy; low I/O; full‑range.
- `backtests/strategies/expiry_iv_crush_play.py`: Expiry‑day premium short; per‑expiry.
- `backtests/strategies/short_straddle_premium_decay.py`: Intraday short straddle scaffold; per‑expiry.
- `backtests/strategies/dispersion_proxy_index_vs_basket.py`: Index vs basket RV stance; logging stance.
- `backtests/strategies/oi_shift_breakout.py`: Flow/OI proxy breakout; ATM strike via spot @10:00.
- `backtests/strategies/opening_range_breakout_options.py`: ORB directional options play.
- `backtests/strategies/vwap_mean_reversion_options.py`: VWAP deviation fades; thresholded.
- `backtests/strategies/calendar_spread_weekly_proxy.py`: Front vs next week IV slope proxy.

### Code: Momentum (new; non‑breaking)
- `backtests/strategies/momentum_scalp_v2.py`:
  - Signals: price momentum over short_secs; optional volume filter; nearest‑available CE/PE strike at expiry.
  - Modes: `buy` (with trend), `sell_opposite` (against trend).
  - Debug flags: `--allow-price-only`, planned `--force-first` for probes.
  - Output: `backtests/results/momentum_scalp_v2_{SYMBOL}_{START}_{END}_{MODE}.parquet`.
- `backtests/strategies/momentum_sanity_probe.py`: Spot‑only writer to validate pipeline (writes `momentum_sanity_*.parquet`).
- `backtests/strategies/momentum_force_probe.py`: Minimal spot scalp writer (writes `momentum_force_*.parquet`).

### Data: Packed Parquet
- Spot: `data/packed/spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet` (tz IST, ns). Columns: `timestamp, close, ...` vendor extras ignored.
- Options: `data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`. Columns: `timestamp, close, vol_delta, ...` (53 raw → normalized to core fields).
- Futures: `data/packed/futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet`.

### Meta
- `meta/expiry_calendar.csv`: Final/scheduled expiries; shift rules; day labels.
- `meta/packed_manifest.csv`: Path, strike, expiry, row counts, min/max timestamps.

### Results: Naming Patterns
- Master status per sweep:
  - `backtests/results/run_all_status_{SYMBOL}_{START}_{END}.parquet`
- Strategy outputs (examples):
  - ATM research: `backtests/results/trades_{SYMBOL}_{START}_{END}.parquet`, `summary_{SYMBOL}_{START}_{END}.parquet`
  - Gamma scalp: `backtests/results/gamma_scalp_{SYMBOL}_{START}_{END}.parquet`
  - IV‑RV: `backtests/results/iv_rv_proxy_{SYMBOL}_{START}_{END}.parquet`
  - Expiry IV crush: `backtests/results/expiry_iv_crush_{SYMBOL}_{DATE}.parquet`
  - Short straddle: `backtests/results/short_straddle_{SYMBOL}_{START}_{END}_{EXPIRY}.parquet`
  - Dispersion: `backtests/results/dispersion_proxy_{SYMBOL}_{START}_{END}.parquet`
  - OI shift: `backtests/results/oi_shift_{SYMBOL}_{DATE}_{EXPIRY}_{STRIKE}.parquet`
  - ORB: `backtests/results/orb_{SYMBOL}_{DATE}_{EXPIRY}.parquet`
  - VWAP MR: `backtests/results/vwap_mr_{SYMBOL}_{DATE}_{EXPIRY}.parquet`
  - Iron condor: `backtests/results/iron_condor_{SYMBOL}_{DATE}_{EXPIRY}.parquet`
  - Calendar proxy: `backtests/results/calendar_proxy_{SYMBOL}_{DATE}.parquet`
  - Momentum v2 (new): `backtests/results/momentum_scalp_v2_{SYMBOL}_{START}_{END}_{MODE}.parquet`

### Logs: Naming Patterns
- Orchestrator main:
  - `backtests/logs/run_all_{SYMBOL}_{YYYY}_{YYYY}_w{N}.out`
- Momentum recent 6M:
  - `backtests/logs/momentum_{SYMBOL}_recent6m.out` (contains Start/End stamps)
- Per‑task error logs: `backtests/logs/{strategy}_{YYYYMMDD_HHMMSS}.log`
- Monitor PIDs: `backtests/logs/*_w12.pid` (or similar)

### Cache
- CE/PE seconds cache: `backtests/cache/seconds/{SYMBOL}/date={YYYY-MM-DD}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`
- Populated on‑demand by ATM research; prewarm scripts can fill ahead of backtests.

### Documentation (existing)
- `docs/PROJECT_WIKI.md`: Comprehensive project wiki (data samples, schemas, caveats).
- `docs/PROJECT_RUNBOOK.md`: Runbook (goals, env, commands, caching, optimizations).
- `docs/strategies/CATALOG.md`: 28 families / 74 variants (from chats).
- `docs/optimizations/GUIDE.md`: 36 optimizations; where/why to apply.
- `docs/chat_notes/*`: Full‑text extracts and manual notes.
- `docs/results/RESULTS_SUMMARY.md`: Rolled‑up tables (generated when fee postprocess runs).
- `docs/SESSION_CONTEXT.md`: This handoff document.

## Appendix: Repo Tree (Key Paths)

```
.
├── backtests/
│   ├── run_all_strategies.py              # Orchestrator (parallel, status parquet, logs)
│   ├── monitor_run.sh                     # One‑shot monitor (health, tails)
│   ├── monitor_loop.sh                    # Periodic monitor + auto‑retry
│   ├── retry_failed_from_logs.py          # Parse per‑task logs, rerun failures
│   ├── postprocess_fees.py                # Fee‑aware aggregation → CSV
│   ├── strategies/
│   │   ├── atm_volume_ultra.py            # ATM±1 CE/PE seconds cache; burst logic
│   │   ├── gamma_scalp_baseline.py        # Delta‑proxy PnL; ±2s tolerant anchors
│   │   ├── iv_rv_spread_proxy.py          # IV vs RV stance proxy
│   │   ├── expiry_iv_crush_play.py        # Expiry‑day premium play
│   │   ├── short_straddle_premium_decay.py
│   │   ├── dispersion_proxy_index_vs_basket.py
│   │   ├── oi_shift_breakout.py
│   │   ├── opening_range_breakout_options.py
│   │   ├── vwap_mean_reversion_options.py
│   │   ├── calendar_spread_weekly_proxy.py
│   │   ├── momentum_scalp_v2.py           # New (non‑breaking) momentum prototype
│   │   ├── momentum_sanity_probe.py       # New (spot‑only sanity writer)
│   │   └── momentum_force_probe.py        # New (minimal spot writer)
│   ├── logs/                              # Run logs, monitor PIDs, per‑task errors
│   └── results/                           # Strategy outputs + status parquets
│
├── data/
│   └── packed/
│       ├── spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet
│       ├── options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet
│       └── futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet
│
├── backtests/cache/seconds/{SYMBOL}/date=YYYY-MM-DD/exp=YYYY-MM-DD/type={CE|PE}/strike={K}.parquet
│                                          # CE/PE seconds cache (per‑second aggregation)
│
├── meta/
│   ├── expiry_calendar.csv                # Final/scheduled expiries; shifts; labels
│   └── packed_manifest.csv                # Path, strike, expiry, rows, tmin/tmax
│
├── docs/
│   ├── PROJECT_WIKI.md                    # Wiki (data samples, schemas, caveats)
│   ├── PROJECT_RUNBOOK.md                 # Runbook (goals, env, commands, caching)
│   ├── strategies/CATALOG.md              # 28 families / 74 variants
│   ├── optimizations/GUIDE.md             # 36 optimizations (how/where)
│   ├── chat_notes/                        # Full‑text extracts + manual notes
│   ├── results/RESULTS_SUMMARY.md         # Rolled‑up tables (after fee postprocess)
│   └── SESSION_CONTEXT.md                 # This handoff document
│
└── volume-multiple-trail-target-percentage-intervals-selling-child-processes-8.js
                                            # Reference Node script (not wired)
```

## Conversation Timeline & Key Decisions

- Initial full-suite backtests (2019–2025) completed for BANKNIFTY and NIFTY with 12 workers; status Parquets and per-strategy outputs exist.
- Data correctness & speed work:
  - Normalized timestamps to IST/ns end‑to‑end (spot and options), fixed tz/unit mismatches.
  - Added ±2s tolerant anchors for gamma scalp (spot anchor and option entry price) to reduce missed anchors with negligible bias.
  - Enabled CE/PE seconds cache (ATM±1) to speed ATM research and downstream strategies.
  - Verified fee model and built fee postprocessor to summarize per-strategy results (gross and net where legs exist).
- Momentum research ask:
  - Design momentum scalp (quick 10–30s trades) with price momentum + options volume confirmation; add buy and sell_opposite modes; ride-the-momentum exits (no fixed hold), trailing, targets/stops.
  - Found that strict strike/expiry selection (exact ATM±1) often blocked entry seconds even with correct pm mask; created v2 momentum with nearest‑available strike selection;
  - Created sanity probes (non‑breaking) to validate parquets and path without modifying original code.
- Visuals & documentation ask:
  - Produce fee‑aware CSVs per symbol/year; produce a rolled‑up Markdown; produce a single all‑context handoff doc.

## Strategy Catalog Details (Wired)

- `atm_volume_ultra.py` (ATM research signal)
  - Inputs: spot seconds; CE/PE seconds (ATM±1); `vol_delta` sum per second.
  - Logic: rolling 30s burst vs 5m baseline; writes `trades_{SYMBOL}_{START}_{END}.parquet` and `summary_{...}.parquet`.
  - CLI: `--multiplier`; (internally uses 30s/300s unless extended).

- `gamma_scalp_baseline.py`
  - Inputs: ATM anchor per hour; CE/PE seconds for that strike/expiry; spot path.
  - Logic: delta‑proxy pnl; ±2s tolerant anchor; per‑expiry tasks.

- `iv_rv_spread_proxy.py`
  - Inputs: implied vs realized vol proxies.
  - Logic: stance proxy over full period; low I/O.

- `expiry_iv_crush_play.py`
  - Inputs: expiry-day data; entry times (10:00, 14:00 variants feasible).
  - Logic: expiry crush; per‑expiry.

- `short_straddle_premium_decay.py`
  - Inputs: per‑expiry data; anchor 11:00.
  - Logic: intraday short straddle scaffold.

- `dispersion_proxy_index_vs_basket.py`
  - Inputs: index vs basket realized vol; stance logging.

- `oi_shift_breakout.py`
  - Inputs: CE/PE vol proxies (OI/flow); ATM by spot 10:00; per-day per‑expiry per‑strike.
  - Logic: flow breakout (with trend/filters recommended).

- `opening_range_breakout_options.py`
  - Inputs: early session range; options proxy.
  - Logic: directional breakout; underperformed without filters.

- `vwap_mean_reversion_options.py`
  - Inputs: VWAP bands; intraday.
  - Logic: fades on deviation; thresholds tunable.

- `calendar_spread_weekly_proxy.py`
  - Inputs: front vs next week IV; slope proxy; writes stance/log.

## Momentum (New; Non‑Breaking)

- `momentum_scalp_v2.py` (prototype)
  - Signals: price momentum over short_secs; optional volume confirmation; nearest‑available CE/PE strike at expiry.
  - Modes: `buy` (ride up/down), `sell_opposite` (fade opposite leg); exits on momentum drop/target/stop/trailing; no fixed max‑hold.
  - Debug: `--allow-price-only`; planned `--force-first` for volatile probes.
  - Outputs: `momentum_scalp_v2_{SYMBOL}_{START}_{END}_{MODE}.parquet`.

- `momentum_sanity_probe.py`, `momentum_force_probe.py`
  - Spot‑only writers to validate pipeline and ensure Parquet writes without touching original files.

## Data Schemas (Typical)

- Spot ticks (packed):
  - Columns: `timestamp (datetime[ns, IST])`, `close` (+ vendor extras ignored)
  - Densification: truncate to 1s, carry forward last close, backfill first close if needed.

- Options ticks (packed):
  - Columns: `timestamp (datetime[μs or ns → cast to ns, IST])`, `symbol`, `opt_type (CE|PE)`, `strike (int)`, `open`, `high`, `low`, `close`, `vol_delta`, `expiry (date)`, `expiry_type (weekly|monthly)`, `is_monthly (i8)`, `is_weekly (i8)`
  - Seconds aggregation: per‑second `vol_delta` sum + last `close`.

- Futures ticks (packed):
  - Columns: `timestamp`, `open`, `high`, `low`, `close`, (vendor `volume` may exist); normalized to IST/ns on read.

- Meta: `expiry_calendar.csv`
  - Columns (typical): `Instrument`, `Expiry_Type`, `Contract_Month`, `Scheduled_Expiry`, `Final_Expiry`, `Shifted`, `Days_Moved_Back`, `Rule`, etc.

## Results Schema Patterns (Common)

- ATM research trades: `trades_{SYMBOL}_{START}_{END}.parquet`
  - Typical: `trade_date`, `entry_ts`, `exit_ts`, `entry_opt`, `exit_opt`, `pnl_pts` (points), and metadata fields.

- Gamma scalp: `gamma_scalp_{SYMBOL}_{START}_{END}.parquet`
  - Typical: `entry_ts`, `exit_ts`, `strike`, `opt_type`, `pnl` (points), `exit_reason`.

- Expiry crush / Iron condor / ORB / VWAP MR / OI shift / Calendar / Dispersion
  - Each follows a similar per‑row trade/stance record model with timestamps and optional per‑day/expiry metadata; fields vary by strategy file.

## Raw Data Scale (earlier inventory)

- Raw options (totals across both symbols): ~85,278 files; ~230.6 GB
  - BANKNIFTY ~44,260 files; NIFTY ~41,018 files (approx)
- Raw futures: ~3.1 GB; raw spot: ~0.46 GB (approx)
- CE/PE seconds cache count (at a point in time): ~25,107 files

## Performance Notes

- “Hours” observed in status are total CPU seconds across many tasks; wall‑clock at 12 workers is much lower.
- Heavy strategies benefit greatly from CE/PE seconds cache and tolerant anchors.
- For multi‑year sweeps, cache + aggressive parallelism + pushdown keep runs tractable.

## Known Issues / TODOs (Momentum)

- Strict ATM±1 strike selection at entry seconds blocks entries on many days. Mitigation: nearest‑available strike selection at expiry.
- Add `--force-first` (debug only) to guarantee a probe entry for validation.
- Once entries land reliably, sweep recent 6M for both symbols/modes; then expand to full range.

## Commands Quickstart

- Orchestrator (full suite):
```
nohup python3 backtests/run_all_strategies.py \
  --symbol BANKNIFTY --start 2019-01-01 --end 2025-07-31 --max-workers 12 \
  > backtests/logs/run_all_BANKNIFTY_2019_2025_w12.out 2>&1 &
```
- Momentum v2 (probe):
```
python3 backtests/strategies/momentum_scalp_v2.py \
  --symbol BANKNIFTY --start 2020-03-12 --end 2020-03-12 \
  --mode buy --short-secs 3 --base-secs 10 --price-thresh 0.2 \
  --vol-mult 0.0 --allow-price-only --force-first
```
- Fees summary:
```
python3 backtests/postprocess_fees.py --symbol BANKNIFTY --lot-size 30
python3 backtests/postprocess_fees.py --symbol NIFTY --lot-size 75
```
