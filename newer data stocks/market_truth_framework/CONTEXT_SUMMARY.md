# Context & Preferences Summary (Market Truth Framework)

This document consolidates the relevant context from:
- Our current chat (Market Truth “truth extraction framework” work + verification findings)
- `newer data stocks/Market Truth Framework Fixes.md` (project history, performance principles, and the Market Truth framework thread)

The goal is to have a single source-of-truth for requirements, assumptions, preferences, and known pitfalls before making further changes.

---

## 1) Big Picture Goals (What you want)

### A) “Market Truth Extraction” system
You want a repeatable pipeline that converts intraday options data into:
- A per-second “core state vector” (spot + canonical options set) suitable for analysis and playback
- Event tables (bursts, liquidity vacuums/pulls/replenish, fear spikes, chop pockets)
- “Truth tables” (distributions and frequencies) sliced by:
  - DTE buckets (0/2/4/6+)
  - time-of-day
  - regime labels
- A serving layer (API) to power an interactive playback/visualization app

### B) Performance and scalability are first-class
From the broader project history (data repacking + high-performance backtesting), the consistent theme is:
- Avoid I/O fragmentation (too many small files)
- Keep data pre-sorted on disk where possible
- Prefer single-pass scans and vectorization over repeated filtering
- Use Polars for columnar reads + projection, and Numba for tight loops when needed

---

## 2) Your Data Model (What we’re working with)

### A) Input data layout (packed)
From `Market Truth Framework Fixes.md` and the current repo:
- Options data is partitioned by trading day:
  - `.../options_date_packed_FULL_v3_SPOT_ENRICHED/<YYYY-MM-DD>/<UNDERLYING>/*.parquet`
- Files are pre-sorted for backtesting by:
  - `expiry → opt_type → strike → timestamp`
- Schema is “wide” and includes (examples):
  - `timestamp` (Datetime[μs]), `timestamp_ns`
  - `expiry` (Date), plus tags (`expiry_type`, `is_weekly`, `is_monthly`)
  - L1–L5 book (`bp0..bp4`, `sp0..sp4`, `bq0..bq4`, `sq0..sq4`, `bo0..bo4`, `so0..so4`)
  - `spot_price` (enriched)
  - `vol_delta` (non-negative per-row volume delta)

### B) Key derived expectations
- The Market Truth framework should produce a *per-second* grid aligned to market hours (typically 09:15–15:30).
- ATM strike selection should be deterministic:
  - `atm_strike = round(spot / strike_step) * strike_step` (strike_step: NIFTY=50, BANKNIFTY=100)

---

## 3) Core Preferences (How you want it built)

### A) Communication style
- “No fluff”: focused verification, issue finding, and concrete next steps.
- When asked to verify: do not modify code.
- When asked to implement: make changes end-to-end and validate.

### B) Engineering style
- Deterministic outputs (stable ATM rule, stable file naming)
- Correctness before expanding feature scope
- No accidental overwrites
- Avoid expensive patterns like:
  - looping over timestamps and `.filter()` per second
  - re-sorting large datasets in RAM

### C) Performance style (from the backtesting project)
- Read only required columns
- Prefer `scan_parquet` and pushdown filters when possible
- Convert categoricals to physical ints for Numba loops
- Use single-pass state machines rather than repeated filters

---

## 4) Market Truth Framework Requirements (What “correct” means)

### A) Outputs
At minimum:
- `features_<UNDERLYING>_<DATE>.parquet` (1 row per second)
- `bursts_<UNDERLYING>_<DATE>.parquet` (event table; file is written even if 0 events)
- (optional later) regimes/liquidity/event tables

### B) Non-negotiable correctness constraints
- **No overwrites across underlyings**: output filenames must include underlying.
- **Load all shards**: if a date folder has multiple parquet files, all must be read.
- **True 1-second grid**: durations and RV windows must not depend on “missing seconds”.
- **DTE calculation must be date-based** (not time-of-day based). With `expiry` stored as a Date:
  - correct: `dte_days = (expiry - timestamp.date()).days`
  - incorrect: `dte_days = (expiry - timestamp).total_days` (mislabels “day before expiry” as 0 DTE)
- **API must reflect underlying-aware paths**.
- **RV windows must be comparable across lengths**: use RMS volatility (`sqrt(mean(r^2))`). If you use `sqrt(sum(r^2))`, the burst condition `RV_10 > k1 × RV_120 (k1>1)` becomes impossible and you’ll silently detect zero bursts.

### C) Tunability
- Burst thresholds must be tunable by underlying and by DTE bucket.

---

## 5) What Was Implemented Initially (And what went wrong)

### A) Initial framework components created
Under `newer data stocks/market_truth_framework/`:
- `preprocessing/core_preprocessor.py`
- `preprocessing/batch_processor.py`
- `preprocessing/liquidity_detector.py`
- `preprocessing/statistics_generator.py`
- `api/market_truth_api.py`
- `watch_progress.sh`
- Docs: `README.md`, `BUG_FIXES.md`, `TEST_RESULTS.md`

### B) Verified critical issues found in the first version
These were validated as real (and they matter):
1. Output filenames lacked underlying → overwrites.
2. Only first parquet shard was loaded.
3. No full per-second grid → gaps distort durations/RV.
4. Acceleration window off-by-one (9 summed, divided by 10).
5. Docs promised an `options_atm` output that wasn’t written.
6. DTE bucketing didn’t match required (0/2/4/6+) buckets.
7. Missing major features from the original spec (± strikes, liquidity integration, regime labels, trade-vs-vacuum scores).
8. Stats generator used `pl.datetime.now()` (invalid).
9. API could not distinguish underlyings.

### C) “Fixed” follow-up had additional issues
The follow-up created `core_preprocessor_FIXED.py` and changed the API/stats, but new issues appeared:
- Batch runner still called the old `core_preprocessor.py`.
- `statistics_generator.py` used `datetime.now()` without importing `datetime`.
- API still had old burst/summary endpoints using non-underlying filenames.
- The “full grid” implementation used a Python loop with per-second `.filter()` → very slow.
- Missing seconds were forward-filled with zeros for book/volume fields → fabricates liquidity collapses/volume resets.
- Grid endpoints were derived from min/max timestamps (included pre-open) instead of fixed 09:15–15:30.
- DTE still used the incorrect time-based method.

---

## 6) Implementation Direction Agreed (What we will do now)

### A) Fix correctness + simplify interface
- Make `preprocessing/core_preprocessor.py` the single canonical preprocessor (batch uses it).
- Underlying-aware filenames everywhere (`features_<U>_<DATE>.parquet`, `bursts_<U>_<DATE>.parquet`).
- Update API routes to include underlying for *features, bursts, summary*.
- Update stats generator to parse underlying/date from filenames and not crash.

### B) Speed and architecture improvements
- Replace per-second Python loops with Polars operations:
  - `scan_parquet` + column projection
  - timestamp truncation to 1s + group-by aggregation
  - join onto a full session grid (09:15–15:30)
  - forward-fill for continuous series (and keep “observed flags” so you can filter synthetic rows)

### C) Scope discipline
- First: correctness + speed + consistent outputs.
- Later (Phase 2): add ± strikes, trade/vacuum decomposition, fear regimes, richer truth tables.
