# NFOpsLab Workspace Wiki

This wiki documents the full workspace: directory layout, data locations and schemas, processing pipelines, calendars/expiries, and concrete samples from raw and packed datasets for BANKNIFTY and NIFTY. It is intended as a single reference to answer “where is X?” and “what does Y look like?” without re-scanning the repo.

## Overview

- Purpose: Normalize NSE options/spot/futures ticks into partitioned Parquet, derive bars, and run backtests (ATM burst strategies).
- Timezone: All packed data uses `Asia/Kolkata` (IST). Raw files often have tz-naive timestamps; packers attach IST without shifting.
- Trading hours: 09:15:00–15:30:00 IST.

## Directory Layout (key paths)

```
data/
  raw/
    options/                    # Raw options ticks (per-strike .parquet, messy schemas)
      - Files: 85,278 total (BANKNIFTY: 44,260; NIFTY: 41,018)
      - Size: ~230.6 GB on disk (raw options only)
    spot/                       # Raw spot ticks split into parts (timestamp, price)
    futures/
      banknifty_futures.parquet # Big consolidated raw futures file (49 cols)
      nifty_futures.parquet     # Big consolidated raw futures file (50 cols)
      banknifty_spot/           # Spot-like raw parts used during futures processing
      nifty_spot/

  (Overall raw folder size: ~234.4 GB; raw spot ~0.46 GB; raw futures ~3.1 GB)

  packed/
    options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet
    spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet
    futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet

  bars/
    spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/bars_1m.parquet
    futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/bars_1m.parquet
    options/   # Skipped (vendor options data has session-wide OHLC issues)

meta/
  expiry_calendar.csv           # Weekly + monthly expiries, final moved dates
  packed_manifest.csv           # Inventory of packed options files with tmin/tmax, rows

backtests/
  atm_volume_ultra.py           # Fast backtest: preloads spots, scans options per-day
  atm_volume*.py                # Other variants and experiments

docs/
  PROJECT_WIKI.md               # This file

Other notable scripts (root):
- simple_pack.py      # Options packer (raw -> packed by expiry/type/strike)
- pack_spot.py        # Spot packer (raw -> packed by trade_date)
- pack_futures.py     # Futures packer (raw -> packed by monthly expiry)
- build_bars.py       # 1m bars (generic)
- build_bars_spot_futures.py # Bars for spot+futures only
- verify_*.py         # Integrity checks, dedup, invariants
```

## Calendars and Expiries

- Source: `meta/expiry_calendar.csv` (835 rows; BANKNIFTY and NIFTY, weekly and monthly).
- Fields (selected): `Instrument`, `Expiry_Type` (Weekly/Monthly), `Final_Expiry` (actual moved date), `Rule`.
- Usage: Packers map `trade_date -> next expiry` via `join_asof(strategy="forward")`. Futures use monthly-only rows.

Sample (head 5, tail 5):

```
FILE: meta/expiry_calendar.csv (835 x 12)
HEAD
Instrument,Expiry_Type,Contract_Month,Scheduled_Expiry,Scheduled_Day,Final_Expiry,Final_Day,Final_Day_Label,Shifted,Days_Moved_Back,Holiday_on_Scheduled?,Rule
BANKNIFTY,Weekly,2019-01,2019-01-03,Thu,2019-01-03,Thu,BANKNIFTY weekly (Thu),No,0,false,BANKNIFTY weekly (Thu)
BANKNIFTY,Weekly,2019-01,2019-01-10,Thu,2019-01-10,Thu,BANKNIFTY weekly (Thu),No,0,false,BANKNIFTY weekly (Thu)
BANKNIFTY,Weekly,2019-01,2019-01-17,Thu,2019-01-17,Thu,BANKNIFTY weekly (Thu),No,0,false,BANKNIFTY weekly (Thu)
BANKNIFTY,Weekly,2019-01,2019-01-24,Thu,2019-01-24,Thu,BANKNIFTY weekly (Thu),No,0,false,BANKNIFTY weekly (Thu)
BANKNIFTY,Monthly,2019-01,2019-01-31,Thu,2019-01-31,Thu,BANKNIFTY monthly (last Thu),No,0,false,BANKNIFTY monthly (last Thu)

TAIL
NIFTY,Weekly,2025-12,2025-12-16,Tue,2025-12-16,Tue,NIFTY weekly (Tue),No,0,false,NIFTY weekly (Tue)
NIFTY,Weekly,2025-12,2025-12-23,Tue,2025-12-23,Tue,NIFTY weekly (Tue),No,0,false,NIFTY weekly (Tue)
BANKNIFTY,Monthly,2025-12,2025-12-30,Tue,2025-12-30,Tue,BANKNIFTY monthly (last Tue),No,0,false,BANKNIFTY monthly (last Tue)
NIFTY,Monthly,2025-12,2025-12-30,Tue,2025-12-30,Tue,NIFTY monthly (last Tue),No,0,false,NIFTY monthly (last Tue)
NIFTY,Weekly,2025-12,2025-12-30,Tue,2025-12-30,Tue,NIFTY weekly (Tue),No,0,false,NIFTY weekly (Tue)
```

## Processing Pipelines

### Options: `simple_pack.py`
- Parses symbol/opt_type/strike from messy filenames (e.g., `banknifty1941128500ce.parquet`).
- Normalizes timestamps:
  - Use `timestamp` or fallback to vendor `ts` when `timestamp` ~ 1970s.
  - Cast to datetime, attach IST with `replace_time_zone` (no time shift).
- Cleans OHLC:
  - Create `close` from any of vendor fields; fill missing `open/high/low` from `close`.
  - Repair vendor zeros and enforce bounds (low ≤ open/close ≤ high).
- Computes `vol_delta` using `volume.diff().clip(0)` or `qty` when available.
- Filters to market hours; dedup by timestamp.
- Adds `trade_date`, maps to next expiry (`join_asof forward` with calendar), drops rows that fail mapping.
- Writes one Parquet per `(symbol, expiry, opt_type, strike)` under `data/packed/options`.
- Final columns: `timestamp,symbol,opt_type,strike,open,high,low,close,vol_delta,expiry,expiry_type,is_monthly,is_weekly`.

### Spot: `pack_spot.py`
- Infers symbol from filename/content; normalizes timestamp; cleans OHLC; computes `vol_delta`.
- Filters to market hours; dedup by timestamp.
- Adds `trade_date`; writes one Parquet per `(symbol, trade_date)` under `data/packed/spot`.
- Columns: `timestamp,symbol,open,high,low,close,vol_delta,trade_date`.

### Futures: `pack_futures.py`
- Uses monthly expiries from calendar (filters `kind == monthly`).
- Normalizes timestamp; cleans OHLC; computes `vol_delta`.
- Filters to market hours; dedup by timestamp.
- Adds `trade_date`; `join_asof` to map to next monthly `expiry`.
- Writes one Parquet per `(symbol, expiry)` under `data/packed/futures`.
- Columns: `timestamp,symbol,open,high,low,close,vol_delta,trade_date,symbol_right,expiry` (the packed schema includes `symbol_right` where present).

### Bars (1-minute)
- `build_bars.py`, `build_bars_spot_futures.py`: produce 1m OHLCV bars for spot and futures.
- Options bars intentionally skipped due to vendor data quality (session-wide OHLC values).

## Data Samples (BANKNIFTY)

The following samples are pulled from this workspace to ground decisions.

### Raw Options (example)
```
FILE: data/raw/options/banknifty1941128500ce.parquet
SHAPE: (57190, 53)
ALL COLUMNS (53):
timestamp, price, qty, avgPrice, volume, volactual, bQty, sQty, open, high, low, close, changeper, lastTradeTime, oi, oiHigh, oiLow, bq0, bp0, bo0, bq1, bp1, bo1, bq2, bp2, bo2, bq3, bp3, bo3, bq4, bp4, bo4, sq0, sp0, so0, sq1, sp1, so1, sq2, sp2, so2, sq3, sp3, so3, sq4, sp4, so4, ts, symbol, opt_type, strike, year, month

FIRST ROW (full):
{ "timestamp": "2019-04-09 09:16:32", "price": 1532.0, "qty": 20, "avgPrice": 0.0, "volume": 0, "volactual": 0, "bQty": 3680, "sQty": 2920, "open": 0.0, "high": 0.0, "low": 0.0, "close": 1532.0, "changeper": 0.0, "lastTradeTime": "2019-04-08 13:27:14", "oi": 3820, "oiHigh": 0, "oiLow": 0, "bq0": 20, "bp0": 1250.65, "bo0": 1, "bq1": 140, "bp1": 1181.35, "bo1": 1, "bq2": 280, "bp2": 1169.85, "bo2": 1, "bq3": 3240, "bp3": 270.45, "bo3": 9, "bq4": 0, "bp4": 0.0, "bo4": 0, "sq0": 160, "sp0": 1300.85, "so0": 1, "sq1": 20, "sp1": 1306.85, "so1": 1, "sq2": 280, "sp2": 1520.1, "so2": 1, "sq3": 2460, "sp3": 1719.25, "so3": 1, "sq4": 0, "sp4": 0.0, "so4": 0, "ts": "2019-04-09 09:16:32", "symbol": "BANKNIFTY", "opt_type": "CE", "strike": 28500, "year": 2019, "month": 4 }

LAST ROW (full):
{ "timestamp": "2019-04-11 15:29:50", "price": 1275.0, "qty": 160, "avgPrice": 1236.62, "volume": 4480, "volactual": 0, "bQty": 11980, "sQty": 7060, "open": 1295.0, "high": 1302.4, "low": 1160.6, "close": 1302.05, "changeper": -2.077493, "lastTradeTime": "2019-04-11 15:29:25", "oi": 480, "oiHigh": 3480, "oiLow": 480, "bq0": 980, "bp0": 1230.05, "bo0": 1, "bq1": 20, "bp1": 1230.0, "bo1": 1, "bq2": 100, "bp2": 1216.2, "bo2": 1, "bq3": 220, "bp3": 1211.1, "bo3": 1, "bq4": 2460, "bp4": 1167.5, "bo4": 1, "sq0": 80, "sp0": 1275.0, "so0": 1, "sq1": 2000, "sp1": 1309.9, "so1": 1, "sq2": 100, "sp2": 1335.8, "so2": 1, "sq3": 220, "sp3": 1350.8, "so3": 1, "sq4": 2000, "sp4": 1399.9, "so4": 1, "ts": "2019-04-11 15:29:50", "symbol": "BANKNIFTY", "opt_type": "CE", "strike": 28500, "year": 2019, "month": 4 }
```

### Packed Options (example)
```
FILE: data/packed/options/BANKNIFTY/201904/exp=2019-04-11/type=CE/strike=28500.parquet
SHAPE: (62791, 13)
COLUMNS: timestamp, symbol, opt_type, strike, open, high, low, close, vol_delta, expiry, expiry_type, is_monthly, is_weekly

FIRST ROW (full):
{ "timestamp": "2019-04-09 09:15:23+05:30", "symbol": "BANKNIFTY", "opt_type": "CE", "strike": 28500, "open": 204.15, "high": 204.15, "low": 0.0, "close": 204.15, "vol_delta": 0, "expiry": "2019-04-11", "expiry_type": "weekly", "is_monthly": 0, "is_weekly": 1 }

LAST ROW (full):
{ "timestamp": "2019-04-11 15:29:59+05:30", "symbol": "BANKNIFTY", "opt_type": "CE", "strike": 28500, "open": 1345.0, "high": 1500.0, "low": 1225.9, "close": 1500.0, "vol_delta": 0, "expiry": "2019-04-11", "expiry_type": "weekly", "is_monthly": 0, "is_weekly": 1 }
```

Notes:
- Vendor options data exhibits session-wide OHLC values; treat `close` carefully for downstream analytics.
- `vol_delta` is derived (see packer) and used by burst logic.

### Raw Spot (example)
```
FILE: data/raw/spot/banknifty_spot_part_001.parquet
SHAPE: (1,000,000, 2)
COLUMNS: timestamp (ns, naive), price

FIRST ROW (full): { "timestamp": "2019-04-09 09:16:33", "price": 29764.75 }
LAST ROW (full):  { "timestamp": "2019-06-17 13:08:47", "price": 30344.0 }
```

### Packed Spot (example)
```
FILE: data/packed/spot/BANKNIFTY/201904/date=2019-04-08/ticks.parquet
SHAPE: (14,982, 8)
COLUMNS: timestamp (ns, IST), symbol, open, high, low, close, vol_delta, trade_date

FIRST ROW (full): { "timestamp": "2019-04-08 09:15:00+05:30", "symbol": "BANKNIFTY", "open": 30203.25, "high": 30203.25, "low": 30203.25, "close": 30203.25, "vol_delta": 0, "trade_date": "2019-04-08" }
LAST ROW (full):  { "timestamp": "2019-04-08 15:30:00+05:30", "symbol": "BANKNIFTY", "open": 29844.35, "high": 29844.35, "low": 29844.35, "close": 29844.35, "vol_delta": 0, "trade_date": "2019-04-08" }
```

### Raw Futures (example)
```
FILE: data/raw/futures/banknifty_futures.parquet
SHAPE: (22,580,623, 49)
ALL COLUMNS (first 20 shown here due to length in printouts include: timestamp, price, qty, avgPrice, volume, bQty, sQty, open, high, low, close, changeper, lastTradeTime, oi, oiHigh, oiLow, bq0, bp0, bo0, ... plus full order book up to level 4, symbol, data_type, processed_at.)

FIRST ROW (full): { full schema present; e.g., timestamp: 2019-12-02 15:30:00, price: 31925.0, qty: 20, avgPrice: 31891.35, volume: 97280, ..., symbol: BANKNIFTY, data_type: futures, processed_at: 2025-08-15 02:59:04.951189 }
LAST ROW (full):  { timestamp: 2025-07-30 15:30:00, price: 56201.6, qty: 35, avgPrice: 56247.54, volume: <value>, ..., symbol: BANKNIFTY, data_type: futures, processed_at: 2025-08-15 03:10:09.434975 }
```

### Packed Futures (example)
```
FILE: data/packed/futures/BANKNIFTY/201903/exp=2019-03-28/ticks.parquet
SHAPE: (337,331, 10)
COLUMNS: timestamp (ns, IST), symbol, open, high, low, close, vol_delta, trade_date, symbol_right, expiry

FIRST ROW (full): { "timestamp": "2019-03-07 09:15:00+05:30", "symbol": "BANKNIFTY", "open": 27692.4, "high": 27692.4, "low": 27692.4, "close": 27692.4, "vol_delta": 580, "trade_date": "2019-03-07", "symbol_right": "BANKNIFTY", "expiry": "2019-03-28" }
LAST ROW (full):  { "timestamp": "2019-03-28 15:30:00+05:30", "symbol": "BANKNIFTY", "open": 30040.9, "high": 30040.9, "low": 30040.9, "close": 30040.9, "vol_delta": 0, "trade_date": "2019-03-28", "symbol_right": "BANKNIFTY", "expiry": "2019-03-28" }
```

## Backtesting (ATM Volume Burst)

- Primary script: `backtests/atm_volume_ultra.py`.
- Core logic:
  - Preload 1-second SPOT grid for each trading day between 09:15–15:30 IST.
  - For anchors each hour (10:00, 11:00, …), compute candidate ATM ±1 strikes.
  - For each candidate (CE/PE), load the day’s options ticks, aggregate per-second.
  - Compute volume burst features:
    - `vol_30s` = rolling sum over last 30 seconds.
    - `base_30s` = rolling mean over last 300 seconds × 30 (i.e., 5-minute baseline scaled to 30 seconds).
    - `burst = vol_30s > multiplier * base_30s` (default multiplier = 1.5; CLI `--multiplier`).
  - Pick earliest post-anchor burst; apply trend filter; simulate via delta proxy on SPOT path.
- I/O expectations:
  - Spot path via glob: `./data/packed/spot/{symbol}/**/date=*/ticks.parquet`.
  - Options path root: `./data/packed/options`.
  - Results written to `backtests/results/` (trades_*.parquet, summary_*.parquet).

## Manifest and Counts

- `meta/packed_manifest.csv` has 76,148 rows (per-option parquet inventory).
  - Columns: `path,symbol,opt_type,strike,expiry,rows,tmin,tmax`.
  - Example rows:
    - `./data/packed/options/BANKNIFTY/202311/exp=2023-11-15/type=CE/strike=41700.parquet, rows=29912, tmin=2023-11-09 09:15:02+05:30, tmax=2023-11-15 15:29:59+05:30`

## Known Data Caveats

- Options vendor data often repeats session-wide OHLC; downstream strategies should avoid building OHLC bars from options ticks and instead rely on SPOT for path + option delta proxies, or use last-trade `close` cautiously.
- Volume deltas are derived; absolute `volume` fields in raw files are cumulative and reset (often at session starts/rollovers). Verified on samples:
  - Raw options sample negative diffs (resets) count: 2 within contract window (handled by `.diff().clip(lower=0)`).
  - Raw futures negative diffs across full file: 19,819 (expected across many days; handled similarly).
- Timezones must be aligned when filtering/joining (use `datetime[ns, Asia/Kolkata]` consistently).

### Timezone clarification (spot)
- Raw spot timestamps are tz-naive wall clock times in IST.
- In `pack_spot.py` we use `replace_time_zone("Asia/Kolkata")` which attaches IST without shifting the wall time. That’s why packed output shows `+05:30` while preserving the same `HH:MM:SS`. There is no GMT addition or time shift.
- Example: Raw first row `2019-04-08 09:15:00` -> Packed `2019-04-08 09:15:00+05:30` (identical wall time). This is correct and keeps spot aligned with options/futures (also stored as IST).

## Useful Scripts Overview

- Validation: `verify_packed_data.py`, `verify_deduplication.py`, `verify_row_counts.py`, `verify_raw_vs_packed.py`, `verify_failures.py`.
- Analyses: `analyze_*`, `comprehensive_data_analysis.py`, `raw_packed_relationship_analysis.txt`.
- Bars: `build_bars.py`, `build_bars_spot_futures.py`, plus logs `bars_spot_futures.log`.
- Backtests: `backtests/README_ATM_VOLUME.md`, `TIMEZONE_ERROR_CONTEXT.md` provide context on past issues fixed.

Note: Utility scripts were created at different times. The three packers (`simple_pack.py`, `pack_spot.py`, `pack_futures.py`) are verified on this workspace. For other helpers, prefer a quick dry run or review before use.

## Backtest Performance Plan (speed focus)

- Preload per-day SPOT 1s grids and reuse across strategy runs.
- Predicate pushdown via Polars `scan_parquet` on options/futures with day-range filters and schema guards.
- Candidate reduction: derive ATM±1 once per anchor from SPOT; load only those strikes.
- Pre-aggregate options to per-second once per (strike,type,day); cache on disk to avoid recompute.
- Parallelize across days/anchors using Polars streaming or joblib; keep memory under control.
- Use `meta/packed_manifest.csv` to short-circuit nonexistent strikes/expiries.
- Consider persistent columnar cache (DuckDB/ClickHouse) for repeated backtests.

## Quick Reference: Paths

- Packed options: `data/packed/options/BANKNIFTY/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`
- Packed spot: `data/packed/spot/BANKNIFTY/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet`
- Packed futures: `data/packed/futures/BANKNIFTY/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet`
- Raw options: `data/raw/options/*.parquet` (per-strike)
- Raw spot: `data/raw/spot/*.parquet` (split parts)
- Raw futures: `data/raw/futures/*.parquet` and spot-like parts folders
- Calendar: `meta/expiry_calendar.csv`
- Manifest: `meta/packed_manifest.csv`

## Notes for Future Work

- If exposing parameters, add CLI flags to backtests (e.g., `--burst-secs`, `--avg-secs`).
- Consider storing normalized timestamps as `datetime[ns, Asia/Kolkata]` everywhere to avoid unit drift.
- For options analytics, prefer SPOT-driven simulations or robust last-trade sampling to avoid OHLC artifacts.

---

This document is generated from the current workspace state; if the data layout evolves, update samples and paths accordingly.

## Additional Notes

- Backtest scripts overview:
  - `backtests/atm_volume_ultra.py`: fastest path; daily SPOT 1s preload, options per-second pre-aggregation, delta-proxy PnL. Anchors at 10:00..15:00. Burst window 30s vs 5m baseline; multiplier default 1.5. Trend filter aligns side with hour open. Writes trades_*.parquet and summary_*.parquet.
  - `backtests/atm_volume*.py`: earlier/alternate variants; prefer `ultra` as canonical.
- Time alignment fixes implemented:
  - All joins/filters now cast literals to `Datetime('ns', Asia/Kolkata)` to avoid Polars dtype mismatches (μs vs ns, UTC vs IST). The dense 1s index is explicitly `ns`.
- Packed counts references:
  - `meta/packed_manifest.csv` has 76,148 rows (one per packed options file). Use it to fast-check availability before loading.
- Bars generation:
  - Spot/futures 1m bars are reliable; options bars intentionally skipped due to vendor OHLC artifact.
- Spot volume:
  - Raw spot lacks a true volume field; `pack_spot.py` sets `vol_delta` to 0. Any volume-based signals should be derived from options/futures, not spot.
- Volume semantics in raw data:
  - Raw options/futures `volume` is cumulative and resets (e.g., at session boundaries or vendor partitions). `.diff().clip(lower=0).fill_null(0)` is required to obtain per-tick deltas.
- Performance guidance:
  - Use `pl.scan_parquet` with predicate pushdown for day/hour slicing.
  - Read only required columns (`timestamp`, `close`, `vol_delta`) for options where possible.
  - Pre-aggregate to per-second once per (strike,type,day) and reuse across anchors/day logic.
  - Cache intermediate per-second frames to disk if repeatedly backtesting the same window.
  - Parallelize across days/anchors carefully; memory footprint scales with number of concurrent per-second series.
- Data quality guardrails:
  - Validate basic OHLC invariants after packing; repair zeros via `ensure_ohlc` as implemented in packers.
  - Keep timestamps tz-attached as IST in packed data; attach only, do not shift wall times.
  - Verify calendar joins with `join_asof` for `trade_date -> expiry`; drop non-mapped rows.
- Useful quick-globs:
  - Spot day files: `data/packed/spot/{SYMBOL}/**/date=*/ticks.parquet`.
  - Option strikes for an expiry: `data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type=*/strike=*.parquet`.
  - Futures contract path: `data/packed/futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet`.
