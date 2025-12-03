# NFO Data Conversion - Complete Analysis Report

## Executive Summary

After reading the ENTIRE ChatGPT conversation file (15,160 lines), I can confirm with 100% certainty:

**PRIMARY CONVERSION SCRIPT: `simple_pack.py`**

Located at `/workspace/simple_pack.py`, this is the definitive script that was settled upon after extensive iteration and debugging documented in the conversation.

---

## 1. All Scripts Identified (Chronological Order)

| Script Name | Purpose | Status |
|-------------|---------|--------|
| `simple_pack.py` | **PRIMARY** - Options data conversion with batched writes | **FINAL VERSION** |
| `pack_spot.py` | Spot (index) tick data conversion | Created after options done |
| `pack_futures.py` | Futures tick data conversion with expiry mapping | Created after options done |
| `build_bars.py` | 1-minute bar builder for all data types | Helper script |
| `atm_volume.py` | ATM volume-burst backtesting strategy | Slow version |
| `atm_volume_ultra.py` | **FAST** per-day preloading backtest | Speed-optimized version |

---

## 2. simple_pack.py - Complete Technical Specification

### 2.1 Key Functions

1. **`parse_filename(path)`** - Lines 34-99
   - Extracts symbol (NIFTY/BANKNIFTY), opt_type (CE/PE), strike from messy filenames
   - Handles various filename patterns like:
     - `banknifty1941128500ce.parquet` → strike=28500
     - `nifty23n0917000pe.parquet` → strike=17000
   - Uses rightmost 5 digits for BANKNIFTY, 4-5 for NIFTY

2. **`load_calendar(path)`** - Lines 103-137
   - Loads expiry calendar from `/workspace/meta/expiry_calendar.csv`
   - Renames columns: Instrument→symbol, Final_Expiry→expiry, Expiry_Type→kind
   - Computes week_index for weekly expiries
   - Adds is_monthly and is_weekly flags

3. **`normalize_timestamp(df)`** - Lines 141-175
   - **CRITICAL**: Uses `dt.replace_time_zone("Asia/Kolkata")` NOT `convert_time_zone`
   - Fixes 1970 bug by using 'ts' column when timestamp shows year ≤ 1971
   - Handles various input types (Utf8, Int, Float, already Datetime)

4. **`ensure_ohlc(df)`** - Lines 177-212
   - Guarantees OHLC columns exist
   - Repairs vendor zeros (replaces with close or computed values)
   - Enforces bounds: low ≤ open,close ≤ high

5. **`compute_vol_delta(df)`** - Lines 214-224
   - Computes volume delta from cumulative volume column
   - Falls back to qty column if volume not present
   - Uses `.diff().clip(lower_bound=0).fill_null(0)`

6. **`process_file(path, cal)`** - Lines 228-287
   - Orchestrates full file processing
   - Filters market hours only: 09:15-15:30 IST
   - Maps trade_date to next expiry via `join_asof(strategy="forward")`
   - Outputs final schema columns only

7. **`_write_partition(out_dir, g)`** - Lines 291-322
   - Writes one parquet file per (symbol, expiry, opt_type, strike)
   - Merges with existing file if present
   - Uses ZSTD compression level 3 with statistics

8. **`_flush_buckets(out_dir, buckets)`** - Lines 324-332
   - Flushes all in-memory buckets to disk
   - Called periodically based on `--flush-every` parameter

### 2.2 Final Output Schema

```
timestamp      - Datetime with IST timezone
symbol         - NIFTY or BANKNIFTY
opt_type       - CE or PE
strike         - Integer strike price
open           - Float64
high           - Float64
low            - Float64
close          - Float64
vol_delta      - Int64 (per-tick volume change)
expiry         - Date (mapped expiry date)
expiry_type    - weekly or monthly
is_monthly     - Int8 (0 or 1)
is_weekly      - Int8 (0 or 1)
```

### 2.3 Output Directory Structure

```
data/packed/options/
  BANKNIFTY/
    201904/
      exp=2019-04-11/
        type=CE/
          strike=29200.parquet
        type=PE/
          strike=29200.parquet
    201911/
      exp=2019-11-07/
        type=CE/
          strike=30400.parquet
```

### 2.4 Usage

```bash
# Smoke test (2000 files)
python -u simple_pack.py --limit 2000 --flush-every 2000

# Full run with periodic flushing
python -u simple_pack.py --flush-every 5000
```

---

## 3. Data Quality & Conversion Results

### 3.1 Verified Results from Conversation

| Metric | Raw Data | Packed Data | Notes |
|--------|----------|-------------|-------|
| Total Size | 230 GB | 16 GB | ~14x compression |
| Total Rows | 5.85 billion | 4.25 billion | 72.6% retention |
| Files | 85,278 | 76,147 | Partitioned by expiry/strike |
| Date Range | 2019 - 2025 | 2019 - 2025 | Full coverage |

### 3.2 Why Row Reduction is Expected

1. **Deduplication**: Same strike appears in multiple raw files
2. **Market Hours Filter**: Pre/post market ticks removed (09:15-15:30 only)
3. **Empty Files**: ~6,914 files (8.1%) were completely empty
4. **Parse Failures**: ~360 files had filename parsing issues

### 3.3 Data Quality Verified

- 0 quality issues found in 100 file sample
- No null timestamps
- No duplicate timestamps
- No price anomalies
- OHLC bounds OK

---

## 4. Expiry Calendar Information

### 4.1 Calendar File

**Location**: `/workspace/meta/expiry_calendar.csv`
- 835 rows covering 2019-2025
- Columns: Instrument, Final_Expiry, Expiry_Type

### 4.2 Key Expiry Rule Changes

**BANKNIFTY:**
| Period | Weekly Day | Monthly Day | Notes |
|--------|------------|-------------|-------|
| 2019-01 to 2023-08 | Thursday | Last Thursday | Original regime |
| 2023-09 to 2024-11-13 | Wednesday | Last Thursday→Wednesday | Weekly moved to Wed |
| 2024-11-14 onwards | DISCONTINUED | Last Wednesday→Tuesday | No more BN weekly |

**NIFTY:**
| Period | Weekly Day | Monthly Day |
|--------|------------|-------------|
| 2019-02 to 2025-08 | Thursday | Last Thursday |
| 2025-09 onwards | Tuesday | Last Tuesday |

### 4.3 Holiday Adjustments

- 36 expiry dates were shifted by 1 trading day due to holidays
- Example: 2024-04-11 (Thu holiday) → 2024-04-10 (Wed)

---

## 5. Era Segmentation for Backtesting

| Era Tag | Period | Description |
|---------|--------|-------------|
| `2019_old` | 2019 | Sparse data, partial days |
| `2020_old` | 2020 | Still limited strikes near ATM |
| `2021_transitional` | 2021 | Improving coverage |
| `2022_good` | 2022 → 2023-08 | All strikes, BN weekly Thu |
| `2023_weekly_wed` | 2023-09 → 2024-11-13 | BN weekly moved to Wed |
| `2024_post_weekly_end` | After 2024-11-13 | BN weekly discontinued |
| `2025_monthly_tue_cutover` | 2025-09+ | Both move to Tuesday |

---

## 6. Critical Technical Decisions

### 6.1 Timestamp Handling
- **USE**: `dt.replace_time_zone("Asia/Kolkata")` - Attaches timezone without shifting values
- **NOT**: `convert_time_zone` - Would shift values incorrectly

### 6.2 Expiry Mapping
- **USE**: `join_asof(strategy="forward")` - Maps each trade_date to NEXT expiry
- Calendar must be sorted by (symbol, expiry) for join_asof to work

### 6.3 1970 Bug Fix
- First row often has corrupt year=1970
- Fix: When `timestamp.year <= 1971`, use 'ts' column instead

### 6.4 Parquet Compression
- ZSTD compression level 3
- Statistics enabled for predicate pushdown

---

## 7. Backtesting Scripts

### 7.1 atm_volume_ultra.py (Fast Version)

Key optimizations for speed:
1. **Per-day preloading**: Each strike read once per day, not per hour
2. **Predicate pushdown**: Only day slice (09:15-15:30) is read
3. **No join_asof in hot loop**: Expiry resolved once per day
4. **Delta-proxy PnL**: Avoids expensive option tick joins

Expected performance: Year of data in ~30 seconds (vs 3+ hours for slow version)

### 7.2 Config Structure (backtests/config.yaml)

```yaml
raw_options_dir: "./data/packed/options"
spot_glob: "./data/packed/spot/{symbol}/**/*.parquet"
calendar_csv: "./meta/expiry_calendar.csv"
session:
  open: "09:15:00"
  close: "15:30:00"
  anchors: ["10:00","11:00","12:00","13:00","14:00","15:00"]
strike_step:
  BANKNIFTY: 100
  NIFTY: 50
signal:
  burst_secs: 30
  avg_secs: 300
  multiplier: 1.5
risk:
  side: "sell"
  target_pct: 0.15
  stop_pct: 0.15
  trail_pct: 0.10
pnl_mode: "delta_proxy"
```

---

## 8. Known Bug Fix in Backtest Script

The `parse_hhmm` function needs to handle both `HH:MM` and `HH:MM:SS` formats:

```python
def parse_hhmm(s: str) -> time:
    s = s.strip()
    parts = s.split(":")
    if len(parts) == 2:
        hh, mm = int(parts[0]), int(parts[1]); ss = 0
    elif len(parts) >= 3:
        hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
    else:
        raise ValueError(f"Bad time string: {s}")
    return time(hh, mm, ss)
```

---

## 9. Project Directory Structure (Final)

```
/workspace/
├── simple_pack.py              # PRIMARY OPTIONS CONVERSION SCRIPT
├── pack_spot.py                # Spot data conversion
├── pack_futures.py             # Futures data conversion
├── build_bars.py               # 1-minute bar builder
├── data/
│   ├── raw/
│   │   ├── options/            # 85,278 raw parquet files (~230 GB)
│   │   ├── futures/            # 2 large futures parquet files
│   │   └── spot/               # Spot price parquet files
│   ├── packed/
│   │   ├── options/            # 76,147 cleaned partitioned files (~16 GB)
│   │   ├── spot/               # Cleaned spot data
│   │   └── futures/            # Cleaned futures data
│   └── bars/                   # 1-minute bars
├── meta/
│   ├── expiry_calendar.csv     # 835 rows, holiday-aware expiries
│   ├── expiry_audit.csv        # Per-file quality audit (~85K rows)
│   └── packed_manifest.csv     # Manifest of packed files
├── backtests/
│   ├── atm_volume.py           # Original backtest (slow)
│   ├── atm_volume_ultra.py     # Fast backtest version
│   ├── config.yaml             # Backtest configuration
│   ├── strategies/             # Trading strategies
│   ├── cache/                  # Seconds cache for fast lookups
│   ├── results/                # Backtest output
│   └── engine/                 # Backtest execution engine
└── docs/
    ├── DATA_CONVERSION_ANALYSIS.md
    ├── DATA_CONVERSION_COMPLETE_ANALYSIS.md  # This file
    └── PROGRESS.md
```

---

## 10. Conclusion

The `simple_pack.py` script at `/workspace/simple_pack.py` is the **DEFINITIVE** and **FINAL** script for converting raw NFO options data to cleaned, partitioned Parquet format.

**Key confirmation points:**
1. Script verified against conversation (matches code shown at lines 11720-12105)
2. All technical decisions documented (timezone handling, expiry mapping, etc.)
3. Data quality verified through conversation results (76,147 files, 4.25B rows)
4. Backtesting infrastructure is ready with fast version (`atm_volume_ultra.py`)

The script has been thoroughly tested on the full dataset and produces high-quality output suitable for backtesting and analysis.
