# NFO Data Conversion Analysis Report

## Summary

This document provides a comprehensive analysis of the data conversion process for NFO (NIFTY/BANKNIFTY) options data from raw MySQL/SQL.gz files to Parquet format. Based on the ChatGPT conversation history and project structure analysis.

---

## 1. The Correct Data Conversion Script

### **Script Name: `simple_pack.py`**

**Location:** `/workspace/simple_pack.py`

This is the primary script used to convert raw options data from the original format to the cleaned, partitioned Parquet format.

### Key Features of simple_pack.py:

1. **Filename Parsing**: Extracts symbol (NIFTY/BANKNIFTY), opt_type (CE/PE), and strike price from filenames like:
   - `banknifty1941128500ce.parquet` -> strike=28500
   - `nifty23n0917000pe.parquet` -> strike=17000

2. **Timestamp Fixing**:
   - Uses `ts` column when vendor wrote 1970 in `timestamp`
   - Attaches IST timezone using `replace_time_zone` (no shifting)
   - Filters market hours only (09:15-15:30)

3. **Expiry Mapping**:
   - Uses calendar file (`meta/expiry_calendar.csv`) to map each trade_date to the NEXT expiry via `join_asof(strategy="forward")`
   - Computes week_index per symbol-month for weekly expiries

4. **Data Cleaning**:
   - Fixes OHLC (repairs vendor zeros)
   - Computes vol_delta from volume/qty
   - Removes duplicate timestamps

5. **Output Schema** (per row):
   - `timestamp, symbol, opt_type, strike, open, high, low, close, vol_delta, expiry, expiry_type, is_monthly, is_weekly`

6. **Partitioning Output**:
   ```
   data/packed/options/
     BANKNIFTY/
       201904/
         exp=2019-04-11/
           type=CE/
             strike=29200.parquet
   ```

---

## 2. Data Structure Overview

### 2.1 Raw Data Location
- **Options**: `/workspace/data/raw/options/` - 65,535 parquet files (~230 GB total)
- **Futures**: `/workspace/data/raw/futures/` - 2 large parquet files (~2.8 GB each)
- **Spot**: `/workspace/data/raw/spot/` - Multiple partitioned parquet files

### 2.2 Options Data Columns (52-53 columns depending on era)

**Standard Options Schema (recent data):**
```
timestamp, price, qty, avgPrice, volume, bQty, sQty, open, high, low, close,
changeper, lastTradeTime, oi, oiHigh, oiLow,
bq0, bp0, bo0, bq1, bp1, bo1, bq2, bp2, bo2, bq3, bp3, bo3, bq4, bp4, bo4,
sq0, sp0, so0, sq1, sp1, so1, sq2, sp2, so2, sq3, sp3, so3, sq4, sp4, so4,
ts, symbol, opt_type, strike, year, month
```

**Older data (2019)** includes an additional column:
- `volactual` (volume actual)

**Minimal data (small files):**
```
timestamp, price, ts, symbol, opt_type, strike, year, month
```

### 2.3 Known Data Quality Issues

1. **1970 First Row Bug**: First row often has `year=1970, month=1` - timestamp column is corrupt but `ts` column is valid
2. **Partial Days**: Some files end before market close (e.g., 2019-10-15 stops at 11:21)
3. **Zero/Null OHLC**: Some records have zeros in open/high/low that need to be repaired
4. **Missing Depth**: Older files have sparse or missing order book data

---

## 3. Expiry Calendar Summary

### Reference File: `/workspace/meta/expiry_calendar.csv`
- **Total rows**: 835
- **Coverage**: 2019-2025

### Key Expiry Rule Changes (BANKNIFTY):
| Period | Weekly Day | Monthly Day | Notes |
|--------|------------|-------------|-------|
| 2019-01 to 2023-08 | Thursday | Last Thursday | Original regime |
| 2023-09 to 2024-11-13 | Wednesday | Last Thursday→Wednesday (from Mar 2024) | Weekly moved to Wed |
| 2024-11-14 onwards | DISCONTINUED | Last Wednesday→Last Tuesday (from Sep 2025) | No more BN weekly |

### Key Expiry Rule Changes (NIFTY):
| Period | Weekly Day | Monthly Day |
|--------|------------|-------------|
| 2019-02 to 2025-08 | Thursday | Last Thursday |
| 2025-09 onwards | Tuesday | Last Tuesday |

### Holiday-Adjusted Expiries
- Total shifted rows: 36 (all by 1 trading day)
- Examples: 2024-04-11 (Thu holiday) → 2024-04-10 (Wed)

---

## 4. Era Segmentation

Based on the ChatGPT conversation, data should be segmented into these eras:

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

## 5. Data Audit Summary

### Audit File: `/workspace/meta/expiry_audit.csv`
- **Total files audited**: ~85,278

### Audit Columns:
- `file`: Path to parquet file
- `ok_read`: Successfully read (1/0)
- `n_rows, n_cols`: Row and column count
- `has_required_cols`: Has mandatory columns
- `has_oi_depth`: Has OI and depth columns
- `first_ts, last_ts`: First and last timestamps
- `first_ts_is_1970`: First row has 1970 bug (1/0)
- `partial_day`: File ends early (1/0)
- `covers_any_weekly_expiry`: Data includes a weekly expiry day
- `covers_monthly_expiry`: Data includes the monthly expiry day
- `expected_weeklies`: List of expected weekly expiries
- `expected_monthly`: Expected monthly expiry

---

## 6. Other Conversion Scripts

### Futures Processing
- `/workspace/pack_futures.py`
- `/workspace/pack_futures_chunked.py`
- `/workspace/pack_futures_simple.py`

### Spot Data Processing
- `/workspace/pack_spot.py`

### Bar Building
- `/workspace/build_bars.py`
- `/workspace/build_bars_spot_futures.py`

---

## 7. Directory Structure Summary

```
/workspace/
├── data/
│   ├── raw/
│   │   ├── options/        # 65,535 raw option parquet files (~230 GB)
│   │   ├── futures/        # 2 large futures parquet files
│   │   └── spot/           # Spot price parquet files
│   ├── packed/
│   │   └── options/        # Cleaned, partitioned output
│   └── bars/               # Minute/second bars
├── meta/
│   ├── expiry_calendar.csv # Holiday-aware expiry dates (835 rows)
│   ├── expiry_audit.csv    # Per-file audit results (~85K rows)
│   └── packed_manifest.csv # Manifest of packed files
├── simple_pack.py          # THE PRIMARY CONVERSION SCRIPT
├── backtests/              # Backtesting engine and strategies
│   ├── strategies/         # Various trading strategies
│   ├── cache/              # Seconds cache for fast lookups
│   └── engine/             # Backtest execution engine
└── nfoops/                 # Operations utilities
```

---

## 8. How to Process New Data

### Prerequisites
1. Ensure expiry calendar is updated: `/workspace/meta/expiry_calendar.csv`
2. Place raw parquet files in: `/workspace/data/raw/options/`

### Run Conversion
```bash
cd /workspace
python simple_pack.py --flush-every 5000
```

### Options
- `--limit N`: Process only N files (for testing)
- `--flush-every K`: Flush to disk every K files (default: 5000)

---

## 9. Data Source Background

From the README in the ChatGPT conversation:

- **Source**: Zerodha WebSocket (Kite Connect API)
- **Collection Start**: ~2020 (some 2019 data exists but sparse)
- **Collection Scope**:
  - 2020-2022: Current week expiry, strikes near spot only
  - 2022+: All strikes for current and next week expiry
- **Table Counts (Aug 2025)**:
  - `das_bankopt`: 44,522 tables
  - `das_niftyopt`: 41,026 tables

---

## 10. Next Steps for Processing Additional Data

If you have new raw SQL.gz files to process:

1. **Extract SQL to tables** using the original DAS system or mysqldump restore
2. **Export tables to Parquet** using appropriate column mapping
3. **Ensure filename convention**: `banknifty{YYWK}{strike}{ce|pe}.parquet`
4. **Place in raw folder**: `/workspace/data/raw/options/`
5. **Run simple_pack.py** to process and partition

---

## Conclusion

The **`simple_pack.py`** script is the correct and most recent script used for converting raw options data to the cleaned, partitioned Parquet format. It handles:
- Timestamp fixing (1970 bug)
- IST timezone attachment
- Expiry mapping via calendar join
- OHLC repair
- Volume delta computation
- Proper partitioning by symbol/expiry/opt_type/strike

The expiry calendar (`meta/expiry_calendar.csv`) is the single source of truth for holiday-adjusted expiry dates and must be kept updated for accurate data processing.
