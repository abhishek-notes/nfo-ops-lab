# NFO Ops Lab Documentation

## Project Overview
A comprehensive NIFTY/BANKNIFTY options, spot, and futures data processing pipeline with ~230GB of tick data normalized and converted to 1-minute bars for backtesting.

## Directory Structure

```
/workspace/
├── data/                       # All processed data
│   ├── raw/                   # Original raw tick data (~230GB)
│   │   ├── options/           # Options tick files
│   │   │   └── *.parquet      # Format: banknifty1941128500ce.parquet
│   │   ├── futures/           # Futures tick data
│   │   │   ├── banknifty_futures.parquet (1.4GB)
│   │   │   └── nifty_futures.parquet (1.4GB)
│   │   └── spot/              # Spot tick data organized by folders
│   ├── packed/                # Normalized and partitioned tick data
│   │   ├── options/           # 76,147 partitioned option files
│   │   │   └── {SYMBOL}/{YYYYMM}/exp={expiry}/type={CE|PE}/strike={strike}.parquet
│   │   ├── spot/              # 2,977 partitioned spot files
│   │   │   └── {SYMBOL}/{YYYYMM}/date={trade_date}/ticks.parquet
│   │   └── futures/           # 138 partitioned futures files
│   │       └── {SYMBOL}/{YYYYMM}/exp={expiry}/ticks.parquet
│   └── bars/                  # 1-minute OHLCV bars
│       ├── options/           # SKIPPED due to data quality issues
│       ├── spot/              # 2,977 spot bar files
│       │   └── {SYMBOL}/{YYYYMM}/date={trade_date}/bars_1m.parquet
│       └── futures/           # 138 futures bar files
│           └── {SYMBOL}/{YYYYMM}/exp={expiry}/bars_1m.parquet
├── meta/                      # Metadata files
│   └── expiry_calendar.csv    # NSE expiry calendar
└── Scripts (root level)       # All processing scripts

## Key Scripts

### Data Processing Scripts
- **`simple_pack.py`**: Main options data packing script. Normalizes timestamps, fixes 1970 issues, deduplicates, and partitions by symbol/expiry/type/strike
- **`pack_spot.py`**: ChatGPT's spot data processing script. Creates one parquet per (symbol, trade_date)
- **`pack_futures.py`**: ChatGPT's futures processing script. Maps to monthly expiries using calendar
- **`pack_futures_simple.py`**: Memory-efficient version for large futures files

### Bar Building Scripts
- **`build_bars.py`**: Original 1-minute bar builder for all data types
- **`build_bars_spot_futures.py`**: Modified version that only builds bars for spot and futures (skips options)

### Utility Scripts
- **`bootstrap.sh`**: Initial setup script that created directory structure
- **`verify_data.py`**: Data verification script to check retention rates

## Data Processing Summary

### Processing Statistics
- **Raw Data**: ~230GB of tick data
- **Options**: 76,147 packed files (72.61% retention after deduplication)
- **Spot**: 2,977 packed files
- **Futures**: 138 packed files
- **Time Period**: 2019-2025 (with some gaps)

## Data Schema and Samples

### 1. OPTIONS Data (Packed Ticks)
Path: `data/packed/options/{SYMBOL}/{YYYYMM}/exp={expiry}/type={CE|PE}/strike={strike}.parquet`

⚠️ **Data Quality Issue**: Options tick data contains session-wide OHLC values instead of actual tick prices, making it unsuitable for bar building.

**Schema (13 columns)**:
```
timestamp         : datetime64[us, Asia/Kolkata]
symbol           : object (string)
opt_type         : object (CE/PE)
strike           : int32
open             : float64
high             : float64
low              : float64
close            : float64
vol_delta        : int64
expiry           : date
expiry_type      : object (weekly/monthly)
is_monthly       : int8 (0/1)
is_weekly        : int8 (0/1)
```

#### Sample: BANKNIFTY 2019-07-11 CE Strike 29000
- **File**: `data/packed/options/BANKNIFTY/201907/exp=2019-07-11/type=CE/strike=29000.parquet`
- **Shape**: 18,672 rows × 13 columns
- **First 5 rows**:
```
timestamp                   symbol     opt_type  strike   open     high     low    close   vol_delta
2019-07-11 09:15:00 IST    BANKNIFTY  CE        29000    1513.7   1513.7   0.0    1513.7  0
2019-07-11 09:15:01 IST    BANKNIFTY  CE        29000    1513.7   1513.7   0.0    1513.7  0
2019-07-11 09:15:02 IST    BANKNIFTY  CE        29000    1513.7   1513.7   0.0    1513.7  0
2019-07-11 09:15:03 IST    BANKNIFTY  CE        29000    1513.7   1513.7   0.0    1513.7  0
2019-07-11 09:15:04 IST    BANKNIFTY  CE        29000    1513.7   1513.7   0.0    1513.7  0
```
- **Last 5 rows**:
```
timestamp                   symbol     opt_type  strike   open     high     low      close   vol_delta
2019-07-11 15:29:54 IST    BANKNIFTY  CE        29000    1596.1   1604.0   1513.7   1599.0  0
2019-07-11 15:29:55 IST    BANKNIFTY  CE        29000    1596.1   1604.0   1513.7   1599.0  0
2019-07-11 15:29:56 IST    BANKNIFTY  CE        29000    1596.1   1604.0   1513.7   1599.0  0
2019-07-11 15:29:57 IST    BANKNIFTY  CE        29000    1596.1   1604.0   1513.7   1513.7  0
2019-07-11 15:29:58 IST    BANKNIFTY  CE        29000    1596.1   1604.0   1513.7   1513.7  0
```

#### Sample: BANKNIFTY 2022-07-21 CE Strike 36500
- **File**: `data/packed/options/BANKNIFTY/202207/exp=2022-07-21/type=CE/strike=36500.parquet`
- **Shape**: 57,509 rows × 13 columns
- **Strike Evolution**: 29,000 (2019) → 36,500 (2022)

#### Sample: BANKNIFTY 2024-02-28 CE Strike 47900
- **File**: `data/packed/options/BANKNIFTY/202402/exp=2024-02-28/type=CE/strike=47900.parquet`
- **Shape**: 60,361 rows × 13 columns
- **Strike Evolution**: 47,900 (2024) - highest observed

### 2. SPOT Data (Packed Ticks)
Path: `data/packed/spot/{SYMBOL}/{YYYYMM}/date={trade_date}/ticks.parquet`

**Schema (8 columns)**:
```
timestamp         : datetime64[ns, Asia/Kolkata]
symbol           : object
open             : float64
high             : float64
low              : float64
close            : float64
vol_delta        : int64
trade_date       : date
```

#### Sample: BANKNIFTY 2019-07-25
- **File**: `data/packed/spot/BANKNIFTY/201907/date=2019-07-25/ticks.parquet`
- **Shape**: 21,743 rows × 8 columns
- **First 5 rows**:
```
timestamp                   symbol     open       high       low        close      vol_delta
2019-07-25 09:15:01 IST    BANKNIFTY  28974.25   28974.25   28974.25   28974.25   0
2019-07-25 09:15:02 IST    BANKNIFTY  28974.25   28974.25   28974.25   28974.25   0
2019-07-25 09:15:03 IST    BANKNIFTY  28974.25   28974.25   28974.25   28974.25   0
2019-07-25 09:15:04 IST    BANKNIFTY  28974.25   28974.25   28974.25   28974.25   0
2019-07-25 09:15:05 IST    BANKNIFTY  28974.25   28974.25   28974.25   28974.25   0
```
- **Last 5 rows**:
```
timestamp                   symbol     open      high      low       close     vol_delta
2019-07-25 15:29:56 IST    BANKNIFTY  29109.2   29109.2   29109.2   29109.2   0
2019-07-25 15:29:57 IST    BANKNIFTY  29109.2   29109.2   29109.2   29109.2   0
2019-07-25 15:29:58 IST    BANKNIFTY  29109.2   29109.2   29109.2   29109.2   0
2019-07-25 15:29:59 IST    BANKNIFTY  29109.2   29109.2   29109.2   29109.2   0
2019-07-25 15:30:00 IST    BANKNIFTY  29109.2   29109.2   29109.2   29109.2   0
```

#### Sample: BANKNIFTY 2023-11-16
- **File**: `data/packed/spot/BANKNIFTY/202311/date=2023-11-16/ticks.parquet`
- **Shape**: 11,070 rows × 8 columns
- **Index Evolution**: ~29,000 (2019) → ~44,000+ (2023)

### 3. FUTURES Data (Packed Ticks)
Path: `data/packed/futures/{SYMBOL}/{YYYYMM}/exp={expiry}/ticks.parquet`

**Schema (10 columns)**:
```
timestamp         : datetime64[ns, Asia/Kolkata]
symbol           : object
open             : float64
high             : float64
low              : float64
close            : float64
vol_delta        : int64
trade_date       : date
symbol_right     : object
expiry           : date
```

#### Sample: BANKNIFTY 2019-12-26 Futures
- **File**: `data/packed/futures/BANKNIFTY/201912/exp=2019-12-26/ticks.parquet`
- **Shape**: 376,057 rows × 10 columns (full contract period)
- **First 5 rows**:
```
timestamp                   symbol     open       high       low        close      vol_delta
2019-12-02 15:30:00 IST    BANKNIFTY  32003.85   32003.85   32003.85   32003.85   97280
2019-12-03 09:15:00 IST    BANKNIFTY  32000.0    32000.0    32000.0    32000.0    0
2019-12-03 09:15:01 IST    BANKNIFTY  32000.0    32000.0    32000.0    32000.0    0
2019-12-03 09:15:02 IST    BANKNIFTY  32000.0    32000.0    32000.0    32000.0    0
2019-12-03 09:15:03 IST    BANKNIFTY  32000.0    32000.0    32000.0    32000.0    1700
```
- **Last 5 rows**:
```
timestamp                   symbol     open       high       low        close      vol_delta
2019-12-26 15:29:56 IST    BANKNIFTY  32307.75   32307.75   32307.75   32307.75   0
2019-12-26 15:29:57 IST    BANKNIFTY  32307.75   32307.75   32307.75   32307.75   0
2019-12-26 15:29:58 IST    BANKNIFTY  32307.75   32307.75   32307.75   32307.75   0
2019-12-26 15:29:59 IST    BANKNIFTY  32307.75   32307.75   32307.75   32307.75   0
2019-12-26 15:30:00 IST    BANKNIFTY  32307.75   32307.75   32307.75   32307.75   0
```

### 4. BAR Data (1-Minute OHLCV)

#### SPOT Bars
Path: `data/bars/spot/{SYMBOL}/{YYYYMM}/date={trade_date}/bars_1m.parquet`
- **Schema**: timestamp, symbol, trade_date, open, high, low, close, volume
- **Count**: 2,977 files
- **Example**: 376 bars per trading day (09:15 to 15:30)

#### FUTURES Bars
Path: `data/bars/futures/{SYMBOL}/{YYYYMM}/exp={expiry}/bars_1m.parquet`
- **Schema**: timestamp, symbol, expiry, open, high, low, close, volume
- **Count**: 138 files
- **Data Quality**: Clean OHLCV with proper price relationships (high >= low, etc.)

## Market Evolution Summary (2019-2024)

| Year | BANKNIFTY Index | Strike Range | Data Points |
|------|-----------------|--------------|-------------|
| 2019 | ~29,000        | 29,000       | 18K-22K/day |
| 2022 | ~36,000        | 36,500       | 13K-15K/day |
| 2023 | ~44,000        | 41,700       | 11K-12K/day |
| 2024 | ~48,000        | 47,900       | 15K-20K/day |

## Important Notes

1. **Timezone**: All timestamps are in Asia/Kolkata (IST)
2. **Trading Hours**: 09:15:00 to 15:30:00 IST
3. **Options Data Issue**: Raw tick data contains session-wide OHLC instead of actual tick prices
4. **Data Retention**: 72.61% retention rate after deduplication (legitimate duplicate removal)
5. **Memory Requirements**: Futures files are large (1.4GB each), require memory-efficient processing

## Next Steps
With spot and futures bars successfully created, the system is ready for:
1. Backtesting strategy implementation
2. DuckDB hot-cache setup
3. ClickHouse backend integration
4. Vectorized momentum scalping strategies
5. Streamlit UI for parameter sweeps