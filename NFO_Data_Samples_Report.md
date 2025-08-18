# NFO Ops Lab Data Samples Report

## Overview
This report presents comprehensive data samples collected from the NFO Ops Lab packed data files, covering OPTIONS, SPOT, and FUTURES data across multiple years (2019-2024).

## Data Structure and File Organization

### Directory Structure
```
/workspace/data/packed/
├── options/
│   ├── BANKNIFTY/
│   │   └── YYYYMM/
│   │       └── exp=YYYY-MM-DD/
│   │           ├── type=CE/
│   │           │   └── strike=XXXXX.parquet
│   │           └── type=PE/
│   │               └── strike=XXXXX.parquet
│   └── NIFTY/
│       └── [similar structure]
├── spot/
│   ├── BANKNIFTY/
│   │   └── YYYYMM/
│   │       └── date=YYYY-MM-DD/
│   │           └── ticks.parquet
│   └── NIFTY/
│       └── [similar structure]
└── futures/
    ├── BANKNIFTY/
    │   └── YYYYMM/
    │       └── exp=YYYY-MM-DD/
    │           └── ticks.parquet
    └── NIFTY/
        └── [similar structure]
```

## Data Samples Analysis

### 1. OPTIONS Data Samples

#### 1.1 2019 BANKNIFTY OPTIONS CE (Sample)
- **File**: `/workspace/data/packed/options/BANKNIFTY/201907/exp=2019-07-11/type=CE/strike=29000.parquet`
- **Shape**: 18,672 rows × 13 columns
- **Date Range**: 2019-07-11 (single day - expiry day)
- **Trading Hours**: 09:15:00 to 15:29:58 IST

**Columns & Data Types**:
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

**Sample Data**:
- **First Row**: `{'timestamp': '2019-07-11 09:15:00+05:30', 'symbol': 'BANKNIFTY', 'opt_type': 'CE', 'strike': 29000, 'open': 1513.7, 'high': 1513.7, 'low': 0.0, 'close': 1513.7, 'vol_delta': 0, 'expiry': '2019-07-11', 'expiry_type': 'weekly', 'is_monthly': 0, 'is_weekly': 1}`
- **Last Row**: `{'timestamp': '2019-07-11 15:29:58+05:30', 'symbol': 'BANKNIFTY', 'opt_type': 'CE', 'strike': 29000, 'open': 1596.1, 'high': 1604.0, 'low': 1513.7, 'close': 1513.7, 'vol_delta': 0, 'expiry': '2019-07-11', 'expiry_type': 'weekly', 'is_monthly': 0, 'is_weekly': 1}`

#### 1.2 2022 BANKNIFTY OPTIONS CE (Sample)
- **File**: `/workspace/data/packed/options/BANKNIFTY/202207/exp=2022-07-21/type=CE/strike=36500.parquet`
- **Shape**: 57,509 rows × 13 columns
- **Date Range**: 2022-07-15 to 2022-07-21 (week leading to expiry)

**Key Observations**:
- Strike prices have increased from 29,000 (2019) to 36,500 (2022)
- Vol_delta shows volume changes, indicating active trading
- Data spans multiple days leading to expiry

**Sample Data**:
- **First Row**: `{'timestamp': '2022-07-15 09:15:00+05:30', 'symbol': 'BANKNIFTY', 'opt_type': 'CE', 'strike': 36500, 'open': 12.0, 'high': 12.8, 'low': 10.6, 'close': 11.65, 'vol_delta': 0}`
- **Last Row**: `{'timestamp': '2022-07-21 15:30:00+05:30', 'symbol': 'BANKNIFTY', 'opt_type': 'CE', 'strike': 36500, 'open': 11.95, 'high': 43.75, 'low': 0.05, 'close': 21.6, 'vol_delta': 2500}`

#### 1.3 2023 BANKNIFTY OPTIONS CE (Sample)  
- **File**: `/workspace/data/packed/options/BANKNIFTY/202311/exp=2023-11-15/type=CE/strike=41700.parquet`
- **Shape**: 29,912 rows × 13 columns
- **Strike Price**: 41,700 (higher than previous years)

#### 1.4 2024 BANKNIFTY OPTIONS CE (Sample)
- **File**: `/workspace/data/packed/options/BANKNIFTY/202402/exp=2024-02-28/type=CE/strike=47900.parquet`
- **Shape**: 60,361 rows × 13 columns
- **Strike Price**: 47,900 (highest observed)
- **Date Range**: 2024-02-22 to 2024-02-28

### 2. SPOT Data Samples

#### 2.1 2019 BANKNIFTY SPOT (Sample)
- **File**: `/workspace/data/packed/spot/BANKNIFTY/201907/date=2019-07-25/ticks.parquet`
- **Shape**: 21,743 rows × 8 columns
- **Index Values**: ~28,974 to ~29,117 range

**Columns & Data Types**:
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

**Sample Data**:
- **First Row**: `{'timestamp': '2019-07-25 09:15:01+05:30', 'symbol': 'BANKNIFTY', 'open': 28974.25, 'high': 28974.25, 'low': 28974.25, 'close': 28974.25, 'vol_delta': 0, 'trade_date': '2019-07-25'}`
- **Last Row**: `{'timestamp': '2019-07-25 15:30:00+05:30', 'symbol': 'BANKNIFTY', 'open': 29109.2, 'high': 29109.2, 'low': 29109.2, 'close': 29109.2, 'vol_delta': 0, 'trade_date': '2019-07-25'}`

#### 2.2 2022 BANKNIFTY SPOT (Sample)
- **File**: `/workspace/data/packed/spot/BANKNIFTY/202207/date=2022-07-25/ticks.parquet`
- **Shape**: 13,813 rows × 8 columns
- **Index Values**: ~36,667 to ~36,789 range (significant increase from 2019)

#### 2.3 2023 BANKNIFTY SPOT (Sample)
- **File**: `/workspace/data/packed/spot/BANKNIFTY/202311/date=2023-11-16/ticks.parquet`
- **Shape**: 11,070 rows × 8 columns
- **Index Values**: ~44,133 to ~44,251 range

### 3. FUTURES Data Samples

#### 3.1 2019 BANKNIFTY FUTURES (Sample)
- **File**: `/workspace/data/packed/futures/BANKNIFTY/201912/exp=2019-12-26/ticks.parquet`
- **Shape**: 376,057 rows × 10 columns
- **Date Range**: 2019-12-02 to 2019-12-26 (full contract period)

**Columns & Data Types**:
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

**Sample Data**:
- **First Row**: `{'timestamp': '2019-12-02 15:30:00+05:30', 'symbol': 'BANKNIFTY', 'open': 32003.85, 'high': 32003.85, 'low': 32003.85, 'close': 32003.85, 'vol_delta': 97280, 'trade_date': '2019-12-02', 'symbol_right': 'BANKNIFTY', 'expiry': '2019-12-26'}`
- **Last Row**: `{'timestamp': '2019-12-26 15:30:00+05:30', 'symbol': 'BANKNIFTY', 'open': 32307.75, 'high': 32307.75, 'low': 32307.75, 'close': 32307.75, 'vol_delta': 0, 'trade_date': '2019-12-26', 'symbol_right': 'BANKNIFTY', 'expiry': '2019-12-26'}`

#### 3.2 2022 BANKNIFTY FUTURES (Sample)
- **File**: `/workspace/data/packed/futures/BANKNIFTY/202207/exp=2022-07-28/ticks.parquet`
- **Shape**: 277,669 rows × 10 columns
- **Price Range**: ~33,396 to ~36,756

#### 3.3 2023 BANKNIFTY FUTURES (Sample)
- **File**: `/workspace/data/packed/futures/BANKNIFTY/202311/exp=2023-11-30/ticks.parquet`
- **Shape**: 227,634 rows × 10 columns

## Key Observations and Insights

### Data Quality and Consistency
1. **Consistent Schema**: All data types maintain consistent column structures across years
2. **Timezone Handling**: All timestamps use Asia/Kolkata timezone
3. **Trading Hours**: Standard market hours from 09:15:00 to 15:30:00 IST
4. **Data Granularity**: Second-level tick data with microsecond precision

### Market Evolution (2019-2024)
1. **Index Growth**: 
   - BANKNIFTY: ~29,000 (2019) → ~44,000+ (2023)
   - Consistent upward trend over the 4-year period

2. **Options Strike Ranges**:
   - 2019: ~29,000 strikes
   - 2022: ~36,500 strikes  
   - 2023: ~41,700 strikes
   - 2024: ~47,900 strikes

3. **Data Volume**:
   - Options: 18K-60K rows per contract
   - Spot: 10K-22K rows per day
   - Futures: 227K-376K rows per contract period

### Technical Specifications
- **File Format**: Apache Parquet with efficient compression
- **Data Types**: Optimized with appropriate precision (int32 for strikes, float64 for prices)
- **Time Resolution**: Microsecond-level timestamp precision
- **Volume Tracking**: Vol_delta field tracks incremental volume changes

### Data Coverage
- **Years Covered**: 2019, 2022, 2023, 2024 (with gaps in 2020-2021)
- **Instruments**: BANKNIFTY and NIFTY both available
- **Options Types**: Both Call (CE) and Put (PE) options
- **Expiry Types**: Both weekly and monthly contracts marked

## File Organization Summary

| Data Type | Files Analyzed | Total Years | Avg Records/File | Key Characteristics |
|-----------|----------------|-------------|------------------|-------------------|
| OPTIONS   | 4 samples      | 2019-2024   | 41,614           | Strike-specific files, second-level data |
| SPOT      | 3 samples      | 2019-2023   | 15,209           | Daily files, complete trading sessions |  
| FUTURES   | 3 samples      | 2019-2023   | 293,787          | Contract-period files, highest volume |

## Recommendations for Documentation

1. **Data Dictionary**: Include comprehensive field definitions for vol_delta and trading flags
2. **Market Context**: Document the significant market growth reflected in strike price evolution
3. **Usage Patterns**: Highlight that futures files contain the most comprehensive tick data
4. **Time Zones**: Emphasize consistent Asia/Kolkata timezone usage across all datasets
5. **Data Gaps**: Note any missing periods or instruments for complete transparency

This dataset represents a comprehensive repository of Indian equity derivatives market data suitable for quantitative analysis, backtesting, and research applications.