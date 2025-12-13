# Market Truth Framework

A comprehensive system for extracting market facts from options data, enabling deep understanding of market behavior patterns.

## Overview

This framework processes tick-by-tick options data to generate:
- **Market truths**: Burst sizes, durations, option responses by DTE
- **Microstructure analysis**: Spreads, depths, order book imbalances
- **Liquidity events**: Pull/replenish classification
- **Regime detection**: Fear spikes, quiet periods, burst events
- **Statistical truth tables**: Quantified market behaviors

## Quick Start

### 1. Preprocessing (Generate Features)

Process a single day:
```bash
cd preprocessing
python core_preprocessor.py --date 2025-08-01 --underlying BANKNIFTY
```

Process all available days:
```bash
python batch_processor.py
```

This generates:
- `market_truth_data/features/features_{date}.parquet` - Per-second state vectors
- `market_truth_data/bursts/bursts_{date}.parquet` - Burst events

### 2. Generate Statistics

```bash
cd preprocessing
python statistics_generator.py
```

This creates `market_truth_data/statistics/truth_tables.json` with comprehensive market statistics.

### 3. Start API Server

```bash
cd api
pip install -r requirements.txt
python market_truth_api.py
```

Access API at: `http://localhost:8000/docs`

## Data Schema

### Features (Per-Second State Vector)

```python
{
    'timestamp': datetime,
    'spot_price': float,
    'atm_strike': int,
    'dte_days': int,
    
    # Spot dynamics
    'ret_1s': float,           # Log return
    'rv_10s': float,           # 10-second realized volatility
    'rv_30s': float,
    'rv_120s': float,
    'accel_10s': float,        # Acceleration metric
    
    # CE (Call) microstructure
    'ce_mid': float,
    'ce_spread': float,
    'ce_bid_depth_5': int,
    'ce_ask_depth_5': int,
    'ce_obi_5': float,         # Order book imbalance
    'ce_depth_slope_bid': float,
    'ce_depth_slope_ask': float,
    'ce_volume': int,
    'dOptCE_1s': float,        # 1s price change
    
    # PE (Put) microstructure 
    'pe_mid': float,
    'pe_spread': float,
    'pe_bid_depth_5': int,
    'pe_ask_depth_5': int,
    'pe_obi_5': float,
    'pe_depth_slope_bid': float,
    'pe_depth_slope_ask': float,
    'pe_volume': int,
    'dOptPE_1s': float,
}
```

### Burst Events

```python
{
    'burst_id': int,
    'start_time': datetime,
    'end_time': datetime,
    'duration_seconds': int,
    'size_points': float,
    'direction': int,          # +1 or -1
    'ce_move': float,          # Option price change
    'pe_move': float,
    'ce_rel_delta': float,     # Option move / spot move
    'pe_rel_delta': float,
    'dte_at_start': int,
    'time_of_day': str,        # 'morning', 'midday', 'afternoon'
}
```

## API Endpoints

### List Available Days
```
GET /days
```

### Get Day Features
```
GET /day/{date}/features?start_sec=0&end_sec=1800&columns=spot_price,rv_10s
```

### Get Day Bursts
```
GET /day/{date}/bursts
```

### Get Statistics
```
GET /statistics
```

### Get Day Summary
```
GET /day/{date}/summary
```

## File Structure

```
market_truth_framework/
├── preprocessing/
│   ├── core_preprocessor.py          # Main preprocessing pipeline
│   ├── batch_processor.py            # Batch runner for all days
│   ├── liquidity_detector.py         # Pull/replenish detection
│   └── statistics_generator.py       # Truth tables generator
│
├── market_truth_data/
│   ├── features/                     # Per-second state vectors
│   │   └── features_{YYYY-MM-DD}.parquet
│   ├── bursts/                       # Burst events
│   │   └── bursts_{YYYY-MM-DD}.parquet
│   └── statistics/                   # Truth tables
│       └── truth_tables.json
│
└── api/
    ├── market_truth_api.py           # FastAPI server
    └── requirements.txt
```

## Performance

- **Processing speed**: ~23 seconds per day (3.7M rows → 21K features)
- **Parallel processing**: 4 workers, ~30 minutes for 81 days
- **Output size**: ~800KB per day (compressed Parquet)
- **Memory**: Processes one day at a time, memory-efficient

## Usage Examples

### Python Analysis

```python
import polars as pl

# Load features for analysis
df = pl.read_parquet("market_truth_data/features/features_2025-08-01.parquet")

# Find high-acceleration moments
bursts = df.filter(pl.col('accel_10s') > 3.0)

# Check microstructure health
healthy = df.filter(
    (pl.col('ce_spread') < 2.0) &
    (pl.col('pe_spread') < 2.0) &
    (pl.col('ce_obi_5').abs() < 0.3)
)

# Analyze option response
high_rv = df.filter(pl.col('rv_10s') > 0.001)
```

### Load Statistics

```python
import json

with open("market_truth_data/statistics/truth_tables.json") as f:
    stats = json.load(f)

# Get burst statistics for 0 DTE
dte_0_stats = stats['burst_statistics']['0']
print(f"Average burst size: {dte_0_stats['avg_burst_size']} points")
print(f"P90 burst size: {dte_0_stats['p90_burst_size']} points")
```

## Burst Detection Algorithm

Burst starts when ALL conditions are met:
1. **Displacement**: `|spot_t - spot_t-10| >= B_points`
2. **Volatility expansion**: `RV_10 > k1 × RV_120`
3. **Acceleration**: `max_step_3s >= B_step`

Burst ends when BOTH are met:
1. **Calming down**: `|spot_t - spot_t-5| < end_points`
2. **Volatility contraction**: `RV_10 < k2 × RV_120`

### Thresholds (Tunable)

**NIFTY**:
- 0 DTE: B_points=8, B_step=2, k1=1.6
- 2 DTE: B_points=10, B_step=3, k1=1.8
- 4+ DTE: B_points=12, B_step=4, k1=2.0

**BANKNIFTY**:
- 0 DTE: B_points=20, B_step=6, k1=1.6
- 2 DTE: B_points=25, B_step=8, k1=1.8
- 4+ DTE: B_points=35, B_step=10, k1=2.0

## Next Steps

1. **Tune thresholds**: Adjust burst detection based on actual market data
2. **Add IV calculation**: Black-Scholes IV for each option
3. **Regime classification**: Implement fear/chop/normal regime detector
4. **Visualization**: Build React frontend with playback controls
5. **Greeks**: Add Delta, Gamma, Vega, Theta calculation

## Dependencies

- Python 3.10+
- polars
- numpy
- numba
- fastapi
- uvicorn

## License

Internal research tool.

---

**Built for deep market understanding and strategy research** 🚀
