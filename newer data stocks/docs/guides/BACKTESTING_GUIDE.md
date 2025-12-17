# Backtesting Guide for Date-Partitioned Options Data (v3)

## Table of Contents
1. [Data Structure Overview](#data-structure-overview)
2. [Loading Data Efficiently](#loading-data-efficiently)
3. [Backtesting Patterns](#backtesting-patterns)
4. [Performance Best Practices](#performance-best-practices)
5. [Example Backtest Scripts](#example-backtest-scripts)
6. [Common Pitfalls](#common-pitfalls)

---

## Data Structure Overview

### Directory Layout
```
data/options_date_packed_FULL_v3_SPOT_ENRICHED/
├── 2025-08-01/
│   ├── BANKNIFTY/part-banknifty-0.parquet
│   └── NIFTY/part-nifty-0.parquet
├── 2025-08-02/
│   ├── BANKNIFTY/part-banknifty-0.parquet
│   └── NIFTY/part-nifty-0.parquet
...
```

### File Content

Each file contains **ALL strikes and expiries** for that day/underlying:

**Columns** (64 total):
- **Identifiers**: `strike`, `opt_type`, `expiry`, `symbol`, `underlying`
- **Timestamps**: `timestamp`, `timestamp_ns`, `date`
- **Prices**: `price`, `open`, `high`, `low`, `close`, `avgPrice`
- **Quantities**: `volume`, `qty`, `oi`, `oiHigh`, `oiLow`
- **Order Book**: `bp0-4`, `sp0-4`, `bq0-4`, `sq0-4`, `bo0-4`, `so0-4` (bid/ask prices, quantities, orders for top 5 levels)
- **Computed**: `changeper`, `vol_delta`, `expiry_type`, `is_monthly`, `is_weekly`
- **Spot-enriched**: `spot_price`, `distance_from_spot`, `moneyness_pct`, `intrinsic_value`, `time_value`, `mid_price`

### Sort Order (CRITICAL!)

Data is sorted by: **`expiry → opt_type → strike → timestamp`**

This enables:
- Contract-change detection by comparison
- Sequential processing without lookups
- Cache-friendly access patterns

### Example Data Snippet
```python
# 2025-11-18/BANKNIFTY/part-banknifty-0.parquet

| expiry     | opt_type | strike  | timestamp           | price  |
|------------|----------|---------|---------------------|--------|
| 2025-11-25 | CE       | 48000.0 | 2025-11-18 09:15:00 | 1245.5 |
| 2025-11-25 | CE       | 48000.0 | 2025-11-18 09:15:01 | 1246.0 |
| ...        | ...      | ...     | ...                 | ...    |
| 2025-11-25 | CE       | 48500.0 | 2025-11-18 09:15:00 | 1180.0 |  ← Strike changed
| ...        | ...      | ...     | ...                 | ...    |
| 2025-11-25 | PE       | 48000.0 | 2025-11-18 09:15:00 | 35.5   |  ← Opt_type changed
| ...        | ...      | ...     | ...                 | ...    |
| 2025-12-30 | CE       | 48000.0 | 2025-11-18 09:15:00 | 1425.0 |  ← Expiry changed
```

---

## Loading Data Efficiently

### 1. Column Projection (Read Only What You Need)

**Always specify columns** - don't read all 58!

```python
import polars as pl

# BAD: Reads all 58 columns (~10x slower)
df = pl.read_parquet('2025-11-18/BANKNIFTY/part-banknifty-0.parquet')

# GOOD: Read only needed columns
df = pl.read_parquet(
    '2025-11-18/BANKNIFTY/part-banknifty-0.parquet',
    columns=['timestamp', 'strike', 'expiry', 'opt_type', 'price', 'volume', 'bp0', 'sp0']
)
```

**Impact**: 5-10x faster I/O depending on column count

### 2. Date Range Loading

**Load multiple days efficiently:**

```python
from pathlib import Path
import polars as pl

def load_date_range(start_date: str, end_date: str, underlying: str = "BANKNIFTY"):
    """
    Load data for a date range.
    
    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
        underlying: 'BANKNIFTY' or 'NIFTY'
    """
    base_dir = Path("data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    
    # Find all matching files
    pattern = f"*/{ underlying}/part-{underlying.lower()}-0.parquet"
    files = sorted(base_dir.glob(pattern))
    
    # Filter by date range
    start = pl.lit(start_date).str.strptime(pl.Date, "%Y-%m-%d")
    end = pl.lit(end_date).str.strptime(pl.Date, "%Y-%m-%d")
    
    selected_files = []
    for f in files:
        # Extract date from path: data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-11-18/BANKNIFTY/...
        date_str = f.parts[-3]
        file_date = pl.lit(date_str).str.strptime(pl.Date, "%Y-%m-%d")
        
        if start <= file_date <= end:
            selected_files.append(f)
    
    # Read and concatenate
    dfs = [pl.read_parquet(f, columns=[...]) for f in selected_files]
    return pl.concat(dfs)
```

### 3. Lazy Loading with Scan (For Large Datasets)

**For multi-month backtests**, use lazy loading:

```python
# Scan all files lazily (doesn't load into memory)
df = pl.scan_parquet('data/options_date_packed_FULL_v3_SPOT_ENRICHED/**/BANKNIFTY/*.parquet')

# Apply filters (pushed down to file reading)
df = (df
    .filter(pl.col('date').is_between('2025-11-01', '2025-11-30'))
    .filter(pl.col('strike') == 50000)
    .filter(pl.col('opt_type') == 'CE')
)

# Only now read the filtered data
result = df.collect()
```

**Benefit**: Polars only reads files/rowgroups matching the filters

### 4. Row Group Skipping

Files have **row group statistics** (min/max per 100K rows):

```python
# This query skips row groups where max_strike < 55000
df = pl.read_parquet(
    '2025-11-18/BANKNIFTY/part-banknifty-0.parquet',
    columns=['strike', 'price']
).filter(pl.col('strike') >= 55000)
```

**Impact**: Can skip 50-80% of file without reading

---

## Backtesting Patterns

### Pattern 1: Fixed Strike/Expiry (Single Contract)

**Use Case**: Backtest a specific contract (e.g., BANKNIFTY Nov25 50000CE)

```python
import polars as pl
import numpy as np
from numba import njit

# 1. Load specific contract
df = pl.read_parquet(
    '2025-11-18/BANKNIFTY/part-banknifty-0.parquet',
    columns=['timestamp', 'price', 'volume', 'bp0', 'sp0']
).filter(
    (pl.col('strike') == 50000) &
    (pl.col('expiry') == pl.date(2025, 11, 25)) &
    (pl.col('opt_type') == 'CE')
).sort('timestamp')  # Should already be sorted, but just in case

# 2. Calculate indicators
df = df.with_columns([
    pl.col('price').ewm_mean(span=5).alias('ema5'),
    pl.col('price').ewm_mean(span=21).alias('ema21'),
])

# 3. Run strategy
@njit
def run_strategy(prices, ema5, ema21, bid0, ask0, volume):
    # ... (strategy logic)
    return pnl, trades

result = run_strategy(
    df['price'].to_numpy(),
    df['ema5'].to_numpy(),
    df['ema21'].to_numpy(),
    df['bp0'].to_numpy(),
    df['sp0'].to_numpy(),
    df['volume'].to_numpy()
)
```

### Pattern 2: All Contracts for a Day (Screen & Select)

**Use Case**: Screen all contracts, pick best one

```python
def backtest_day_screening(date: str, underlying: str = "BANKNIFTY"):
    """
    Screen all contracts for a day, pick most liquid one.
    """
    df = pl.read_parquet(
        f'data/options_date_packed_FULL_v3_SPOT_ENRICHED/{date}/{underlying}/part-{underlying.lower()}-0.parquet',
        columns=['strike', 'expiry', 'opt_type', 'volume', 'price', 'timestamp']
    )
    
    # 1. Calculate average volume per contract
    contract_volumes = df.group_by(['strike', 'expiry', 'opt_type']).agg([
        pl.col('volume').mean().alias('avg_volume'),
        pl.count().alias('tick_count')
    ])
    
    # 2. Filter: Only liquid contracts (avg volume > 100)
    liquid = contract_volumes.filter(pl.col('avg_volume') > 100)
    
    # 3. Pick highest volume contract
    best_contract = liquid.sort('avg_volume', descending=True).row(0, named=True)
    
    # 4. Backtest that specific contract
    contract_data = df.filter(
        (pl.col('strike') == best_contract['strike']) &
        (pl.col('expiry') == best_contract['expiry']) &
        (pl.col('opt_type') == best_contract['opt_type'])
    )
    
    # Run strategy on contract_data...
    return strategy_result
```

### Pattern 3: Dynamic Strike Selection (ATM/ITM/OTM)

**Use Case**: Trade ATM strike dynamically (requires spot price data)

**Note**: v3 data includes `spot_price`, so ATM/ITM/OTM selection can be done directly from the packed file.

**Example with separate spot file**:

```python
def backtest_atm_strategy(date: str):
    """
    Trade ATM strike, updates every 5 minutes.
    """
    # Load options data
    options = pl.read_parquet(
        f'data/options_date_packed_FULL_v3_SPOT_ENRICHED/{date}/BANKNIFTY/part-banknifty-0.parquet',
        columns=['timestamp', 'strike', 'expiry', 'opt_type', 'price', 'bp0', 'sp0']
    ).filter(
        pl.col('expiry') == pl.date(2025, 11, 25)  # Only Nov expiry
    )
    
    # Load spot data (hypothetical)
    spot = pl.read_parquet(f'spot_data/{date}/BANKNIFTY.parquet')
    
    # Resample spot to 5min candles
    spot_5min = spot.groupby_dynamic('timestamp', every='5m').agg([
        pl.col('price').last().alias('spot_price')
    ])
    
    # For each 5-min interval, find ATM strike
    results = []
    for interval in spot_5min.iter_rows(named=True):
        spot_price = interval['spot_price']
        interval_time = interval['timestamp']
        
        # Find nearest strike (round to nearest 100)
        atm_strike = round(spot_price / 100) * 100
        
        # Get data for this strike during this interval
        contract_data = options.filter(
            (pl.col('strike') == atm_strike) &
            (pl.col('timestamp') >= interval_time) &
            (pl.col('timestamp') < interval_time + timedelta(minutes=5))
        )
        
        # Run strategy on this segment...
        result = run_strategy(contract_data)
        results.append(result)
    
    return aggregate_results(results)
```

### Pattern 4: All Contracts in Parallel (Full Scan)

**Use Case**: Backtest ALL contracts to find best performers

**Implementation**: Use the hyper-optimized approach from session

```python
import polars as pl
import numpy as np
from numba import njit
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

@njit(fastmath=True, nogil=True)
def run_strategy_presorted(strikes, opt_types_int, prices, bid0, ask0, volume):
    """
    Process all contracts in one pass.
    Detects contract changes by comparing strike/opt_type.
    """
    n = len(prices)
    
    # EMA constants
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    
    # Per-contract results (simplified - just totals here)
    total_pnl = 0.0
    total_trades = 0
    
    # State
    pos = 0
    entry = 0.0
    ema5 = prices[0]
    ema21 = prices[0]
    
    for i in range(1, n):
        # Detect contract change
        if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
            # Force exit if in position
            if pos == 1:
                total_pnl += prices[i-1] - entry
                pos = 0
            
            # Reset for new contract
            ema5 = prices[i]
            ema21 = prices[i]
            continue
        
        # Update EMAs
        ema5 = prices[i] * alpha5 + ema5 * (1 - alpha5)
        ema21 = prices[i] * alpha21 + ema21 * (1 - alpha21)
        
        # Strategy logic
        spread_ok = False
        if ask0[i] > 0 and bid0[i] > 0:
            mid = 0.5 * (ask0[i] + bid0[i])
            if mid > 0 and ((ask0[i] - bid0[i]) / mid) <= 0.0005:
                spread_ok = True
        
        if pos == 0:
            if ema5 > ema21 and spread_ok and volume[i] >= 1:
                pos = 1
                entry = prices[i]
                total_trades += 1
        else:
            end_contract = (i == n - 1) or (strikes[i+1] != strikes[i]) or (opt_types_int[i+1] != opt_types_int[i])
            if ema21 >= ema5 or end_contract:
                total_pnl += prices[i] - entry
                pos = 0
    
    return total_pnl, total_trades

def process_file(file_path: Path):
    """Process one day's data."""
    df = pl.read_parquet(file_path, columns=[
        'strike', 'opt_type', 'price', 'bp0', 'sp0', 'volume'
    ])
    
    # Convert to numpy (zero-copy)
    strikes = df['strike'].cast(pl.Float64).to_numpy()
    types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
    prices = df['price'].cast(pl.Float64).fill_null(0).to_numpy()
    bid0 = df['bp0'].cast(pl.Float64).fill_null(0).to_numpy()
    ask0 = df['sp0'].cast(pl.Float64).fill_null(0).to_numpy()
    volume = df['volume'].cast(pl.Float64).fill_null(0).to_numpy()
    
    return run_strategy_presorted(strikes, types_int, prices, bid0, ask0, volume)

# Main execution
files = list(Path('data/options_date_packed_FULL_v3_SPOT_ENRICHED').rglob('BANKNIFTY/*.parquet'))

with ProcessPoolExecutor(max_workers=24) as executor:
    results = list(executor.map(process_file, files))

total_pnl = sum(r[0] for r in results)
total_trades = sum(r[1] for r in results)

print(f"Total PnL: {total_pnl:.2f}")
print(f"Total Trades: {total_trades}")
```

---

## Performance Best Practices

### 1. Leverage Sort Order

**DO**: Process data sequentially, detect contract changes

```python
# Data is sorted by (expiry, opt_type, strike, timestamp)
# Detect new contract by comparison:
if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
    # New contract - reset state
```

**DON'T**: Use groupby (slower, allocates memory)

```python
# SLOW: Creates intermediate tables
for contract in df.group_by(['strike', 'expiry', 'opt_type']):
    # Process contract
```

### 2. Use Inline Calculations

**DO**: Calculate indicators inline in Numba

```python
@njit
def strategy(prices, ...):
    alpha = 2.0 / 6.0
    ema = prices[0]
    
    for i in range(1, len(prices)):
        ema = prices[i] * alpha + ema * (1 - alpha)
        # Use ema immediately
```

**DON'T**: Pre-calculate large arrays

```python
# SLOW: Allocates 475M × 8 bytes = 3.8GB per indicator
ema5 = df['price'].ewm_mean(span=5).to_numpy()
ema21 = df['price'].ewm_mean(span=21).to_numpy()
```

### 3. Physical Categoricals for Enums

**DO**: Convert categorical columns to integers

```python
# Convert CE/PE to 0/1
types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()

# In Numba: integer comparison (fast)
if opt_types_int[i] != opt_types_int[i-1]:
```

**DON'T**: Use string comparisons in hot loops

```python
# SLOW: String comparison
if opt_types[i] != opt_types[i-1]:
```

### 4. Column Projection

**DO**: Read only needed columns

```python
df = pl.read_parquet(file, columns=['timestamp', 'price', 'volume', ...])
```

**DON'T**: Read all 58 columns

```python
df = pl.read_parquet(file)  # Reads everything
```

### 5. Use Numba for Hot Loops

**DO**: Compile strategy logic with Numba

```python
@njit(fastmath=True, nogil=True)
def strategy(...):
    for i in range(n):
        # Compiled to machine code
```

**DON'T**: Use Python loops

```python
for i in range(n):
    # Interpreted (100-1000x slower)
```

### 6. Parallel Processing

**DO**: Use ProcessPoolExecutor (true parallelism)

```python
from concurrent.futures import ProcessPoolExecutor

with ProcessPoolExecutor(max_workers=24) as executor:
    results = list(executor.map(process_file, files))
```

**DON'T**: Use ThreadPoolExecutor (GIL limited)

```python
# SLOWER: Python GIL limits to single-core
with ThreadPoolExecutor(max_workers=24) as executor:
```

---

## Example Backtest Scripts

### Example 1: Simple EMA Crossover (Single Contract)

```python
#!/usr/bin/env python3
"""
Simple backtest: EMA crossover on specific contract.
"""

import polars as pl
import numpy as np
from numba import njit

@njit
def ema_crossover_strategy(prices, span_fast=5, span_slow=21):
    """
    Long when fast EMA > slow EMA, exit when slow EMA >= fast EMA.
    """
    n = len(prices)
    
    # Constants
    alpha_fast = 2.0 / (span_fast + 1)
    alpha_slow = 2.0 / (span_slow + 1)
    
    # Initialize
    ema_fast = prices[0]
    ema_slow = prices[0]
    pos = 0
    entry = 0.0
    pnl = 0.0
    trades = 0
    
    for i in range(1, n):
        price = prices[i]
        
        # Update EMAs
        ema_fast = price * alpha_fast + ema_fast * (1 - alpha_fast)
        ema_slow = price * alpha_slow + ema_slow * (1 - alpha_slow)
        
        # Strategy
        if pos == 0:
            if ema_fast > ema_slow:
                pos = 1
                entry = price
                trades += 1
        else:
            if ema_slow >= ema_fast or i == n - 1:
                pnl += price - entry
                pos = 0
    
    return pnl, trades

# Load data
df = pl.read_parquet(
    'data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-11-18/BANKNIFTY/part-banknifty-0.parquet',
    columns=['timestamp', 'strike', 'expiry', 'opt_type', 'price']
).filter(
    (pl.col('strike') == 50000) &
    (pl.col('expiry') == pl.date(2025, 11, 25)) &
    (pl.col('opt_type') == 'CE')
)

# Run backtest
prices = df['price'].cast(pl.Float64).to_numpy()
pnl, trades = ema_crossover_strategy(prices)

print(f"PnL: {pnl:.2f}")
print(f"Trades: {trades}")
```

### Example 2: Multi-Day Backtest with Filtering

```python
#!/usr/bin/env python3
"""
Multi-day backtest with liquidity filtering.
"""

from pathlib import Path
import polars as pl
import numpy as np
from numba import njit

def backtest_date_range(start_date: str, end_date: str):
    """
    Backtest all days in range, only liquid contracts.
    """
    base_dir = Path('data/options_date_packed_FULL_v3_SPOT_ENRICHED')
    dates = [d.name for d in sorted(base_dir.iterdir()) if d.is_dir()]
    
    # Filter dates
    dates = [d for d in dates if start_date <= d <= end_date]
    
    results = []
    for date in dates:
        file = base_dir / date / 'BANKNIFTY' / 'part-banknifty-0.parquet'
        
        # Load day's data
        df = pl.read_parquet(file, columns=[
            'strike', 'expiry', 'opt_type', 'price', 'volume', 'timestamp'
        ])
        
        # Filter: Only Nov expiry, only liquid strikes (avg volume > 50)
        liquid_contracts = df.group_by(['strike', 'expiry', 'opt_type']).agg([
            pl.col('volume').mean().alias('avg_vol')
        ]).filter(
            (pl.col('expiry') == pl.date(2025, 11, 25)) &
            (pl.col('avg_vol') > 50)
        )
        
        # Process each liquid contract
        for contract in liquid_contracts.iter_rows(named=True):
            contract_data = df.filter(
                (pl.col('strike') == contract['strike']) &
                (pl.col('expiry') == contract['expiry']) &
                (pl.col('opt_type') == contract['opt_type'])
            ).sort('timestamp')
            
            prices = contract_data['price'].cast(pl.Float64).to_numpy()
            pnl, trades = ema_crossover_strategy(prices)  # From Example 1
            
            results.append({
                'date': date,
                'strike': contract['strike'],
                'expiry': str(contract['expiry']),
                'opt_type': contract['opt_type'],
                'pnl': pnl,
                'trades': trades
            })
    
    return pl.DataFrame(results)

# Run
results = backtest_date_range('2025-11-01', '2025-11-30')
print(results.sort('pnl', descending=True).head(10))
```

### Example 3: Intraday Time-of-Day Filter

```python
"""
Example: Only trade between 10 AM - 2 PM.
"""

import polars as pl

# Load data
df = pl.read_parquet(
    'data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-11-18/BANKNIFTY/part-banknifty-0.parquet',
    columns=['timestamp', 'strike', 'expiry', 'opt_type', 'price']
).filter(
    (pl.col('strike') == 50000) &
    (pl.col('expiry') == pl.date(2025, 11, 25)) &
    (pl.col('opt_type') == 'CE')
)

# Filter by time of day
df = df.filter(
    (pl.col('timestamp').dt.hour() >= 10) &
    (pl.col('timestamp').dt.hour() < 14)
)

# Run strategy on filtered data
prices = df['price'].to_numpy()
pnl, trades = ema_crossover_strategy(prices)
```

---

## Common Pitfalls

### Pitfall 1: Not Accounting for Contract Changes

**Problem**:
```python
# BAD: Processes multiple contracts as if they're one
df = pl.read_parquet(file)
prices = df['price'].to_numpy()
pnl, trades = strategy(prices)  # ← Treats different strikes as same contract!
```

**Solution**:
```python
# GOOD: Filter to single contract OR detect changes
df = df.filter(
    (pl.col('strike') == 50000) &
    (pl.col('expiry') == pl.date(2025, 11, 25)) &
    (pl.col('opt_type') == 'CE')
)
```

### Pitfall 2: Ignoring Sort Order

**Problem**:
```python
# BAD: Assumes random access
for contract in unique_contracts:
    contract_data = df.filter(...)  # ← Scans entire file 538 times!
```

**Solution**:
```python
# GOOD: Process sequentially leveraging sort order
for i in range(len(df)):
    if contract_changed(i):
        reset_state()
    process_row(i)
```

### Pitfall 3: Reading All Columns

**Problem**:
```python
# BAD: Reads all 58 columns
df = pl.read_parquet(file)
```

**Solution**:
```python
# GOOD: Read only what you need
df = pl.read_parquet(file, columns=['timestamp', 'price', 'volume'])
```

### Pitfall 4: String Comparisons in Hot Loops

**Problem**:
```python
# BAD: String comparison (slow)
if df['opt_type'][i] != df['opt_type'][i-1]:
```

**Solution**:
```python
# GOOD: Integer comparison (10x faster)
types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
if types_int[i] != types_int[i-1]:
```

### Pitfall 5: Not Using Numba

**Problem**:
```python
# BAD: Python loop (100-1000x slower)
pnl = 0
for i in range(len(prices)):
    # Strategy logic
```

**Solution**:
```python
# GOOD: Compiled with Numba
@njit(fastmath=True, nogil=True)
def strategy(prices):
    pnl = 0
    for i in range(len(prices)):
        # Same logic, but compiled!
```

### Pitfall 6: Creating Large Intermediate Arrays

**Problem**:
```python
# BAD: Allocates 3.8GB per indicator
ema5 = df['price'].ewm_mean(span=5).to_numpy()
ema21 = df['price'].ewm_mean(span=21).to_numpy()
```

**Solution**:
```python
# GOOD: Calculate inline
@njit
def strategy(prices):
    ema5 = prices[0]
    ema21 = prices[0]
    
    for i in range(1, len(prices)):
        ema5 = prices[i] * alpha5 + ema5 * (1 - alpha5)
        # Use ema5 immediately
```

---

## Performance Targets

Based on session results:

| Operation | Expected Speed | Notes |
|-----------|---------------|-------|
| **File Reading** (column projection) | 50-100M rows/s | 6 columns vs 58 |
| **Numba Strategy** (presorted) | 150-200M rows/s | Zero-copy, inline EMA |
| **Full Backtest** (multi-file) | 150-180M rows/s | Limited by disk I/O |

**Bottleneck**: Always disk I/O first, then memory bandwidth, then CPU.

---

## Quick Reference

### Loading Data
```python
# Single file
df = pl.read_parquet(file, columns=[...])

# Date range
df = pl.concat([pl.read_parquet(f, columns=[...]) for f in files])

# Lazy scan (large datasets)
df = pl.scan_parquet('path/**/BANKNIFTY/*.parquet').filter(...).collect()
```

### Filtering Contracts
```python
# Single contract
df.filter(
    (pl.col('strike') == 50000) &
    (pl.col('expiry') == pl.date(2025, 11, 25)) &
    (pl.col('opt_type') == 'CE')
)

# Multiple strikes
df.filter(pl.col('strike').is_in([50000, 50500, 51000]))

# Expiry range
df.filter(pl.col('expiry').is_between('2025-11-01', '2025-11-30'))
```

### Numba Template
```python
@njit(fastmath=True, nogil=True)
def strategy(prices, ...):
    # Initialize
    pos = 0
    pnl = 0.0
    
    # Loop
    for i in range(1, len(prices)):
        # Strategy logic
        pass
    
    return pnl, trades
```

### Parallel Processing
```python
from concurrent.futures import ProcessPoolExecutor

with ProcessPoolExecutor(max_workers=24) as executor:
    results = list(executor.map(process_file, files))
```

---

## Summary

**Key Principles for Fast Backtesting**:

1. **Read only what you need** (column projection)
2. **Leverage sort order** (sequential processing, detect changes)
3. **Use Numba for hot loops** (100-1000x speedup)
4. **Calculate inline** (avoid large array allocations)
5. **Physical categoricals** (integer comparisons)
6. **Parallel processing** (ProcessPoolExecutor, not ThreadPoolExecutor)

**Expected Performance**: 150-200M rows/sec on properly sorted data

**Data Format**: Pre-sorted by `expiry → opt_type → strike → timestamp`, enabling efficient contract-change detection and cache-friendly access.
