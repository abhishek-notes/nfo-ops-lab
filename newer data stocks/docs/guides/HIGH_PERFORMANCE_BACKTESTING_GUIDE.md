# HIGH-PERFORMANCE BACKTESTING GUIDE
## Technical Manual for Date-Partitioned Options Data

**For**: Quantitative Developers & Strategy Engineers  
**Performance Target**: 150-200M rows/sec  
**Data Structure**: Date-partitioned Parquet (sorted)  
**Last Updated**: December 2025

---

## Table of Contents
1. [The Golden Rule](#the-golden-rule)
2. [The Numba State Machine Pattern](#the-numba-state-machine-pattern)
3. [How to Handle Mixed Expiries](#how-to-handle-mixed-expiries)
4. [Step-by-Step: Adding a New Strategy](#step-by-step-adding-a-new-strategy)
5. [Performance Optimization Checklist](#performance-optimization-checklist)
6. [Advanced Patterns](#advanced-patterns)
7. [Troubleshooting & Debugging](#troubleshooting--debugging)

---

## The Golden Rule

### Rule #1: NEVER Sort in Your Strategy Script

**WHY**: The data is already sorted on disk by `['expiry', 'opt_type', 'strike', 'timestamp']`.

**BAD** ❌:
```python
# This adds 140 seconds of wasted time for 475M rows!
df = pl.read_parquet(file)
df = df.sort(['expiry', 'opt_type', 'strike', 'timestamp'])  # ← UNNECESSARY!
```

**GOOD** ✓:
```python
# Data is already sorted, just read it!
df = pl.read_parquet(file, columns=['strike', 'opt_type', 'price', ...])
# Use directly - NO SORTING NEEDED
```

**Performance Impact**:
```
With unnecessary sort:    3.0M rows/s  (157 seconds)
Without sorting:          161.6M rows/s (2.9 seconds)
Speedup:                  53.9x just from removing one line!
```

**How to Verify Sort Order**:
```python
df = pl.read_parquet('data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-11-18/BANKNIFTY/part-banknifty-0.parquet')
print(f"Sorted by expiry: {df['expiry'].is_sorted()}")  # Should be True
```

If `is_sorted()` returns `False`, your data needs to be re-sorted using `resort_packed_data.py`.

### Rule #2: NEVER Filter Inside a Loop

**WHY**: Each `.filter()` scans the entire DataFrame. With 538 contracts per file, that's 538 full scans!

**BAD** ❌:
```python
df = pl.read_parquet(file)  # 5.7M rows

# Get unique contracts
contracts = df.select(['strike', 'expiry', 'opt_type']).unique()  # 538 contracts

for contract in contracts.iter_rows():  # ← 538 iterations
    # This scans all 5.7M rows EVERY TIME!
    contract_df = df.filter(
        (pl.col('strike') == contract['strike']) &
        (pl.col('expiry') == contract['expiry']) &
        (pl.col('opt_type') == contract['opt_type'])
    )  # ← Total: 3.07 BILLION row comparisons!
    
    process(contract_df)
```

**GOOD** ✓:
```python
df = pl.read_parquet(file, columns=[...])

# Extract arrays
strikes = df['strike'].to_numpy()
opt_types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
prices = df['price'].to_numpy()

# Single-pass processing
for i in range(1, len(strikes)):
    # Detect contract change by comparison (O(1) operation)
    if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
        # New contract - reset state
        reset_for_new_contract()
```

**Performance Impact**:
```
With 538 filters:    2.5M rows/s  (190 seconds)
Single-pass:         161.6M rows/s (2.9 seconds)
Speedup:             64.6x
```

### Rule #3: Calculate Indicators Inline

**WHY**: Pre-calculating indicators creates massive arrays that saturate memory bandwidth.

**BAD** ❌:
```python
# Calculate EMAs for entire file (475M rows × 2 indicators)
df = df.with_columns([
    pl.col('price').ewm_mean(span=5).alias('ema5'),   # ← 3.8GB array
    pl.col('price').ewm_mean(span=21).alias('ema21')  # ← 3.8GB array
])

# Convert to numpy
ema5_array = df['ema5'].to_numpy()
ema21_array = df['ema21'].to_numpy()

# Memory bandwidth: Reading 7.6GB from RAM = bottleneck!
```

**GOOD** ✓:
```python
@njit
def strategy(prices, ...):
    # EMA constants (calculated once)
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    
    # Initialize EMAs
    ema5 = prices[0]
    ema21 = prices[0]
    
    for i in range(1, len(prices)):
        # Calculate EMAs on-the-fly in CPU register (0 bytes allocated!)
        ema5 = prices[i] * alpha5 + ema5 * (1 - alpha5)
        ema21 = prices[i] * alpha21 + ema21 * (1 - alpha21)
        
        # Use immediately
        if ema5 > ema21:
            # Entry logic
```

**Performance Impact**:
```
Memory Allocated:
  Pre-calculated: 7.6GB (475M rows × 2 indicators × 8 bytes)
  Inline:         0 bytes (calculated in register)

Memory Bandwidth:
  Pre-calculated: 193 MB/s (memory bus saturated)
  Inline:         7.9 GB/s (disk I/O limited)

Speedup:          ~40x from eliminating memory bandwidth bottleneck
```

---

## The Numba State Machine Pattern

### Core Concept: Contract-Change Detection

**Key Insight**: Because data is sorted by `['expiry', 'opt_type', 'strike', 'timestamp']`, contracts appear in contiguous blocks:

```
Row | Expiry     | Opt_Type | Strike  | Timestamp
----|------------|----------|---------|-------------------
0   | 2025-11-25 | CE       | 48000.0 | 2025-11-18 09:15:00
1   | 2025-11-25 | CE       | 48000.0 | 2025-11-18 09:15:01  ← Same contract
2   | 2025-11-25 | CE       | 48000.0 | 2025-11-18 09:15:02  ← Same contract
3   | 2025-11-25 | CE       | 48000.0 | 2025-11-18 09:15:03  ← Same contract
4   | 2025-11-25 | CE       | 48500.0 | 2025-11-18 09:15:00  ← NEW CONTRACT (strike changed!)
5   | 2025-11-25 | CE       | 48500.0 | 2025-11-18 09:15:01  ← Same contract
...
100 | 2025-11-25 | PE       | 48000.0 | 2025-11-18 09:15:00  ← NEW CONTRACT (opt_type changed!)
```

**Detection Logic**:
```python
if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
    # New contract started!
```

### Complete State Machine Template

**From `strategy_benchmark_PRESORTED.py` (Lines 26-94)**:

```python
@njit(fastmath=True, nogil=True)
def run_strategy_sorted(strikes, opt_types_int, prices, bid0, ask0, volume):
    """
    State machine for pre-sorted data.
    
    Args:
        strikes: np.array of strike prices (Float64)
        opt_types_int: np.array of option types as integers (CE=0, PE=1)
        prices: np.array of last traded prices
        bid0: np.array of best bid prices
        ask0: np.array of best ask prices
        volume: np.array of cumulative volumes
    
    Returns:
        total_pnl: Float (sum of all profits/losses)
        total_trades: Int (number of round-trip trades)
    """
    n = len(prices)
    if n < 2:
        return 0.0, 0
    
    # ===== STEP 1: INITIALIZE CONSTANTS =====
    # EMA constants (calculated once, not per-row)
    alpha5 = 2.0 / 6.0    # For EMA(5):  α = 2 / (span + 1)
    alpha21 = 2.0 / 22.0  # For EMA(21): α = 2 / (span + 1)
    
    # ===== STEP 2: INITIALIZE STATE VARIABLES =====
    # These get reset whenever contract changes
    pos = 0           # Position: 0=flat, 1=long
    entry_price = 0.0 # Entry price for current position
    ema5 = prices[0]  # Fast EMA (initialized with first price)
    ema21 = prices[0] # Slow EMA (initialized with first price)
    
    # Aggregators (never reset)
    total_pnl = 0.0
    total_trades = 0
    
    # ===== STEP 3: MAIN LOOP (starts at index 1) =====
    for i in range(1, n):
        price = prices[i]
        
        # ===== STEP 4: CONTRACT CHANGE DETECTION =====
        # Compare current row with previous row
        contract_changed = (strikes[i] != strikes[i-1]) or \
                          (opt_types_int[i] != opt_types_int[i-1])
        
        if contract_changed:
            # Force exit if in position
            if pos == 1:
                # Use previous row's price (last price of old contract)
                total_pnl += prices[i-1] - entry_price
                pos = 0
            
            # Reset state for new contract
            ema5 = price
            ema21 = price
            entry_price = 0.0
            # pos already 0 from exit above
            
            continue  # Skip to next iteration (first row of new contract)
        
        # ===== STEP 5: UPDATE INDICATORS (Inline) =====
        # Exponential Moving Average formula:
        # EMA_new = price × α + EMA_old × (1 - α)
        ema5 = price * alpha5 + ema5 * (1 - alpha5)
        ema21 = price * alpha21 + ema21 * (1 - alpha21)
        
        # ===== STEP 6: CHECK ENTRY CONDITIONS =====
        # Spread condition: Bid-ask spread ≤ 5 basis points
        spread_ok = False
        if ask0[i] > 0.0 and bid0[i] > 0.0:
            mid = 0.5 * (ask0[i] + bid0[i])
            if mid > 0.0:
                spread_bps = (ask0[i] - bid0[i]) / mid
                if spread_bps <= 0.0005:  # 0.0005 = 0.05% = 5 bps
                    spread_ok = True
        
        # Volume condition
        vol_ok = volume[i] >= 1.0
        
        # ===== STEP 7: STATE MACHINE LOGIC =====
        if pos == 0:  # Currently flat
            # ENTRY CONDITION: EMA5 > EMA21 AND spread OK AND volume OK
            if (ema5 > ema21) and spread_ok and vol_ok:
                pos = 1
                entry_price = price
                total_trades += 1
        
        else:  # pos == 1, currently long
            # ===== STEP 8: EXIT CONDITIONS ======
            # Check if next row is a different contract (lookahead)
            end_of_contract = (i == n - 1) or \
                            (strikes[i+1] != strikes[i]) or \
                            (opt_types_int[i+1] != opt_types_int[i])
            
            # EXIT CONDITION: EMA21 >= EMA5 OR end of contract
            if (ema21 >= ema5) or end_of_contract:
                total_pnl += price - entry_price
                pos = 0
    
    return total_pnl, total_trades
```

### Key Pattern Elements Explained

#### 1. Physical Categorical Integers

**Why**: String comparisons are slow in Numba.

**Conversion**:
```python
# Before passing to Numba
types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()

# Result: CE → 0, PE → 1 (arbitrary but consistent)
```

**In Numba**:
```python
# Integer comparison (10x faster than string)
if opt_types_int[i] != opt_types_int[i-1]:
    # Type changed (CE → PE or PE → CE)
```

#### 2. Contract Change Detection with Lookahead

**Why**: Need to exit positions at end of contract.

**Pattern**:
```python
# Check if NEXT row is different contract
end_of_contract = (i == n - 1) or \  # Last row of file
                 (strikes[i+1] != strikes[i]) or \  # Strike changes
                 (opt_types_int[i+1] != opt_types_int[i])  # Type changes

if end_of_contract:
    # Force exit before contract changes
    if pos == 1:
        total_pnl += prices[i] - entry_price
        pos = 0
```

**Alternative** (without lookahead):
```python
# Detect change on next iteration
if contract_changed:
    # Exit using PREVIOUS row's price
    if pos == 1:
        total_pnl += prices[i-1] - entry_price
        pos = 0
    reset_state()
    continue
```

#### 3. Inline Indicator Calculation

**EMA Formula**:
```python
# Exponential Moving Average (EMA)
# α = 2 / (span + 1)
# EMA_new = price × α + EMA_old × (1 - α)

alpha = 2.0 / (span + 1)
ema = price * alpha + ema * (1 - alpha)
```

**For Multiple EMAs**:
```python
# Calculate all constants once
alpha5 = 2.0 / 6.0
alpha21 = 2.0 / 22.0
alpha50 = 2.0 / 51.0

# Initialize
ema5 = ema21 = ema50 = prices[0]

# Update in loop
ema5 = price * alpha5 + ema5 * (1 - alpha5)
ema21 = price * alpha21 + ema21 * (1 - alpha21)
ema50 = price * alpha50 + ema50 * (1 - alpha50)
```

**For RSI** (Relative Strength Index):
```python
# Initialize
avg_gain = 0.0
avg_loss = 0.0
periods = 14
alpha_rsi = 1.0 / periods

# In loop
change = prices[i] - prices[i-1]
gain = max(change, 0.0)
loss = abs(min(change, 0.0))

avg_gain = gain * alpha_rsi + avg_gain * (1 - alpha_rsi)
avg_loss = loss * alpha_rsi + avg_loss * (1 - alpha_rsi)

rsi = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss)) if avg_loss > 0 else 100.0
```

---

## How to Handle Mixed Expiries

### Understanding the Data Structure

**Key Fact**: Each file contains **ALL expiries** traded that day.

**Example File** (`2025-11-18/BANKNIFTY/part-banknifty-0.parquet`):
```python
import polars as pl

df = pl.read_parquet('2025-11-18/BANKNIFTY/part-banknifty-0.parquet')
print(df.select(['expiry']).unique().sort('expiry'))

# Output:
┌────────────┐
│ expiry     │
├────────────┤
│ 2025-11-25 │  ← Nov monthly expiry
│ 2025-12-30 │  ← Dec monthly expiry
│ 2026-01-27 │  ← Jan monthly expiry
└────────────┘
```

### Sort Order Guarantees

**Because data is sorted by `['expiry', 'opt_type', 'strike', 'timestamp']`**:

1. **All Nov contracts appear first**, then Dec, then Jan
2. **Within each expiry**, all CE contracts, then all PE contracts
3. **Within each CE/PE**, strikes are sorted ascending
4. **Within each strike**, timestamps are chronological

**Visual Representation**:
```
Rows 1-200000:    Nov 2025, CE, strikes 48000-65000
Rows 200001-400000: Nov 2025, PE, strikes 48000-65000
Rows 400001-450000: Dec 2025, CE, strikes 48000-65000
Rows 450001-500000: Dec 2025, PE, strikes 48000-65000
```

### Automatic Expiry Separation

**You don't need to filter by expiry!** The sort order handles it automatically.

**Example**: Your strategy will naturally:
1. Process all Nov CE contracts
2. Process all Nov PE contracts
3. **Automatically reset** when expiry changes (Nov→Dec)
4. Process all Dec CE contracts
5. Process all Dec PE contracts

**Contract Change Detection** catches expiry changes implicitly:
```python
@njit
def strategy(strikes, opt_types_int, expiries_int, prices, ...):
    for i in range(1, n):
        # This detects ANY contract change (strike, type, OR expiry)
        if (strikes[i] != strikes[i-1]) or \
           (opt_types_int[i] != opt_types_int[i-1]) or \
           (expiries_int[i] != expiries_int[i-1]):  # ← Expiry check optional
            reset_state()
```

**Pro Tip**: You don't even need to include expiry in the check! Because data is sorted by expiry→opt_type→strike, if expiry changes, opt_type or strike will also change (it will reset to first strike of next expiry).

### Filtering for Specific Expiry (If Needed)

**When**: You only want to trade Nov expiry contracts.

**Option 1** (Filter in Polars - before Numba):
```python
# Read file
df = pl.read_parquet(file, columns=[...])

# Filter to Nov expiry only
df = df.filter(pl.col('expiry') == pl.date(2025, 11, 25))

# Process normally
strikes = df['strike'].to_numpy()
...
```

**Option 2** (Skip in Numba - during processing):
```python
@njit
def strategy(strikes, opt_types_int, expiries_int, prices, ...):
    target_expiry = expiries_int[0]  # Assume first row is target expiry
    
    for i in range(1, n):
        # Skip if not target expiry
        if expiries_int[i] != target_expiry:
            continue  # Skip to next row
        
        # Process only target expiry
        ...
```

**Recommendation**: Use Option 1 (filter in Polars) if you're excluding most data. Use Option 2 (skip in Numba) if you're conditionally processing some rows.

---

## Step-by-Step: Adding a New Strategy

### Step 1: Copy the Template

```bash
cp strategy_benchmark_PRESORTED.py my_new_strategy.py
```

### Step 2: Define Your Strategy Logic

**Identify**:
1. **Entry conditions**: When to open a position
2. **Exit conditions**: When to close a position
3. **Indicators needed**: EMAs, RSI, MACD, etc.
4. **Additional data**: Volume, spread, OI, etc.

**Example Strategy**: "Bollinger Band Mean Reversion"

**Entry**: Price touches lower band AND volume > average  
**Exit**: Price touches middle band OR upper band

### Step 3: Modify the run_strategy_sorted Function

**Original EMA Crossover**:
```python
@njit(fastmath=True, nogil=True)
def run_strategy_sorted(strikes, opt_types_int, prices, bid0, ask0, volume):
    # EMA crossover logic
    alpha5 = 2.0 / 6.0
    alpha21 = 2.0 / 22.0
    ema5 = ema21 = prices[0]
    
    for i in range(1, n):
        if contract_changed(i):
            reset_state()
            continue
        
        ema5 = price * alpha5 + ema5 * (1 - alpha5)
        ema21 = price * alpha21 + ema21 * (1 - alpha21)
        
        if ema5 > ema21:
            enter_long()
        elif ema21 >= ema5:
            exit_long()
```

**Modified for Bollinger Bands**:
```python
@njit(fastmath=True, nogil=True)
def run_strategy_sorted(strikes, opt_types_int, prices, volume):
    n = len(prices)
    
    # Bollinger Band parameters
    period = 20
    num_std = 2.0
    
    # State for rolling calculations
    price_buffer = np.zeros(period)  # Circular buffer for last 20 prices
    buffer_idx = 0
    buffer_filled = False
    
    # Strategy state
    pos = 0
    entry_price = 0.0
    total_pnl = 0.0
    total_trades = 0
    
    # Volume moving average state
    alpha_vol = 2.0 / 21.0
    avg_volume = volume[0]
    
    for i in range(1, n):
        price = prices[i]
        
        # Contract change detection
        if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
            if pos == 1:
                total_pnl += prices[i-1] - entry_price
                pos = 0
            
            # Reset Bollinger Band state
            price_buffer[:] = 0.0
            buffer_idx = 0
            buffer_filled = False
            avg_volume = volume[i]
            continue
        
        # Update price buffer (circular buffer)
        price_buffer[buffer_idx] = price
        buffer_idx = (buffer_idx + 1) % period
        if buffer_idx == 0:
            buffer_filled = True
        
        # Calculate Bollinger Bands (only if buffer is full)
        if buffer_filled:
            # Mean
            mean = np.mean(price_buffer)
            
            # Standard deviation
            std = np.std(price_buffer)
            
            # Bands
            upper_band = mean + num_std * std
            lower_band = mean - num_std * std
            middle_band = mean
            
            # Update average volume
            avg_volume = volume[i] * alpha_vol + avg_volume * (1 - alpha_vol)
            
            # Strategy logic
            if pos == 0:
                # ENTRY: Price touches lower band AND volume > average
                if price <= lower_band and volume[i] > avg_volume:
                    pos = 1
                    entry_price = price
                    total_trades += 1
            else:
                # EXIT: Price touches middle or upper band
                if price >= middle_band:
                    total_pnl += price - entry_price
                    pos = 0
    
    return total_pnl, total_trades
```

### Step 4: Update Column Loading

**Identify needed columns** from your strategy:

```python
def process_file_presorted(file_path):
    # Read only what you need
    df = pl.read_parquet(file_path, columns=[
        'strike',      # For contract change detection
        'opt_type',    # For contract change detection
        'price',       # For Bollinger Bands
        'volume'       # For volume condition
        # bid0, ask0 not needed for this strategy
    ])
    
    # Convert to numpy
    strikes = df['strike'].to_numpy()
    types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
    prices = df['price'].to_numpy()
    volume = df['volume'].cast(pl.Float64).fill_null(0).to_numpy()
    
    # Call strategy
    return run_strategy_sorted(strikes, types_int, prices, volume)
```

### Step 5: Test on Small Sample

**Run on one day first**:
```bash
# Create test directory with single file
mkdir -p test_data/2025-11-18/BANKNIFTY
cp data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-11-18/BANKNIFTY/*.parquet test_data/2025-11-18/BANKNIFTY/

# Run strategy
python my_new_strategy.py --data-dir test_data --workers 1
```

**Verify output**:
```
Total rows:          5,697,943
Total trades:        342
Total PnL:           125.50
Elapsed:             0.5 s
Throughput:          11,395,886 rows/s
```

**Look for**:
- Reasonable number of trades (not 0, not millions)
- PnL makes sense (not NaN, not infinity)
- No errors/warnings

### Step 6: Run Full Backtest

```bash
python my_new_strategy.py --workers 24
```

---

## Performance Optimization Checklist

### Data Loading Optimizations

#### ✓ Column Projection
```python
# Read only needed columns (5-10x faster I/O)
df = pl.read_parquet(file, columns=['strike', 'opt_type', 'price'])
```

#### ✓ Physical Categoricals
```python
# Convert categorical strings to integers (10x faster comparisons)
types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
```

#### ✓ Fill Nulls Before Conversion
```python
# Handle missing data gracefully (avoids NaN propagation)
prices = df['price'].cast(pl.Float64).fill_null(0.0).to_numpy()
```

### Numba Optimizations

#### ✓ Use @njit Decorator
```python
# JIT compile to machine code (100-1000x speedup)
@njit(fastmath=True, nogil=True)
def strategy(...):
    pass
```

**Options**:
- `fastmath=True`: Aggressive floating-point optimizations (may break IEEE 754)
- `nogil=True`: Release Python GIL for true parallelism
- `cache=True`: Cache compiled code between runs (faster startup)

#### ✓ Inline Calculations
```python
# Calculate indicators on-the-fly (zero memory allocation)
ema = price * alpha + ema * (1 - alpha)
```

#### ✓ Avoid Python Objects
```python
# BAD: Python list inside Numba
results = []  # ← Slow!

# GOOD: NumPy array or scalar accumulation
total_pnl = 0.0  # ← Fast!
```

#### ✓ Use NumPy Functions
```python
# Numba supports most NumPy functions
mean = np.mean(price_buffer)
std = np.std(price_buffer)
```

### Parallel Processing Optimizations

#### ✓ ProcessPoolExecutor (Not ThreadPool)
```python
# Use processes for CPU-bound tasks
from concurrent.futures import ProcessPoolExecutor

with ProcessPoolExecutor(max_workers=24) as executor:
    results = list(executor.map(process_file, files))
```

#### ✓ Optimal Chunk Size
```python
# Balance overhead vs parallelism
chunk_size = 10  # Files per worker task

# Too small: Process startup overhead dominates
# Too large: Poor work distribution (some workers idle)
```

#### ✓ Monitor Progress
```python
# Use as_completed for real-time feedback
for fut in as_completed(futures):
    result = fut.result()
    print(f"Processed {rows:,} rows at {rate:.1f}M rows/s")
```

### Memory Optimizations

#### ✓ Minimize Array Allocations
```python
# Every allocation = memory bandwidth cost
# Pre-calculate only if reused multiple times

# BAD: Allocate array for single use
ema_array = calculate_ema_array(prices)  # 3.8GB
if ema_array[i] > threshold:  # Used once

# GOOD: Calculate on demand
ema = calculate_ema_scalar(price)  # 8 bytes in register
if ema > threshold:
```

#### ✓ Reuse Buffers
```python
# For indicators needing history (e.g., Bollinger Bands)
price_buffer = np.zeros(20)  # Allocated once

for i in range(n):
    price_buffer[i % 20] = prices[i]  # Circular buffer (no reallocation)
```

### Disk I/O Optimizations

#### ✓ Sequential File Access
```python
# Access files in sorted order (better SSD performance)
files = sorted(data_dir.rglob("*.parquet"))
```

#### ✓ Batch File Reading
```python
# Read multiple small files at once (amortize overhead)
dfs = [pl.read_parquet(f) for f in chunk_of_files]
combined = pl.concat(dfs)
```

---

## Advanced Patterns

### Pattern 1: Dynamic Strike Selection (ATM Trading)

**Challenge**: Trade at-the-money strike, which changes based on spot price.

**Solution**: Load spot data separately, join or filter.

```python
def backtest_atm_strategy(date: str):
    # Load options data
    options = pl.read_parquet(f'data/options_date_packed_FULL_v3_SPOT_ENRICHED/{date}/BANKNIFTY/*.parquet')
    
    # Load spot data (hypothetical - you need to create this)
    spot = pl.read_parquet(f'spot_data/{date}.parquet')
    
    # Resample spot to 5-minute intervals
    spot_5min = spot.groupby_dynamic('timestamp', every='5m').agg([
        pl.col('price').last().alias('spot_price')
    ])
    
    # For each 5-min interval
    for interval in spot_5min.iter_rows(named=True):
        spot_price = interval['spot_price']
        timestamp = interval['timestamp']
        
        # Find ATM strike (nearest 100)
        atm_strike = round(spot_price / 100) * 100
        
        # Filter options data for this strike and time window
        df = options.filter(
            (pl.col('strike') == atm_strike) &
            (pl.col('timestamp') >= timestamp) &
            (pl.col('timestamp') < timestamp + timedelta(minutes=5))
        )
        
        # Run strategy on this segment
        result = run_strategy(df)
```

### Pattern 2: Multi-Leg Strategies (Spreads)

**Example**: Bull Call Spread (Buy ATM call, sell OTM call)

```python
@njit
def bull_call_spread(strikes, opt_types_int, prices_atm, prices_otm):
    """
    Simultaneously trade two strikes:
    - Long ATM call
    - Short OTM call (+100 strike)
    """
    n = len(strikes)
    
    # State for both legs
    pos_atm = 0
    pos_otm = 0
    entry_atm = 0.0
    entry_otm = 0.0
    total_pnl = 0.0
    
    for i in range(1, n):
        # Entry: Buy ATM, Sell OTM simultaneously
        if pos_atm == 0 and entry_condition:
            pos_atm = 1
            pos_otm = -1  # Short position
            entry_atm = prices_atm[i]
            entry_otm = prices_otm[i]
        
        # Exit: Close both legs
        elif exit_condition:
            pnl_atm = prices_atm[i] - entry_atm  # Long PnL
            pnl_otm = entry_otm - prices_otm[i]  # Short PnL (reversed)
            total_pnl += pnl_atm + pnl_otm
            
            pos_atm = 0
            pos_otm = 0
    
    return total_pnl
```

**Data Preparation**:
```python
# Filter for two strikes
df_atm = df.filter(pl.col('strike') == atm_strike)
df_otm = df.filter(pl.col('strike') == atm_strike + 100)

# Align timestamps (join or interpolate)
df_aligned = df_atm.join(df_otm, on='timestamp', suffix='_otm')

# Extract arrays
prices_atm = df_aligned['price'].to_numpy()
prices_otm = df_aligned['price_otm'].to_numpy()
```

### Pattern 3: Time-of-Day Filters

**Example**: Only trade between 10 AM - 2 PM.

**Option 1** (Filter in Polars):
```python
df = pl.read_parquet(file).filter(
    (pl.col('timestamp').dt.hour() >= 10) &
    (pl.col('timestamp').dt.hour() < 14)
)
```

**Option 2** (Skip in Numba):
```python
@njit
def strategy(strikes, timestamps_hour, ...):
    for i in range(1, n):
        # Skip if outside trading hours
        if timestamps_hour[i] < 10 or timestamps_hour[i] >= 14:
            continue
        
        # Process only 10 AM - 2 PM
        ...
```

### Pattern 4: Rolling Window Indicators

**Challenge**: Calculate indicator over last N rows (e.g., 20-period Bollinger Bands).

**Circular Buffer Pattern**:
```python
@njit
def strategy_with_rolling_window(prices, ...):
    period = 20
    buffer = np.zeros(period)  # Circular buffer
    idx = 0
    filled = False
    
    for i in range(1, len(prices)):
        # Add to buffer
        buffer[idx] = prices[i]
        idx = (idx + 1) % period
        
        if idx == 0:
            filled = True  # Buffer full after first cycle
        
        # Only calculate after buffer is full
        if filled:
            mean = np.mean(buffer)
            std = np.std(buffer)
            # Use mean, std...
```

---

## Troubleshooting & Debugging

### Issue 1: "Data is not sorted" Error

**Symptom**:
```python
df = pl.read_parquet(file)
print(df['expiry'].is_sorted())  # Returns False
```

**Solution**:
```bash
# Run resort script
python resort_packed_data.py
```

**Verification**:
```python
df = pl.read_parquet(file)
print(df['expiry'].is_sorted())  # Should return True now
```

### Issue 2: NaN or Inf in Results

**Symptom**:
```
Total PnL: nan
Total trades: 0
```

**Causes & Solutions**:

1. **Division by zero**:
```python
# BAD
spread = (ask - bid) / mid

# GOOD - check for zero
if mid > 0:
    spread = (ask - bid) / mid
else:
    spread = 0.0
```

2. **Null values**:
```python
# BAD - nulls become NaN
prices = df['price'].to_numpy()

# GOOD - fill nulls first
prices = df['price'].fill_null(0.0).to_numpy()
```

### Issue 3: Very Few Trades

**Symptom**:
```
Total trades: 5  (expected thousands)
```

**Debug**:
```python
# Add debug counters in Numba
@njit
def strategy(...):
    entry_attempts = 0
    spread_fails = 0
    volume_fails = 0
    ema_fails = 0
    
    for i in range(1, n):
        if ema5 > ema21:
            entry_attempts += 1
            if not spread_ok:
                spread_fails += 1
            elif not vol_ok:
                volume_fails += 1
            else:
                # Entry succeeded
                ...
    
    print(f"Entry attempts: {entry_attempts}")
    print(f"Failed due to spread: {spread_fails}")
    print(f"Failed due to volume: {volume_fails}")
```

### Issue 4: Slow Performance

**Symptom**:
```
Throughput: 5M rows/s  (expected 150M+)
```

**Checklist**:
1. Remove `.sort()` if data is pre-sorted
2. Check for `.filter()` inside loops
3. Verify Numba compilation (should see "Compiling..." on first run)
4. Check column projection (read only needed columns)
5. Verify `@njit` decorator is present
6. Check for Python operations inside Numba (use NumPy instead)

**Profiling**:
```python
import time

t0 = time.perf_counter()
df =pl.read_parquet(file)
print(f"Read: {time.perf_counter() - t0:.3f}s")

t0 = time.perf_counter()
types_int = df['opt_type'].cast(pl.Categorical).to_physical().to_numpy()
print(f"Convert: {time.perf_counter() - t0:.3f}s")

t0 = time.perf_counter()
pnl, trades = run_strategy(...)
print(f"Strategy: {time.perf_counter() - t0:.3f}s")
```

---

## Quick Reference

### Numba-Compatible NumPy Functions

**Statistical**:
- `np.mean()`, `np.std()`, `np.min()`, `np.max()`
- `np.percentile()`, `np.median()`

**Mathematical**:
- `np.abs()`, `np.sqrt()`, `np.exp()`, `np.log()`
- `np.sin()`, `np.cos()`, `np.tan()`

**Array Operations**:
- `np.sum()`, `np.prod()`, `np.cumsum()`
- `np.argmin()`, `np.argmax()`

**NOT Supported** (use pure Python):
- String operations
- List comprehensions
- Dictionary operations
- Object-oriented features

### Common Patterns Cheat Sheet

**EMA Calculation**:
```python
alpha = 2.0 / (span + 1)
ema = price * alpha + ema * (1 - alpha)
```

**Contract Change Detection**:
```python
if (strikes[i] != strikes[i-1]) or (opt_types_int[i] != opt_types_int[i-1]):
    reset_state()
```

**Spread Check (5 bps)**:
```python
if ask > 0 and bid > 0:
    mid = 0.5 * (ask + bid)
    if mid > 0 and ((ask - bid) / mid) <= 0.0005:
        spread_ok = True
```

**Circular Buffer**:
```python
buffer[idx] = value
idx = (idx + 1) % buffer_size
```

---

## Performance Targets

**Expected throughput** on sorted data:

| Data Size | Workers | Expected Speed | Time |
|-----------|---------|----------------|------|
| 5M rows (1 file) | 1 | 30-50M rows/s | 0.1-0.2s |
| 50M rows (10 files) | 4 | 80-120M rows/s | 0.4-0.6s |
| 475M rows (115 files) | 24 | 150-200M rows/s | 2-3s |

**If below 50M rows/sec**, check:
1. Data is sorted on disk
2. No `.sort()` in code
3. Inline indicator calculations
4. Column projection used
5. Numba compilation successful

---

## Summary: The Three Pillars of Performance

### 1. Pre-Sorted Data
- ✓ No runtime sorting (saves 140s)
- ✓ Contract-change detection via comparison
- ✓ Sequential access (cache-friendly)

### 2. Inline Calculations
- ✓ Zero memory allocation
- ✓ CPU register computation
- ✓ Eliminates memory bandwidth bottleneck

### 3. Numba Compilation
- ✓ Machine code execution
- ✓ 100-1000x speedup vs Python
- ✓ True parallelism (`nogil=True`)

**Result**: **150-200M rows/sec** on properly optimized code!
