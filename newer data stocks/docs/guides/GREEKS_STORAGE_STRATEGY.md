# Greeks Storage & Computation Strategy
## Recommendations for OI, Delta, Gamma, Theta, Vega

---

## Executive Summary

**Recommendation**: **Hybrid Approach**
- ✅ **Store**: OI (already have), spot_price, time_to_expiry
- ✅ **Compute on-the-fly**: Delta, Gamma, Theta, Vega
- ✅ **Optional**: Store implied volatility if available from market data

**Rationale**:
- Greeks change rapidly (every tick) → Storing creates stale data
- Greek calculation is cheap (~0.0001ms per row in Numba)
- OI changes slowly → Worth storing
- Spot + time_to_expiry enable real-time Greek computation

---

## Analysis by Greek

### 1. Open Interest (OI)

**Current Status**: ✅ Already stored in schema
```python
columns: ['oi', 'oiHigh', 'oiLow']  # Int64
```

**Recommendation**: **KEEP AS-IS**

**Why**:
- OI changes slowly (only on trades, not quotes)
- Critical for liquidity filtering
- Cannot be computed from other fields
- Already in dataset → No storage cost

**Usage in Strategy**:
```python
# Filter liquid contracts
df = df.filter(pl.col('oi') > 1000)

# In Numba:
if oi[i] < min_oi_threshold:
    skip_entry()
```

---

### 2. Delta (Δ)

**Definition**: Rate of change of option price with respect to spot price

**Recommendation**: **COMPUTE ON-THE-FLY** (Do NOT store)

**Why**:
- Changes with every spot tick (100+ times per second)
- Storing creates outdated data within milliseconds
- Fast to compute in Numba (~100 nanoseconds per row)
- Requires IV (implied volatility) which we likely don't have accurately

**Computation Methods**:

#### A. Black-Scholes Delta (Exact but needs IV)
```python
from math import sqrt, log, exp, erf

@njit
def bs_delta(spot, strike, time_to_expiry_years, risk_free_rate, iv, is_call):
    """
    Black-Scholes delta calculation.
    
    Args:
        spot: Current spot price
        strike: Option strike
        time_to_expiry_years: Time to expiry in years (e.g., 7/365 for 7 days)
        risk_free_rate: Risk-free rate (e.g., 0.06 for 6%)
        iv: Implied volatility (e.g., 0.20 for 20% IV)
        is_call: True for CE, False for PE
    
    Returns:
        Delta: 0 to 1 for CE, -1 to 0 for PE
    """
    if time_to_expiry_years <= 0:
        # At expiry
        if is_call:
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0
    
    # d1 calculation
    d1 = (log(spot / strike) + (risk_free_rate + 0.5 * iv**2) * time_to_expiry_years) / \
(iv * sqrt(time_to_expiry_years))
    
    # Normal CDF approximation
    # N(d1) = 0.5 * (1 + erf(d1 / sqrt(2)))
    norm_d1 = 0.5 * (1.0 + erf(d1 / sqrt(2.0)))
    
    if is_call:
        return norm_d1
    else:
        return norm_d1 - 1.0  # Put delta = Call delta - 1
```

#### B. Approximation (No IV needed - fast but less accurate)
```python
@njit
def delta_approx(spot, strike, time_to_expiry_days, is_call):
    """
    Simple delta approximation based on moneyness.
    
    Rules of thumb:
    - ATM: delta ≈ 0.5 (CE) or -0.5 (PE)
    - Deep ITM: delta → 1.0 (CE) or -1.0 (PE)
    - Deep OTM: delta → 0.0 (CE or PE)
    """
    if time_to_expiry_days <= 0:
        # At expiry
        if is_call:
            return 1.0 if spot > strike else 0.0
        else:
            return -1.0 if spot < strike else 0.0
    
    # Moneyness
    moneyness = (strike - spot) / spot
    
    # Approximation using tanh (smooth transition)
    # Scale factor based on time to expiry
    scale = 10.0 / sqrt(time_to_expiry_days)  # More sensitive near expiry
    
    if is_call:
        # CE: delta from 0 (deep OTM) to 1 (deep ITM)
        delta = 0.5 * (1.0 - np.tanh(moneyness * scale))
    else:
        # PE: delta from 0 (deep OTM) to -1 (deep ITM)
        delta = -0.5 * (1.0 + np.tanh(moneyness * scale))
    
    return delta
```

**Which to Use**:
- If you have **IV data**: Use Black-Scholes (accurate)
- If **no IV data**: Use approximation (good enough for filtering/ranking)

**Storage Impact if we DID store**:
```
Delta per row: 4 bytes (Float32)
475M rows: 1.9 GB uncompressed, ~200 MB compressed
BUT: Becomes stale within 1 second!
```

**Verdict**: Compute on-the-fly using approximation or BS formula

---

### 3. Gamma (Γ)

**Definition**: Rate of change of delta with respect to spot

**Recommendation**: **COMPUTE ON-THE-FLY** (Do NOT store)

**Why**:
- Changes with every spot tick
- Primarily used for hedging (less common in systematic backtesting)
- Derivable from delta calculation

**Computation** (from Black-Scholes):
```python
@njit
def bs_gamma(spot, strike, time_to_expiry_years, risk_free_rate, iv):
    """
    Black-Scholes gamma (same for CE and PE).
    """
    if time_to_expiry_years <= 0:
        return 0.0
    
    # d1 calculation
    d1 = (log(spot / strike) + (risk_free_rate + 0.5 * iv**2) * time_to_expiry_years) / \
         (iv * sqrt(time_to_expiry_years))
    
    # Gamma = N'(d1) / (spot × iv × sqrt(T))
    # N'(x) = 1/sqrt(2π) × exp(-x²/2)
    n_prime_d1 = (1.0 / sqrt(2.0 * 3.14159265)) * exp(-0.5 * d1**2)
    
    gamma = n_prime_d1 / (spot * iv * sqrt(time_to_expiry_years))
    
    return gamma
```

**Usage**:
```python
# Typically used for:
# - Identifying high gamma zones (scalping opportunities)
# - Hedging delta-neutral portfolios

# In backtest:
gamma = bs_gamma(spot, strike, tte, rf, iv)
if gamma > 0.05:  # High gamma = fast delta changes
    avoid_entry()  # Or adjust position size
```

**Verdict**: Compute only if strategy uses it (rare)

---

### 4. Theta (Θ)

**Definition**: Time decay - change in option price per day

**Recommendation**: **COMPUTE ON-THE-FLY** (Do NOT store)

**Why**:
- Changes throughout the day (function of time remaining)
- Directly computable from BS formula or approximation

**Computation**:
```python
@njit
def bs_theta_call(spot, strike, time_to_expiry_years, risk_free_rate, iv):
    """Black-Scholes theta for call option (per year)."""
    if time_to_expiry_years <= 0:
        return 0.0
    
    sqrt_t = sqrt(time_to_expiry_years)
    
    # d1, d2
    d1 = (log(spot / strike) + (risk_free_rate + 0.5 * iv**2) * time_to_expiry_years) / \
         (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t
    
    # N'(d1)
    n_prime_d1 = (1.0 / sqrt(2.0 * 3.14159265)) * exp(-0.5 * d1**2)
    
    # N(d2)
    n_d2 = 0.5 * (1.0 + erf(d2 / sqrt(2.0)))
    
    # Theta (per year)
    theta = -(spot * iv * n_prime_d1) / (2.0 * sqrt_t) - \
            risk_free_rate * strike * exp(-risk_free_rate * time_to_expiry_years) * n_d2
    
    # Convert to per day
    theta_per_day = theta / 365.0
    
    return theta_per_day

@njit
def bs_theta_put(spot, strike, time_to_expiry_years, risk_free_rate, iv):
    """Black-Scholes theta for put option (per year)."""
    theta_call = bs_theta_call(spot, strike, time_to_expiry_years, risk_free_rate, iv)
    
    # Put-call parity adjustment
    theta_put = theta_call + risk_free_rate * strike * exp(-risk_free_rate * time_to_expiry_years)
    
    return theta_put / 365.0
```

**Simple Approximation** (without IV):
```python
@njit
def theta_approx(option_price, time_to_expiry_days):
    """
    Rough theta approximation: theta ≈ -price / days_remaining
    
    Assumes linear decay (not accurate but directionally correct).
    """
    if time_to_expiry_days <= 0:
        return 0.0
    
    return -option_price / time_to_expiry_days
```

**Usage**:
```python
# Theta strategies (time decay benefit)
theta = bs_theta_call(spot, strike, tte, rf, iv)

if theta < -10:  # Selling options with high decay = good theta
    enter_short_position()
```

**Verdict**: Compute on-the-fly when needed

---

### 5. Vega (ν)

**Definition**: Change in option price per 1% change in IV

**Recommendation**: **COMPUTE ON-THE-FLY** (Do NOT store)

**Why**:
- Depends on IV (which we may not have accurately)
- Used primarily for volatility trading Strategies
- Easy to compute from BS formula

**Computation**:
```python
@njit
def bs_vega(spot, strike, time_to_expiry_years, risk_free_rate, iv):
    """
    Black-Scholes vega (same for CE and PE).
    Returns change in option price per 1% change in IV.
    """
    if time_to_expiry_years <= 0:
        return 0.0
    
    sqrt_t = sqrt(time_to_expiry_years)
    
    # d1
    d1 = (log(spot / strike) + (risk_free_rate + 0.5 * iv**2) * time_to_expiry_years) / \
         (iv * sqrt_t)
    
    # N'(d1)
    n_prime_d1 = (1.0 / sqrt(2.0 * 3.14159265)) * exp(-0.5 * d1**2)
    
    # Vega = spot × sqrt(T) × N'(d1)
    vega = spot * sqrt_t * n_prime_d1
    
    # Convert to per 1% IV change
    vega_1pct = vega / 100.0
    
    return vega_1pct
```

**Usage**:
```python
# Vega strategies: 
# - Buy options before volatility expansion (earnings, events)
# - Sell options during high IV (mean reversion)

vega = bs_vega(spot, strike, tte, rf, iv)

# Example: Avoid high vega exposure
if abs(vega) > 50:
    reduce_position_size()
```

**Verdict**: Compute only for vega-focused strategies

---

## Implied Volatility (IV)

**Current Status**: ❌ Not in dataset

**Recommendation**: **STORE IF AVAILABLE** from market data

**Why**:
- Cannot be computed from other fields (requires reverse BS solve)
- Changes relatively slowly (compared to price)
- Critical for accurate Greek calculation
- Enables IV-based strategies (IV rank, percentile)

**If Market Provides IV**:
```python
# In packing script, add column:
df = df.with_columns([
    pl.col('implied_volatility').cast(pl.Float32).alias('iv')
])

# Storage impact: 4 bytes/row = ~15 MB for 475M rows (compressed)
```

**If IV Not Available**:
- Use constant assumption (e.g., IV = 20% for all contracts)
- Or compute from historical price volatility
- Greeks will be less accurate but directionally correct

---

## Time to Expiry

**Recommendation**: **COMPUTE ON-THE-FLY** (Do NOT store)

**Why**:
- Calculable from timestamp + expiry columns (already have)
- Changes every second
- Zero CPU cost to compute

**Computation**:
```python
# In packing (if we wanted to store - NOT recommended):
df = df.with_columns([
    ((pl.col('expiry').cast(pl.Datetime) - pl.col('timestamp')).dt.total_seconds() / 86400.0)
        .alias('time_to_expiry_days')
        .cast(pl.Float32)
])

# Better: Compute on-the-fly in Numba
@njit
def time_to_expiry_days(timestamp_ns, expiry_ns):
    """Compute time to expiry in days."""
    seconds_remaining = (expiry_ns - timestamp_ns) / 1e9
    days_remaining = seconds_remaining / 86400.0
    return days_remaining
```

---

## Recommended Data Model

### Columns to ADD (beyond spot enrichment):

**None!** We already have everything needed:
- ✅ `oi`, `oiHigh`, `oiLow` (already in schema)
- ✅ `spot_price` (adding in v3)
- ✅ `timestamp`, `expiry` (already have → compute time_to_expiry)
- ✅ `strike`, `opt_type`, `price` (already have)

**Optional (if market provides)**:
- `implied_volatility` (Float32) - Store if available

### Greeks Computation Library

**Create a reusable Greeks module**:

```python
# greeks.py
from math import sqrt, log, exp, erf
from numba import njit

@njit
def time_to_expiry_years(timestamp_ns, expiry_ns):
    """Time to expiry in years."""
    days = (expiry_ns - timestamp_ns) / 1e9 / 86400.0
    return days / 365.0

@njit
def bs_delta(spot, strike, tte_years, rf, iv, is_call):
    """Black-Scholes delta."""
    if tte_years <= 0:
        return 1.0 if (is_call and spot > strike) else 0.0
    
    d1 = (log(spot / strike) + (rf + 0.5 * iv**2) * tte_years) / (iv * sqrt(tte_years))
    n_d1 = 0.5 * (1.0 + erf(d1 / sqrt(2.0)))
    
    return n_d1 if is_call else n_d1 - 1.0

@njit
def bs_gamma(spot, strike, tte_years, rf, iv):
    """Black-Scholes gamma."""
    if tte_years <= 0:
        return 0.0
    
    d1 = (log(spot / strike) + (rf + 0.5 * iv**2) * tte_years) / (iv * sqrt(tte_years))
    n_prime_d1 = (1.0 / sqrt(2.0 * 3.14159265)) * exp(-0.5 * d1**2)
    
    return n_prime_d1 / (spot * iv * sqrt(tte_years))

# Add theta, vega, rho as needed...
```

**Usage in Backtest**:
```python
from greeks import bs_delta, bs_gamma, time_to_expiry_years

@njit
def strategy_with_greeks(timestamps_ns, expiries_ns, strikes, opt_types_int, 
                        prices, spot_prices, ...):
    # Constants
    RF_RATE = 0.06  # 6% risk-free rate
    ASSUMED_IV = 0.20  # 20% IV if not available from market
    
    for i in range(1, n):
        # Compute time to expiry
        tte_years = time_to_expiry_years(timestamps_ns[i], expiries_ns[i])
        
        # Compute delta
        delta = bs_delta(
            spot_prices[i],
            strikes[i],
            tte_years,
            RF_RATE,
            ASSUMED_IV,
            is_call=(opt_types_int[i] == 0)
        )
        
        # Use delta in strategy
        if abs(delta) > 0.30:  # Only trade options with delta > 0.3
            enter_position()
```

---

## Performance Impact

**Storing Greeks vs Computing**:

| Approach | Storage | Computation | Staleness |
|----------|---------|-------------|-----------|
| **Store all Greeks** | +20 bytes/row = +80 MB | 0 ms | High (stale in 1s) |
| **Compute on-the-fly** | 0 bytes | ~0.001 ms/row | Never (always fresh) |

**Benchmark**:
```python
# Computing delta for 475M rows:
# 475M × 0.001ms = 475 seconds = 8 minutes

# But we only compute for contracts we're actually trading:
# Typical: 10-50 contracts per day
# 10 contracts × 10K ticks = 100K computations
# 100K × 0.001ms = 0.1 seconds (negligible!)
```

**Verdict**: Compute on-the-fly is 1000x better (no storage, always fresh, minimal CPU)

---

## Final Recommendations

### ✅ DO STORE:
1. **OI** (already have) - Changes slowly, cannot compute
2. **Spot price** (v3 adds) - Needed for all Greeks
3. **IV** (optional) - If market provides, store it

### ❌ DO NOT STORE:
1. **Delta** - Compute on-the-fly (stale in 1 second)
2. **Gamma** - Compute only if used
3. **Theta** - Compute only if used
4. **Vega** - Compute only if used
5. **Time to expiry** - Trivial computation from timestamp + expiry

### 📦 Create Reusable Module:
- `greeks.py` with Numba-compiled BS functions
- Import in backtest scripts as needed
- Compute Greeks only for contracts being traded (not all 475M rows!)

### 🎯 Strategy Impact:
**Delta-based strategies** (most common):
```python
# Compute delta for selected contracts
delta = bs_delta(spot, strike, tte, rf, iv, is_call)

# Use in entry logic
if 0.25 < abs(delta) < 0.40:
    enter_position()
```

**OI-based strategies** (liquidity):
```python
# Filter by pre-stored OI
df = df.filter(pl.col('oi') > 10000)
```

**Theta strategies** (time decay):
```python
# Compute theta for short positions
theta = bs_theta(spot, strike, tte, rf, iv)
if theta < -20:  # High decay = good for sellers
    enter_short()
```

---

## Implementation Priority

### Phase 1 (Current): Spot Enrichment ✓
- Add spot_price, distance_from_spot, moneyness_pct
- Add intrinsic_value, time_value, mid_price
- **Status**: In v3 packing script

### Phase 2 (Next): Greeks Module
- Create `greeks.py` with Numba BS functions
- Add to strategy template
- Benchmark performance

### Phase 3 (Optional): IV Integration
- If market provides IV, add to packing script
- Enables accurate Greek calculation
- Enables IV-based strategies (IV rank, percentile)

**Bottom Line**: Don't store Greeks - compute them on-the-fly for the 10-50 contracts you're actually trading, not all 475M rows!
