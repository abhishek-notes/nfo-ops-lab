#!/usr/bin/env python3
"""
Minimal test to isolate Numba date issue
"""
import polars as pl
import numpy as np
from numba import njit
from pathlib import Path

# Load minimal data
data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
sample_date = sorted([d for d in data_dir.glob("*") if d.is_dir() and "1970" not in d.name])[5]
files = list((sample_date / "BANKNIFTY").glob("*.parquet"))

df = pl.read_parquet(files[0], columns=[
    'timestamp', 'strike', 'opt_type', 'price'
]).filter(pl.col('timestamp').dt.year() > 1970).head(1000)

print(f"Loaded {len(df)} rows from {sample_date.name}")

# Convert dates EXACTLY like selling strategy
dates = df['timestamp'].dt.date().cast(pl.Int32).to_numpy()
hours = df['timestamp'].dt.hour().cast(pl.Int32).to_numpy()
times = hours * 3600

print(f"Dates type: {dates.dtype}")
print(f"Dates range: {dates.min()} to {dates.max()}")
print(f"Times type: {times.dtype}")
print(f"Times range: {times.min()} to {times.max()}")

# Now test in Numba - minimal function
@njit
def test_numba(dates_arr, times_arr):
    """Minimal Numba test"""
    for i in range(len(dates_arr)):
        d = dates_arr[i]
        t = times_arr[i]
        # Just access them
        if d > 0 and t >= 0:
            pass
    return len(dates_arr)

try:
    result = test_numba(dates, times)
    print(f"\n✅ Numba test PASSED: processed {result} rows")
except Exception as e:
    print(f"\n❌ Numba test FAILED: {e}")

# Now test with the actual strategy function inputs
strikes = df['strike'].to_numpy()
opt_types = (df['opt_type'] == 'PE').cast(pl.Int8).to_numpy()
prices = df['price'].to_numpy()

print(f"\nStrike type: {strikes.dtype}, range: {strikes.min():.0f} to {strikes.max():.0f}")
print(f"Opt types: {opt_types.dtype}, unique: {np.unique(opt_types)}")
print(f"Prices type: {prices.dtype}, range: {prices.min():.2f} to {prices.max():.2f}")

@njit
def test_numba_full(dates_arr, times_arr, strikes_arr, opt_arr, prices_arr):
    """Full test matching strategy"""
    count = 0
    for i in range(len(dates_arr)):
        if dates_arr[i] > 20000 and times_arr[i] >= 43200:  # After 12PM
            count += 1
    return count

try:
    result = test_numba_full(dates, times, strikes, opt_types, prices)
    print(f"\n✅ Full Numba test PASSED: {result} rows in time window")
except Exception as e:
    print(f"\n❌ Full Numba test FAILED: {e}")
