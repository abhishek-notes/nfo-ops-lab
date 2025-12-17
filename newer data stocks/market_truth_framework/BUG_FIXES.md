# Market Truth Framework - Bug Fixes

## Critical Bugs Identified by User (All Valid ✅)

### 1. **File Naming - No Underlying Suffix** ✅
**Issue**: `features_{date}.parquet` has no underlying, BANKNIFTY overwrites NIFTY
**Impact**: Only 81 files possible instead of 162, API can't distinguish
**Fix**: Change to `features_{underlying}_{date}.parquet`

### 2. **Single File Loading** ✅
**Issue**: `files = list(date_dir.glob("*.parquet"))` then `df = pl.read_parquet(files[0])`
**Impact**: Drops all data except first shard
**Fix**: Load all files: `df = pl.read_parquet(files)` or `pl.concat([pl.read_parquet(f) for f in files])`

### 3. **Sparse Timestamps (No Grid Fill)** ✅
**Issue**: Only processes timestamps where data exists, never fills 1-second grid
**Impact**: Burst durations wrong, gaps invisible, stats unreliable
**Fix**: Reindex to full 1-second grid from 9:15:00 to 15:30:00

### 4. **Acceleration Calculation Bug** ✅
**Issue**: Line 169: `range(idx - window + 1, idx)` sums 9 values, divides by 10
**Impact**: Inflated acceleration values
**Fix**: Include `idx` so we sum exactly `window` values: `range(idx - window + 1, idx + 1)`

### 5. **Missing options_atm Output** ✅
**Issue**: Docstring promises it, but never saved
**Impact**: Data schema mismatch
**Fix**: Either save it or remove from docs

### 6. **DTE Bucketing Wrong** ✅
**Issue**: Only 3 bins (0, ≤2, >2), not 4 bins (0, 2, 4, 6)
**Impact**: Can't analyze 4/6 DTE separately
**Fix**: Add proper bucketing logic

### 7. **Missing Features from Spec** ✅
**Issue**: No ±1/±2 strikes, no liquidity integration, no regime labels
**Impact**: Incomplete feature set
**Fix**: Implement missing features or document as Phase 2

### 8. **Polars datetime Bug** ✅
**Issue**: Line 191: `pl.datetime.now()` doesn't exist
**Impact**: Statistics generator crashes
**Fix**: Use `datetime.now()` from Python stdlib

### 9. **API Can't Handle Underlyings** ✅
**Issue**: Reads `features_{date}.parquet` without underlying
**Impact**: Can only serve whichever ran last
**Fix**: Update API to handle `features_{underlying}_{date}.parquet`

### 10. **Burst Volatility Condition Impossible (RV Definition)** ✅
**Issue**: If RV is computed as `sqrt(sum(r_i^2))`, then `RV_10 <= RV_120` always (larger window contains the smaller window), so the condition `RV_10 > k1 × RV_120` can never trigger for `k1 > 1`.
**Impact**: Burst detector silently returns zero bursts even on real bursty days.
**Fix**: Use RMS volatility so different window lengths are comparable: `RV_w = sqrt(mean(r_i^2) over window)`.

---

## Fix Priority

**Critical (Must Fix Now)**:
1. File naming (underlying suffix)
2. Load all parquet files
3. Datetime bug
4. API underlying handling

**Important (Should Fix)**:
5. Sparse timestamps → full grid
6. Acceleration calculation
7. DTE bucketing

**Can Defer**:
8. Missing features (mark as TODO)
9. options_atm output (remove from docs)

---

## Implementation Plan

1. Stop current batch processing ✅
2. Clear bad outputs ✅
3. Fix core_preprocessor.py (all 7 issues)
4. Fix statistics_generator.py (datetime bug)
5. Fix batch_processor.py (underlying handling)
6. Fix api (underlying in paths)
7. Test on 1 day both underlyings
8. Restart batch

---

Estimated fix time: 30 minutes
