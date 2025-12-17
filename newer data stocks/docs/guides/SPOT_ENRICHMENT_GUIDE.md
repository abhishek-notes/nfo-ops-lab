# Spot Enrichment Implementation Guide

## Quick Start

### 1. Extract Spot Data (One-Time)
```bash
cd "newer data stocks"

# Extract NIFTY and BANKNIFTY spot from all SQL dumps
python3 scripts/spot_extraction/extract_spot_data.py \
  --data-dirs "data/new 2025 data" \
  --output-dir "data/spot_data" \
  --symbols NIFTY BANKNIFTY
```

**Output**:
- `data/spot_data/NIFTY_all.parquet` - Consolidated NIFTY spot (all dates)
- `data/spot_data/BANKNIFTY_all.parquet` - Consolidated BANKNIFTY spot (all dates)
- Per-folder files: `data/spot_data/NIFTY_aug_1_to_aug_13_new_stocks_data.parquet`, etc.

### 2. Repack One Folder (Test)
```bash
# Test on one folder first
python3 scripts/data_processing/repack_raw_to_date_v3_SPOT_ENRICHED.py \
  --input-dir "data/new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options" \
  --output-dir "data/options_date_packed_FULL_v3_SPOT_ENRICHED" \
  --expiry-calendar "config/expiry_calendar.csv" \
  --spot-dir "data/spot_data"
```

**Verify output**:
```python
import polars as pl

# Check one file
df = pl.read_parquet('data/options_date_packed_FULL_v3_SPOT_ENRICHED/2025-11-04/BANKNIFTY/part-banknifty-0.parquet')

# Verify new columns
print(df.columns)
# Should include: spot_price, distance_from_spot, moneyness_pct,
#                 intrinsic_value, time_value, mid_price

# Check spot join success
print(f"Spot price nulls: {df['spot_price'].is_null().sum()}")  # Should be low (<1%)
```

### 3. Batch Process All Folders
```bash
# Process all 9 folders automatically
bash scripts/batch/batch_repack_with_spot.sh
```

**Time**: ~45-60 minutes for all folders

**Output**: `data/options_date_packed_FULL_v3_SPOT_ENRICHED/` with 115+ files

---

## Schema Changes

### New Columns (6 total)

| Column | Type | Formula | Use Case |
|--------|------|---------|----------|
| `spot_price` | Float32 | Joined from spot data | ATM detection, Greeks |
| `distance_from_spot` | Float32 | `strike - spot_price` | ATM = argmin(abs(distance)) |
| `moneyness_pct` | Float32 | `(strike - spot) / spot * 100` | OTM/ITM classification |
| `intrinsic_value` | Float32 | `max(0, (spot - strike) × sign)` | Risk management |
| `time_value` | Float32 | `price - intrinsic_value` | Extrinsic premium |
| `mid_price` | Float32 | `(bp0 + sp0) / 2` | Better entry/exit prices |

### Total Schema Now
- **Original**: 58 columns (52 raw + 6 computed)
- **v3 with spot**: **64 columns** (58 + 6 spot-enriched)

### Storage Impact
- **Size increase**: ~3% (6 Float32 columns = 24 bytes/row)
- **Current**: ~258 MB → **v3**: ~266 MB (compressed)
- **Negligible**: +8 MB total

---

## Greeks Strategy (from GREEKS_STORAGE_STRATEGY.md)

### ✅ STORE:
1. **OI** - Already have (`oi`, `oiHigh`, `oiLow`)
2. **Spot price** - Added in v3
3. **IV** - Store if market provides (currently don't have)

### ❌ COMPUTE ON-THE-FLY:
1. **Delta** - BS formula or approximation (~0.0001ms per row)
2. **Gamma** - Rare use, compute only when needed
3. **Theta** - Compute for theta strategies
4. **Vega** - Compute for vol strategies
5. **Time to expiry** - Trivial: `(expiry_ns - timestamp_ns) / 86400`

### Rationale:
- Greeks change every tick (stale within 1 second if stored)
- Computation is fast in Numba (~100 nanoseconds)
- Only compute for contracts being traded (10-50 per day, not 475M rows!)
- Storage would add 20+ bytes/row = 80+ MB for stale data

---

## Troubleshooting

### Issue 1: "No spot data found"
**Symptoms**:
```
WARNING: Spot data not found at data/spot_data/NIFTY_all.parquet
Will pack without spot enrichment (columns will be null)
```

**Solution**:
```bash
# Run spot extraction first
python3 scripts/spot_extraction/extract_spot_data.py \
  --data-dirs "data/new 2025 data" \
  --output-dir "data/spot_data"
```

### Issue 2: Low spot join success rate
**Symptoms**:
```
Spot join success: 85.2% (123M/145M rows)
WARNING: Low join success rate! Check timestamp alignment.
```

**Causes**:
- Pre-market / post-market options data (spot not trading)
- Missing spot ticks in SQL dumps
- Timestamp format mismatch

**Solutions**:
1. Check spot data coverage:
```python
spot = pl.read_parquet('data/spot_data/NIFTY_all.parquet')
print(f"Spot date range: {spot['timestamp'].min()} to {spot['timestamp'].max()}")
```

2. Filter options to market hours in packing script:
```python
# Add to packing script before spot join
combined = combined.filter(
    (pl.col('timestamp').dt.hour() >= 9) &
    (pl.col('timestamp').dt.hour() <= 15)
)
```

### Issue 3: SQL parsing errors
**Symptoms**:
```
Warning: Skipping row due to parse error: invalid literal for float()
```

**Causes**:
- Malformed SQL INSERT statements
- Encoding issues

**Solutions**:
- Check SQL dump integrity: `gunzip -t das_nse_mod.sql.gz`
- Skip problematic rows (already handled with try/except)

---

## Performance Notes

### Spot Extraction Time
- **Per folder**: ~30 seconds
- **All 9 folders**: ~4-5 minutes
- **Consolidation**: ~10 seconds

### Repacking with Spot
- **Per folder**: ~3-5 minutes (similar to v2)
- **Spot join overhead**: <1 second per folder (negligible)
- **All 9 folders**: ~45-60 minutes

### Expected Throughput Maintained
- **v2 (no spot)**: 161M rows/sec
- **v3 (with spot)**: **~158M rows/sec** (2% slower due to 6 extra columns)
- **Still I/O bound, not CPU bound**

---

## Next Steps

1. **Run spot extraction** → Creates `data/spot_data/`
2. **Test on one folder** → Verify schema
3. **Run batch script** → Process all data
4. **Update benchmark** → Test ATM straddle strategy
5. **Create Greeks module** → Implement `greeks.py` for on-the-fly calculations

---

## File Summary

| File | Purpose | Status |
|------|---------|--------|
| `scripts/spot_extraction/extract_spot_data.py` | Parse SQL dumps for NIFTY/BANKNIFTY | ✓ Ready |
| `scripts/data_processing/repack_raw_to_date_v3_SPOT_ENRICHED.py` | Pack with spot enrichment | ✓ Ready |
| `scripts/batch/batch_repack_with_spot.sh` | Automate all folders | ⚠️ Uses absolute paths (edit if needed) |
| `GREEKS_STORAGE_STRATEGY.md` | Greeks recommendations | ✓ Documented |
| `data/spot_data/` | Spot parquet files | Run extraction |
| `data/options_date_packed_FULL_v3_SPOT_ENRICHED/` | Output directory | Run packing |

**Ready to execute!** Start with `python3 scripts/spot_extraction/extract_spot_data.py ...`
