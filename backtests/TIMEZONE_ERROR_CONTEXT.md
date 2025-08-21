# Timezone Error Context for ChatGPT

## Issue
The ATM volume backtest script is encountering timezone comparison errors in Polars when trying to filter DataFrames by datetime.

## Error Details
```
polars.exceptions.SchemaError: could not evaluate '>=' comparison between series 'timestamp' of dtype: datetime[ns, Asia/Kolkata] and series 'literal' of dtype: datetime[μs, UTC]
```

## Problem Areas

1. **SpotStore.load_range()** method:
   - When filtering: `lf.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))`
   - The timestamp column is in Asia/Kolkata timezone
   - The start/end parameters come as UTC or timezone-naive

2. **datetime_range creation**:
   - `pl.datetime_range(start, end, "1s", time_zone=IST)` requires `eager=True` parameter in newer Polars versions
   - Without it, returns an expression instead of actual values

3. **Timezone mismatches**:
   - Spot data timestamps are in datetime[ns, Asia/Kolkata]
   - Options data timestamps are in datetime[μs, Asia/Kolkata] 
   - Function parameters often come as UTC or timezone-naive

## Data Structure
- Spot files: `./data/packed/spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet`
- Options files: `./data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`

## What We Need
Please provide the proper way to handle timezone comparisons in Polars for this use case, maintaining all the original logic and functionality of your script.