# ATM Volume Backtest - Running Status

## Overview
The ATM volume-burst backtest strategy is now running for the full year period (July 2024 to July 2025).

## Running Processes
- **BANKNIFTY**: PID 3204 - RUNNING
- **NIFTY**: PID 3245 - RUNNING

## Progress
- Processing speed: ~30 seconds per trading day
- Total days to process: 366 days
- Estimated completion time: ~3 hours for each symbol

## How to Monitor

### Check Process Status
```bash
ps -p 3204,3245
```

### Watch Real-time Progress
```bash
# BANKNIFTY progress
tail -f backtests/logs/banknifty_2024-07_2025-07.log

# NIFTY progress  
tail -f backtests/logs/nifty_2024-07_2025-07.log
```

### Quick Status Check
```bash
python3 backtest_status.py
```

## Expected Output Files
Once completed, results will be saved to:

1. **BANKNIFTY Results**:
   - `backtests/results/trades_BANKNIFTY_2024-07-01_2025-07-01.parquet`
   - `backtests/results/summary_BANKNIFTY_2024-07-01_2025-07-01.parquet`

2. **NIFTY Results**:
   - `backtests/results/trades_NIFTY_2024-07-01_2025-07-01.parquet`
   - `backtests/results/summary_NIFTY_2024-07-01_2025-07-01.parquet`

## Strategy Parameters
- **Signal**: 30-second volume burst vs 5-minute baseline (1.5x multiplier)
- **Strikes**: ATM ± 1 strikes (both CE & PE)
- **Entry**: First volume burst signal per hour (10:00, 11:00, 12:00, 13:00, 14:00, 15:00)
- **Risk**: 15% target, 15% stop-loss, 10% trailing stop
- **Side**: Selling options
- **P&L Mode**: Delta-proxy (using 0.5 delta for ATM, 0.4 for near strikes)

## View Results After Completion
```python
import polars as pl

# Load BANKNIFTY results
trades = pl.read_parquet("backtests/results/trades_BANKNIFTY_2024-07-01_2025-07-01.parquet")
summary = pl.read_parquet("backtests/results/summary_BANKNIFTY_2024-07-01_2025-07-01.parquet")

# Quick stats
print(f"Total trades: {len(trades)}")
print(f"Total P&L: {trades['pnl_pts'].sum():.2f} points")
print(f"Win rate: {(trades['exit_reason'] == 'target').mean() * 100:.1f}%")
print(f"Days traded: {len(summary)}")
```