#!/bin/bash

# ATM Volume Backtest Runner - Full Year (July 2024 to July 2025)
# This script runs the backtest in background and logs output

echo "Starting ATM Volume Backtest for full year period..."
echo "Period: July 2024 to July 2025"
echo "Symbols: BANKNIFTY and NIFTY"
echo ""

# Create log directory
mkdir -p backtests/logs

# Run BANKNIFTY backtest
echo "Running BANKNIFTY backtest (2024-07-01 to 2025-07-01)..."
nohup python3 backtests/atm_volume.py \
    --symbol BANKNIFTY \
    --start 2024-07-01 \
    --end 2025-07-01 \
    > backtests/logs/banknifty_2024-07_2025-07.log 2>&1 &

BANKNIFTY_PID=$!
echo "BANKNIFTY backtest started with PID: $BANKNIFTY_PID"

# Give it a moment before starting the next
sleep 2

# Run NIFTY backtest
echo "Running NIFTY backtest (2024-07-01 to 2025-07-01)..."
nohup python3 backtests/atm_volume.py \
    --symbol NIFTY \
    --start 2024-07-01 \
    --end 2025-07-01 \
    > backtests/logs/nifty_2024-07_2025-07.log 2>&1 &

NIFTY_PID=$!
echo "NIFTY backtest started with PID: $NIFTY_PID"

echo ""
echo "Both backtests are running in background!"
echo ""
echo "Monitor progress with:"
echo "  tail -f backtests/logs/banknifty_2024-07_2025-07.log"
echo "  tail -f backtests/logs/nifty_2024-07_2025-07.log"
echo ""
echo "Check process status with:"
echo "  ps -p $BANKNIFTY_PID"
echo "  ps -p $NIFTY_PID"
echo ""
echo "Results will be saved to:"
echo "  backtests/results/trades_BANKNIFTY_2024-07-01_2025-07-01.parquet"
echo "  backtests/results/summary_BANKNIFTY_2024-07-01_2025-07-01.parquet"
echo "  backtests/results/trades_NIFTY_2024-07-01_2025-07-01.parquet"
echo "  backtests/results/summary_NIFTY_2024-07-01_2025-07-01.parquet"