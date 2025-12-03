#!/usr/bin/env bash
set -euo pipefail

# Sample EOD gamma scalp for BANKNIFTY on a single day/expiry.
# Usage:
#   bash backtests/scripts/run_gamma_sample_bnf.sh

SYMBOL=BANKNIFTY
DATE=2025-03-03
EXPIRY=2025-03-26

# Ensure repo root is on PYTHONPATH; load pinned interpreter if present
ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}

# 1) 12:00 with tighter brackets + per-second debug
$PY backtests/strategies/gamma_scalp_eod.py \
  --symbol "$SYMBOL" \
  --start "$DATE" \
  --end "$DATE" \
  --expiry "$EXPIRY" \
  --anchors 12:00 \
  --target-frac 1.01 \
  --stop-frac 0.99 \
  --trail-pts 0.0 \
  --debug-sample "${DATE} 12:00:00"

# 2) 13:00 as contrast
$PY backtests/strategies/gamma_scalp_eod.py \
  --symbol "$SYMBOL" \
  --start "$DATE" \
  --end "$DATE" \
  --expiry "$EXPIRY" \
  --anchors 13:00 \
  --target-frac 1.02 \
  --stop-frac 0.98 \
  --trail-pts 0.0

echo "Done. Outputs (if any):"
echo "  backtests/results/gamma_scalp_eod_${SYMBOL}_${DATE}_${DATE}_${EXPIRY}.parquet"
echo "  backtests/results/gamma_eod_debug_${SYMBOL}_${DATE}_120000_${EXPIRY}.csv (debug)"
