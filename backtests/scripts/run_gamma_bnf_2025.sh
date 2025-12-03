#!/usr/bin/env bash
set -euo pipefail

# EOD gamma scalp for BANKNIFTY across 2025 monthly expiries available in this repo.
# Runs a 2-day window into each expiry with multiple anchors.
# Usage:
#   bash backtests/scripts/run_gamma_bnf_2025.sh

SYMBOL=BANKNIFTY
# Ensure repo root is on PYTHONPATH; load pinned interpreter if present
ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}
ANCHORS=10:00,11:00,12:00,13:00,14:00
EXPS=(
  2025-01-29
  2025-02-25
  2025-03-26
  2025-04-30
)

for EXP in "${EXPS[@]}"; do
  # Portable 2-day subtraction without heredoc quoting issues
  START=$($PY -c 'import sys,datetime as d; x=sys.argv[1]; y=d.date.fromisoformat(x)-d.timedelta(days=2); print(y.isoformat())' "$EXP")
  echo "Running: $SYMBOL $START -> $EXP (anchors=$ANCHORS)"
  $PY backtests/strategies/gamma_scalp_eod.py \
    --symbol "$SYMBOL" \
    --start "$START" \
    --end "$EXP" \
    --expiry "$EXP" \
    --anchors "$ANCHORS" \
    --target-frac 1.02 \
    --stop-frac 0.98 \
    --trail-pts 0.0
done

echo "Done. Parquets written under backtests/results/gamma_scalp_eod_${SYMBOL}_*.parquet"
