#!/usr/bin/env bash
set -euo pipefail

# EOD gamma scalp for BANKNIFTY across 2025 monthly expiries with looser brackets to ensure activity.
# Usage:
#   bash backtests/scripts/run_gamma_bnf_2025_loose.sh

if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}

SYMBOL=BANKNIFTY
ANCHORS=10:00,11:00,12:00,13:00,14:00
EXPS=(
  2025-01-29
  2025-02-25
  2025-03-26
  2025-04-30
)

for EXP in "${EXPS[@]}"; do
  START=$($PY -c 'import sys,datetime as d; x=sys.argv[1]; y=d.date.fromisoformat(x)-d.timedelta(days=2); print(y.isoformat())' "$EXP")
  echo "Running (loose): $SYMBOL $START -> $EXP (anchors=$ANCHORS)"
  $PY backtests/strategies/gamma_scalp_eod.py \
    --symbol "$SYMBOL" \
    --start "$START" \
    --end "$EXP" \
    --expiry "$EXP" \
    --anchors "$ANCHORS" \
    --target-frac 1.005 \
    --stop-frac 0.995 \
    --trail-pts 0.0 || true
done

echo "Done (loose)."

