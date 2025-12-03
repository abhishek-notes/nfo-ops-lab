#!/usr/bin/env bash
set -euo pipefail

# Runs three microburst variants on BANKNIFTY for 2024-12-01 -> 2025-07-31
# and saves outputs with _v1/_v2/_v3 suffixes.
# Usage:
#   bash backtests/scripts/run_microburst_banknifty_variants.sh

SYMBOL=BANKNIFTY
# Ensure repo root is on PYTHONPATH; load pinned interpreter if present
ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}
START=2024-12-01
END=2025-07-31

# v1
$PY backtests/strategies/microburst_seller.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  --price-thresh 3.0 \
  --vol-mult 1.10 \
  --target-pts 2.0 \
  --stop-pts 2.0 \
  --trail-pts 1.0 \
  --cooldown-secs 60
cp backtests/results/microburst_seller_${SYMBOL}_${START}_${END}.parquet \
   backtests/results/microburst_seller_${SYMBOL}_${START}_${END}_v1.parquet || true

# v2
$PY backtests/strategies/microburst_seller.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  --price-thresh 2.5 \
  --vol-mult 1.05 \
  --target-pts 2.0 \
  --stop-pts 2.0 \
  --trail-pts 1.0 \
  --cooldown-secs 60
cp backtests/results/microburst_seller_${SYMBOL}_${START}_${END}.parquet \
   backtests/results/microburst_seller_${SYMBOL}_${START}_${END}_v2.parquet || true

# v3
$PY backtests/strategies/microburst_seller.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  --price-thresh 3.5 \
  --vol-mult 1.15 \
  --target-pts 2.0 \
  --stop-pts 2.0 \
  --trail-pts 1.0 \
  --cooldown-secs 60
cp backtests/results/microburst_seller_${SYMBOL}_${START}_${END}.parquet \
   backtests/results/microburst_seller_${SYMBOL}_${START}_${END}_v3.parquet || true

echo "Done. Variant files saved with _v1/_v2/_v3 suffixes."
