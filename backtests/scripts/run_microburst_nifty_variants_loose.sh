#!/usr/bin/env bash
set -euo pipefail

# Looser microburst variants for NIFTY to ensure trades for validation.
# Usage:
#   bash backtests/scripts/run_microburst_nifty_variants_loose.sh

# Ensure repo root is on PYTHONPATH; load pinned interpreter if present
ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}

SYMBOL=NIFTY
START=2024-07-01
END=2025-07-31

# v1 (looser)
$PY backtests/strategies/microburst_seller.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  --price-thresh 1.0 \
  --vol-mult 1.05 \
  --target-pts 1.5 \
  --stop-pts 1.5 \
  --trail-pts 0.8 \
  --cooldown-secs 60 || true
cp backtests/results/microburst_seller_${SYMBOL}_${START}_${END}.parquet \
   backtests/results/microburst_seller_${SYMBOL}_${START}_${END}_v1.parquet || true

# v2 (mid)
$PY backtests/strategies/microburst_seller.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  --price-thresh 1.2 \
  --vol-mult 1.05 \
  --target-pts 1.5 \
  --stop-pts 1.5 \
  --trail-pts 0.8 \
  --cooldown-secs 60 || true
cp backtests/results/microburst_seller_${SYMBOL}_${START}_${END}.parquet \
   backtests/results/microburst_seller_${SYMBOL}_${START}_${END}_v2.parquet || true

# v3 (strict)
$PY backtests/strategies/microburst_seller.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  --price-thresh 0.8 \
  --vol-mult 1.05 \
  --target-pts 1.5 \
  --stop-pts 1.5 \
  --trail-pts 0.8 \
  --cooldown-secs 60 || true
cp backtests/results/microburst_seller_${SYMBOL}_${START}_${END}.parquet \
   backtests/results/microburst_seller_${SYMBOL}_${START}_${END}_v3.parquet || true

echo "Done (loose)."
