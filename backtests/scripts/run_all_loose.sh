#!/usr/bin/env bash
set -euo pipefail

# Looser orchestrator: setup env, gamma sample (loose), gamma 2025 (loose), microburst variants (loose).
# Usage:
#   bash backtests/scripts/run_all_loose.sh

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"

bash backtests/scripts/setup_env.sh
if [ -f backtests/scripts/.python_bin ]; then
  export PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
  echo "Selected interpreter: $PYTHON_BIN"
fi

bash backtests/scripts/run_gamma_sample_bnf_loose.sh || true
bash backtests/scripts/run_gamma_bnf_2025_loose.sh || true
bash backtests/scripts/summarize_gamma_bnf_2025.sh || true

bash backtests/scripts/run_microburst_nifty_variants_loose.sh || true
bash backtests/scripts/run_microburst_banknifty_variants_loose.sh || true

echo "All (loose) tasks attempted. Check backtests/results/."
