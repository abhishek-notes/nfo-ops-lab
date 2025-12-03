#!/usr/bin/env bash
set -euo pipefail

# One-shot orchestrator: sets up env, runs gamma sample + year, and microburst variants.
# Usage:
#   bash backtests/scripts/run_all.sh

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"

# 0) Environment
bash backtests/scripts/setup_env.sh
# load chosen interpreter
if [ -f backtests/scripts/.python_bin ]; then
  export PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
  echo "Selected interpreter: $PYTHON_BIN"
fi

# 1) Gamma sample (BANKNIFTY, with per-second debug)
bash backtests/scripts/run_gamma_sample_bnf.sh || true

# 2) Gamma one-year (BANKNIFTY monthly in repo)
bash backtests/scripts/run_gamma_bnf_2025.sh || true
bash backtests/scripts/summarize_gamma_bnf_2025.sh || true

# 3) Microburst variants (NIFTY)
bash backtests/scripts/run_microburst_nifty_variants.sh || true

# 4) Microburst variants (BANKNIFTY)
bash backtests/scripts/run_microburst_banknifty_variants.sh || true

echo "All tasks attempted. Check backtests/results/ for outputs."
