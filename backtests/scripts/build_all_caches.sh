#!/usr/bin/env bash
set -euo pipefail

# Build per-second option ladder caches over a date range with nohup + logs.
# - Ensures a local venv with required packages.
# - Runs the Python builder with PYTHONPATH set to repo root.
#
# Usage examples:
#   ./backtests/scripts/build_all_caches.sh --symbol NIFTY --start 2019-01-01 --end 2025-12-31
#   ./backtests/scripts/build_all_caches.sh --symbol NIFTY --start 2023-11-01 --end 2023-11-30

SYMBOL="NIFTY"
START="2019-01-01"
END="2025-12-31"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbol)
      SYMBOL="$2"; shift 2 ;;
    --start)
      START="$2"; shift 2 ;;
    --end)
      END="$2"; shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

PY3_VENV="$ROOT_DIR/.venv/bin/python3"
PY_VENV="$ROOT_DIR/.venv/bin/python"

# Ensure a private venv so installs don't hit system protections
if [[ ! -x "$PY3_VENV" && ! -x "$PY_VENV" ]]; then
  echo "[setup] Creating venv at .venv" >&2
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    python3 -m venv .venv
  else
    echo "[error] python3 not found; install Python 3 first" >&2
    exit 1
  fi
fi

# Prefer python3 inside venv, fallback to python
if [[ -x "$PY3_VENV" ]]; then
  PY="$PY3_VENV"
elif [[ -x "$PY_VENV" ]]; then
  PY="$PY_VENV"
else
  echo "[error] venv exists but no python executable found in .venv/bin" >&2
  exit 1
fi

echo "[setup] Using interpreter: $PY" >&2
echo "[setup] Ensuring dependencies (polars, dateutil) with: $PY -m pip" >&2
"$PY" -m pip install -q --upgrade pip >/dev/null 2>&1 || true
"$PY" -m pip install -q -r backtests/requirements.txt || {
  echo "[error] Failed to install dependencies into .venv" >&2
  exit 1
}

export PYTHONPATH="${ROOT_DIR}"

LOG_DIR="$ROOT_DIR/backtests/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/prewarm_${SYMBOL}_${START}_${END}.out"
PID_FILE="$LOG_DIR/prewarm_${SYMBOL}_${START}_${END}.pid"

echo "[run] Building caches: SYMBOL=${SYMBOL} RANGE=${START}..${END}" >&2
set -x
nohup "$PY" backtests/cache/build_seconds_cache.py \
  --symbol "$SYMBOL" \
  --start "$START" \
  --end "$END" \
  > "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
set +x

echo "[ok] Started. PID=$(cat "$PID_FILE")" >&2
echo "[log] tail -f $LOG_FILE" >&2
echo "[ps ] ps -p $(cat "$PID_FILE") -o pid,etime,command" >&2
