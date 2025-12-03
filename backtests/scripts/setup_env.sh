#!/usr/bin/env bash
set -euo pipefail

choose_python() {
  # Respect explicit override
  if [ -n "${PYTHON_BIN:-}" ]; then
    echo "$PYTHON_BIN"; return
  fi
  # Try common candidates in order
  for cand in python3.12 python3.11 python3 /usr/bin/python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
      echo "$cand"; return
    fi
  done
  echo "python3"
}

PY=$(choose_python)
echo "Using Python: $($PY -c 'import sys; print(sys.executable)')"
echo "Python version: $($PY -c 'import sys; print(sys.version)')"

# Try to import polars; if missing, install via this interpreter
if ! $PY - << 'PY' >/dev/null 2>&1
import polars
PY
then
  echo "Installing dependencies for $($PY -c 'import sys; print(sys.executable)') ..."
  $PY -m pip install --upgrade pip >/dev/null 2>&1 || true
  $PY -m pip install -r backtests/requirements.txt || exit 1
  if [ -f requirements.txt ]; then
    $PY -m pip install -r requirements.txt || true
  fi
fi

# Final sanity check
$PY - << 'PY'
import polars as pl
from dateutil import tz
print('Polars OK:', pl.__version__)
print('dateutil OK:', tz.gettz('Asia/Kolkata') is not None)
PY

# Persist chosen interpreter for other scripts
echo "$($PY -c 'import sys; print(sys.executable)')" > backtests/scripts/.python_bin
echo "Saved interpreter to backtests/scripts/.python_bin"
