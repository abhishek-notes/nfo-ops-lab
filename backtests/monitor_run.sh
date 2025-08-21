#!/usr/bin/env bash
set -euo pipefail
LOG=${1:-backtests/logs/run_all_BANKNIFTY_2019_2025.out}
PIDFILE=${2:-}
echo "[monitor] time=$(date) host=$(hostname)"
if [[ -n "$PIDFILE" && -f "$PIDFILE" ]]; then
  PID=$(cat "$PIDFILE")
  if ps -p "$PID" > /dev/null; then
    echo "[monitor] runner pid=$PID is running"
    ps -o pid,etime,pcpu,pmem,command -p "$PID" || true
  else
    echo "[monitor] runner pid from $PIDFILE not running"
  fi
fi
echo "[monitor] recent results:"
ls -lt backtests/results | head -n 20 || true
echo "[monitor] tail -n 80 of $LOG"
tail -n 80 "$LOG" || true
