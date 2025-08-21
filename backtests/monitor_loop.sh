#!/usr/bin/env bash
# Monitor loop: check run progress periodically and auto-retry failed tasks from logs.
set -euo pipefail
LOG=${1:-backtests/logs/run_all_BANKNIFTY_2019_2025.out}
PIDFILE=${2:-backtests/logs/run_all.pid}
DURATION_MIN=${3:-15}
SLEEP_SEC=${4:-60}

echo "[monitor_loop] start $(date) for ${DURATION_MIN}m (every ${SLEEP_SEC}s)"
ITER=$(( DURATION_MIN * 60 / SLEEP_SEC ))
for i in $(seq 1 ${ITER}); do
  if [[ -f "$PIDFILE" ]]; then PID=$(cat "$PIDFILE"); else PID=""; fi
  if [[ -n "$PID" ]] && ps -p "$PID" > /dev/null; then
    echo "[monitor_loop] $(date) PID=$PID OK (iter $i/${ITER})"
  else
    echo "[monitor_loop] $(date) runner not found; continuing checks"
  fi
  # Print tail
  tail -n 40 "$LOG" || true
  # Count results files
  if [[ -d backtests/results ]]; then
    echo "[monitor_loop] results count=$(ls -1 backtests/results | wc -l | tr -d ' ')"
    ls -1 backtests/results | awk -F'_' '{print $1}' | sort | uniq -c | sort -nr | head -n 10 || true
  fi
  # Retry any failure logs not yet retried
  for lf in backtests/logs/*.log; do
    [[ -e "$lf" ]] || continue
    case "$lf" in
      *run_all*|*schema_audit*|*retried*) continue ;;
    esac
    echo "[monitor_loop] retrying failed task from $lf"
    python3 backtests/retry_failed_from_logs.py "$lf" || true
  done
  sleep "$SLEEP_SEC"
done
echo "[monitor_loop] end $(date)"

