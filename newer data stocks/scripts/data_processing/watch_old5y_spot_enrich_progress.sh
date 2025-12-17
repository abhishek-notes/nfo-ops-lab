#!/bin/bash
set -euo pipefail

OUT_DIR_DEFAULT="/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v3_SPOT_ENRICHED"
LOG_DEFAULT="newer data stocks/logs/old5y_spot_enrich.log"

OUT_DIR="${1:-$OUT_DIR_DEFAULT}"
LOG_FILE="${2:-$LOG_DEFAULT}"

while true; do
  clear
  echo "================================================================================"
  echo "OLD5Y SPOT ENRICH - PROGRESS"
  echo "================================================================================"
  echo ""
  echo "Output dir: $OUT_DIR"
  echo "Log file:   $LOG_FILE"
  echo ""

  bn_count=$(find "$OUT_DIR" -type f -path "*/BANKNIFTY/part-banknifty-0.parquet" 2>/dev/null | wc -l | tr -d ' ')
  nf_count=$(find "$OUT_DIR" -type f -path "*/NIFTY/part-nifty-0.parquet" 2>/dev/null | wc -l | tr -d ' ')

  echo "Files written:"
  echo "  BANKNIFTY: $bn_count"
  echo "  NIFTY:     $nf_count"
  echo ""

  echo "Most recent outputs:"
  find "$OUT_DIR" -type f -name "part-*.parquet" -print0 2>/dev/null | xargs -0 ls -lt 2>/dev/null | head -5 | sed 's/^/  /' || true
  echo ""

  echo "Recent log lines:"
  if [ -f "$LOG_FILE" ]; then
    tail -15 "$LOG_FILE" | sed 's/^/  /'
  else
    echo "  (log not found yet)"
  fi

  echo ""
  echo "Press Ctrl+C to exit | Refreshing every 5 seconds..."
  sleep 5
done

