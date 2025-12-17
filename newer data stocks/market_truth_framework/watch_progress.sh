#!/bin/bash
# Watch batch processing progress

INPUT_DIR="../data/options_date_packed_FULL_v3_SPOT_ENRICHED"

# Compute expected outputs once (counts <date>/<underlying> folders with parquet files).
if [ -d "$INPUT_DIR" ]; then
  expected_total=$(
    find "$INPUT_DIR" -maxdepth 2 -mindepth 2 -type d \( -name "BANKNIFTY" -o -name "NIFTY" \) \
      -exec sh -c 'ls -1 "$1"/*.parquet >/dev/null 2>&1' _ {} \; -print \
      | wc -l | tr -d ' '
  )
else
  expected_total=0
fi

while true; do
  clear
  echo "================================================================================"
  echo "MARKET TRUTH FRAMEWORK - BATCH PROCESSING MONITOR"
  echo "================================================================================"
  echo ""
  
  # Count completed files
  features_count=$(ls -1 market_truth_data/features/*.parquet 2>/dev/null | wc -l | tr -d ' ')
  bursts_count=$(ls -1 market_truth_data/bursts/*.parquet 2>/dev/null | wc -l | tr -d ' ')
  
  echo "📊 Progress:"
  if [ "$expected_total" -gt 0 ]; then
    echo "  Features files: $features_count / $expected_total"
  else
    echo "  Features files: $features_count"
  fi
  echo "  Bursts files:   $bursts_count"
  echo ""
  
  # Calculate percentage
  total_expected=$expected_total
  if [ "$total_expected" -gt 0 ]; then
    percent=$((features_count * 100 / total_expected))
  else
    percent=0
  fi
  
  # Progress bar
  filled=$((percent / 2))
  empty=$((50 - filled))
  bar=$(printf "%${filled}s" | tr ' ' '█')
  empty_bar=$(printf "%${empty}s" | tr ' ' '░')
  
  echo "  [$bar$empty_bar] $percent%"
  echo ""
  
  # Show latest files
  echo "📁 Latest processed:"
  ls -lt market_truth_data/features/*.parquet 2>/dev/null | head -5 | awk '{print "  " $9 " (" $5 ")"}'
  echo ""
  
  # Show log tail
  echo "📝 Recent log activity:"
  if [ -f preprocessing/batch_processing.log ]; then
    tail -5 preprocessing/batch_processing.log | sed 's/^/  /'
  else
    echo "  (log not found)"
  fi
  
  echo ""
  echo "Press Ctrl+C to exit | Refreshing every 3 seconds..."
  
  sleep 3
done
