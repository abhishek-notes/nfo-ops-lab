#!/bin/bash
# Watch batch processing progress

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
  echo "  Features files: $features_count / 162 (BANKNIFTY + NIFTY = 81×2)"
  echo "  Bursts files:   $bursts_count"
  echo ""
  
  # Calculate percentage
  total_expected=162
  percent=$((features_count * 100 / total_expected))
  
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
