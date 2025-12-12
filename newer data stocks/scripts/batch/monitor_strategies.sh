#!/bin/bash

# Monitor strategy execution progress

echo "Monitoring strategy execution..."
echo ""

while true; do
    clear
    echo "=========================================="
    echo "Strategy Execution Monitor"
    echo "=========================================="
    echo ""
    
    # Check if process is running
    if ps aux | grep "run_all_strategies.py" | grep -v grep > /dev/null; then
        echo "Status: ✓ RUNNING"
        echo ""
        
        # Show last 15 lines of output
        echo "Last output:"
        echo "----------"
        tail -15 strategy_execution.log 2>/dev/null || echo "No log file yet"
        echo ""
        
        # Count results
        if [ -d "strategy_results" ]; then
            csv_count=$(ls strategy_results/*.csv 2>/dev/null | wc -l)
            echo "Results generated: $csv_count CSV files"
        else
            echo "Results generated: 0 CSV files"
        fi
    else
        echo "Status: ✗ NOT RUNNING (completed or failed)"
        echo ""
        
        # Show final results
        if [ -d "strategy_results" ]; then
            csv_count=$(ls strategy_results/*.csv 2>/dev/null | wc -l)
            echo "Final results: $csv_count CSV files"
            echo ""
            echo "Files:"
            ls -lh strategy_results/*.csv 2>/dev/null | tail -10
        fi
        
        echo ""
        echo "Last output:"
        echo "----------"
        tail -30 strategy_execution.log 2>/dev/null || echo "No log file"
        
        break
    fi
    
    echo ""
    echo "Press Ctrl+C to stop monitoring"
    sleep 5
done

echo "=========================================="
echo "Monitoring complete"
echo "=========================================="
