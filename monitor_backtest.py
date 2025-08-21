#!/usr/bin/env python3
"""Monitor running backtest progress and show stats"""

import time
import subprocess
from pathlib import Path
import polars as pl

def check_process(pid):
    """Check if process is still running"""
    try:
        subprocess.check_call(['ps', '-p', str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except:
        return False

def get_log_tail(log_file, lines=5):
    """Get last N lines from log file"""
    if not Path(log_file).exists():
        return "Log file not yet created..."
    
    try:
        with open(log_file, 'r') as f:
            all_lines = f.readlines()
            return ''.join(all_lines[-lines:])
    except:
        return "Error reading log file"

def check_results(symbol, start, end):
    """Check if results files exist and show summary"""
    trades_file = f"backtests/results/trades_{symbol}_{start}_{end}.parquet"
    summary_file = f"backtests/results/summary_{symbol}_{start}_{end}.parquet"
    
    if Path(trades_file).exists():
        trades = pl.read_parquet(trades_file)
        summary = pl.read_parquet(summary_file)
        
        total_trades = len(trades)
        total_pnl = trades["pnl_pts"].sum()
        win_rate = (trades["exit_reason"] == "target").mean() * 100
        
        print(f"\n{symbol} Results:")
        print(f"  Total Trades: {total_trades}")
        print(f"  Total P&L: {total_pnl:.2f} points")
        print(f"  Win Rate: {win_rate:.1f}%")
        print(f"  Days Processed: {len(summary)}")
        return True
    return False

def main():
    banknifty_pid = 3126
    nifty_pid = 3128
    start_date = "2024-07-01"
    end_date = "2025-07-01"
    
    print("Monitoring ATM Volume Backtest Progress...")
    print("=" * 60)
    
    while True:
        # Check process status
        banknifty_running = check_process(banknifty_pid)
        nifty_running = check_process(nifty_pid)
        
        print(f"\nProcess Status:")
        print(f"  BANKNIFTY (PID {banknifty_pid}): {'RUNNING' if banknifty_running else 'COMPLETED'}")
        print(f"  NIFTY (PID {nifty_pid}): {'RUNNING' if nifty_running else 'COMPLETED'}")
        
        # Show log tails
        print(f"\nBANKNIFTY Log (last 3 lines):")
        print(get_log_tail("backtests/logs/banknifty_2024-07_2025-07.log", 3))
        
        print(f"\nNIFTY Log (last 3 lines):")
        print(get_log_tail("backtests/logs/nifty_2024-07_2025-07.log", 3))
        
        # Check for results
        banknifty_done = check_results("BANKNIFTY", start_date, end_date)
        nifty_done = check_results("NIFTY", start_date, end_date)
        
        # Exit if both completed
        if not banknifty_running and not nifty_running:
            print("\nBoth backtests completed!")
            if banknifty_done and nifty_done:
                print("\nAll results files generated successfully.")
            break
        
        print("\n" + "-" * 60)
        print("Checking again in 30 seconds... (Ctrl+C to stop monitoring)")
        time.sleep(30)

if __name__ == "__main__":
    main()