#!/usr/bin/env python3
"""Quick status check for running backtests"""

import subprocess
from pathlib import Path
import polars as pl
from datetime import datetime

def check_status():
    print(f"ATM Volume Backtest Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Process status
    processes = {
        "BANKNIFTY": 3204,
        "NIFTY": 3245
    }
    
    for symbol, pid in processes.items():
        try:
            subprocess.check_call(['ps', '-p', str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            status = "RUNNING"
        except:
            status = "COMPLETED/STOPPED"
        print(f"{symbol} (PID {pid}): {status}")
    
    print("\nLog Files:")
    logs = {
        "BANKNIFTY": "backtests/logs/banknifty_2024-07_2025-07.log",
        "NIFTY": "backtests/logs/nifty_2024-07_2025-07.log"
    }
    
    for symbol, log_file in logs.items():
        if Path(log_file).exists():
            size = Path(log_file).stat().st_size
            print(f"  {symbol}: {log_file} ({size} bytes)")
            # Get last line
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        last_line = lines[-1].strip()
                        print(f"    Progress: {last_line}")
            except:
                pass
    
    print("\nExpected Results:")
    print("  backtests/results/trades_BANKNIFTY_2024-07-01_2025-07-01.parquet")
    print("  backtests/results/summary_BANKNIFTY_2024-07-01_2025-07-01.parquet")
    print("  backtests/results/trades_NIFTY_2024-07-01_2025-07-01.parquet")
    print("  backtests/results/summary_NIFTY_2024-07-01_2025-07-01.parquet")
    
    # Check if any results exist yet
    print("\nExisting Results:")
    results_dir = Path("backtests/results")
    if results_dir.exists():
        for f in results_dir.glob("*_2024-07-01_2025-07-01.parquet"):
            print(f"  Found: {f}")

if __name__ == "__main__":
    check_status()