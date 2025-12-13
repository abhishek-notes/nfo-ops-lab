#!/usr/bin/env python3
"""
Batch Processor - Process All Trading Days
===========================================

Runs core_preprocessor.py on all available trading days.
"""

from pathlib import Path
from datetime import datetime
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed


def get_available_dates(data_dir: Path, underlying: str):
    """Get all available trading dates"""
    dates = []
    
    for date_dir in sorted(data_dir.glob("*")):
        if not date_dir.is_dir() or "1970" in date_dir.name:
            continue
        
        underlying_dir = date_dir / underlying
        if underlying_dir.exists():
            files = list(underlying_dir.glob("*.parquet"))
            if files:
                dates.append(date_dir.name)
    
    return sorted(dates)


def process_single_day(date: str, underlying: str, data_dir: str, output_dir: str):
    """Process a single day (for parallel execution)"""
    cmd = [
        'python',
        'core_preprocessor.py',
        '--date', date,
        '--underlying', underlying,
        '--data-dir', data_dir,
        '--output-dir', output_dir
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 min timeout per day
        )
        
        if result.returncode == 0:
            return {'date': date, 'status': 'success', 'output': result.stdout}
        else:
            return {'date': date, 'status': 'error', 'output': result.stderr}
    
    except subprocess.TimeoutExpired:
        return {'date': date, 'status': 'timeout', 'output': 'Processing timeout'}
    except Exception as e:
        return {'date': date, 'status': 'error', 'output': str(e)}


def main():
    print("="*80)
    print("MARKET TRUTH FRAMEWORK - BATCH PROCESSOR")
    print("="*80)
    
    data_dir = Path("../../data/options_date_packed_FULL_v3_SPOT_ENRICHED")
    output_dir = Path("../market_truth_data")
    
    # Process both underlyings
    for underlying in ['BANKNIFTY', 'NIFTY']:
        print(f"\n{'='*80}")
        print(f"Processing {underlying}")
        print(f"{'='*80}")
        
        # Get available dates
        dates = get_available_dates(data_dir, underlying)
        print(f"\nFound {len(dates)} trading days")
        
        if not dates:
            print(f"❌ No data found for {underlying}")
            continue
        
        # Process in parallel (4 workers)
        start_time = time.time()
        results = []
        
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    process_single_day,
                    date,
                    underlying,
                    str(data_dir),
                    str(output_dir)
                ): date
                for date in dates
            }
            
            for i, future in enumerate(as_completed(futures), 1):
                date = futures[future]
                result = future.result()
                results.append(result)
                
                status_icon = "✓" if result['status'] == 'success' else "✗"
                print(f"  [{i}/{len(dates)}] {status_icon} {date} ({result['status']})")
        
        elapsed = time.time() - start_time
        
        # Summary
        successes = sum(1 for r in results if r['status'] == 'success')
        failures = sum(1 for r in results if r['status'] != 'success')
        
        print(f"\n{'='*80}")
        print(f"SUMMARY - {underlying}")
        print(f"{'='*80}")
        print(f"  Success: {successes}/{len(dates)}")
        print(f"  Failures: {failures}/{len(dates)}")
        print(f"  Time: {elapsed/60:.1f} minutes")
        print(f"  Avg: {elapsed/len(dates):.1f}s per day")
        
        # Show failures if any
        if failures > 0:
            print(f"\nFailed dates:")
            for r in results:
                if r['status'] != 'success':
                    print(f"  - {r['date']}: {r['status']}")
    
    print(f"\n{'='*80}")
    print("✓ BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
