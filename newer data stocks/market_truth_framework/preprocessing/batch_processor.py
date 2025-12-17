#!/usr/bin/env python3
"""
Batch Processor - Process All Trading Days
===========================================

Runs core_preprocessor.py on all available trading days.
"""

from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os


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


def process_single_day(
    date: str,
    underlying: str,
    data_dir: str,
    output_dir: str,
    overwrite: bool,
    polars_threads: int | None,
):
    """Process a single day (for parallel execution)"""
    output_root = Path(output_dir)
    features_file = output_root / "features" / f"features_{underlying}_{date}.parquet"

    if features_file.exists() and not overwrite:
        return {"date": date, "status": "skipped", "output": "Already processed"}

    try:
        if polars_threads is not None and polars_threads > 0:
            os.environ["POLARS_MAX_THREADS"] = str(polars_threads)

        from core_preprocessor import preprocess_day  # import after POLARS_MAX_THREADS is set

        preprocess_day(date, underlying, Path(data_dir), output_root)
        if features_file.exists():
            return {"date": date, "status": "success", "output": "OK"}
        return {"date": date, "status": "error", "output": "No output written"}
    except Exception as e:
        return {"date": date, "status": "error", "output": str(e)}


def main():
    print("="*80)
    print("MARKET TRUTH FRAMEWORK - BATCH PROCESSOR")
    print("="*80)
    
    base_dir = Path(__file__).resolve().parent
    data_dir = (base_dir / "../../data/options_date_packed_FULL_v3_SPOT_ENRICHED").resolve()
    output_dir = (base_dir / "../market_truth_data").resolve()

    import argparse

    parser = argparse.ArgumentParser(description="Batch process all days for both underlyings")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    parser.add_argument(
        "--polars-threads",
        type=int,
        default=0,
        help="Polars threads per worker (0=auto; recommended 1 when --workers > 1)",
    )
    args = parser.parse_args()

    polars_threads: int | None
    if args.polars_threads and args.polars_threads > 0:
        polars_threads = args.polars_threads
    else:
        polars_threads = 1 if args.workers > 1 else None
    
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
        
        def run_with_executor(executor_cls, max_workers: int):
            with executor_cls(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        process_single_day,
                        date,
                        underlying,
                        str(data_dir),
                        str(output_dir),
                        args.overwrite,
                        polars_threads,
                    ): date
                    for date in dates
                }

                for i, future in enumerate(as_completed(futures), 1):
                    date = futures[future]
                    result = future.result()
                    results.append(result)

                    status_icon = "✓" if result["status"] == "success" else ("-" if result["status"] == "skipped" else "✗")
                    print(f"  [{i}/{len(dates)}] {status_icon} {date} ({result['status']})")

        try:
            run_with_executor(ProcessPoolExecutor, args.workers)
        except PermissionError as e:
            # In this environment ProcessPoolExecutor may be blocked. Thread parallelism can also
            # be unstable with heavy Polars workloads, so default to sequential processing.
            print(f\"  ⚠️  ProcessPoolExecutor not permitted ({e}); falling back to ThreadPoolExecutor (sequential)\")
            run_with_executor(ThreadPoolExecutor, 1)
        
        elapsed = time.time() - start_time
        
        # Summary
        successes = sum(1 for r in results if r["status"] == "success")
        skipped = sum(1 for r in results if r["status"] == "skipped")
        failures = sum(1 for r in results if r["status"] not in ("success", "skipped"))
        
        print(f"\n{'='*80}")
        print(f"SUMMARY - {underlying}")
        print(f"{'='*80}")
        print(f"  Success: {successes}/{len(dates)}")
        print(f"  Skipped: {skipped}/{len(dates)}")
        print(f"  Failures: {failures}/{len(dates)}")
        print(f"  Time: {elapsed/60:.1f} minutes")
        print(f"  Avg: {elapsed/len(dates):.1f}s per day")
        
        # Show failures if any
        if failures > 0:
            print(f"\nFailed dates:")
            for r in results:
                if r["status"] not in ("success", "skipped"):
                    print(f"  - {r['date']}: {r['status']}")
    
    print(f"\n{'='*80}")
    print("✓ BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
