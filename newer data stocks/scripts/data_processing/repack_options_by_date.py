#!/usr/bin/env python3
"""
Repack Options Data by Date - Optimized for Backtesting

This script transforms per-contract parquet files into date-partitioned format.
Current: One file per contract with multiple days of data
Target: One partition per (date, underlying) with all strikes

Author: Based on reference chat and performance requirements
"""

from __future__ import annotations

import argparse
import re
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import polars as pl
import pyarrow.dataset as ds

import polars as pl
import pyarrow.dataset as ds

# Updated regex to handle both formats:
# Format 1: {underlying}{YY}{MONTH_NAME}{strike}{ce|pe} - e.g., banknifty24dec49000pe
# Format 2: {underlying}{YY}{M}{DD}{strike}{ce|pe} - e.g., nifty2520623950ce
FILENAME_REGEX_MONTHNAME = re.compile(
    r"([a-z]+)(\d{2})([a-z]{3})(\d+)(ce|pe)", 
    re.IGNORECASE
)
FILENAME_REGEX_NUMERIC = re.compile(
    r"([a-z]+)(\d{2})(\d{1,2})(\d{2})(\d+)(ce|pe)", 
    re.IGNORECASE
)

# Month name to number mapping
MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}


@dataclass
class ParsedFilename:
    """Metadata extracted from filename"""
    underlying: str
    year: int
    month: int  
    day: int
    strike: int
    opt_type: str  # 'CE' or 'PE'
    
    @property
    def expiry_date(self) -> str:
        """Format as YYYY-MM-DD"""
        return f"20{self.year:02d}-{self.month:02d}-{self.day:02d}"


def parse_filename(filename: str) -> Optional[ParsedFilename]:
    """
    Parse option contract filename to extract metadata.
    
    Supports two formats:
    1. Month name format: banknifty24dec49000pe -> BANKNIFTY, 2024-12-?? expiry, strike 49000, PE
    2. Numeric format: nifty2520623950ce -> NIFTY, 2025-02-06 expiry, strike 23950, CE
    
    Note: For month name format, we don't have the exact day, so we estimate expiry day.
    """
    stem = Path(filename).stem.lower()
    
    # Try month name format first (more common in your sample data)
    match = FILENAME_REGEX_MONTHNAME.match(stem)
    if match:
        underlying, yy, month_str, strike, opt_type = match.groups()
        
        month_num = MONTH_MAP.get(month_str.lower())
        if not month_num:
            return None
        
        # Estimate expiry day based on typical option expiry
        # Bank Nifty: typically last Wednesday of month
        # Nifty: typically last Thursday of month
        # For now, we'll use day 1 and let user refine if needed
        day = 1  # Placeholder - can be refined with actual expiry calendar
        
        return ParsedFilename(
            underlying=underlying.upper(),
            year=int(yy),
            month=month_num,
            day=day,
            strike=int(strike),
            opt_type=opt_type.upper()
        )
    
    # Try numeric format
    match = FILENAME_REGEX_NUMERIC.match(stem)
    if match:
        underlying, yy, m, dd, strike, opt_type = match.groups()
        
        return ParsedFilename(
            underlying=underlying.upper(),
            year=int(yy),
            month=int(m),
            day=int(dd),
            strike=int(strike),
            opt_type=opt_type.upper()
        )



def process_file_batch(
    file_paths: List[Path],
    output_dir: Path,
    keep_columns: Optional[List[str]] = None,
    compression: str = "zstd"
) -> dict:
    """
    Process a batch of option files and write to date-partitioned format.
    
    Args:
        file_paths: List of parquet file paths to process
        output_dir: Base output directory for partitioned data
        keep_columns: List of columns to keep (None = keep all)
        compression: Parquet compression codec
        
    Returns:
        Dictionary with processing statistics
    """
    stats = {
        "files_processed": 0,
        "files_skipped": 0,
        "total_rows": 0,
        "errors": []
    }
    
    # Default columns optimized for backtesting
    if keep_columns is None:
        keep_columns = [
            # Core data
            "timestamp", "price", "volume", "qty",
            # Order book
            "bp0", "sp0", "bq0", "sq0",
            "bp1", "sp1", "bq1", "sq1",
            "bp2", "sp2", "bq2", "sq2",
            "bp3", "sp3", "bq3", "sq3",  
            "bp4", "sp4", "bq4", "sq4",
            # OI and OHLC
            "oi", "oiHigh", "oiLow",
            "open", "high", "low", "close",
            # Metadata from existing file (if present)
            "symbol", "opt_type", "strike", "year", "month"
        ]
    
    for file_path in file_paths:
        try:
            # Parse metadata from filename
            parsed = parse_filename(file_path.name)
            if not parsed:
                stats["files_skipped"] += 1
                stats["errors"].append(f"Could not parse filename: {file_path.name}")
                continue
            
            # Read the file
            df = pl.read_parquet(file_path)
            
            # Filter to columns that exist
            existing_cols = set(df.columns)
            cols_to_select = [c for c in keep_columns if c in existing_cols]
            
            if "timestamp" not in existing_cols:
                stats["files_skipped"] += 1
                stats["errors"].append(f"No timestamp column: {file_path.name}")
                continue
            
            # Transform the data
            df = (
                df.select(["timestamp"] + [c for c in cols_to_select if c != "timestamp"])
                .with_columns([
                    # Add metadata from filename
                    pl.lit(parsed.underlying).alias("underlying"),
                    pl.lit(parsed.strike).cast(pl.Int32).alias("strike"),
                    pl.lit(parsed.opt_type).cast(pl.Categorical).alias("opt_type"),
                    pl.lit(parsed.expiry_date).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("expiry_date"),
                    
                    # Extract date for partitioning
                    pl.col("timestamp").dt.date().alias("date"),
                    
                    # Ensure consistent types for numeric columns
                    pl.col("price").cast(pl.Float64),
                ])
            )
            
            # Cast order book columns to Float64 if they exist
            for col in ["bp0", "sp0", "bp1", "sp1", "bp2", "sp2", "bp3", "sp3", "bp4", "sp4"]:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
            
            # Drop rows without valid timestamp or expiry_date
            df = df.drop_nulls(subset=["timestamp", "price", "expiry_date"])
            
            if df.is_empty():
                stats["files_skipped"] += 1
                continue
            
            # Write to partitioned dataset
            # PyArrow will create /date=YYYY-MM-DD/underlying=NIFTY/ structure
            ds.write_dataset(
                df.to_arrow(),
                base_dir=str(output_dir),
                format="parquet",
                partitioning=["date", "underlying"],
                existing_data_behavior="overwrite_or_ignore",
                file_options=ds.ParquetFileFormat().make_write_options(compression=compression),
                basename_template=f"part-{file_path.stem}-{{i}}.parquet"
            )
            
            stats["files_processed"] += 1
            stats["total_rows"] += len(df)
            
        except Exception as e:
            stats["files_skipped"] += 1
            stats["errors"].append(f"Error processing {file_path.name}: {str(e)}")
    
    return stats


def repack_options_data(
    input_dir: Path,
    output_dir: Path,
    batch_size: int = 1000,
    workers: int = 8,
    keep_columns: Optional[List[str]] = None,
    sample: Optional[int] = None
):
    """
    Main function to repack option data files.
    
    Args:
        input_dir: Directory containing raw parquet files
        output_dir: Directory for partitioned output
        batch_size: Files per worker batch
        workers: Number of parallel workers
        keep_columns: Columns to retain (None = use defaults)
        sample: Process only first N files for testing (None = all)
    """
    print("=" * 60)
    print("Data Repacking Script - Options to Date-Partitioned Format")
    print("=" * 60)
    
    # Find all parquet files
    print(f"\nScanning input directory: {input_dir}")
    all_files = list(input_dir.glob("*.parquet"))
    
    if sample:
        all_files = all_files[:sample]
        print(f"Sample mode: Processing only {sample} files")
    
    total_files = len(all_files)
    print(f"Found {total_files} parquet files")
    
    if total_files == 0:
        print("No files to process. Exiting.")
        return
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Split into batches
    batches = [
        all_files[i:i + batch_size]
        for i in range(0, total_files, batch_size)
    ]
    
    print(f"\nProcessing in {len(batches)} batches with {workers} workers")
    print(f"Output directory: {output_dir}\n")
    
    # Process in parallel
    start_time = time.time()
    total_stats = {
        "files_processed": 0,
        "files_skipped": 0,
        "total_rows": 0,
        "errors": []
    }
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_file_batch,
                batch,
                output_dir,
                keep_columns
            ): i for i, batch in enumerate(batches)
        }
        
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                stats = future.result()
                total_stats["files_processed"] += stats["files_processed"]
                total_stats["files_skipped"] += stats["files_skipped"]
                total_stats["total_rows"] += stats["total_rows"]
                total_stats["errors"].extend(stats["errors"])
                
                print(f"Batch {batch_idx + 1}/{len(batches)} complete: "
                      f"{stats['files_processed']} processed, "
                      f"{stats['files_skipped']} skipped, "
                      f"{stats['total_rows']:,} rows")
                      
            except Exception as e:
                print(f"Batch {batch_idx + 1} failed: {e}")
    
    elapsed = time.time() - start_time
    
    # Print summary
    print("\n" + "=" * 60)
    print("REPACKING COMPLETE")
    print("=" * 60)
    print(f"Files processed: {total_stats['files_processed']}/{total_files}")
    print(f"Files skipped: {total_stats['files_skipped']}")
    print(f"Total rows written: {total_stats['total_rows']:,}")
    print(f"Time elapsed: {elapsed:.2f} seconds")
    print(f"Throughput: {total_stats['total_rows'] / elapsed:,.0f} rows/sec")
    
    if total_stats["errors"]:
        print(f"\nErrors encountered: {len(total_stats['errors'])}")
        print("First 10 errors:")
        for error in total_stats["errors"][:10]:
            print(f"  - {error}")
    
    print(f"\nOutput written to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Repack option data from per-contract to date-partitioned format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test on small sample
  python repack_options_by_date.py \\
    --input-dir "new 2025 data/Processed Samples/raw options data samples parquet" \\
    --output-dir "test_repacked" \\
    --sample 100
  
  # Full repack with custom settings
  python repack_options_by_date.py \\
    --input-dir /path/to/raw/options \\
    --output-dir /path/to/repacked/options \\
    --workers 16 \\
    --batch-size 2000
        """
    )
    
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing raw option parquet files"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for date-partitioned data"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of files to process per batch (default: 1000)"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers (default: 8)"
    )
    
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Process only first N files for testing (default: all)"
    )
    
    args = parser.parse_args()
    
    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}")
        return 1
    
    repack_options_data(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        workers=args.workers,
        sample=args.sample
    )
    
    return 0


if __name__ == "__main__":
    exit(main())
