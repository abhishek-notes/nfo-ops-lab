#!/usr/bin/env python3
"""
Find which raw files failed to process during packing
"""
import os
from pathlib import Path
import glob

# Get all raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
print(f"Total raw files: {len(raw_files)}")

# Get all packed files
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))
print(f"Total packed files: {len(packed_files)}")

# Extract strike info from packed files to build a set of processed files
processed = set()
for pf in packed_files:
    # Extract symbol, strike, opt_type from path
    parts = str(pf).split('/')
    for part in parts:
        if 'strike=' in part:
            strike = int(part.replace('strike=', '').replace('.parquet', ''))
        if 'type=' in part:
            opt_type = part.replace('type=', '')
    
    # Get symbol from path
    if 'BANKNIFTY' in str(pf):
        symbol = 'BANKNIFTY'
    else:
        symbol = 'NIFTY'
    
    # Add to processed set
    for rf in raw_files:
        base = os.path.basename(rf).lower()
        if str(strike) in base and opt_type.lower() in base and symbol.lower() in base:
            processed.add(rf)

# Find unprocessed files
unprocessed = set(raw_files) - processed
print(f"\nUnprocessed files: {len(unprocessed)}")

if unprocessed:
    print("\nSample of unprocessed files:")
    sample = sorted(list(unprocessed))[:20]
    for f in sample:
        print(f"  {f}")
    
    # Analyze patterns
    print("\nAnalyzing unprocessed file patterns:")
    patterns = {}
    for f in unprocessed:
        base = os.path.basename(f)
        # Try to identify pattern
        if 'banknifty' in base.lower():
            symbol = 'BANKNIFTY'
        elif 'nifty' in base.lower():
            symbol = 'NIFTY'
        else:
            symbol = 'UNKNOWN'
        
        # Check for date patterns
        if any(month in base.lower() for month in ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']):
            pattern = f"{symbol}_MONTHLY_NAME"
        elif len(base) > 30:
            pattern = f"{symbol}_LONG_FILENAME"
        else:
            pattern = f"{symbol}_OTHER"
        
        patterns[pattern] = patterns.get(pattern, 0) + 1
    
    print("\nPattern counts:")
    for pattern, count in sorted(patterns.items(), key=lambda x: -x[1]):
        print(f"  {pattern}: {count}")