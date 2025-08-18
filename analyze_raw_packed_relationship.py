#!/usr/bin/env python3
"""
Analyze the many-to-one relationship between raw and packed files.
Understand why 84,918 raw files map to 76,147 packed files.
"""
import os
import glob
from pathlib import Path
from collections import defaultdict
import re
import pandas as pd

print("Analyzing raw vs packed file relationships...")
print("=" * 80)

# Get all raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
print(f"Total raw files: {len(raw_files)}")

# Get all packed files
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))
print(f"Total packed files: {len(packed_files)}")

# Parse raw files to extract (symbol, strike, opt_type, date)
raw_combinations = []
raw_by_strike = defaultdict(list)  # (symbol, strike, opt_type) -> list of files

for rf in raw_files:
    basename = os.path.basename(rf).lower()
    
    # Determine symbol
    if basename.startswith('banknifty'):
        symbol = 'BANKNIFTY'
        rest = basename[9:]
    elif basename.startswith('nifty'):
        symbol = 'NIFTY'
        rest = basename[5:]
    else:
        continue
    
    # Determine option type
    if rest.endswith('ce.parquet'):
        opt_type = 'CE'
        core = rest[:-10]
    elif rest.endswith('pe.parquet'):
        opt_type = 'PE'
        core = rest[:-10]
    else:
        continue
    
    # Extract strike from trailing digits
    digits_match = re.search(r'(\d+)$', core)
    if not digits_match:
        continue
    
    digits = digits_match.group(1)
    
    # Get the date portion (everything before the strike)
    date_portion = core[:digits_match.start()]
    
    # Try different strike interpretations
    strikes_to_try = []
    if symbol == 'BANKNIFTY':
        if len(digits) >= 5:
            strikes_to_try.append(int(digits[-5:]))
        if len(digits) >= 6:
            strikes_to_try.append(int(digits[-6:]))
    else:
        if len(digits) >= 5:
            strikes_to_try.append(int(digits[-5:]))
        if len(digits) >= 4:
            strikes_to_try.append(int(digits[-4:]))
    
    for strike in strikes_to_try:
        raw_combinations.append({
            'file': rf,
            'symbol': symbol,
            'strike': strike,
            'opt_type': opt_type,
            'date_portion': date_portion,
            'full_core': core
        })
        raw_by_strike[(symbol, strike, opt_type)].append(rf)
        break  # Use the first valid strike

# Parse packed files to extract (symbol, expiry, opt_type, strike)
packed_combinations = []
packed_by_expiry_strike = defaultdict(list)  # (symbol, expiry, opt_type, strike) -> list of files

for pf in packed_files:
    path_str = str(pf)
    
    # Extract symbol
    if 'BANKNIFTY' in path_str:
        symbol = 'BANKNIFTY'
    else:
        symbol = 'NIFTY'
    
    # Extract expiry
    expiry_match = re.search(r'exp=(\d{4}-\d{2}-\d{2})', path_str)
    if expiry_match:
        expiry = expiry_match.group(1)
    else:
        continue
    
    # Extract strike
    strike_match = re.search(r'strike=(\d+)\.parquet', path_str)
    if strike_match:
        strike = int(strike_match.group(1))
    else:
        continue
    
    # Extract option type
    if '/type=CE/' in path_str:
        opt_type = 'CE'
    elif '/type=PE/' in path_str:
        opt_type = 'PE'
    else:
        continue
    
    packed_combinations.append({
        'file': pf,
        'symbol': symbol,
        'expiry': expiry,
        'strike': strike,
        'opt_type': opt_type
    })
    packed_by_expiry_strike[(symbol, expiry, opt_type, strike)].append(pf)

# Analysis 1: Count unique combinations
print("\n" + "=" * 80)
print("UNIQUE COMBINATIONS ANALYSIS")
print("=" * 80)

# Unique (symbol, strike, opt_type) in raw files
raw_unique_strikes = set()
for combo in raw_combinations:
    raw_unique_strikes.add((combo['symbol'], combo['strike'], combo['opt_type']))

print(f"\nUnique (symbol, strike, opt_type) combinations in RAW files: {len(raw_unique_strikes)}")

# Unique (symbol, expiry, opt_type, strike) in packed files
packed_unique_expiry_strikes = set()
for combo in packed_combinations:
    packed_unique_expiry_strikes.add((combo['symbol'], combo['expiry'], combo['opt_type'], combo['strike']))

print(f"Unique (symbol, expiry, opt_type, strike) combinations in PACKED files: {len(packed_unique_expiry_strikes)}")

# Analysis 2: Many-to-one relationship
print("\n" + "=" * 80)
print("MANY-TO-ONE RELATIONSHIP ANALYSIS")
print("=" * 80)

# For each unique (symbol, strike, opt_type), count how many raw files exist
raw_file_counts = defaultdict(int)
for combo in raw_combinations:
    key = (combo['symbol'], combo['strike'], combo['opt_type'])
    raw_file_counts[key] += 1

# Distribution of raw files per (symbol, strike, opt_type)
distribution = defaultdict(int)
for count in raw_file_counts.values():
    distribution[count] += 1

print("\nDistribution of raw files per unique (symbol, strike, opt_type):")
for num_files in sorted(distribution.keys()):
    print(f"  {num_files} raw files: {distribution[num_files]} combinations")

# Calculate total expected raw files if all were processed
total_expected_raw = sum(count * combinations for count, combinations in distribution.items())
print(f"\nTotal raw files (calculated): {total_expected_raw}")

# Analysis 3: Sample many-to-one mappings
print("\n" + "=" * 80)
print("SAMPLE MANY-TO-ONE MAPPINGS")
print("=" * 80)

# Find some examples where multiple raw files map to one strike
examples_found = 0
for key, files in sorted(raw_by_strike.items()):
    if len(files) > 1 and examples_found < 5:
        symbol, strike, opt_type = key
        print(f"\n{symbol} Strike={strike} {opt_type}: {len(files)} raw files")
        for i, f in enumerate(files[:5]):  # Show up to 5 files
            basename = os.path.basename(f)
            print(f"  {i+1}. {basename}")
        if len(files) > 5:
            print(f"  ... and {len(files) - 5} more files")
        examples_found += 1

# Analysis 4: Check which raw strikes are missing from packed files
print("\n" + "=" * 80)
print("MISSING STRIKES ANALYSIS")
print("=" * 80)

# Get all unique (symbol, strike, opt_type) from packed files (ignoring expiry)
packed_strikes = set()
for combo in packed_combinations:
    packed_strikes.add((combo['symbol'], combo['strike'], combo['opt_type']))

# Find raw strikes not in packed
missing_strikes = raw_unique_strikes - packed_strikes
print(f"\nUnique (symbol, strike, opt_type) in RAW but not in PACKED: {len(missing_strikes)}")

# Show some examples
print("\nSample missing strikes:")
for i, (symbol, strike, opt_type) in enumerate(sorted(missing_strikes)[:10]):
    num_files = len(raw_by_strike[(symbol, strike, opt_type)])
    print(f"  {i+1}. {symbol} Strike={strike} {opt_type} ({num_files} raw files)")

# Analysis 5: Date patterns in raw files
print("\n" + "=" * 80)
print("DATE PATTERNS IN RAW FILES")
print("=" * 80)

# Analyze the date portions
date_pattern_counts = defaultdict(int)
for combo in raw_combinations:
    date_portion = combo['date_portion']
    
    # Categorize date patterns
    if re.search(r'\d{2}[a-z]{3}\d{2}', date_portion):
        pattern = 'DDMMMYY'
    elif re.search(r'\d{4}', date_portion):
        pattern = 'CONTAINS_4DIGIT'
    elif re.search(r'\d{6}', date_portion):
        pattern = 'CONTAINS_6DIGIT'
    elif len(date_portion) == 0:
        pattern = 'NO_DATE'
    else:
        pattern = 'OTHER'
    
    date_pattern_counts[pattern] += 1

print("\nDate pattern distribution in raw files:")
for pattern, count in sorted(date_pattern_counts.items(), key=lambda x: -x[1]):
    print(f"  {pattern}: {count} occurrences")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\nTotal raw files: {len(raw_files)}")
print(f"Total parsed raw combinations: {len(raw_combinations)}")
print(f"Unique (symbol, strike, opt_type) in raw: {len(raw_unique_strikes)}")
print(f"\nTotal packed files: {len(packed_files)}")
print(f"Unique (symbol, expiry, strike, opt_type) in packed: {len(packed_unique_expiry_strikes)}")
print(f"\nRaw strikes missing from packed: {len(missing_strikes)}")

# Calculate the many-to-one ratio
if len(packed_unique_expiry_strikes) > 0:
    avg_raw_per_packed = len(raw_combinations) / len(packed_unique_expiry_strikes)
    print(f"\nAverage raw files per packed file: {avg_raw_per_packed:.2f}")

# Write detailed analysis to file
output_file = "raw_packed_relationship_analysis.txt"
with open(output_file, 'w') as f:
    f.write("RAW TO PACKED FILE RELATIONSHIP ANALYSIS\n")
    f.write("=" * 80 + "\n\n")
    
    f.write(f"Total raw files: {len(raw_files)}\n")
    f.write(f"Total packed files: {len(packed_files)}\n")
    f.write(f"Unique (symbol, strike, opt_type) in raw: {len(raw_unique_strikes)}\n")
    f.write(f"Unique (symbol, expiry, strike, opt_type) in packed: {len(packed_unique_expiry_strikes)}\n")
    
    f.write("\n\nMISSING STRIKES (not in packed files):\n")
    f.write("=" * 60 + "\n")
    for symbol, strike, opt_type in sorted(missing_strikes):
        num_files = len(raw_by_strike[(symbol, strike, opt_type)])
        f.write(f"{symbol}\t{strike}\t{opt_type}\t({num_files} raw files)\n")
        # List the actual files
        for rf in raw_by_strike[(symbol, strike, opt_type)][:5]:
            f.write(f"  - {os.path.basename(rf)}\n")
        if num_files > 5:
            f.write(f"  ... and {num_files - 5} more\n")

print(f"\nDetailed analysis written to: {output_file}")