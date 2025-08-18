#!/usr/bin/env python3
"""
Analyze the discrepancy between the original script's count of "processed" files
and the actual many-to-one relationship.
"""
import os
import glob
from pathlib import Path
from collections import defaultdict
import re

print("Analyzing processing discrepancy...")
print("=" * 80)

# Get all raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
print(f"Total raw files: {len(raw_files)}")

# Get all packed files
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))
print(f"Total packed files: {len(packed_files)}")

# Build the mapping exactly as the original script does
packed_mapping = defaultdict(set)

for pf in packed_files:
    path_str = str(pf)
    
    # Extract symbol
    if 'BANKNIFTY' in path_str:
        symbol = 'BANKNIFTY'
    else:
        symbol = 'NIFTY'
    
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
    
    packed_mapping[(symbol, strike, opt_type)].add(pf)

print(f"\nUnique (symbol, strike, opt_type) combinations in packed mapping: {len(packed_mapping)}")

# Now check each raw file using the same logic as the original script
processed_files = set()
unprocessed_files = []

for rf in raw_files:
    basename = os.path.basename(rf).lower()
    found = False
    
    # Determine symbol
    if basename.startswith('banknifty'):
        symbol = 'BANKNIFTY'
        rest = basename[9:]
    elif basename.startswith('nifty'):
        symbol = 'NIFTY'
        rest = basename[5:]
    else:
        unprocessed_files.append(rf)
        continue
    
    # Determine option type
    if rest.endswith('ce.parquet'):
        opt_type = 'CE'
        core = rest[:-10]
    elif rest.endswith('pe.parquet'):
        opt_type = 'PE'
        core = rest[:-10]
    else:
        unprocessed_files.append(rf)
        continue
    
    # Extract strike from trailing digits
    digits_match = re.search(r'(\d+)$', core)
    if not digits_match:
        unprocessed_files.append(rf)
        continue
    
    digits = digits_match.group(1)
    
    # Try different strike interpretations (same as original)
    strikes_to_try = []
    
    if symbol == 'BANKNIFTY':
        if len(digits) >= 5:
            strikes_to_try.append(int(digits[-5:]))
        if len(digits) >= 6:
            strikes_to_try.append(int(digits[-6:]))
        if len(digits) >= 4:
            strikes_to_try.append(int(digits[-4:]))
    else:
        if len(digits) >= 5:
            strikes_to_try.append(int(digits[-5:]))
        if len(digits) >= 4:
            strikes_to_try.append(int(digits[-4:]))
    
    # Check if any of these strikes were packed
    for strike in strikes_to_try:
        if (symbol, strike, opt_type) in packed_mapping:
            processed_files.add(rf)
            found = True
            break
    
    if not found:
        unprocessed_files.append(rf)

print(f"\nUsing original script logic:")
print(f"Processed files: {len(processed_files)}")
print(f"Unprocessed files: {len(unprocessed_files)}")
print(f"Total: {len(processed_files) + len(unprocessed_files)}")

# Now let's understand the many-to-one relationship better
print("\n" + "=" * 80)
print("UNDERSTANDING THE MANY-TO-ONE RELATIONSHIP")
print("=" * 80)

# Count how many raw files map to each (symbol, strike, opt_type)
raw_to_strike_mapping = defaultdict(list)

for rf in processed_files:
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
    
    # Extract strike
    digits_match = re.search(r'(\d+)$', core)
    if digits_match:
        digits = digits_match.group(1)
        
        # Use the same logic to find the strike that matched
        strikes_to_try = []
        if symbol == 'BANKNIFTY':
            if len(digits) >= 5:
                strikes_to_try.append(int(digits[-5:]))
            if len(digits) >= 6:
                strikes_to_try.append(int(digits[-6:]))
            if len(digits) >= 4:
                strikes_to_try.append(int(digits[-4:]))
        else:
            if len(digits) >= 5:
                strikes_to_try.append(int(digits[-5:]))
            if len(digits) >= 4:
                strikes_to_try.append(int(digits[-4:]))
        
        for strike in strikes_to_try:
            if (symbol, strike, opt_type) in packed_mapping:
                raw_to_strike_mapping[(symbol, strike, opt_type)].append(rf)
                break

# Count packed files per (symbol, strike, opt_type)
packed_files_per_strike = {}
for key, files in packed_mapping.items():
    packed_files_per_strike[key] = len(files)

print(f"\nTotal unique (symbol, strike, opt_type) with processed raw files: {len(raw_to_strike_mapping)}")
print(f"Total unique (symbol, strike, opt_type) in packed files: {len(packed_mapping)}")

# Show the distribution of packed files per strike
packed_distribution = defaultdict(int)
for count in packed_files_per_strike.values():
    packed_distribution[count] += 1

print("\nDistribution of PACKED files per (symbol, strike, opt_type):")
for num_files in sorted(packed_distribution.keys()):
    print(f"  {num_files} packed files: {packed_distribution[num_files]} strike combinations")

# Calculate total packed files from this
total_packed_calculated = sum(count * combinations for count, combinations in packed_distribution.items())
print(f"\nTotal packed files (calculated): {total_packed_calculated}")

# Show some examples of strikes with multiple packed files
print("\n" + "=" * 80)
print("EXAMPLES OF STRIKES WITH MULTIPLE PACKED FILES")
print("=" * 80)

examples_shown = 0
for key, files in sorted(packed_mapping.items()):
    if len(files) > 1 and examples_shown < 5:
        symbol, strike, opt_type = key
        print(f"\n{symbol} Strike={strike} {opt_type}: {len(files)} packed files")
        # Extract expiries from these files
        expiries = set()
        for f in files:
            expiry_match = re.search(r'exp=(\d{4}-\d{2}-\d{2})', str(f))
            if expiry_match:
                expiries.add(expiry_match.group(1))
        print(f"  Expiries: {sorted(expiries)}")
        examples_shown += 1

# Summary of the issue
print("\n" + "=" * 80)
print("SUMMARY OF THE DISCREPANCY")
print("=" * 80)
print(f"\nThe original script counts {len(processed_files)} raw files as 'processed'")
print(f"This is because it finds {len(raw_to_strike_mapping)} unique (symbol, strike, opt_type) combinations")
print(f"that exist in the packed files.")
print(f"\nHowever, there are {len(packed_files)} packed files because:")
print(f"- Each (symbol, strike, opt_type) can have multiple expiries")
print(f"- The packed files are organized by (symbol, expiry, opt_type, strike)")
print(f"- Multiple raw files with the same strike but different dates get consolidated")
print(f"  into packed files organized by expiry date")

# Calculate the average expiries per strike
total_expiries = sum(len(files) for files in packed_mapping.values())
avg_expiries = total_expiries / len(packed_mapping) if len(packed_mapping) > 0 else 0
print(f"\nAverage expiries per (symbol, strike, opt_type): {avg_expiries:.2f}")