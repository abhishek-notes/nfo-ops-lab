#!/usr/bin/env python3
"""
Detailed analysis of which raw files failed to process during packing
"""
import os
import glob
from pathlib import Path
from collections import defaultdict
import re

# Get all raw files
raw_files = glob.glob("./data/raw/options/*.parquet")
print(f"Total raw files: {len(raw_files)}")

# Get all packed files
packed_files = list(Path("./data/packed/options").rglob("*.parquet"))
print(f"Total packed files: {len(packed_files)}")

# Build a mapping of what was successfully packed
# Key: (symbol, strike, opt_type) -> set of raw filenames
packed_mapping = defaultdict(set)

for pf in packed_files:
    # Extract details from path like:
    # ./data/packed/options/BANKNIFTY/202301/exp=2023-01-05/type=CE/strike=41000.parquet
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

# Now check each raw file
processed_files = set()
unprocessed_files = []
pattern_counts = defaultdict(int)

for rf in raw_files:
    basename = os.path.basename(rf).lower()
    found = False
    
    # Try to parse the filename
    # Examples:
    # banknifty1941128500ce.parquet
    # nifty23n0917000pe.parquet
    
    # Determine symbol
    if basename.startswith('banknifty'):
        symbol = 'BANKNIFTY'
        rest = basename[9:]  # Remove 'banknifty'
    elif basename.startswith('nifty'):
        symbol = 'NIFTY'
        rest = basename[5:]  # Remove 'nifty'
    else:
        unprocessed_files.append((rf, "UNKNOWN_SYMBOL"))
        pattern_counts["UNKNOWN_SYMBOL"] += 1
        continue
    
    # Determine option type
    if rest.endswith('ce.parquet'):
        opt_type = 'CE'
        core = rest[:-10]  # Remove 'ce.parquet'
    elif rest.endswith('pe.parquet'):
        opt_type = 'PE'
        core = rest[:-10]  # Remove 'pe.parquet'
    else:
        unprocessed_files.append((rf, f"{symbol}_NO_OPT_TYPE"))
        pattern_counts[f"{symbol}_NO_OPT_TYPE"] += 1
        continue
    
    # Extract strike from trailing digits
    digits_match = re.search(r'(\d+)$', core)
    if not digits_match:
        unprocessed_files.append((rf, f"{symbol}_NO_STRIKE"))
        pattern_counts[f"{symbol}_NO_STRIKE"] += 1
        continue
    
    digits = digits_match.group(1)
    
    # Try different strike interpretations
    strikes_to_try = []
    
    if symbol == 'BANKNIFTY':
        # For BANKNIFTY, try 5-digit, 6-digit, 4-digit
        if len(digits) >= 5:
            strikes_to_try.append(int(digits[-5:]))
        if len(digits) >= 6:
            strikes_to_try.append(int(digits[-6:]))
        if len(digits) >= 4:
            strikes_to_try.append(int(digits[-4:]))
    else:
        # For NIFTY, try 5-digit, 4-digit
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
        # Categorize the failure
        if len(digits) > 10:
            reason = f"{symbol}_VERY_LONG_DIGITS_{len(digits)}"
        elif len(strikes_to_try) == 0:
            reason = f"{symbol}_INVALID_STRIKE_DIGITS_{len(digits)}"
        else:
            reason = f"{symbol}_STRIKE_NOT_FOUND_{strikes_to_try}"
        
        unprocessed_files.append((rf, reason))
        pattern_counts[reason.split('_')[0] + '_' + reason.split('_')[1] + '_' + reason.split('_')[2]] += 1

# Summary
print(f"\nProcessed files: {len(processed_files)}")
print(f"Unprocessed files: {len(unprocessed_files)}")
print(f"Verification: {len(processed_files) + len(unprocessed_files)} = {len(raw_files)} ✓")

# Pattern analysis
print("\n" + "="*60)
print("FAILURE PATTERNS")
print("="*60)

# Aggregate patterns
aggregated_patterns = defaultdict(int)
for pattern, count in pattern_counts.items():
    # Simplify pattern names
    if 'VERY_LONG_DIGITS' in pattern:
        key = pattern.split('_')[0] + '_VERY_LONG_DIGITS'
    elif 'INVALID_STRIKE' in pattern:
        key = pattern.split('_')[0] + '_INVALID_STRIKE'
    elif 'STRIKE_NOT_FOUND' in pattern:
        key = pattern.split('_')[0] + '_STRIKE_NOT_FOUND'
    else:
        key = pattern
    aggregated_patterns[key] += count

for pattern, count in sorted(aggregated_patterns.items(), key=lambda x: -x[1]):
    print(f"{pattern}: {count}")

# Sample failures
print("\n" + "="*60)
print("SAMPLE FAILED FILES (first 20)")
print("="*60)

for i, (file, reason) in enumerate(unprocessed_files[:20]):
    print(f"{i+1}. {os.path.basename(file)}")
    print(f"   Reason: {reason}")

# Write full list to file
output_file = "failed_files_analysis.txt"
with open(output_file, 'w') as f:
    f.write("COMPLETE LIST OF FAILED FILES\n")
    f.write("="*60 + "\n\n")
    
    for file, reason in sorted(unprocessed_files):
        f.write(f"{file}\t{reason}\n")
    
    f.write("\n\nSUMMARY BY PATTERN\n")
    f.write("="*60 + "\n")
    for pattern, count in sorted(aggregated_patterns.items(), key=lambda x: -x[1]):
        f.write(f"{pattern}: {count}\n")

print(f"\nFull analysis written to: {output_file}")

# Check specific filename patterns
print("\n" + "="*60)
print("FILENAME ANALYSIS")
print("="*60)

# Analyze filename lengths and patterns
length_dist = defaultdict(int)
date_patterns = defaultdict(int)

for rf in raw_files:
    basename = os.path.basename(rf)
    length_dist[len(basename)] += 1
    
    # Check for month names
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    has_month = any(month in basename.lower() for month in months)
    if has_month:
        date_patterns['HAS_MONTH_NAME'] += 1
    
    # Check for year patterns
    if re.search(r'20\d{2}', basename):
        date_patterns['HAS_YEAR_4DIGIT'] += 1
    elif re.search(r'\d{2}[a-z]{3}\d{2}', basename.lower()):
        date_patterns['HAS_DDMMMYY'] += 1

print("\nFilename length distribution:")
for length in sorted(length_dist.keys()):
    print(f"  Length {length}: {length_dist[length]} files")

print("\nDate pattern distribution:")
for pattern, count in date_patterns.items():
    print(f"  {pattern}: {count} files")