#!/usr/bin/env python3
"""
Analyze duplication patterns - why do we have duplicate data across files?
"""
import polars as pl
import glob
import os
import re
from pathlib import Path
from collections import defaultdict, Counter
import random

print("DUPLICATION PATTERN ANALYSIS")
print("=" * 80)

# First, let's understand the file naming patterns
raw_files = glob.glob("./data/raw/options/*.parquet")
print(f"Total raw files: {len(raw_files)}")

# Analyze naming patterns
naming_patterns = defaultdict(list)
year_month_dist = defaultdict(int)

for rf in raw_files:
    basename = os.path.basename(rf).lower()
    
    # Extract components
    if "banknifty" in basename:
        symbol = "BANKNIFTY"
    elif "nifty" in basename:
        symbol = "NIFTY"
    else:
        continue
    
    # Try to extract date patterns
    # Pattern 1: YYMMDD format (e.g., 230915)
    date_match1 = re.search(r'(\d{6})', basename)
    # Pattern 2: YYmon format (e.g., 23sep)
    date_match2 = re.search(r'(\d{2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', basename)
    # Pattern 3: YYmonDD format (e.g., 23sep15)
    date_match3 = re.search(r'(\d{2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{1,2})', basename)
    # Pattern 4: Other year patterns
    date_match4 = re.search(r'20(\d{2})', basename)
    
    if date_match1:
        pattern = "YYMMDD"
        # Extract year/month
        yymmdd = date_match1.group(1)
        year = "20" + yymmdd[:2]
        month = yymmdd[2:4]
        year_month_dist[f"{year}-{month}"] += 1
    elif date_match3:
        pattern = "YYmonDD"
        year = "20" + date_match3.group(1)
        month_name = date_match3.group(2)
        year_month_dist[f"{year}-{month_name}"] += 1
    elif date_match2:
        pattern = "YYmon"
        year = "20" + date_match2.group(1)
        month_name = date_match2.group(2)
        year_month_dist[f"{year}-{month_name}"] += 1
    elif date_match4:
        pattern = "OTHER"
        year = date_match4.group(0)
        year_month_dist[year] += 1
    else:
        pattern = "UNKNOWN"
    
    naming_patterns[pattern].append(rf)

print("\nFile naming patterns:")
for pattern, files in naming_patterns.items():
    print(f"  {pattern}: {len(files)} files")

print("\nYear-Month distribution (top 20):")
for ym, count in sorted(year_month_dist.items(), key=lambda x: -x[1])[:20]:
    print(f"  {ym}: {count} files")

# Now let's understand WHY duplicates exist
print("\n" + "="*80)
print("UNDERSTANDING DUPLICATION SOURCES")
print("="*80)

print("\nPossible reasons for duplicate data:")
print("1. Multiple expiry files for same strike (weekly + monthly)")
print("2. Overlapping date ranges in different contract files")
print("3. Data vendor providing redundant data")
print("4. Contract roll-over periods")

# Let's check a specific example
print("\n" + "="*80)
print("EXAMPLE ANALYSIS: BANKNIFTY 45000 CE")
print("="*80)

# Find all files for this strike
bn_45000_ce = [f for f in raw_files if "banknifty" in f.lower() and "45000ce" in f.lower()]
print(f"\nFound {len(bn_45000_ce)} files for BANKNIFTY 45000 CE")

# Analyze date overlaps
if len(bn_45000_ce) >= 3:
    file_dates = []
    
    for f in bn_45000_ce[:5]:  # Check first 5
        try:
            df = pl.read_parquet(f)
            if df.height > 0 and "timestamp" in df.columns:
                dates = df.select(pl.col("timestamp").dt.date()).unique()["timestamp"].to_list()
                file_dates.append({
                    'file': os.path.basename(f),
                    'dates': set(dates),
                    'date_range': f"{min(dates)} to {max(dates)}",
                    'num_dates': len(dates)
                })
        except:
            pass
    
    # Show overlaps
    print("\nDate ranges in files:")
    for fd in file_dates:
        print(f"\n{fd['file']}:")
        print(f"  Date range: {fd['date_range']}")
        print(f"  Trading days: {fd['num_dates']}")
    
    # Check for overlapping dates
    print("\nChecking for date overlaps:")
    for i in range(len(file_dates)):
        for j in range(i+1, len(file_dates)):
            overlap = file_dates[i]['dates'] & file_dates[j]['dates']
            if overlap:
                print(f"\n{file_dates[i]['file']} ∩ {file_dates[j]['file']}:")
                print(f"  Overlapping dates: {sorted(overlap)[:5]}{'...' if len(overlap) > 5 else ''}")
                print(f"  Total overlap: {len(overlap)} days")

# Random sampling script
print("\n" + "="*80)
print("RANDOM DUPLICATE VERIFICATION (50 files)")
print("="*80)

# Sample 50 random files
sample_size = min(50, len(raw_files))
sampled_files = random.sample(raw_files, sample_size)

# Group by strike
strike_groups = defaultdict(list)
for sf in sampled_files:
    basename = os.path.basename(sf).lower()
    
    # Extract strike
    match = re.search(r'(\d{4,6})(ce|pe)', basename)
    if match:
        strike = match.group(1)
        opt_type = match.group(2)
        
        if "banknifty" in basename:
            symbol = "BANKNIFTY"
        elif "nifty" in basename:
            symbol = "NIFTY"
        else:
            continue
            
        key = f"{symbol}_{strike}_{opt_type}"
        strike_groups[key].append(sf)

# Analyze groups with multiple files
print(f"\nAnalyzing {len(sampled_files)} randomly sampled files...")
print(f"Unique strike combinations: {len(strike_groups)}")

duplicates_found = 0
total_overlap_rows = 0

for strike_key, files in strike_groups.items():
    if len(files) > 1:
        print(f"\n{strike_key} - {len(files)} files in sample:")
        
        # Load all files and check for timestamp overlaps
        all_timestamps = []
        file_data = []
        
        for f in files:
            try:
                df = pl.read_parquet(f)
                if df.height > 0 and "timestamp" in df.columns:
                    timestamps = df["timestamp"].to_list()
                    all_timestamps.extend(timestamps)
                    file_data.append({
                        'file': os.path.basename(f),
                        'timestamps': set(timestamps),
                        'count': len(timestamps)
                    })
            except:
                pass
        
        # Count duplicates
        ts_counter = Counter(all_timestamps)
        duplicate_timestamps = {ts: count for ts, count in ts_counter.items() if count > 1}
        
        if duplicate_timestamps:
            duplicates_found += len(duplicate_timestamps)
            total_overlap_rows += sum(count - 1 for count in duplicate_timestamps.values())
            
            print(f"  Files:")
            for fd in file_data:
                print(f"    {fd['file']}: {fd['count']:,} rows")
            print(f"  Duplicate timestamps: {len(duplicate_timestamps):,}")
            print(f"  Total duplicate rows: {sum(count - 1 for count in duplicate_timestamps.values()):,}")

print(f"\nSummary of random sample:")
print(f"  Total duplicate timestamps found: {duplicates_found:,}")
print(f"  Total duplicate rows: {total_overlap_rows:,}")

# Create verification script
print("\n" + "="*80)
print("Creating verification script...")

verification_script = """#!/usr/bin/env python3
'''
Random verification script - checks 50 random strike combinations for data integrity
'''
import polars as pl
import glob
import random
import re
from collections import defaultdict, Counter
from pathlib import Path

def verify_strike_data(symbol, strike, opt_type):
    '''Verify data integrity for a specific strike'''
    # Find raw files
    raw_files = glob.glob("./data/raw/options/*.parquet")
    raw_matches = []
    
    for rf in raw_files:
        basename = rf.lower()
        if symbol.lower() in basename and str(strike) in basename and opt_type.lower() in basename:
            raw_matches.append(rf)
    
    if not raw_matches:
        return None
    
    # Load all raw data
    all_raw_timestamps = []
    raw_total_rows = 0
    
    for rf in raw_matches:
        try:
            df = pl.read_parquet(rf)
            if df.height > 0 and "timestamp" in df.columns:
                raw_total_rows += df.height
                all_raw_timestamps.extend(df["timestamp"].to_list())
        except:
            pass
    
    # Find packed files
    packed_files = list(Path("./data/packed/options").rglob(f"*strike={strike}.parquet"))
    packed_matches = [pf for pf in packed_files if symbol in str(pf) and f"type={opt_type}" in str(pf)]
    
    # Load all packed data
    packed_total_rows = 0
    
    for pf in packed_matches:
        try:
            df = pl.read_parquet(pf)
            packed_total_rows += df.height
        except:
            pass
    
    # Count unique timestamps
    unique_timestamps = len(set(all_raw_timestamps))
    
    return {
        'strike': f"{symbol} {strike} {opt_type}",
        'raw_files': len(raw_matches),
        'raw_rows': raw_total_rows,
        'unique_timestamps': unique_timestamps,
        'duplicates': raw_total_rows - unique_timestamps,
        'packed_files': len(packed_matches),
        'packed_rows': packed_total_rows,
        'retention': packed_total_rows / unique_timestamps * 100 if unique_timestamps > 0 else 0
    }

# Main verification
print("RANDOM STRIKE VERIFICATION")
print("=" * 80)

# Get all unique strikes
raw_files = glob.glob("./data/raw/options/*.parquet")
strikes = set()

for rf in raw_files[:5000]:  # Sample to find strikes
    basename = rf.lower()
    match = re.search(r'(\d{4,6})(ce|pe)', basename)
    if match:
        strike = int(match.group(1)[-5:]) if len(match.group(1)) >= 5 else int(match.group(1))
        opt_type = match.group(2).upper()
        symbol = "BANKNIFTY" if "banknifty" in basename else "NIFTY"
        strikes.add((symbol, strike, opt_type))

# Sample 50 strikes
sample_strikes = random.sample(list(strikes), min(50, len(strikes)))

print(f"Verifying {len(sample_strikes)} random strikes...\\n")

results = []
for symbol, strike, opt_type in sample_strikes:
    result = verify_strike_data(symbol, strike, opt_type)
    if result:
        results.append(result)
        if result['duplicates'] > 0:
            print(f"{result['strike']}:")
            print(f"  Raw: {result['raw_rows']:,} rows ({result['raw_files']} files)")
            print(f"  Unique: {result['unique_timestamps']:,} timestamps")
            print(f"  Duplicates: {result['duplicates']:,} ({result['duplicates']/result['raw_rows']*100:.1f}%)")
            print(f"  Packed: {result['packed_rows']:,} rows")
            print(f"  Retention: {result['retention']:.1f}%\\n")

# Summary
total_raw = sum(r['raw_rows'] for r in results)
total_unique = sum(r['unique_timestamps'] for r in results)
total_duplicates = sum(r['duplicates'] for r in results)
total_packed = sum(r['packed_rows'] for r in results)

print("\\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Strikes verified: {len(results)}")
print(f"Total raw rows: {total_raw:,}")
print(f"Total unique timestamps: {total_unique:,}")
print(f"Total duplicates: {total_duplicates:,} ({total_duplicates/total_raw*100:.1f}%)")
print(f"Total packed rows: {total_packed:,}")
print(f"Overall retention: {total_packed/total_unique*100:.1f}%")

if total_packed < total_unique * 0.95:  # Less than 95% retention
    print("\\n⚠️  WARNING: Possible data loss detected!")
else:
    print("\\n✅ Data integrity verified - duplicates removed, unique data preserved")
"""

with open("verify_random_strikes.py", "w") as f:
    f.write(verification_script)

print("Created verify_random_strikes.py")
print("\nRun it with: python3 verify_random_strikes.py")