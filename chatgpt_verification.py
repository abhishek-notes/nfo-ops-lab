#!/usr/bin/env python3
"""ChatGPT's verification steps"""

print("="*80)
print("STEP 1: Quick sample verification (50 random files)")
print("="*80)

import polars as pl, glob, random, re
from datetime import time

paths = glob.glob("./data/packed/options/*/*/exp=*/type=*/strike=*.parquet")
print(f"Found {len(paths)} packed partitions")
sample = random.sample(paths, min(50, len(paths)))

bad = 0
for p in sample:
    m = re.search(r"/options/(.+?)/(\d{6})/exp=(\d{4}-\d{2}-\d{2})/type=(CE|PE)/strike=(\d+)\.parquet$", p)
    if not m:
        print("PATH PARSE FAIL ->", p); bad += 1; continue
    sym, yyyymm, exp, typ, strike = m.groups()

    df = pl.read_parquet(p)

    checks = {
        "one_symbol_matches_path": (df["symbol"].n_unique()==1 and df["symbol"][0]==sym),
        "one_type_matches_path":   (df["opt_type"].n_unique()==1 and df["opt_type"][0]==typ),
        "one_strike_matches_path": (df["strike"].n_unique()==1 and int(df["strike"][0])==int(strike)),
        "one_expiry_matches_path": (df["expiry"].n_unique()==1 and str(df["expiry"][0])==exp),
        "no_null_timestamps":      (df.filter(pl.col("timestamp").is_null()).height==0),
        "no_dupe_timestamps":      (df.height==df["timestamp"].n_unique()),
        "within_market_hours":     (df.filter((pl.col("timestamp").dt.time()<time(9,15)) | (pl.col("timestamp").dt.time()>time(15,30))).height==0),
        "ohlc_bounds_ok":          (df.filter((pl.col("low")>pl.min_horizontal("open","close")) | (pl.max_horizontal("open","close")>pl.col("high"))).height==0),
        "vol_delta_nonneg":        (df.filter(pl.col("vol_delta")<0).height==0),
    }

    if not all(checks.values()):
        bad += 1
        print("\nFAIL ->", p)
        for k,v in checks.items():
            if not v: print("  -", k)

print("\nBad files in sample:", bad)

print("\n"+"="*80)
print("STEP 2: Build a manifest")
print("="*80)

rows=[]
for i, p in enumerate(paths):
    if i % 1000 == 0:
        print(f"Processing {i}/{len(paths)} files...")
    try:
        df = pl.read_parquet(p, columns=["timestamp","symbol","opt_type","strike","expiry"])
        if df.is_empty(): continue
        rows.append({
            "path": p,
            "symbol": df["symbol"][0],
            "opt_type": df["opt_type"][0],
            "strike": int(df["strike"][0]),
            "expiry": str(df["expiry"][0]),
            "rows": df.height,
            "tmin": str(df["timestamp"].min()),
            "tmax": str(df["timestamp"].max()),
        })
    except:
        pass

import os
os.makedirs("meta", exist_ok=True)
pl.DataFrame(rows).write_csv("meta/packed_manifest.csv")
print(f"Wrote meta/packed_manifest.csv with {len(rows)} partitions")

print("\n"+"="*80)
print("STEP 3: Spot-check a specific file")
print("="*80)

p = "data/packed/options/BANKNIFTY/201911/exp=2019-11-07/type=CE/strike=30400.parquet"
if os.path.exists(p):
    df = pl.read_parquet(p)
    print(f"File: {p}")
    print(f"Rows: {df.height}")
    print("\nHead:")
    print(df.head(5))
    print("\nTail:")
    print(df.tail(5))
else:
    print(f"File not found: {p}")

print("\n"+"="*80)
print("STEP 4: Test 1-minute bar resampling")
print("="*80)

if os.path.exists(p):
    scan = pl.scan_parquet(p)
    bars = (
        scan
        .group_by_dynamic(index_column="timestamp", every="1m", closed="left")
        .agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("vol_delta").sum().alias("volume"),
        ])
        .collect()
    )
    print("1-minute bars sample:")
    print(bars.head())
    
    os.makedirs("data/packed/examples", exist_ok=True)
    bars.write_parquet("data/packed/examples/BN_2019-11-07_CE_30400_1m.parquet")
    print("✅ Wrote 1m bars -> data/packed/examples/BN_2019-11-07_CE_30400_1m.parquet")

print("\n"+"="*80)
print("STEP 5: Test multi-file scanning")
print("="*80)

# All CE files for BANKNIFTY Nov 2019, any strike
scan = pl.scan_parquet("data/packed/options/BANKNIFTY/201911/exp=*/type=CE/strike=*.parquet")
total = scan.select(pl.len()).collect()
print(f"Total rows across BANKNIFTY Nov 2019 CE files: {total[0,0]:,}")

print("\n"+"="*80)
print("ALL VERIFICATION STEPS COMPLETE!")
print("="*80)