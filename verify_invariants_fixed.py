#!/usr/bin/env python3
"""ChatGPT's internal invariants check - fixed for Polars"""
import polars as pl
import glob
import random
import re
from datetime import time

paths = glob.glob("./data/packed/options/*/*/exp=*/type=*/strike=*.parquet")
print(f"Found {len(paths)} packed partitions")
sample = random.sample(paths, min(50, len(paths)))
bad = 0

for p in sample:
    m = re.search(r"/options/(.+?)/(\d{6})/exp=(\d{4}-\d{2}-\d{2})/type=(CE|PE)/strike=(\d+)\.parquet$", p)
    if not m: 
        print("PATH PARSE FAIL ->", p)
        bad += 1
        continue
    
    sym, yyyymm, exp, typ, strike = m.groups()
    df = pl.read_parquet(p)
    
    # Perform checks
    checks = {
        "one_symbol_matches_path": (df["symbol"].n_unique() == 1 and df["symbol"][0] == sym),
        "one_type_matches_path": (df["opt_type"].n_unique() == 1 and df["opt_type"][0] == typ),
        "one_strike_matches_path": (df["strike"].n_unique() == 1 and int(df["strike"][0]) == int(strike)),
        "one_expiry_matches_path": (df["expiry"].n_unique() == 1 and str(df["expiry"][0]) == exp),
        "no_null_timestamps": (df["timestamp"].is_null().sum() == 0),
        "no_dupe_timestamps": (df.height == df["timestamp"].n_unique()),
        "within_market_hours": (
            df.filter(
                (pl.col("timestamp").dt.time() < time(9, 15)) | 
                (pl.col("timestamp").dt.time() > time(15, 30))
            ).height == 0
        ),
        "ohlc_bounds_ok": (
            df.filter(
                (pl.col("low") > pl.min_horizontal("open", "close")) |
                (pl.max_horizontal("open", "close") > pl.col("high"))
            ).height == 0
        ),
        "vol_delta_nonneg": (df.filter(pl.col("vol_delta") < 0).height == 0),
    }
    
    if not all(checks.values()):
        bad += 1
        print(f"\nFAIL -> {p}")
        for k, v in checks.items():
            if not v: 
                print(f"  - {k}")

print(f"\n✅ Bad files in sample: {bad} out of {len(sample)}")
if bad == 0:
    print("All internal invariants passed!")