#!/usr/bin/env python3
# Build 1-minute bars for spot and futures only (skip options due to data quality issues)
import os, glob, argparse
from pathlib import Path
import polars as pl

def build_for(root_in: str, root_out: str, by_cols: list[str]):
    files = [p for p in glob.glob(os.path.join(root_in, "**/*.parquet"), recursive=True)]
    print(f"Bars: scanning {len(files)} parquet(s) under {root_in}")
    for i, p in enumerate(files, 1):
        if i%100==0: print(f"... {i}/{len(files)}")
        df = pl.read_parquet(p)
        if "timestamp" not in df.columns: continue
        by = [c for c in by_cols if c in df.columns]
        out = (df
            .sort("timestamp")
            .group_by_dynamic(index_column="timestamp", every="1m", period="1m", group_by=by, closed="left")
            .agg([
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("vol_delta").sum().alias("volume"),
            ])
            .drop_nulls(["open","high","low","close"])
        )
        # mirror the same folder, write bars next to ticks
        rel = os.path.relpath(p, root_in)
        out_path = Path(root_out)/Path(rel).with_name("bars_1m.parquet")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.write_parquet(str(out_path), compression="zstd", compression_level=3, statistics=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot_in", default="./data/packed/spot")
    ap.add_argument("--spot_out", default="./data/bars/spot")
    ap.add_argument("--fut_in", default="./data/packed/futures")
    ap.add_argument("--fut_out", default="./data/bars/futures")
    args = ap.parse_args()

    print("Building bars for SPOT data...")
    build_for(args.spot_in, args.spot_out, by_cols=["symbol","trade_date"])
    
    print("\nBuilding bars for FUTURES data...")
    build_for(args.fut_in, args.fut_out, by_cols=["symbol","expiry"])
    
    print("\nDone!")

if __name__ == "__main__":
    main()