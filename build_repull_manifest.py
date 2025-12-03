#!/usr/bin/env python3
"""
Build a repull manifest from the corruption report.

Reads analysis_results/corrupted_raw_options.csv and emits a text file with
glob-style patterns to repull, grouped by (symbol, hint). This keeps the list
compact and easy to use with vendor export tools or rsync.

Usage:
  python build_repull_manifest.py [--symbol BANKNIFTY|NIFTY] [--min-count 1]

Outputs:
  analysis_results/repull_manifest_options.txt

Hint format:
  'hint' is derived as the filename leading part like 'banknifty25may'. The
  glob will be 'data/raw/options/{hint}*.parquet' which covers CE & PE & strikes.
"""

import argparse
from pathlib import Path
import polars as pl

IN_CSV = Path("analysis_results/corrupted_raw_options.csv")
OUT_TXT = Path("analysis_results/repull_manifest_options.txt")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", choices=["BANKNIFTY", "NIFTY"], default=None)
    ap.add_argument("--min-count", type=int, default=1,
                    help="Only include groups with at least this many files corrupted")
    args = ap.parse_args()

    if not IN_CSV.exists():
        raise SystemExit(f"Missing input: {IN_CSV}")

    df = pl.read_csv(IN_CSV)
    cols = set(df.columns)
    required = {"symbol", "hint", "file"}
    if not required.issubset(cols):
        raise SystemExit(f"CSV missing required columns: {required - cols}")

    g = df
    if args.symbol:
        g = g.filter(pl.col("symbol") == args.symbol)

    # group and count per (symbol, hint)
    grp = (
        g.group_by(["symbol", "hint"]).agg(pl.len().alias("files"))
         .filter(pl.col("files") >= args.min_count)
         .sort(["symbol", pl.col("files").desc(), "hint"]) 
    )

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    with OUT_TXT.open("w") as f:
        for row in grp.iter_rows(named=True):
            sym = row["symbol"]
            hint = row["hint"]
            count = row["files"]
            glob = f"data/raw/options/{hint}*.parquet"
            f.write(f"# {sym}\t{hint}\t{count} files\n{glob}\n")

    print(f"Wrote manifest: {OUT_TXT}")
    print("Top entries:\n")
    print(grp.head(20))


if __name__ == "__main__":
    main()

