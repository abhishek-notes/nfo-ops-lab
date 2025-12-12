#!/bin/bash

# Batch extract spot data from all SQL dumps using the original extraction script

# Source shell profile to get python aliases
if [ -f ~/.zshrc ]; then
    source ~/.zshrc
fi

# Use explicit python from the alias
PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"

set -e

echo "======================================================================"
echo "BATCH SPOT EXTRACTION (Using Original Script)"
echo "======================================================================"
echo ""

BASE_DIR="/Users/abhishek/workspace/nfo/newer data stocks"
NEW_DATA_DIR="$BASE_DIR/new 2025 data"
SPOT_OUTPUT_DIR="$BASE_DIR/spot_data"

# Create output directory
mkdir -p "$SPOT_OUTPUT_DIR"

# Data folders
declare -a folders=(
    "aug 1 to aug 13 new stocks data"
    "aug 13 to aug 29 new stocks data"
    "aug 14 to 10 sep new stocks data"
    "aug 29 to sep 23 new stocks data"
    "sep 23 to oct 6 new stocks data"
    "oct 7 to oct 20 new stocks data"
    "oct 20 to nov 3 new stocks data"
    "nov 4 to nov 18 new stocks data"
    "nov 18 to 1 dec new stocks data"
)

echo "Extracting spot data from ${#folders[@]} folders..."
echo ""

total_start=$(date +%s)
folder_count=0

for folder in "${folders[@]}"; do
    folder_count=$((folder_count + 1))
    
    echo ">>> Processing folder $folder_count/${#folders[@]}: $folder"
    echo "----------------------------------------------------------------------"
    
    sql_file="$NEW_DATA_DIR/$folder/das_nse_mod.sql.gz"
    
    if [ ! -f "$sql_file" ]; then
        echo "WARNING: SQL file not found, skipping: $sql_file"
        echo ""
        continue
    fi
    
    # Extract spot data
    $PYTHON extract_spot_indices.py \
        "$sql_file" \
        --output "$SPOT_OUTPUT_DIR/${folder// /_}" \
        --tables NIFTY BANKNIFTY
    
    echo ""
done

total_end=$(date +%s)
total_elapsed=$((total_end - total_start))

echo "======================================================================"
echo "BATCH EXTRACTION COMPLETE"
echo "======================================================================"
echo ""
echo "Processed folders: ${#folders[@]}"
echo "Total time: ${total_elapsed}s ($((total_elapsed / 60))m $((total_elapsed % 60))s)"
echo ""
echo "Output directory: $SPOT_OUTPUT_DIR"
echo ""

# Now consolidate all spot files
echo "Consolidating spot files..."
$PYTHON << 'PYEND'
import polars as pl
from pathlib import Path

spot_dir = Path("spot_data")

# Consolidate NIFTY
nifty_files = list(spot_dir.rglob("nifty_spot.parquet"))
if nifty_files:
    nifty_dfs = [pl.read_parquet(f) for f in nifty_files]
    nifty_all = pl.concat(nifty_dfs).sort('timestamp').unique(['timestamp'])
    nifty_all.write_parquet(spot_dir / "NIFTY_all.parquet", compression='zstd')
    print(f"✓ NIFTY: {len(nifty_all):,} rows ({nifty_all['timestamp'].min()} to {nifty_all['timestamp'].max()})")

# Consolidate BANKNIFTY  
banknifty_files = list(spot_dir.rglob("banknifty_spot.parquet"))
if banknifty_files:
    banknifty_dfs = [pl.read_parquet(f) for f in banknifty_files]
    banknifty_all = pl.concat(banknifty_dfs).sort('timestamp').unique(['timestamp'])
    banknifty_all.write_parquet(spot_dir / "BANKNIFTY_all.parquet", compression='zstd')
    print(f"✓ BANKNIFTY: {len(banknifty_all):,} rows ({banknifty_all['timestamp'].min()} to {banknifty_all['timestamp'].max()})")

print("")
print("Consolidated files created:")
print(f"  - {spot_dir}/NIFTY_all.parquet")
print(f"  - {spot_dir}/BANKNIFTY_all.parquet")
PYEND

echo ""
echo "✓ Spot extraction complete!"
echo "======================================================================"
