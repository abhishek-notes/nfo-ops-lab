#!/bin/bash

# Batch process all data folders: Extract spot + Repack with spot enrichment
# This script processes all 9 raw data folders sequentially

# Source shell profile to get python aliases
if [ -f ~/.zshrc ]; then
    source ~/.zshrc
fi

# Use explicit python from the alias
PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"

set -e  # Exit on error

echo "======================================================================"
echo "BATCH PROCESSING: Spot Extraction + Options Repacking (v3)"
echo "======================================================================"
echo ""

# ==================== CONFIGURATION ====================

BASE_DIR="/Users/abhishek/workspace/nfo/newer data stocks"
NEW_DATA_DIR="$BASE_DIR/new 2025 data"
SPOT_OUTPUT_DIR="$BASE_DIR/spot_data"
OPTIONS_OUTPUT_DIR="$BASE_DIR/options_date_packed_FULL_v3_SPOT_ENRICHED"
EXPIRY_CALENDAR="$BASE_DIR/expiry_calendar.csv"

# Data folders (in chronological order)
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

# ==================== STEP 1: EXTRACT SPOT DATA ====================

echo "STEP 1/3: Extracting spot data from SQL dumps"
echo "----------------------------------------------------------------------"
echo "Processing ${#folders[@]} folders..."
echo ""

$PYTHON extract_spot_data.py \
    --data-dirs "$NEW_DATA_DIR" \
    --output-dir "$SPOT_OUTPUT_DIR" \
    --symbols NIFTY BANKNIFTY

if [ $? -ne 0 ]; then
    echo "ERROR: Spot extraction failed!"
    exit 1
fi

echo ""
echo "✓ Spot extraction complete!"
echo ""

# ==================== STEP 2: VERIFY SPOT DATA ====================

echo "STEP 2/3: Verifying spot data"
echo "----------------------------------------------------------------------"

if [ ! -f "$SPOT_OUTPUT_DIR/NIFTY_all.parquet" ]; then
    echo "ERROR: NIFTY spot data not found!"
    exit 1
fi

if [ ! -f "$SPOT_OUTPUT_DIR/BANKNIFTY_all.parquet" ]; then
    echo "ERROR: BANKNIFTY spot data not found!"
    exit 1
fi

echo "Spot data files found:"
ls -lh "$SPOT_OUTPUT_DIR"/*_all.parquet

echo ""
echo "✓ Spot data verified!"
echo ""

# ==================== STEP 3: REPACK OPTIONS WITH SPOT ====================

echo "STEP 3/3: Repacking options data with spot enrichment"
echo "----------------------------------------------------------------------"
echo "Processing ${#folders[@]} folders sequentially..."
echo ""

total_start=$(date +%s)
folder_count=0

for folder in "${folders[@]}"; do
    folder_count=$((folder_count + 1))
    
    echo ""
    echo ">>> Processing folder $folder_count/${#folders[@]}: $folder"
    echo "----------------------------------------------------------------------"
    
    # Parquet files are in processed_output/raw_options subdirectory
    input_dir="$NEW_DATA_DIR/$folder/processed_output/raw_options"
    
    if [ ! -d "$input_dir" ]; then
        echo "WARNING: Folder not found, skipping: $input_dir"
        continue
    fi
    
    # Check if folder has parquet files
    parquet_count=$(find "$input_dir" -name "*.parquet" -type f | wc -l)
    
    if [ "$parquet_count" -eq 0 ]; then
        echo "WARNING: No parquet files found in $folder, skipping"
        continue
    fi
    
    echo "Found $parquet_count parquet files"
    echo ""
    
    # Run v3 packer
    start_time=$(date +%s)
    
    $PYTHON repack_raw_to_date_v3_SPOT_ENRICHED.py \
        --input-dir "$input_dir" \
        --output-dir "$OPTIONS_OUTPUT_DIR" \
        --expiry-calendar "$EXPIRY_CALENDAR" \
        --spot-dir "$SPOT_OUTPUT_DIR"
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Repacking failed for folder: $folder"
        exit 1
    fi
    
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    
    echo ""
    echo "✓ Folder $folder_count/${#folders[@]} complete in ${elapsed}s"
    echo ""
    
done

total_end=$(date +%s)
total_elapsed=$((total_end - total_start))

# ==================== SUMMARY ====================

echo ""
echo "======================================================================"
echo "BATCH PROCESSING COMPLETE!"
echo "======================================================================"
echo ""
echo "Processed folders: ${#folders[@]}"
echo "Total time: ${total_elapsed}s ($((total_elapsed / 60))m $((total_elapsed % 60))s)"
echo ""
echo "Output directory: $OPTIONS_OUTPUT_DIR"
echo ""
echo "New schema includes spot-enriched columns:"
echo "  - spot_price (Float32)"
echo "  - distance_from_spot (Float32)"
echo "  - moneyness_pct (Float32)"
echo "  - intrinsic_value (Float32)"
echo "  - time_value (Float32)"
echo "  - mid_price (Float32)"
echo ""
echo "✓ All data repacked with spot enrichment!"
echo ""

# Show summary of output
echo "Output summary:"
find "$OPTIONS_OUTPUT_DIR" -name "*.parquet" | wc -l | xargs echo "Total parquet files:"
du -sh "$OPTIONS_OUTPUT_DIR" | awk '{print "Total size: " $1}'

echo ""
echo "======================================================================"
