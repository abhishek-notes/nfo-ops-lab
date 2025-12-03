#!/bin/bash
# Full NFO Data Processing Pipeline
# Run this script to process all SQL.gz files

set -e

BASE_DIR="/Users/abhishek/workspace/nfo/newer data stocks"
INPUT_DIR="$BASE_DIR/new data to process"
OUTPUT_DIR="$BASE_DIR/processed_output"
RAW_DIR="$OUTPUT_DIR/raw_options"
PACKED_DIR="$OUTPUT_DIR/packed_options"
CALENDAR="/workspace/meta/expiry_calendar.csv"

echo "=============================================="
echo "NFO Data Processing Pipeline"
echo "=============================================="
echo "Input:    $INPUT_DIR"
echo "Output:   $OUTPUT_DIR"
echo "Calendar: $CALENDAR"
echo "=============================================="

# Step 1: Extract BANKNIFTY options
echo ""
echo "[STEP 1/4] Extracting BANKNIFTY options from SQL.gz..."
python3 "$BASE_DIR/extract_sql_fast.py" \
    "$INPUT_DIR/das_bankopt_mod.sql.gz" \
    -o "$RAW_DIR" \
    --symbol BANKNIFTY

# Step 2: Extract NIFTY options
echo ""
echo "[STEP 2/4] Extracting NIFTY options from SQL.gz..."
python3 "$BASE_DIR/extract_sql_fast.py" \
    "$INPUT_DIR/das_niftyopt_mod.sql.gz" \
    -o "$RAW_DIR" \
    --symbol NIFTY

# Step 3: Pack the extracted data
echo ""
echo "[STEP 3/4] Packing raw parquet files..."
python3 "$BASE_DIR/pack_raw_options.py" \
    --raw-dir "$RAW_DIR" \
    --out-dir "$PACKED_DIR" \
    --calendar "$CALENDAR"

# Step 4: Verify output
echo ""
echo "[STEP 4/4] Verifying output..."
python3 "$BASE_DIR/verify_output.py" \
    --packed-dir "$PACKED_DIR" \
    --sample-dir "$BASE_DIR/Banknifty packed samples"

echo ""
echo "=============================================="
echo "Processing complete!"
echo "=============================================="
echo "Raw parquet:    $RAW_DIR"
echo "Packed parquet: $PACKED_DIR"
