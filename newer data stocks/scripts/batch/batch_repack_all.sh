#!/bin/bash
# Batch process all raw_options folders

set -e  # Exit on error

OUTPUT_DIR="options_date_packed_FULL"
SCRIPT="repack_raw_to_date_FINAL.py"

echo "======================================================================"
echo "BATCH PROCESSING ALL RAW OPTIONS DATA"
echo "======================================================================"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Array of all input directories
DIRS=(
    "new 2025 data/aug 1 to aug 13 new stocks data/processed_output/raw_options"
    "new 2025 data/aug 13 to aug 29 new stocks data/processed_output/raw_options"
    "new 2025 data/aug 14 to 10 sep new stocks data/processed_output/raw_options"
    "new 2025 data/aug 29 to sep 23 new stocks data/processed_output/raw_options"
    "new 2025 data/sep 23 to oct 6 new stocks data/processed_output/raw_options"
    "new 2025 data/oct 7 to oct 20 new stocks data/processed_output/raw_options"
    "new 2025 data/oct 20 to nov 3 new stocks data/processed_output/raw_options"
    "new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options"
    "new 2025 data/nov 18 to 1 dec new stocks data/processed_output/raw_options"
)

TOTAL=${#DIRS[@]}
CURRENT=0

echo "Found $TOTAL directories to process"
echo ""

# Process each directory
for DIR in "${DIRS[@]}"; do
    CURRENT=$((CURRENT + 1))
    
    echo "======================================================================"
    echo "[$CURRENT/$TOTAL] Processing: $DIR"
    echo "======================================================================"
    
    if [ ! -d "$DIR" ]; then
        echo "WARNING: Directory not found, skipping: $DIR"
        echo ""
        continue
    fi
    
    # Count files
    FILE_COUNT=$(find "$DIR" -name "*.parquet" | wc -l | tr -d ' ')
    echo "Files in directory: $FILE_COUNT"
    echo ""
    
    # Run the repacking script
    python "$SCRIPT" \
        --input-dir "$DIR" \
        --output-dir "$OUTPUT_DIR" \
        --expiry-calendar "expiry_calendar.csv"
    
    echo ""
    echo "[$CURRENT/$TOTAL] COMPLETED: $DIR"
    echo ""
done

echo "======================================================================"
echo "BATCH PROCESSING COMPLETE!"
echo "======================================================================"
echo "All data written to: $OUTPUT_DIR"
echo ""
echo "Summary:"
du -sh "$OUTPUT_DIR"
find "$OUTPUT_DIR" -name "*.parquet" | wc -l | awk '{print $1 " parquet files created"}'
