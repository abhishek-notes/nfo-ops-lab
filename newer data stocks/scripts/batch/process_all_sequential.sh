#!/bin/bash
# Sequential processing of all raw_options folders
# Run with: ./process_all_sequential.sh

set -e  # Exit on any error

OUTPUT_DIR="options_date_packed_FULL"
SCRIPT="repack_raw_to_date_FINAL.py"

echo "=========================================="
echo "Processing All Raw Options Data"
echo "=========================================="
echo "Output: $OUTPUT_DIR"
echo ""
echo "Started at: $(date)"
echo ""

# Process each folder sequentially
echo "[1/9] Aug 1-13..."
python "$SCRIPT" \
  --input-dir "new 2025 data/aug 1 to aug 13 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[2/9] Aug 13-29..."
python "$SCRIPT" \
  --input-dir "new 2025 data/aug 13 to aug 29 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[3/9] Aug 14 - Sep 10..."
python "$SCRIPT" \
  --input-dir "new 2025 data/aug 14 to 10 sep new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[4/9] Aug 29 - Sep 23..."
python "$SCRIPT" \
  --input-dir "new 2025 data/aug 29 to sep 23 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[5/9] Sep 23 - Oct 6..."
python "$SCRIPT" \
  --input-dir "new 2025 data/sep 23 to oct 6 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[6/9] Oct 7-20..."
python "$SCRIPT" \
  --input-dir "new 2025 data/oct 7 to oct 20 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[7/9] Oct 20 - Nov 3..."
python "$SCRIPT" \
  --input-dir "new 2025 data/oct 20 to nov 3 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[8/9] Nov 4-18..."
python "$SCRIPT" \
  --input-dir "new 2025 data/nov 4 to nov 18 new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "[9/9] Nov 18 - Dec 1..."
python "$SCRIPT" \
  --input-dir "new 2025 data/nov 18 to 1 dec new stocks data/processed_output/raw_options" \
  --output-dir "$OUTPUT_DIR"

echo ""
echo "=========================================="
echo "ALL PROCESSING COMPLETE!"
echo "=========================================="
echo "Finished at: $(date)"
echo ""
echo "Output directory: $OUTPUT_DIR"
du -sh "$OUTPUT_DIR"
echo ""
echo "Total parquet files:"
find "$OUTPUT_DIR" -name "*.parquet" | wc -l
echo ""
echo "Date range:"
ls -1 "$OUTPUT_DIR" | head -5
echo "..."
ls -1 "$OUTPUT_DIR" | tail -5
