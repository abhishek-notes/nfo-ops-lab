# Summary: Raw vs Packed Files Processing Discrepancy

## The Issue
The `check_failed_files_detailed.py` script reports:
- 84,918 raw files as "processed"
- 360 raw files as "unprocessed"
- But there are only 76,147 packed output files

## Root Cause
The discrepancy arises from a fundamental misunderstanding of the data structure:

1. **Raw files** are identified by: `(symbol, strike, opt_type, date)`
   - Example: `banknifty20mar14100ce.parquet` represents BANKNIFTY 14100 CE for some date in March 2020

2. **Packed files** are organized by: `(symbol, expiry, opt_type, strike)`
   - Example: `/data/packed/options/BANKNIFTY/202003/exp=2020-03-26/type=CE/strike=14100.parquet`

3. **The script's counting logic** checks if a raw file's `(symbol, strike, opt_type)` exists anywhere in the packed files, ignoring the date/expiry dimension.

## The Many-to-One Relationship

### Key Findings:
- **85,278** total raw files
- **1,698** unique `(symbol, strike, opt_type)` combinations in packed files
- **76,147** packed files (unique `(symbol, expiry, opt_type, strike)` combinations)

### Why 84,918 "processed" files?
- Multiple raw files with the same strike but different dates all get counted as "processed" if ANY packed file exists with that strike
- Example: If there are 50 raw files for BANKNIFTY 14100 CE across different dates, and even one packed file exists for this strike, ALL 50 raw files are counted as "processed"

### The actual relationship:
- Each `(symbol, strike, opt_type)` appears in an average of **44.85 different expiries**
- This explains why 1,698 unique strikes × 44.85 average expiries ≈ 76,147 packed files

## Examples of Many-to-One Mapping

For BANKNIFTY Strike=14100 CE:
- 2 raw files: `banknifty20mar14100ce.parquet`, `banknifty20may14100ce.parquet`
- 2 packed files with expiries: 2020-03-26, 2020-05-28

Some strikes have up to 136 different expiries in the packed data!

## The 360 "Unprocessed" Files
These are raw files whose strikes don't exist in ANY packed file:
- 498 unique `(symbol, strike, opt_type)` combinations are missing from packed files
- Examples include strikes like BANKNIFTY 13100-13800 (from March 2020) and strikes above 57600 (from 2025 data)

## Conclusion
The script's logic is flawed because it treats the presence of ANY packed file with a given strike as evidence that ALL raw files with that strike were processed. In reality:
- The packing process creates one file per unique `(symbol, expiry, opt_type, strike)`
- Multiple raw files may contribute data to the same packed file
- Some raw files may not have been processed at all, even if other files with the same strike were processed

To properly track processing, the script would need to:
1. Parse the actual dates from raw filenames
2. Map them to the corresponding expiry dates
3. Check if a packed file exists for that specific `(symbol, expiry, opt_type, strike)` combination