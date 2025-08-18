import polars as pl
import glob

# Find NIFTY spot data for Jan 2024
files = sorted(glob.glob("data/packed/spot/NIFTY/202401/date=*/ticks.parquet"))
if files:
    print(f"Found {len(files)} NIFTY spot files for Jan 2024")
    # Read one file
    df = pl.read_parquet(files[0])
    print(f"\nFile: {files[0]}")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print("\nFirst 5 rows:")
    print(df.head())
else:
    # Try any NIFTY spot file
    files = sorted(glob.glob("data/packed/spot/NIFTY/*/date=*/ticks.parquet"))
    if files:
        print(f"Found {len(files)} total NIFTY spot files")
        df = pl.read_parquet(files[0])
        print(f"\nFile: {files[0]}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns}")
        print("\nFirst 5 rows:")
        print(df.head())
    else:
        print("No NIFTY spot files found")