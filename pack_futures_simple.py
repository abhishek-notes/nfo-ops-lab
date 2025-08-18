#!/usr/bin/env python3
# Simple memory-efficient futures packing
import os, glob, argparse
from pathlib import Path
from datetime import time
import polars as pl

RAW_DIR = "./data/raw/futures"
OUT_DIR = "./data/packed/futures"
CAL_PATH = "./meta/expiry_calendar.csv"

def load_calendar(path: str) -> pl.DataFrame:
    cal = pl.read_csv(path).rename({"Instrument":"symbol","Final_Expiry":"expiry","Expiry_Type":"kind"})
    cal = (cal.select(
            pl.col("symbol").str.to_uppercase(),
            pl.col("kind").str.to_lowercase(),
            pl.col("expiry").str.strptime(pl.Date, strict=False).alias("expiry"))
          .drop_nulls(["symbol","kind","expiry"])
          .filter(pl.col("kind")=="monthly")
          .select(["symbol","expiry"])
          .unique()
          .sort(["symbol","expiry"])
    )
    return cal

def infer_symbol(path: str) -> str | None:
    base = os.path.basename(path).lower()
    if "banknifty" in base: return "BANKNIFTY"
    if "nifty" in base: return "NIFTY"
    return None

def process_file(path: str, cal: pl.DataFrame) -> None:
    sym = infer_symbol(path)
    if not sym: 
        print(f"skip (symbol?): {path}")
        return
    
    print(f"\nProcessing {path} for {sym}...")
    
    # Read only necessary columns to save memory
    try:
        # First check what columns exist
        schema = pl.read_parquet_schema(path)
        print(f"  Schema: {list(schema.keys())[:10]}...")  # Show first 10 columns
        
        # Identify timestamp and price columns
        cols_to_read = []
        
        # Timestamp columns
        for col in ["timestamp", "ts", "Timestamp", "TS"]:
            if col in schema:
                cols_to_read.append(col)
                
        # Price columns
        for col in ["close", "Close", "ltp", "LTP", "price", "Price", "last", "Last", "avgPrice", "AvgPrice"]:
            if col in schema:
                cols_to_read.append(col)
                break  # Only need one price column
                
        # Volume columns
        for col in ["volume", "Volume", "qty", "Qty"]:
            if col in schema:
                cols_to_read.append(col)
                break
                
        print(f"  Reading columns: {cols_to_read}")
        
        # Read file with selected columns
        df = pl.read_parquet(path, columns=cols_to_read)
        print(f"  Loaded {df.height:,} rows")
        
        # Process timestamps
        if "timestamp" not in df.columns and "ts" in df.columns:
            df = df.with_columns(pl.col("ts").alias("timestamp"))
        elif "Timestamp" in df.columns:
            df = df.with_columns(pl.col("Timestamp").alias("timestamp"))
        elif "TS" in df.columns:
            df = df.with_columns(pl.col("TS").alias("timestamp"))
            
        if "timestamp" not in df.columns:
            print(f"  No timestamp column found!")
            return
            
        # Parse timestamp
        dt = df["timestamp"].dtype
        if dt == pl.Utf8:
            df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
        elif dt.is_integer() or dt.is_float():
            df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ns"), strict=False))
            
        # Fix 1970 timestamps if we have ts column
        if "ts" in df.columns and "ts" != "timestamp":
            ts_parsed = pl.col("ts").str.strptime(pl.Datetime, strict=False) if df["ts"].dtype == pl.Utf8 else pl.col("ts").cast(pl.Datetime, strict=False)
            df = df.with_columns(pl.when(pl.col("timestamp").dt.year()<=1971).then(ts_parsed).otherwise(pl.col("timestamp")).alias("timestamp"))
            
        # Set timezone
        df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("Asia/Kolkata"))
        
        # Get close price
        close_col = None
        for col in ["close", "Close", "ltp", "LTP", "price", "Price", "last", "Last", "avgPrice", "AvgPrice"]:
            if col in df.columns:
                close_col = col
                break
                
        if close_col:
            df = df.with_columns(pl.col(close_col).cast(pl.Float64).alias("close"))
        else:
            df = df.with_columns(pl.lit(100.0).alias("close"))  # Default price
            
        # Add OHLC columns
        df = df.with_columns([
            pl.col("close").alias("open"),
            pl.col("close").alias("high"),
            pl.col("close").alias("low")
        ])
        
        # Volume delta
        if "volume" in df.columns or "Volume" in df.columns:
            vol_col = "volume" if "volume" in df.columns else "Volume"
            df = df.with_columns(pl.col(vol_col).cast(pl.Int64).alias("_v"))
            df = df.sort("timestamp").with_columns(pl.col("_v").diff().clip(lower_bound=0).fill_null(0).alias("vol_delta")).drop("_v")
        elif "qty" in df.columns or "Qty" in df.columns:
            qty_col = "qty" if "qty" in df.columns else "Qty"
            df = df.with_columns(pl.col(qty_col).cast(pl.Int64).fill_null(0).alias("vol_delta"))
        else:
            df = df.with_columns(pl.lit(0, dtype=pl.Int64).alias("vol_delta"))
            
        # Filter and prepare
        df = (df
              .filter(pl.col("timestamp").is_not_null())
              .unique(["timestamp"])
              .filter((pl.col("timestamp").dt.time()>=time(9,15,0)) & (pl.col("timestamp").dt.time()<=time(15,30,0)))
              .with_columns([
                  pl.lit(sym).alias("symbol"),
                  pl.col("timestamp").dt.date().alias("trade_date"),
              ])
              .select(["timestamp","symbol","open","high","low","close","vol_delta","trade_date"])
              .sort("timestamp")
        )
        
        print(f"  After filtering: {df.height:,} rows")
        
        # Map to expiry
        cal_s = cal.filter(pl.col("symbol")==sym).sort("expiry")
        if cal_s.is_empty():
            print(f"  No calendar entries for {sym}")
            return
            
        df = df.join_asof(cal_s, left_on="trade_date", right_on="expiry", strategy="forward")
        df = df.drop_nulls(["expiry"])
        
        print(f"  After expiry mapping: {df.height:,} rows")
        
        # Group by expiry and save
        for exp in df.select("expiry").unique().to_series():
            g = df.filter(pl.col("expiry")==exp)
            yyyymm = exp.isoformat()[:4] + exp.isoformat()[5:7]
            out_path = Path(OUT_DIR)/sym/yyyymm/f"exp={exp.isoformat()}"/"ticks.parquet"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            g.write_parquet(str(out_path), compression="zstd", compression_level=3, statistics=True)
            print(f"  Wrote {out_path} ({g.height:,} rows)")
            
    except Exception as e:
        print(f"Error processing {path}: {e}")
        import traceback
        traceback.print_exc()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=RAW_DIR)
    ap.add_argument("--out", default=OUT_DIR)
    ap.add_argument("--cal", default=CAL_PATH)
    args = ap.parse_args()

    cal = load_calendar(args.cal)
    files = sorted(glob.glob(os.path.join(args.raw, "*.parquet")))
    # Filter only the main futures files
    files = [f for f in files if "futures.parquet" in f]
    print(f"FUTURES: {len(files)} files")

    for i, p in enumerate(files, 1):
        print(f"\n{'='*60}")
        print(f"File {i}/{len(files)}")
        process_file(p, cal)

if __name__ == "__main__":
    main()