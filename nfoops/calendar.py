from __future__ import annotations
import polars as pl

def read_calendar(csv_path: str) -> pl.DataFrame:
    # Read the actual calendar format you have
    cal = pl.read_csv(
        csv_path,
        try_parse_dates=True,
    )
    
    # Rename columns to match expected format
    cal = cal.rename({
        "Instrument": "symbol",
        "Final_Expiry": "expiry",
        "Expiry_Type": "kind"
    })
    
    # Convert kind to lowercase, expiry is already parsed as date by try_parse_dates=True
    cal = cal.with_columns([
        pl.col("kind").str.to_lowercase().alias("kind")
    ])
    
    # Add is_monthly and is_weekly columns
    cal = cal.with_columns([
        (pl.col("kind") == "monthly").cast(pl.Int8).alias("is_monthly"),
        (pl.col("kind") == "weekly").cast(pl.Int8).alias("is_weekly")
    ])
    
    # Add week_index (1 for first week, 2 for second, etc.)
    # This is a simplified version - you might need to adjust based on your needs
    cal = cal.with_columns([
        pl.when(pl.col("is_weekly") == 1)
        .then((pl.col("expiry").dt.day() - 1) // 7 + 1)
        .otherwise(0)
        .alias("week_index")
    ])
    
    # Select only required columns
    return cal.select(["symbol", "expiry", "kind", "week_index", "is_monthly", "is_weekly"])

def attach_expiry(df: pl.DataFrame, cal: pl.DataFrame) -> pl.DataFrame:
    # Assign expiry by month for symbol; pick the nearest expiry >= ts within same month.
    # We precompute month buckets.
    dfm = df.with_columns([
        pl.col("timestamp").dt.date().dt.truncate("1mo").alias("month_start"),
    ])
    calm = cal.with_columns(pl.col("expiry").dt.truncate("1mo").alias("month_start"))
    j = dfm.join(calm, on=["symbol","month_start"], how="left")
    # Filter to rows where ts <= expiry within that month, and choose the first such expiry per (symbol,strike,opt_type,ts)
    j = j.filter(pl.col("timestamp").dt.date() <= pl.col("expiry"))
    # pick earliest expiry ≥ ts by group
    j = j.sort(["symbol","opt_type","strike","timestamp","expiry"]).with_columns(
        pl.col("expiry").first().over(["symbol","opt_type","strike","timestamp"]).alias("_expiry_first")
    ).with_columns(pl.col("_expiry_first").alias("expiry")).drop("_expiry_first","month_start")
    return j.select(df.columns + ["expiry","kind","week_index","is_monthly","is_weekly"])