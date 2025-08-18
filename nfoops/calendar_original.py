from __future__ import annotations
import polars as pl

def read_calendar(csv_path: str) -> pl.DataFrame:
    # Expect columns at least: symbol, expiry, kind (weekly|monthly), week_index, is_monthly, is_weekly
    cal = pl.read_csv(
        csv_path,
        try_parse_dates=True,
        dtypes={
            "symbol": pl.Utf8,
            "expiry": pl.Date,
        },
    )
    if "week_index" not in cal.columns and "week_of_month" in cal.columns:
        cal = cal.rename({"week_of_month": "week_index"})
    # derive flags if missing
    if "kind" not in cal.columns:
        cal = cal.with_columns(pl.when(pl.col("is_monthly")==1).then("monthly").otherwise("weekly").alias("kind"))
    if "is_monthly" not in cal.columns:
        cal = cal.with_columns((pl.col("kind")=="monthly").cast(pl.Int8).alias("is_monthly"))
    if "is_weekly" not in cal.columns:
        cal = cal.with_columns((pl.col("kind")=="weekly").cast(pl.Int8).alias("is_weekly"))
    # ensure week_index exists for weekly rows
    if "week_index" not in cal.columns:
        cal = cal.with_columns(pl.when(pl.col("is_weekly")==1).then(1).otherwise(0).alias("week_index"))
    return cal

def attach_expiry(df: pl.DataFrame, cal: pl.DataFrame) -> pl.DataFrame:
    # Assign expiry by month for symbol; pick the nearest expiry >= ts within same month.
    # We precompute month buckets.
    dfm = df.with_columns([
        pl.col("timestamp").dt.truncate("1mo").alias("month_start"),
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
