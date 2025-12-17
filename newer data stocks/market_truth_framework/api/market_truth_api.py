"""
Market Truth API - FastAPI Backend
===================================

Serves preprocessed market truth data for visualization and analysis.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import polars as pl
from typing import Optional, List
import json

app = FastAPI(title="Market Truth API", version="1.0.0")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "market_truth_data"
FEATURES_DIR = DATA_DIR / "features"
BURSTS_DIR = DATA_DIR / "bursts"
REGIMES_DIR = DATA_DIR / "regimes"
STATS_DIR = DATA_DIR / "statistics"

# Simple in-memory cache
_cache = {}


@app.get("/")
def root():
    """API root"""
    return {
        "name": "Market Truth API",
        "version": "1.0.0",
        "endpoints": [
            "/days",
            "/day/{underlying}/{date}/features",
            "/day/{underlying}/{date}/bursts",
            "/day/{underlying}/{date}/regimes",
            "/day/{underlying}/{date}/summary",
            "/statistics",
        ]
    }


@app.get("/days")
def list_days():
    """
    List all available trading days (across both underlyings)
    
    Returns:
        {"days": {"BANKNIFTY": [...], "NIFTY": [...]}}
    """
    days_by_underlying = {"BANKNIFTY": set(), "NIFTY": set()}
    
    for f in sorted(FEATURES_DIR.glob("features_*.parquet")):
        name_parts = f.stem.split('_')  # features_BANKNIFTY_2025-08-01
        if len(name_parts) >= 3:
            underlying = name_parts[1]
            date = name_parts[2]
            if underlying in days_by_underlying:
                days_by_underlying[underlying].add(date)
    
    return {
        "days_by_underlying": {k: sorted(v) for k, v in days_by_underlying.items()},
        "total": sum(len(v) for v in days_by_underlying.values()),
    }


@app.get("/day/{underlying}/{date}/features")
def get_features(
    underlying: str,
    date: str,
    start_sec: Optional[int] = None,
    end_sec: Optional[int] = None,
    columns: Optional[str] = None
):
    """
    Get feature data for a specific day and underlying
    
    Args:
        underlying: 'BANKNIFTY' or 'NIFTY'
        date: Trading date (YYYY-MM-DD)
        start_sec: Optional start time (seconds since start)
        end_sec: Optional end time (seconds since start)
        columns: Optional comma-separated list of columns
    
    Returns:
        Feature data as JSON
    """
    features_file = FEATURES_DIR / f"features_{underlying}_{date}.parquet"
    
    if not features_file.exists():
        raise HTTPException(status_code=404, detail=f"Data not found for {underlying} on {date}")
    
    # Check cache
    cache_key = f"features_{underlying}_{date}"
    
    if cache_key not in _cache:
        df = pl.read_parquet(features_file)
        _cache[cache_key] = df
    else:
        df = _cache[cache_key]
    
    # Filter by time range if provided
    if start_sec is not None or end_sec is not None:
        # Compute seconds since start (9:15 AM)
        first_time = df['timestamp'][0]
        df = df.with_columns([
            ((pl.col('timestamp') - first_time).dt.total_seconds()).alias('seconds_since_start')
        ])
        
        if start_sec is not None:
            df = df.filter(pl.col('seconds_since_start') >= start_sec)
        if end_sec is not None:
            df = df.filter(pl.col('seconds_since_start') <= end_sec)
    
    # Select specific columns if requested
    if columns:
        col_list = [c.strip() for c in columns.split(',')]
        # Ensure timestamp is always included
        if 'timestamp' not in col_list:
            col_list = ['timestamp'] + col_list
        df = df.select(col_list)
    
    # Convert to dict and return
    return {
        "underlying": underlying,
        "date": date,
        "rows": len(df),
        "data": df.to_dicts()
    }


@app.get("/day/{underlying}/{date}/bursts")
def get_bursts(underlying: str, date: str):
    """
    Get burst events for a specific day
    
    Args:
        date: Trading date (YYYY-MM-DD)
    
    Returns:
        Burst events as JSON
    """
    bursts_file = BURSTS_DIR / f"bursts_{underlying}_{date}.parquet"
    
    if not bursts_file.exists():
        # Return empty if no bursts file (might be a quiet day)
        return {"underlying": underlying, "date": date, "bursts": [], "count": 0}
    
    df = pl.read_parquet(bursts_file)
    
    return {
        "underlying": underlying,
        "date": date,
        "bursts": df.to_dicts(),
        "count": len(df)
    }


@app.get("/day/{underlying}/{date}/regimes")
def get_regimes(underlying: str, date: str):
    """
    Get regime labels for a specific day (one row per second).
    """
    regimes_file = REGIMES_DIR / f"regimes_{underlying}_{date}.parquet"

    if not regimes_file.exists():
        return {"underlying": underlying, "date": date, "regimes": [], "count": 0}

    cache_key = f"regimes_{underlying}_{date}"
    if cache_key not in _cache:
        df = pl.read_parquet(regimes_file)
        _cache[cache_key] = df
    else:
        df = _cache[cache_key]

    return {
        "underlying": underlying,
        "date": date,
        "regimes": df.to_dicts(),
        "count": len(df),
    }


@app.get("/statistics")
def get_statistics():
    """
    Get overall market statistics (truth tables)
    
    Returns:
        Statistics JSON
    """
    stats_file = STATS_DIR / "truth_tables.json"
    
    if not stats_file.exists():
        raise HTTPException(status_code=404, detail="Statistics not yet generated")
    
    with open(stats_file, 'r') as f:
        stats = json.load(f)
    
    return stats


@app.get("/day/{underlying}/{date}/summary")
def get_day_summary(underlying: str, date: str):
    """
    Get quick summary for a trading day
    
    Returns:
        Summary statistics for the day
    """
    features_file = FEATURES_DIR / f"features_{underlying}_{date}.parquet"
    
    if not features_file.exists():
        raise HTTPException(status_code=404, detail=f"Data not found for date: {date}")
    
    df = pl.read_parquet(features_file)
    
    # Compute summary
    summary = {
        "underlying": underlying,
        "date": date,
        "total_seconds": len(df),
        "spot_range": {
            "min": float(df['spot_price'].min()),
            "max": float(df['spot_price'].max()),
            "open": float(df['spot_price'][0]),
            "close": float(df['spot_price'][-1]),
        },
        "max_rv_10s": float(df['rv_10s'].max()),
        "max_rv_120s": float(df['rv_120s'].max()),
        "max_acceleration": float(df['accel_10s'].max()),
        "avg_ce_spread": float(df['ce_spread'].mean()),
        "avg_pe_spread": float(df['pe_spread'].mean()),
    }
    
    # Add burst count if available
    bursts_file = BURSTS_DIR / f"bursts_{underlying}_{date}.parquet"
    if bursts_file.exists():
        bursts_df = pl.read_parquet(bursts_file)
        summary['burst_count'] = len(bursts_df)
    else:
        summary['burst_count'] = 0
    
    return summary


if __name__ == "__main__":
    import uvicorn
    
    print("="*80)
    print("MARKET TRUTH API")
    print("="*80)
    print("\nStarting FastAPI server...")
    print("API will be available at: http://localhost:8000")
    print("API docs: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
