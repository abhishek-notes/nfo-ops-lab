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
DATA_DIR = Path("../market_truth_data")
FEATURES_DIR = DATA_DIR / "features"
BURSTS_DIR = DATA_DIR / "bursts"
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
            "/day/{date}/features",
            "/day/{date}/bursts",
            "/statistics",
        ]
    }


@app.get("/days")
def list_days():
    """
    List all available trading days
    
    Returns:
        {"days": ["2025-08-01", "2025-08-04", ...]}
    """
    days = sorted([
        f.stem.replace("features_", "") 
        for f in FEATURES_DIR.glob("features_*.parquet")
    ])
    
    return {"days": days, "count": len(days)}


@app.get("/day/{date}/features")
def get_features(
    date: str,
    start_sec: Optional[int] = None,
    end_sec: Optional[int] = None,
    columns: Optional[str] = None
):
    """
    Get feature data for a specific day
    
    Args:
        date: Trading date (YYYY-MM-DD)
        start_sec: Optional start time (seconds since 9:15 AM)
        end_sec: Optional end time (seconds since 9:15 AM)
        columns: Optional comma-separated list of columns
    
    Returns:
        Feature data as JSON
    """
    features_file = FEATURES_DIR / f"features_{date}.parquet"
    
    if not features_file.exists():
        raise HTTPException(status_code=404, detail=f"Data not found for date: {date}")
    
    # Check cache
    cache_key = f"features_{date}"
    
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
        "date": date,
        "rows": len(df),
        "data": df.to_dicts()
    }


@app.get("/day/{date}/bursts")
def get_bursts(date: str):
    """
    Get burst events for a specific day
    
    Args:
        date: Trading date (YYYY-MM-DD)
    
    Returns:
        Burst events as JSON
    """
    bursts_file = BURSTS_DIR / f"bursts_{date}.parquet"
    
    if not bursts_file.exists():
        # Return empty if no bursts file (might be a quiet day)
        return {"date": date, "bursts": [], "count": 0}
    
    df = pl.read_parquet(bursts_file)
    
    return {
        "date": date,
        "bursts": df.to_dicts(),
        "count": len(df)
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


@app.get("/day/{date}/summary")
def get_day_summary(date: str):
    """
    Get quick summary for a trading day
    
    Returns:
        Summary statistics for the day
    """
    features_file = FEATURES_DIR / f"features_{date}.parquet"
    
    if not features_file.exists():
        raise HTTPException(status_code=404, detail=f"Data not found for date: {date}")
    
    df = pl.read_parquet(features_file)
    
    # Compute summary
    summary = {
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
    bursts_file = BURSTS_DIR / f"bursts_{date}.parquet"
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
