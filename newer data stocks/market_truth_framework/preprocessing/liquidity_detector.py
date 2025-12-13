#!/usr/bin/env python3
"""
Liquidity Event Detector
=========================

Detects pull/replenish events and flicker regimes from order book data.
"""

from pathlib import Path
import polars as pl
import numpy as np
from numba import njit
from dataclasses import dataclass
from typing import List


# Liquidity parameters
LIQUIDITY_PARAMS = {
    'p_drop': 0.35,      # 35% depth drop = pull
    'p_rise': 0.25,      # 25% depth rise = replenish
    'vol_small': 5,      # Minimal volume threshold
}


@dataclass
class LiquidityEvent:
    """Single liquidity event"""
    timestamp: np.datetime64
    event_type: str  # 'bid_pull', 'bid_replenish', 'ask_pull', 'ask_replenish'
    magnitude: float
    opt_type: str  # 'CE' or 'PE'


@njit
def detect_liquidity_events_numba(
    timestamps: np.ndarray,
    bid_depths: np.ndarray,
    ask_depths: np.ndarray,
    spreads: np.ndarray,
    volumes: np.ndarray,
    p_drop: float,
    p_rise: float,
    vol_small: float
):
    """
    Detect pull/replenish events using Numba
    
    Returns arrays for event creation
    """
    n = len(timestamps)
    max_events = min(n, 10000)
    
    # Output arrays
    event_indices = np.zeros(max_events, dtype=np.int32)
    event_types = np.zeros(max_events, dtype=np.int8)  # 0=bid_pull, 1=bid_replenish, 2=ask_pull, 3=ask_replenish
    event_magnitudes = np.zeros(max_events, dtype=np.float64)
    
    event_count = 0
    
    for t in range(1, n):
        if event_count >= max_events:
            break
        
        # Compute changes
        bid_depth_pct_change = (bid_depths[t] - bid_depths[t-1]) / (bid_depths[t-1] + 1e-10)
        ask_depth_pct_change = (ask_depths[t] - ask_depths[t-1]) / (ask_depths[t-1] + 1e-10)
        
        vol_1s = volumes[t] - volumes[t-1]
        spread_widened_bid = spreads[t] > spreads[t-1]
        spread_tightened = spreads[t] <= spreads[t-1]
        
        # BID PULL
        if (bid_depth_pct_change <= -p_drop and 
            vol_1s <= vol_small and 
            spread_widened_bid):
            
            event_indices[event_count] = t
            event_types[event_count] = 0
            event_magnitudes[event_count] = abs(bid_depth_pct_change)
            event_count += 1
        
        # BID REPLENISH
        elif (bid_depth_pct_change >= p_rise and spread_tightened):
            event_indices[event_count] = t
            event_types[event_count] = 1
            event_magnitudes[event_count] = bid_depth_pct_change
            event_count += 1
        
        # ASK PULL
        if (ask_depth_pct_change <= -p_drop and 
            vol_1s <= vol_small and 
            spread_widened_bid):
            
            event_indices[event_count] = t
            event_types[event_count] = 2
            event_magnitudes[event_count] = abs(ask_depth_pct_change)
            event_count += 1
        
        # ASK REPLENISH
        elif (ask_depth_pct_change >= p_rise and spread_tightened):
            event_indices[event_count] = t
            event_types[event_count] = 3
            event_magnitudes[event_count] = ask_depth_pct_change
            event_count += 1
    
    return event_indices[:event_count], event_types[:event_count], event_magnitudes[:event_count]


def detect_flicker_regime(events: List[LiquidityEvent], window_seconds: int = 30) -> List[tuple]:
    """
    Detect flicker regimes (high churn in liquidity)
    
    Returns list of (start_time, end_time) tuples
    """
    if not events:
        return []
    
    # Group events into time windows
    flicker_windows = []
    
    min_time = min(e.timestamp for e in events)
    max_time = max(e.timestamp for e in events)
    
    current = min_time
    window_duration = np.timedelta64(window_seconds, 's')
    
    while current < max_time:
        window_end = current + window_duration
        
        # Count events in window
        window_events = [e for e in events 
                        if current <= e.timestamp < window_end]
        
        pulls = sum(1 for e in window_events if 'pull' in e.event_type)
        replenishes = sum(1 for e in window_events if 'replenish' in e.event_type)
        
        # Flicker if high churn
        if pulls >= 5 and replenishes >= 5:
            flicker_windows.append((current, window_end))
        
        current = window_end
    
    return flicker_windows


def add_liquidity_events_to_features(features_file: Path, opt_type: str):
    """
    Add liquidity event columns to features file
    
    Args:
        features_file: Path to features_{date}.parquet
        opt_type: 'CE' or 'PE'
    """
    # Load features
    df = pl.read_parquet(features_file)
    
    # Extract relevant columns
    timestamps = df['timestamp'].to_numpy()
    
    if opt_type == 'CE':
        bid_depths = df['ce_bid_depth_5'].to_numpy()
        ask_depths = df['ce_ask_depth_5'].to_numpy()
        spreads = df['ce_spread'].to_numpy()
        volumes = df['ce_volume'].to_numpy()
    else:
        bid_depths = df['pe_bid_depth_5'].to_numpy()
        ask_depths = df['pe_ask_depth_5'].to_numpy()
        spreads = df['pe_spread'].to_numpy()
        volumes = df['pe_volume'].to_numpy()
    
    # Detect events
    event_indices, event_types, event_mags = detect_liquidity_events_numba(
        timestamps,
        bid_depths.astype(np.float64),
        ask_depths.astype(np.float64),
        spreads,
        volumes.astype(np.float64),
        LIQUIDITY_PARAMS['p_drop'],
        LIQUIDITY_PARAMS['p_rise'],
        LIQUIDITY_PARAMS['vol_small']
    )
    
    # Create event markers column
    event_type_map = {0: 'bid_pull', 1: 'bid_replenish', 2: 'ask_pull', 3: 'ask_replenish'}
    
    # Initialize columns
    n = len(df)
    pull_rate_30s = np.zeros(n)
    replenish_rate_30s = np.zeros(n)
    
    # Compute rolling 30s rates
    for t in range(30, n):
        window_start = t - 30
        window_events = []
        
        for i in range(len(event_indices)):
            if window_start <= event_indices[i] < t:
                window_events.append(event_types[i])
        
        pulls = sum(1 for et in window_events if et in [0, 2])  # bid_pull or ask_pull
        replenishes = sum(1 for et in window_events if et in [1, 3])
        
        pull_rate_30s[t] = pulls / 30.0
        replenish_rate_30s[t] = replenishes / 30.0
    
    # Add to dataframe
    prefix = opt_type.lower()
    df = df.with_columns([
        pl.Series(f'{prefix}_pull_rate_30s', pull_rate_30s),
        pl.Series(f'{prefix}_replenish_rate_30s', replenish_rate_30s),
    ])
    
    # Save updated features
    df.write_parquet(features_file)
    
    return len(event_indices)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Add liquidity events to features')
    parser.add_argument('--features-file', required=True, help='Path to features parquet file')
    
    args = parser.parse_args()
    
    features_file = Path(args.features_file)
    
    print(f"Processing {features_file}...")
    
    # Add for both CE and PE
    ce_events = add_liquidity_events_to_features(features_file, 'CE')
    pe_events = add_liquidity_events_to_features(features_file, 'PE')
    
    print(f"  CE events: {ce_events}")
    print(f"  PE events: {pe_events}")
    print(f"  ✓ Updated {features_file}")
