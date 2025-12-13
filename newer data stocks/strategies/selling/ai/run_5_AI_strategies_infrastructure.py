#!/usr/bin/env python3
"""
5 ADVANCED AI STRATEGIES - Full Implementation
With proper pre-computation of volatility and EMAs
"""

from pathlib import Path
from datetime import time, date
from dataclasses import dataclass
from typing import List, Tuple
import polars as pl
import numpy as np
from numba import njit
import csv
import gc
import time as time_mod


@dataclass
class Trade:
    entry_date: str
    entry_time: str
    exit_date: str
    exit_time: str
    ce_strike: float
    pe_strike: float
    ce_entry_price: float
    pe_entry_price: float
    ce_exit_price: float
    pe_exit_price: float
    pnl: float
    hold_duration_minutes: int
    exit_reason: str
    strategy_name: str
    adjustments: int = 0


def prepare_volatility_and_ema(df: pl.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Pre-compute 1-min volatility and 5-min EMAs
    Returns: (timestamps_sec, realized_vol_array, ema5_array, ema21_array)
    """
    # Extract unique spot prices with timestamps
    spot_df = df.select(['timestamp', 'spot_price']).unique(subset=['timestamp']).sort('timestamp')
    
    # 1-minute candles for volatility
    spot_1min = spot_df.group_by_dynamic('timestamp', every='1m').agg([
        pl.col('spot_price').first().alias('open'),
        pl.col('spot_price').max().alias('high'),
        pl.col('spot_price').min().alias('low'),
        pl.col('spot_price').last().alias('close'),
    ])
    
    # Calculate realized volatility (rolling 20-period std of returns, annualized)
    returns = spot_1min.select(
        pl.col('close').pct_change().alias('returns')
    )
    
    realized_vol = returns.select(
        (pl.col('returns').rolling_std(20) * np.sqrt(252 * 375)).fill_null(20.0).alias('vol')
    )['vol'].to_numpy()
    
    # 5-minute candles for EMA
    spot_5min = spot_df.group_by_dynamic('timestamp', every='5m').agg([
        pl.col('spot_price').last().alias('close'),
    ])
    
    # Calculate EMAs
    ema5 = spot_5min.select(
        pl.col('close').ewm_mean(span=5).alias('ema5')
    )['ema5'].to_numpy()
    
    ema21 = spot_5min.select(
        pl.col('close').ewm_mean(span=21).alias('ema21')
    )['ema21'].to_numpy()
    
    # Convert timestamps to seconds for lookup
    timestamps_1min = spot_1min.select(
        (pl.col('timestamp').dt.hour() * 3600 + 
         pl.col('timestamp').dt.minute() * 60 +
         pl.col('timestamp').dt.second()).alias('sec')
    )['sec'].to_numpy()
    
    timestamps_5min = spot_5min.select(
        (pl.col('timestamp').dt.hour() * 3600 + 
         pl.col('timestamp').dt.minute() * 60 +
         pl.col('timestamp').dt.second()).alias('sec')
    )['sec'].to_numpy()
    
    return timestamps_1min, realized_vol, timestamps_5min, ema5, ema21


@njit
def find_atm_strikes(distances: np.ndarray, opt_types: np.ndarray):
    """Find ATM strikes"""
    ce_idx = -1
    pe_idx = -1
    min_ce_dist = 999999.0
    min_pe_dist = 999999.0
    
    for i in range(len(distances)):
        abs_dist = abs(distances[i])
        if opt_types[i] == 0:  # CE
            if abs_dist < min_ce_dist:
                min_ce_dist = abs_dist
                ce_idx = i
        else:  # PE
            if abs_dist < min_pe_dist:
                min_pe_dist = abs_dist
                pe_idx = i
    
    return ce_idx, pe_idx


@njit
def get_vol_at_time(current_time_sec: int, time_array: np.ndarray, vol_array: np.ndarray) -> float:
    """Lookup volatility at current time"""
    # Find closest time
    idx = 0
    min_diff = 999999
    for i in range(len(time_array)):
        diff = abs(time_array[i] - current_time_sec)
        if diff < min_diff:
            min_diff = diff
            idx = i
    
    return vol_array[idx] if idx < len(vol_array) else 20.0


@njit
def get_ema_at_time(current_time_sec: int, time_array: np.ndarray, ema5: np.ndarray, ema21: np.ndarray) -> Tuple[float, float]:
    """Lookup EMA values at current time"""
    idx = 0
    min_diff = 999999
    for i in range(len(time_array)):
        diff = abs(time_array[i] - current_time_sec)
        if diff < min_diff:
            min_diff = diff
            idx = i
    
    if idx < len(ema5):
        return ema5[idx], ema21[idx]
    return 0.0, 0.0


# Due to length constraints, I'll create this as a multi-file approach:
# 1. First file: Infrastructure and Strategy 1-2
# 2. Second file: Strategy 3-5
# Then a runner that combines them

def int_to_date(date_int):
    return date.fromordinal(date_int + 719163)


def sec_to_time(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return time(h, m, s)


print("Infrastructure complete. Implementing strategies next...")
