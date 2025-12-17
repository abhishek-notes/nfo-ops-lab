#!/usr/bin/env python3
"""
Market Truth Framework - Core Preprocessor
===========================================

Extracts per-second market state vectors from raw options data (ATM CE/PE).

Outputs:
- features_{UNDERLYING}_{date}.parquet: Per-second state vectors (RV, spreads, depths, OBI, etc.)
- bursts_{UNDERLYING}_{date}.parquet: All burst events with option responses

Usage:
    python core_preprocessor.py --date 2025-08-01 --underlying BANKNIFTY
"""

from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Optional
from datetime import datetime, time
import polars as pl
import numpy as np
from numba import njit
import argparse


# ============================================================================
# CONFIGURATION
# ============================================================================

BURST_PARAMS_NIFTY = {
    0: {'B_points': 8, 'B_step': 2, 'k1': 1.6, 'k2': 1.2, 'end_points': 2},
    2: {'B_points': 10, 'B_step': 3, 'k1': 1.8, 'k2': 1.3, 'end_points': 2},
    4: {'B_points': 12, 'B_step': 4, 'k1': 2.0, 'k2': 1.4, 'end_points': 3},
}

BURST_PARAMS_BANKNIFTY = {
    0: {'B_points': 20, 'B_step': 6, 'k1': 1.6, 'k2': 1.2, 'end_points': 5},
    2: {'B_points': 25, 'B_step': 8, 'k1': 1.8, 'k2': 1.3, 'end_points': 6},
    4: {'B_points': 35, 'B_step': 10, 'k1': 2.0, 'k2': 1.4, 'end_points': 8},
}

STRIKE_STEPS = {
    'NIFTY': 50,
    'BANKNIFTY': 100,
}

# Trading session (IST) assumed in packed timestamps.
SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

# Liquidity (pull/replenish) detection parameters (ATM CE/PE)
LIQUIDITY_P_DROP = 0.35      # 35% depth drop in 1s
LIQUIDITY_P_RISE = 0.25      # 25% depth rise in 1s
LIQUIDITY_VOL_SMALL = 5      # <= this vol_delta counts as "no trades"
LIQUIDITY_RATE_WINDOW_S = 30 # rolling window for rates
FLICKER_PULLS_MIN = 5
FLICKER_REPLENISH_MIN = 5
FLICKER_SPREAD_CHANGES_MIN = 10


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class BurstEvent:
    """Complete burst event record"""
    burst_id: int
    start_time: np.datetime64
    end_time: np.datetime64
    duration_seconds: int
    
    # Spot movement
    size_points: float
    direction: int  # +1 or -1
    start_price: float
    end_price: float
    max_price: float
    min_price: float
    
    # Option response
    ce_move: float
    pe_move: float
    ce_rel_delta: float
    pe_rel_delta: float
    
    # Context
    dte_at_start: int
    time_of_day: str
    
    # Classification (will be added later)
    trade_driven_score: float = 0.0
    vacuum_driven_score: float = 0.0
    classification: str = 'unknown'


# ============================================================================
# ATM SELECTION
# ============================================================================

def get_atm_strike(spot_price: float, strike_step: int) -> float:
    """
    Deterministic ATM selection
    
    Args:
        spot_price: Current spot price
        strike_step: Strike interval (50 for NIFTY, 100 for BANKNIFTY)
    
    Returns:
        ATM strike (rounded to nearest strike_step)
    """
    return round(spot_price / strike_step) * strike_step


# ============================================================================
# REALIZED VOLATILITY CALCULATIONS
# ============================================================================

@njit(cache=True)
def compute_log_returns(prices: np.ndarray) -> np.ndarray:
    """Compute log returns: ln(p_t / p_t-1)"""
    returns = np.zeros(len(prices))
    returns[0] = 0.0  # First return is 0
    
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            returns[i] = np.log(prices[i] / prices[i-1])
        else:
            returns[i] = 0.0
    
    return returns


@njit(cache=True)
def compute_rv_window(returns: np.ndarray, idx: int, window: int) -> float:
    """
    Compute realized volatility over window

    We use RMS volatility so different window lengths are comparable:
    RV = sqrt(mean(r_i^2) over window)
    """
    if idx < window:
        return 0.0
    
    sum_sq = 0.0
    for i in range(idx - window + 1, idx + 1):
        sum_sq += returns[i] ** 2
    
    return np.sqrt(sum_sq / window)


@njit(cache=True)
def compute_all_rv_windows(returns: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute RV_10, RV_30, RV_120 for all timestamps
    
    Returns: (rv_10, rv_30, rv_120)
    """
    n = len(returns)
    rv_10 = np.zeros(n)
    rv_30 = np.zeros(n)
    rv_120 = np.zeros(n)
    
    for i in range(n):
        rv_10[i] = compute_rv_window(returns, i, 10)
        rv_30[i] = compute_rv_window(returns, i, 30)
        rv_120[i] = compute_rv_window(returns, i, 120)
    
    return rv_10, rv_30, rv_120


@njit(cache=True)
def compute_acceleration(returns: np.ndarray, idx: int, window: int = 10) -> float:
    """
    Compute acceleration: |ret_1s| / (mean|ret| over window + eps)
    """
    if idx < window:
        return 0.0
    
    current_abs_ret = abs(returns[idx])
    
    # Mean absolute return over window
    sum_abs = 0.0
    # Include idx so we sum exactly `window` values (fixes off-by-one).
    for i in range(idx - window + 1, idx + 1):
        sum_abs += abs(returns[i])
    
    mean_abs = sum_abs / window
    
    if mean_abs < 1e-10:
        return 0.0
    
    return current_abs_ret / (mean_abs + 1e-10)


@njit(cache=True)
def compute_acceleration_series(returns: np.ndarray, window: int = 10) -> np.ndarray:
    """Compute accel_10s for all indices (Numba, single pass)."""
    n = len(returns)
    accel = np.zeros(n)

    for idx in range(n):
        if idx < window:
            accel[idx] = 0.0
            continue

        current_abs_ret = abs(returns[idx])
        sum_abs = 0.0
        for i in range(idx - window + 1, idx + 1):
            sum_abs += abs(returns[i])

        mean_abs = sum_abs / window
        if mean_abs < 1e-10:
            accel[idx] = 0.0
        else:
            accel[idx] = current_abs_ret / (mean_abs + 1e-10)

    return accel


# ============================================================================
# LIQUIDITY (PULL / REPLENISH)
# ============================================================================

@njit(cache=True)
def compute_pull_replenish_events(
    bid_depth: np.ndarray,
    ask_depth: np.ndarray,
    spread: np.ndarray,
    vol_delta: np.ndarray,
    p_drop: float,
    p_rise: float,
    vol_small: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Infer pull/replenish events from 1-second snapshots (no cancels available).

    Returns:
      pull_event[t]      = 1 if either bid-pull or ask-pull at t
      replenish_event[t] = 1 if either bid-replenish or ask-replenish at t
      spread_change[t]   = 1 if spread changed at t (proxy for instability)
    """
    n = len(spread)
    pull_event = np.zeros(n, dtype=np.int8)
    replenish_event = np.zeros(n, dtype=np.int8)
    spread_change = np.zeros(n, dtype=np.int8)

    for t in range(1, n):
        # Spread stability proxy
        spread_change[t] = 1 if spread[t] != spread[t - 1] else 0

        # Percent changes (safe)
        bid_prev = bid_depth[t - 1]
        ask_prev = ask_depth[t - 1]
        bid_chg = (bid_depth[t] - bid_prev) / (bid_prev + 1e-10)
        ask_chg = (ask_depth[t] - ask_prev) / (ask_prev + 1e-10)

        v = vol_delta[t]
        spread_widened = spread[t] > spread[t - 1]
        spread_tight_or_same = spread[t] <= spread[t - 1]

        bid_pull = (bid_chg <= -p_drop) and (v <= vol_small) and spread_widened
        ask_pull = (ask_chg <= -p_drop) and (v <= vol_small) and spread_widened
        if bid_pull or ask_pull:
            pull_event[t] = 1

        bid_replenish = (bid_chg >= p_rise) and spread_tight_or_same
        ask_replenish = (ask_chg >= p_rise) and spread_tight_or_same
        if bid_replenish or ask_replenish:
            replenish_event[t] = 1

    return pull_event, replenish_event, spread_change


def _rolling_count(events: np.ndarray, window: int) -> np.ndarray:
    """Rolling count over the last `window` samples (fixed-window denominator)."""
    if window <= 0:
        raise ValueError("window must be > 0")
    cs = np.cumsum(events.astype(np.int32))
    out = cs.copy()
    if len(out) > window:
        out[window:] = cs[window:] - cs[:-window]
    return out


# ============================================================================
# BURST DETECTION
# ============================================================================

@njit(cache=True)
def get_burst_params(dte: int, underlying_code: int):
    """
    Get burst detection parameters based on DTE bucket.

    underlying_code: 0=NIFTY, 1=BANKNIFTY
    Buckets: 0, <=2, <=4, 6+
    """
    if underlying_code == 0:  # NIFTY
        if dte <= 0:
            return 8.0, 2.0, 1.6, 1.2, 2.0
        elif dte <= 2:
            return 10.0, 3.0, 1.8, 1.3, 2.0
        elif dte <= 4:
            return 12.0, 4.0, 2.0, 1.4, 3.0
        else:
            return 15.0, 5.0, 2.2, 1.5, 4.0

    # BANKNIFTY
    if dte <= 0:
        return 20.0, 6.0, 1.6, 1.2, 5.0
    elif dte <= 2:
        return 25.0, 8.0, 1.8, 1.3, 6.0
    elif dte <= 4:
        return 35.0, 10.0, 2.0, 1.4, 8.0
    else:
        return 45.0, 12.0, 2.2, 1.5, 10.0


@njit(cache=True)
def detect_bursts_numba(
    timestamps: np.ndarray,
    spot_prices: np.ndarray,
    returns: np.ndarray,
    rv_10: np.ndarray,
    rv_120: np.ndarray,
    dte_days: np.ndarray,
    ce_mids: np.ndarray,
    pe_mids: np.ndarray,
    underlying_code: int  # 0=NIFTY, 1=BANKNIFTY
):
    """
    Detect burst events using Numba for speed
    
    Returns arrays for creating BurstEvent objects
    """
    n = len(spot_prices)
    max_bursts = 1000
    
    # Output arrays
    burst_ids = np.zeros(max_bursts, dtype=np.int32)
    start_indices = np.zeros(max_bursts, dtype=np.int32)
    end_indices = np.zeros(max_bursts, dtype=np.int32)
    sizes = np.zeros(max_bursts, dtype=np.float64)
    directions = np.zeros(max_bursts, dtype=np.int32)
    
    burst_count = 0
    in_burst = False
    burst_start_idx = 0
    
    for t in range(120, n):  # Need 120s history for RV_120
        dte = int(dte_days[t])
        
        # Get parameters
        B_points, B_step, k1, k2, end_points = get_burst_params(dte, underlying_code)
        
        if not in_burst:
            # === CHECK BURST START CONDITIONS ===
            
            # A: Displacement (spot moved >= B_points in last 10s)
            if t >= 10:
                displacement = abs(spot_prices[t] - spot_prices[t-10])
                cond_a = displacement >= B_points
            else:
                cond_a = False
            
            # B: Volatility expansion (RV_10 > k1 * RV_120)
            if rv_120[t] > 0:
                cond_b = rv_10[t] > k1 * rv_120[t]
            else:
                cond_b = False
            
            # C: Acceleration (max single step in last 3s >= B_step)
            max_step = 0.0
            if t >= 3:
                for i in range(3):
                    step = abs(spot_prices[t-i] - spot_prices[t-i-1])
                    if step > max_step:
                        max_step = step
            cond_c = max_step >= B_step
            
            # Start burst if all conditions true
            if cond_a and cond_b and cond_c:
                in_burst = True
                burst_start_idx = t
        
        else:
            # === CHECK BURST END CONDITIONS ===
            
            # Recent movement small (< end_points in last 5s)
            if t >= 5:
                recent_move = abs(spot_prices[t] - spot_prices[t-5])
                cond_end_1 = recent_move < end_points
            else:
                cond_end_1 = False
            
            # Volatility contracted (RV_10 < k2 * RV_120)
            if rv_120[t] > 0:
                cond_end_2 = rv_10[t] < k2 * rv_120[t]
            else:
                cond_end_2 = False
            
            # End burst if both conditions true
            if cond_end_1 and cond_end_2:
                if burst_count < max_bursts:
                    # Record burst
                    burst_ids[burst_count] = burst_count
                    start_indices[burst_count] = burst_start_idx
                    end_indices[burst_count] = t
                    
                    # Calculate size and direction
                    burst_spots = spot_prices[burst_start_idx:t+1]
                    size = np.max(burst_spots) - np.min(burst_spots)
                    direction = 1 if spot_prices[t] > spot_prices[burst_start_idx] else -1
                    
                    sizes[burst_count] = size
                    directions[burst_count] = direction
                    
                    burst_count += 1
                
                in_burst = False
    
    # Trim arrays to actual burst count
    return (
        burst_ids[:burst_count],
        start_indices[:burst_count],
        end_indices[:burst_count],
        sizes[:burst_count],
        directions[:burst_count]
    )


def create_burst_events(
    burst_data: Tuple,
    timestamps: np.ndarray,
    spot_prices: np.ndarray,
    dte_days: np.ndarray,
    ce_mids: np.ndarray,
    pe_mids: np.ndarray
) -> List[BurstEvent]:
    """Convert burst detection output to BurstEvent objects"""
    
    burst_ids, start_idxs, end_idxs, sizes, directions = burst_data
    
    events = []
    
    for i in range(len(burst_ids)):
        start_idx = start_idxs[i]
        end_idx = end_idxs[i]
        
        # Calculate option responses
        ce_move = ce_mids[end_idx] - ce_mids[start_idx]
        pe_move = pe_mids[end_idx] - pe_mids[start_idx]
        
        spot_move = spot_prices[end_idx] - spot_prices[start_idx]
        
        if abs(spot_move) > 0.01:
            ce_rel_delta = ce_move / spot_move
            pe_rel_delta = pe_move / spot_move
        else:
            ce_rel_delta = 0.0
            pe_rel_delta = 0.0
        
        # Time of day classification
        start_time = timestamps[start_idx]
        hour = start_time.astype('datetime64[h]').astype(int) % 24
        
        if hour < 11:
            time_of_day = 'morning'
        elif hour < 14:
            time_of_day = 'midday'
        else:
            time_of_day = 'afternoon'
        
        event = BurstEvent(
            burst_id=int(burst_ids[i]),
            start_time=timestamps[start_idx],
            end_time=timestamps[end_idx],
            duration_seconds=int(end_idx - start_idx),
            size_points=float(sizes[i]),
            direction=int(directions[i]),
            start_price=float(spot_prices[start_idx]),
            end_price=float(spot_prices[end_idx]),
            max_price=float(np.max(spot_prices[start_idx:end_idx+1])),
            min_price=float(np.min(spot_prices[start_idx:end_idx+1])),
            ce_move=float(ce_move),
            pe_move=float(pe_move),
            ce_rel_delta=float(ce_rel_delta),
            pe_rel_delta=float(pe_rel_delta),
            dte_at_start=int(dte_days[start_idx]),
            time_of_day=time_of_day
        )
        
        events.append(event)
    
    return events


# ============================================================================
# MICROSTRUCTURE CALCULATIONS
# ============================================================================

def compute_microstructure_metrics(row):
    """
    Compute microstructure health metrics for a single option
    
    Args:
        row: Polars row with bp0-bp4, sp0-sp4, bq0-bq4, sq0-sq4
    
    Returns:
        dict with spread, mid, depths, OBI, slopes
    """
    # Spread and mid
    spread = row['sp0'] - row['bp0']
    mid = (row['sp0'] + row['bp0']) / 2.0
    
    # Depths (sum of quantities at all 5 levels)
    bid_depth_5 = sum(row[f'bq{i}'] for i in range(5))
    ask_depth_5 = sum(row[f'sq{i}'] for i in range(5))
    
    # Order Book Imbalance
    total_depth = bid_depth_5 + ask_depth_5
    if total_depth > 0:
        obi_5 = (bid_depth_5 - ask_depth_5) / total_depth
    else:
        obi_5 = 0.0
    
    # Depth slopes (front-loadedness)
    if bid_depth_5 > 0:
        depth_slope_bid = row['bq0'] / bid_depth_5
    else:
        depth_slope_bid = 0.0
    
    if ask_depth_5 > 0:
        depth_slope_ask = row['sq0'] / ask_depth_5
    else:
        depth_slope_ask = 0.0
    
    return {
        'spread': spread,
        'mid': mid,
        'bid_depth_5': bid_depth_5,
        'ask_depth_5': ask_depth_5,
        'obi_5': obi_5,
        'depth_slope_bid': depth_slope_bid,
        'depth_slope_ask': depth_slope_ask,
    }


def _trading_session_bounds(trading_date: str) -> Tuple[datetime, datetime]:
    day = datetime.strptime(trading_date, "%Y-%m-%d").date()
    start = datetime.combine(day, SESSION_START)
    end = datetime.combine(day, SESSION_END)
    return start, end


def _session_grid(trading_date: str) -> pl.DataFrame:
    start, end = _trading_session_bounds(trading_date)
    return pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start=start, end=end, interval="1s", eager=True
            )
        }
    )


def _atm_strike_expr(spot_col: str, strike_step: int) -> pl.Expr:
    """
    Polars expression for deterministic ATM strike:
    round(spot / strike_step) * strike_step
    """
    return (
        (pl.col(spot_col).cast(pl.Float64) / strike_step).round(0).cast(pl.Int64) * strike_step
    ).cast(pl.Int32)


def _round_for_output(
    features_df: pl.DataFrame,
    price_decimals: int = 2,
    ratio_decimals: int = 3,
    small_decimals: int = 6,
) -> pl.DataFrame:
    """
    Round selected float columns for readability without destroying signal.

    - price-like columns: 2 decimals (typical tick sizes)
    - ratio/score columns: 3 decimals
    - small-magnitude columns (returns/RV): 6 decimals (so they don't become 0.00)
    """
    price_cols = [
        "spot_price",
        "ce_mid",
        "pe_mid",
        "ce_spread",
        "pe_spread",
        "dOptCE_1s",
        "dOptPE_1s",
    ]
    # Around-ATM strikes (±1/±2): mid/spread columns, if present.
    for side in ("ce", "pe"):
        for off in ("m2", "m1", "p1", "p2"):
            price_cols.append(f"{side}_mid_{off}")
            price_cols.append(f"{side}_spread_{off}")
    ratio_cols = [
        "ce_obi_5",
        "pe_obi_5",
        "ce_depth_slope_bid",
        "pe_depth_slope_bid",
        "ce_depth_slope_ask",
        "pe_depth_slope_ask",
        "accel_10s",
    ]
    small_cols = ["ret_1s", "rv_10s", "rv_30s", "rv_120s"]
    small_cols += ["ret_5s", "ret_10s"]
    ratio_cols += [
        "ce_pull_rate_30s",
        "ce_replenish_rate_30s",
        "ce_net_liquidity_30s",
        "pe_pull_rate_30s",
        "pe_replenish_rate_30s",
        "pe_net_liquidity_30s",
    ]

    exprs: list[pl.Expr] = []
    for col in price_cols:
        if col in features_df.columns:
            exprs.append(pl.col(col).cast(pl.Float64).round(price_decimals).alias(col))
    for col in ratio_cols:
        if col in features_df.columns:
            exprs.append(pl.col(col).cast(pl.Float64).round(ratio_decimals).alias(col))
    for col in small_cols:
        if col in features_df.columns:
            exprs.append(pl.col(col).cast(pl.Float64).round(small_decimals).alias(col))

    if not exprs:
        return features_df
    return features_df.with_columns(exprs)


# ============================================================================
# MAIN PREPROCESSING FUNCTION
# ============================================================================

def preprocess_day(
    date: str,
    underlying: str,
    data_dir: Path,
    output_dir: Path,
    *,
    round_output: bool = True,
    price_decimals: int = 2,
    ratio_decimals: int = 3,
    small_decimals: int = 6,
):
    """
    Main preprocessing function for one trading day
    
    Args:
        date: Trading date (YYYY-MM-DD)
        underlying: 'NIFTY' or 'BANKNIFTY'
        data_dir: Path to raw data
        output_dir: Path for output files
    """
    print(f"{'='*80}")
    print(f"Processing {underlying} on {date}")
    print(f"{'='*80}\n")

    # Load data (all parquet shards)
    print("Loading data...")
    date_dir = data_dir / date / underlying
    if not date_dir.exists():
        print(f"❌ Date directory not found: {date_dir}")
        return

    files = sorted(date_dir.glob("*.parquet"))
    if not files:
        print(f"❌ No parquet files found in {date_dir}")
        return

    file_paths = [str(f) for f in files]
    lf_all = pl.scan_parquet(file_paths)

    # Filter to nearest expiry (min Date). Since packed data is sorted by expiry,
    # we can usually read only the first row of each shard to find the minimum,
    # falling back to a full scan if needed.
    print("Filtering to nearest expiry...")
    nearest_expiry = None
    try:
        first_expiries: list = []
        for fp in files:
            head = pl.read_parquet(fp, columns=["expiry"], n_rows=1)
            if head.height:
                first_expiries.append(head["expiry"][0])
        if first_expiries:
            nearest_expiry = min(first_expiries)
    except Exception:
        nearest_expiry = None

    if nearest_expiry is None:
        nearest_expiry = lf_all.select(pl.col("expiry").min()).collect().item()

    print(f"  Nearest expiry: {nearest_expiry}")

    strike_step = STRIKE_STEPS[underlying]
    session_start, session_end = _trading_session_bounds(date)

    required_cols = [
        "timestamp",
        "spot_price",
        "expiry",
        "opt_type",
        "strike",
        "bp0",
        "sp0",
        "bq0",
        "bq1",
        "bq2",
        "bq3",
        "bq4",
        "sq0",
        "sq1",
        "sq2",
        "sq3",
        "sq4",
        "volume",
        "vol_delta",
    ]

    lf = (
        lf_all.filter(pl.col("expiry") == nearest_expiry)
        .select(required_cols)
        .with_columns(pl.col("timestamp").dt.truncate("1s").alias("timestamp_sec"))
        .filter(pl.col("timestamp_sec").is_between(session_start, session_end, closed="both"))
    )

    # Per-second spot stream
    print("Building per-second spot stream...")
    spot_df = (
        lf.select(["timestamp_sec", "spot_price", "expiry"])
        .group_by("timestamp_sec")
        .agg([pl.first("spot_price").alias("spot_price"), pl.first("expiry").alias("expiry")])
        .with_columns(
            [
                # expiry is Date; compute DTE using timestamp's DATE (not datetime).
                ((pl.col("expiry") - pl.col("timestamp_sec").dt.date()).dt.total_days())
                .cast(pl.Int32)
                .alias("dte_days"),
                _atm_strike_expr("spot_price", strike_step).alias("atm_strike"),
                pl.lit(1).cast(pl.Int8).alias("spot_observed"),
            ]
        )
        .select(["timestamp_sec", "spot_price", "atm_strike", "dte_days", "spot_observed"])
        .collect()
        .sort("timestamp_sec")
        .rename({"timestamp_sec": "timestamp"})
    )

    if len(spot_df) == 0:
        print("❌ No rows found in trading session; nothing to process.")
        return

    # Per-second ATM option stream (CE/PE)
    print("Building per-second ATM CE/PE stream...")
    row_atm_strike = _atm_strike_expr("spot_price", strike_step).alias("row_atm_strike")
    bid_depth_expr = pl.sum_horizontal([pl.col(f"bq{i}") for i in range(5)])
    ask_depth_expr = pl.sum_horizontal([pl.col(f"sq{i}") for i in range(5)])
    eps = 1e-10

    opt_sec_long = (
        lf.with_columns([row_atm_strike])
        .filter(pl.col("strike") == pl.col("row_atm_strike"))
        .filter(pl.col("opt_type").is_in(["CE", "PE"]))
        .with_columns(
            [
                (pl.col("sp0") - pl.col("bp0")).alias("spread"),
                ((pl.col("sp0") + pl.col("bp0")) / 2.0).alias("mid"),
                bid_depth_expr.alias("bid_depth_5"),
                ask_depth_expr.alias("ask_depth_5"),
                ((bid_depth_expr - ask_depth_expr) / (bid_depth_expr + ask_depth_expr + eps)).alias("obi_5"),
                (pl.col("bq0") / (bid_depth_expr + eps)).alias("depth_slope_bid"),
                (pl.col("sq0") / (ask_depth_expr + eps)).alias("depth_slope_ask"),
                pl.lit(1).cast(pl.Int8).alias("observed"),
            ]
        )
        .group_by(["timestamp_sec", "opt_type"])
        .agg(
            [
                pl.first("mid").alias("mid"),
                pl.first("spread").alias("spread"),
                pl.first("bid_depth_5").alias("bid_depth_5"),
                pl.first("ask_depth_5").alias("ask_depth_5"),
                pl.first("obi_5").alias("obi_5"),
                pl.first("depth_slope_bid").alias("depth_slope_bid"),
                pl.first("depth_slope_ask").alias("depth_slope_ask"),
                pl.first("volume").alias("volume"),
                pl.first("vol_delta").alias("vol_delta"),
                pl.first("observed").alias("observed"),
            ]
        )
        .collect()
        .sort(["timestamp_sec", "opt_type"])
    )

    opt_wide = opt_sec_long.pivot(
        values=[
            "mid",
            "spread",
            "bid_depth_5",
            "ask_depth_5",
            "obi_5",
            "depth_slope_bid",
            "depth_slope_ask",
            "volume",
            "vol_delta",
            "observed",
        ],
        index="timestamp_sec",
        on="opt_type",
        aggregate_function="first",
    ).rename({"timestamp_sec": "timestamp"})

    rename_map = {
        "mid_CE": "ce_mid",
        "mid_PE": "pe_mid",
        "spread_CE": "ce_spread",
        "spread_PE": "pe_spread",
        "bid_depth_5_CE": "ce_bid_depth_5",
        "bid_depth_5_PE": "pe_bid_depth_5",
        "ask_depth_5_CE": "ce_ask_depth_5",
        "ask_depth_5_PE": "pe_ask_depth_5",
        "obi_5_CE": "ce_obi_5",
        "obi_5_PE": "pe_obi_5",
        "depth_slope_bid_CE": "ce_depth_slope_bid",
        "depth_slope_bid_PE": "pe_depth_slope_bid",
        "depth_slope_ask_CE": "ce_depth_slope_ask",
        "depth_slope_ask_PE": "pe_depth_slope_ask",
        "volume_CE": "ce_volume",
        "volume_PE": "pe_volume",
        "vol_delta_CE": "ce_vol_delta",
        "vol_delta_PE": "pe_vol_delta",
        "observed_CE": "ce_observed",
        "observed_PE": "pe_observed",
    }
    opt_wide = opt_wide.rename({k: v for k, v in rename_map.items() if k in opt_wide.columns})

    if "ce_mid" not in opt_wide.columns or "pe_mid" not in opt_wide.columns:
        print("❌ Could not build both ATM CE and ATM PE streams for this day.")
        return

    # Per-second around-ATM (±1, ±2 strikes) for shape/skew/gamma proxies.
    print("Building per-second ±1/±2 around-ATM stream...")
    strike_offset_expr = (
        (pl.col("strike").cast(pl.Int64) - pl.col("row_atm_strike").cast(pl.Int64))
        .floordiv(strike_step)
        .cast(pl.Int32)
        .alias("strike_offset")
    )

    opt_off_long = (
        lf.with_columns([row_atm_strike]).with_columns([strike_offset_expr])
        .filter(pl.col("strike_offset").is_in([-2, -1, 1, 2]))
        .filter(pl.col("opt_type").is_in(["CE", "PE"]))
        .with_columns(
            [
                (pl.col("sp0") - pl.col("bp0")).alias("spread"),
                ((pl.col("sp0") + pl.col("bp0")) / 2.0).alias("mid"),
                pl.lit(1).cast(pl.Int8).alias("observed"),
            ]
        )
        .group_by(["timestamp_sec", "opt_type", "strike_offset"])
        .agg(
            [
                pl.first("mid").alias("mid"),
                pl.first("spread").alias("spread"),
                pl.first("vol_delta").alias("vol_delta"),
                pl.first("observed").alias("observed"),
            ]
        )
        .collect()
        .sort(["timestamp_sec", "opt_type", "strike_offset"])
    )

    opt_off_wide: Optional[pl.DataFrame]
    if opt_off_long.height > 0:
        opt_off_wide = (
            opt_off_long.with_columns(
                (
                    pl.col("opt_type")
                    + pl.lit("_")
                    + pl.col("strike_offset").cast(pl.Int32).cast(pl.Utf8)
                ).alias("key")
            )
            .pivot(
                values=["mid", "spread", "vol_delta", "observed"],
                index="timestamp_sec",
                on="key",
                aggregate_function="first",
            )
            .rename({"timestamp_sec": "timestamp"})
        )

        rename_off: dict[str, str] = {}
        for opt_type, prefix in (("CE", "ce"), ("PE", "pe")):
            for strike_offset, label in ((-2, "m2"), (-1, "m1"), (1, "p1"), (2, "p2")):
                key = f"{opt_type}_{strike_offset}"
                rename_off[f"mid_{key}"] = f"{prefix}_mid_{label}"
                rename_off[f"spread_{key}"] = f"{prefix}_spread_{label}"
                rename_off[f"vol_delta_{key}"] = f"{prefix}_vol_delta_{label}"
                rename_off[f"observed_{key}"] = f"{prefix}_observed_{label}"

        opt_off_wide = opt_off_wide.rename({k: v for k, v in rename_off.items() if k in opt_off_wide.columns})
    else:
        opt_off_wide = None

    # Join to full session grid and fill gaps.
    print("Joining to full 1-second session grid...")
    grid = _session_grid(date)
    features_df = (
        grid.join(spot_df, on="timestamp", how="left")
        .join(opt_wide, on="timestamp", how="left")
        .pipe(lambda df: df.join(opt_off_wide, on="timestamp", how="left") if opt_off_wide is not None else df)
        .with_columns(pl.lit(underlying).alias("underlying"))
        .sort("timestamp")
    )

    # Observed flags should not be forward-filled.
    observed_cols = [c for c in features_df.columns if "_observed" in c]
    features_df = features_df.with_columns([pl.col(c).fill_null(0).cast(pl.Int8) for c in observed_cols])

    # vol_delta is an increment; missing seconds should be 0, not forward-filled.
    vol_delta_cols = [c for c in features_df.columns if "vol_delta" in c]
    features_df = features_df.with_columns([pl.col(c).fill_null(0).cast(pl.Int64) for c in vol_delta_cols])

    fill_cols = [
        c
        for c in features_df.columns
        if c not in {"timestamp", "underlying", *observed_cols, *vol_delta_cols}
    ]
    if fill_cols:
        features_df = features_df.with_columns([pl.col(c).fill_null(strategy="forward") for c in fill_cols])
        features_df = features_df.with_columns([pl.col(c).fill_null(strategy="backward") for c in fill_cols])

    # Primary key (useful for downstream joins and frontend playback).
    features_df = features_df.with_columns(
        pl.col("timestamp").dt.epoch(time_unit="ns").cast(pl.Int64).alias("timestamp_ns")
    )

    print(f"  Created {len(features_df):,} per-second rows (full session grid)")
    
    # Compute returns and RV
    print("Computing returns and realized volatility...")
    
    spot_prices = features_df['spot_price'].cast(pl.Float64).to_numpy()
    returns = compute_log_returns(spot_prices)
    rv_10, rv_30, rv_120 = compute_all_rv_windows(returns)

    # Multi-horizon returns (log, additive)
    cum_ret = np.cumsum(returns)
    ret_5s = np.zeros(len(returns))
    ret_10s = np.zeros(len(returns))
    if len(returns) > 5:
        ret_5s[5:] = cum_ret[5:] - cum_ret[:-5]
    if len(returns) > 10:
        ret_10s[10:] = cum_ret[10:] - cum_ret[:-10]
    
    # Add to DataFrame
    features_df = features_df.with_columns([
        pl.Series('ret_1s', returns),
        pl.Series('ret_5s', ret_5s),
        pl.Series('ret_10s', ret_10s),
        pl.Series('rv_10s', rv_10),
        pl.Series('rv_30s', rv_30),
        pl.Series('rv_120s', rv_120),
    ])
    
    # Compute acceleration
    print("Computing acceleration...")
    accel = compute_acceleration_series(returns, window=10)
    features_df = features_df.with_columns([pl.Series('accel_10s', accel)])
    
    # Compute option deltas (1s changes)
    print("Computing option deltas...")
    ce_mids = features_df['ce_mid'].cast(pl.Float64).to_numpy()
    pe_mids = features_df['pe_mid'].cast(pl.Float64).to_numpy()
    
    dOptCE_1s = np.concatenate([[0.0], np.diff(ce_mids)])
    dOptPE_1s = np.concatenate([[0.0], np.diff(pe_mids)])
    
    features_df = features_df.with_columns([
        pl.Series('dOptCE_1s', dOptCE_1s),
        pl.Series('dOptPE_1s', dOptPE_1s),
    ])

    # Flow intensity proxies (options)
    if "ce_vol_delta" in features_df.columns and "pe_vol_delta" in features_df.columns:
        opt_vol_1s = (
            features_df["ce_vol_delta"].cast(pl.Int64) + features_df["pe_vol_delta"].cast(pl.Int64)
        ).to_numpy()
        opt_active_1s = (opt_vol_1s > 0).astype(np.int8)
        features_df = features_df.with_columns(
            [
                pl.Series("opt_vol_1s", opt_vol_1s),
                pl.Series("opt_active_1s", opt_active_1s),
            ]
        )

    # Liquidity pull/replenish rates (ATM CE/PE)
    print("Computing pull/replenish + flicker metrics...")
    for prefix in ("ce", "pe"):
        bid_depth_col = f"{prefix}_bid_depth_5"
        ask_depth_col = f"{prefix}_ask_depth_5"
        spread_col = f"{prefix}_spread"
        vol_delta_col = f"{prefix}_vol_delta"

        if (
            bid_depth_col not in features_df.columns
            or ask_depth_col not in features_df.columns
            or spread_col not in features_df.columns
            or vol_delta_col not in features_df.columns
        ):
            continue

        bid_depth = features_df[bid_depth_col].cast(pl.Float64).to_numpy()
        ask_depth = features_df[ask_depth_col].cast(pl.Float64).to_numpy()
        spread = features_df[spread_col].cast(pl.Float64).to_numpy()
        vol_delta = features_df[vol_delta_col].cast(pl.Float64).to_numpy()

        pull_ev, repl_ev, spread_chg = compute_pull_replenish_events(
            bid_depth,
            ask_depth,
            spread,
            vol_delta,
            LIQUIDITY_P_DROP,
            LIQUIDITY_P_RISE,
            LIQUIDITY_VOL_SMALL,
        )

        window = LIQUIDITY_RATE_WINDOW_S
        pull_ct = _rolling_count(pull_ev, window)
        repl_ct = _rolling_count(repl_ev, window)
        spread_ct = _rolling_count(spread_chg, window)

        pull_rate = pull_ct.astype(np.float64) / window
        repl_rate = repl_ct.astype(np.float64) / window
        net_rate = repl_rate - pull_rate

        flicker = (
            (pull_ct >= FLICKER_PULLS_MIN)
            & (repl_ct >= FLICKER_REPLENISH_MIN)
            & (spread_ct >= FLICKER_SPREAD_CHANGES_MIN)
        ).astype(np.int8)

        features_df = features_df.with_columns(
            [
                pl.Series(f"{prefix}_pull_rate_30s", pull_rate),
                pl.Series(f"{prefix}_replenish_rate_30s", repl_rate),
                pl.Series(f"{prefix}_net_liquidity_30s", net_rate),
                pl.Series(f"{prefix}_flicker_30s", flicker),
            ]
        )

    if "ce_flicker_30s" in features_df.columns or "pe_flicker_30s" in features_df.columns:
        flicker_exprs: list[pl.Expr] = []
        flicker_exprs.append(pl.col("ce_flicker_30s").cast(pl.Int8) if "ce_flicker_30s" in features_df.columns else pl.lit(0).cast(pl.Int8))
        flicker_exprs.append(pl.col("pe_flicker_30s").cast(pl.Int8) if "pe_flicker_30s" in features_df.columns else pl.lit(0).cast(pl.Int8))
        features_df = features_df.with_columns([pl.max_horizontal(flicker_exprs).alias("flicker_30s")])
    
    # Detect bursts
    print("Detecting bursts...")
    
    timestamps_np = features_df['timestamp'].to_numpy()
    dte_days_np = features_df['dte_days'].to_numpy()
    
    underlying_code = 0 if underlying == 'NIFTY' else 1
    
    burst_data = detect_bursts_numba(
        timestamps_np,
        spot_prices,
        returns,
        rv_10,
        rv_120,
        dte_days_np,
        ce_mids,
        pe_mids,
        underlying_code
    )
    
    burst_events = create_burst_events(
        burst_data,
        timestamps_np,
        spot_prices,
        dte_days_np,
        ce_mids,
        pe_mids
    )
    
    print(f"  Detected {len(burst_events)} burst events")

    # ------------------------------------------------------------------------
    # Burst-level decomposition: trade pressure vs liquidity vacuum (ATM-based)
    # ------------------------------------------------------------------------
    burst_ids, start_idxs, end_idxs, _sizes, _dirs = burst_data
    burst_scores = None
    if len(burst_ids) > 0:
        ce_spread = features_df["ce_spread"].cast(pl.Float64).to_numpy()
        pe_spread = features_df["pe_spread"].cast(pl.Float64).to_numpy()
        spread_total = ce_spread + pe_spread

        ce_bid_d = features_df["ce_bid_depth_5"].cast(pl.Float64).to_numpy()
        ce_ask_d = features_df["ce_ask_depth_5"].cast(pl.Float64).to_numpy()
        pe_bid_d = features_df["pe_bid_depth_5"].cast(pl.Float64).to_numpy()
        pe_ask_d = features_df["pe_ask_depth_5"].cast(pl.Float64).to_numpy()
        depth_total = ce_bid_d + ce_ask_d + pe_bid_d + pe_ask_d

        opt_vol_1s = (
            features_df["opt_vol_1s"].cast(pl.Int64).to_numpy()
            if "opt_vol_1s" in features_df.columns
            else (features_df["ce_vol_delta"].cast(pl.Int64) + features_df["pe_vol_delta"].cast(pl.Int64)).to_numpy()
        )

        tps = np.zeros(len(burst_ids), dtype=np.float64)
        vs = np.zeros(len(burst_ids), dtype=np.float64)
        spread_increase_pct = np.zeros(len(burst_ids), dtype=np.float64)
        depth_drop_pct = np.zeros(len(burst_ids), dtype=np.float64)
        opt_vol_sum = np.zeros(len(burst_ids), dtype=np.int64)

        for i in range(len(burst_ids)):
            s = int(start_idxs[i])
            e = int(end_idxs[i])
            if e < s:
                s, e = e, s
            if s < 0:
                s = 0
            if e >= len(spot_prices):
                e = len(spot_prices) - 1

            dur = max(e - s, 1)
            vol_sum = int(opt_vol_1s[s : e + 1].sum())
            opt_vol_sum[i] = vol_sum
            tps[i] = vol_sum / dur

            sp0 = spread_total[s]
            sp_max = float(np.max(spread_total[s : e + 1]))
            sp_inc = max(0.0, sp_max - float(sp0))
            spread_increase_pct[i] = sp_inc / (float(sp0) + 1e-10)

            d0 = depth_total[s]
            d_min = float(np.min(depth_total[s : e + 1]))
            d_drop = max(0.0, float(d0) - d_min)
            depth_drop_pct[i] = d_drop / (float(d0) + 1e-10)

            spot_move = abs(float(spot_prices[e]) - float(spot_prices[s]))
            vs[i] = (
                0.60 * spread_increase_pct[i]
                + 0.30 * depth_drop_pct[i]
                + 0.10 * (spot_move / (1.0 + float(vol_sum)))
            )

        burst_scores = {
            "tps": tps,
            "vs": vs,
            "spread_increase_pct": spread_increase_pct,
            "depth_drop_pct": depth_drop_pct,
            "opt_vol_sum": opt_vol_sum,
        }

    # ------------------------------------------------------------------------
    # Market-state classifier (per second): burst / fear / flicker / chop
    # ------------------------------------------------------------------------
    n = len(features_df)
    in_burst = np.zeros(n, dtype=np.int8)
    burst_id_series = np.full(n, -1, dtype=np.int32)
    if len(burst_ids) > 0:
        for i in range(len(burst_ids)):
            s = int(start_idxs[i])
            e = int(end_idxs[i])
            if e < s:
                s, e = e, s
            s = max(s, 0)
            e = min(e, n - 1)
            in_burst[s : e + 1] = 1
            burst_id_series[s : e + 1] = int(burst_ids[i])

    # Fear (heuristic): after a down-burst with strong PE response + liquidity snap-back.
    fear_event = np.zeros(len(burst_ids), dtype=np.int8)
    fear_active = np.zeros(n, dtype=np.int8)
    if len(burst_ids) > 0 and "ce_pull_rate_30s" in features_df.columns and "pe_pull_rate_30s" in features_df.columns:
        rv_10_np = features_df["rv_10s"].cast(pl.Float64).to_numpy()
        pull_rate = np.maximum(
            features_df["ce_pull_rate_30s"].cast(pl.Float64).to_numpy(),
            features_df["pe_pull_rate_30s"].cast(pl.Float64).to_numpy(),
        )
        repl_rate = np.maximum(
            features_df["ce_replenish_rate_30s"].cast(pl.Float64).to_numpy(),
            features_df["pe_replenish_rate_30s"].cast(pl.Float64).to_numpy(),
        )
        fear_window = 60  # seconds after burst ends
        for i in range(len(burst_ids)):
            s = int(start_idxs[i])
            e = int(end_idxs[i])
            if e < s:
                s, e = e, s
            s = max(s, 0)
            e = min(e, n - 1)

            # Must be a down burst
            if int(_dirs[i]) != -1:
                continue

            # Strong PE response (magnitude)
            pe_rel = abs(float(burst_events[i].pe_rel_delta))
            if pe_rel < 1.0:
                continue

            # Liquidity pull at start, then replenish afterwards
            pull0 = float(pull_rate[s])
            if pull0 < 0.10:
                continue

            post_start = min(e + 1, n - 1)
            post_end = min(e + 31, n)
            if post_start >= post_end:
                continue

            pull_post = float(np.mean(pull_rate[post_start:post_end]))
            repl_post = float(np.mean(repl_rate[post_start:post_end]))
            if repl_post <= pull_post:
                continue

            # Volatility decays across the burst
            if float(rv_10_np[e]) >= float(rv_10_np[s]):
                continue

            fear_event[i] = 1

            fear_s = min(e + 1, n)  # after burst end
            fear_e = min(e + 1 + fear_window, n)
            fear_active[fear_s:fear_e] = 1

    # Chop pockets: low RV + low activity + stable spread (heuristic, day-adaptive)
    chop_active = np.zeros(n, dtype=np.int8)
    if "opt_vol_1s" in features_df.columns:
        rv_30_np = features_df["rv_30s"].cast(pl.Float64).to_numpy()
        opt_vol_1s = features_df["opt_vol_1s"].cast(pl.Int64).to_numpy()
        spread_total = (
            features_df["ce_spread"].cast(pl.Float64) + features_df["pe_spread"].cast(pl.Float64)
        ).to_numpy()

        # Rolling 30s volume sum
        w = 30
        vol_cs = np.cumsum(opt_vol_1s.astype(np.int64))
        vol_30s = vol_cs.copy()
        if n > w:
            vol_30s[w:] = vol_cs[w:] - vol_cs[:-w]

        rv_low = float(np.quantile(rv_30_np, 0.25))
        vol_low = float(np.quantile(vol_30s.astype(np.float64), 0.25))
        spread_low = float(np.quantile(spread_total, 0.50))

        is_chop = (
            (rv_30_np <= rv_low)
            & (vol_30s.astype(np.float64) <= vol_low)
            & (spread_total <= spread_low)
            & (in_burst == 0)
            & (fear_active == 0)
        )
        chop_active = is_chop.astype(np.int8)

    # Regime code precedence: normal < chop < flicker < burst ; fear overrides outside bursts.
    regime_code = np.zeros(n, dtype=np.int8)
    regime_code[chop_active == 1] = 1
    if "flicker_30s" in features_df.columns:
        regime_code[features_df["flicker_30s"].cast(pl.Int8).to_numpy() == 1] = 2
    regime_code[in_burst == 1] = 3
    regime_code[(fear_active == 1) & (in_burst == 0)] = 4

    regime_labels = np.array(["normal", "chop", "flicker", "burst", "fear"], dtype=object)[regime_code]

    features_df = features_df.with_columns(
        [
            pl.Series("in_burst", in_burst),
            pl.Series("burst_id", burst_id_series),
            pl.Series("fear_active", fear_active),
            pl.Series("chop_active", chop_active),
            pl.Series("regime_code", regime_code),
            pl.Series("regime", regime_labels),
        ]
    )

    if round_output:
        features_df = _round_for_output(
            features_df,
            price_decimals=price_decimals,
            ratio_decimals=ratio_decimals,
            small_decimals=small_decimals,
        )

    # Save outputs
    print("\nSaving outputs...")

    (output_dir / "features").mkdir(parents=True, exist_ok=True)
    (output_dir / "bursts").mkdir(parents=True, exist_ok=True)

    # Features
    features_file = output_dir / "features" / f"features_{underlying}_{date}.parquet"
    features_df.write_parquet(features_file)
    print(f"  ✓ Saved features: {features_file}")
    
    # Bursts
    bursts_schema = {
        "underlying": pl.String,
        "burst_id": pl.Int32,
        "start_time": pl.Datetime(time_unit="us", time_zone=None),
        "end_time": pl.Datetime(time_unit="us", time_zone=None),
        "duration_seconds": pl.Int32,
        "size_points": pl.Float64,
        "direction": pl.Int32,
        "start_price": pl.Float64,
        "end_price": pl.Float64,
        "max_price": pl.Float64,
        "min_price": pl.Float64,
        "ce_move": pl.Float64,
        "pe_move": pl.Float64,
        "ce_rel_delta": pl.Float64,
        "pe_rel_delta": pl.Float64,
        "dte_at_start": pl.Int32,
        "time_of_day": pl.String,
        "tps": pl.Float64,
        "vacuum_score": pl.Float64,
        "spread_increase_pct": pl.Float64,
        "depth_drop_pct": pl.Float64,
        "opt_vol_sum": pl.Int64,
        "fear_event": pl.Int8,
    }

    if burst_events:
        start_times = np.array([e.start_time for e in burst_events], dtype="datetime64[us]")
        end_times = np.array([e.end_time for e in burst_events], dtype="datetime64[us]")
        if burst_scores is None:
            burst_scores = {
                "tps": np.zeros(len(burst_events), dtype=np.float64),
                "vs": np.zeros(len(burst_events), dtype=np.float64),
                "spread_increase_pct": np.zeros(len(burst_events), dtype=np.float64),
                "depth_drop_pct": np.zeros(len(burst_events), dtype=np.float64),
                "opt_vol_sum": np.zeros(len(burst_events), dtype=np.int64),
            }
        bursts_data = {
            "underlying": [underlying for _ in burst_events],
            "burst_id": [e.burst_id for e in burst_events],
            "start_time": start_times,
            "end_time": end_times,
            "duration_seconds": [e.duration_seconds for e in burst_events],
            "size_points": [e.size_points for e in burst_events],
            "direction": [e.direction for e in burst_events],
            "start_price": [e.start_price for e in burst_events],
            "end_price": [e.end_price for e in burst_events],
            "max_price": [e.max_price for e in burst_events],
            "min_price": [e.min_price for e in burst_events],
            "ce_move": [e.ce_move for e in burst_events],
            "pe_move": [e.pe_move for e in burst_events],
            "ce_rel_delta": [e.ce_rel_delta for e in burst_events],
            "pe_rel_delta": [e.pe_rel_delta for e in burst_events],
            "dte_at_start": [e.dte_at_start for e in burst_events],
            "time_of_day": [e.time_of_day for e in burst_events],
            "tps": burst_scores["tps"],
            "vacuum_score": burst_scores["vs"],
            "spread_increase_pct": burst_scores["spread_increase_pct"],
            "depth_drop_pct": burst_scores["depth_drop_pct"],
            "opt_vol_sum": burst_scores["opt_vol_sum"],
            "fear_event": fear_event,
        }
        bursts_df = pl.DataFrame(bursts_data, schema=bursts_schema)
    else:
        bursts_df = pl.DataFrame(schema=bursts_schema)

    if round_output and bursts_df.height > 0:
        bursts_df = bursts_df.with_columns(
            [
                pl.col("size_points").round(price_decimals),
                pl.col("start_price").round(price_decimals),
                pl.col("end_price").round(price_decimals),
                pl.col("max_price").round(price_decimals),
                pl.col("min_price").round(price_decimals),
                pl.col("ce_move").round(price_decimals),
                pl.col("pe_move").round(price_decimals),
                pl.col("ce_rel_delta").round(ratio_decimals),
                pl.col("pe_rel_delta").round(ratio_decimals),
            ]
        )

    bursts_file = output_dir / "bursts" / f"bursts_{underlying}_{date}.parquet"
    bursts_df.write_parquet(bursts_file)
    print(f"  ✓ Saved bursts: {bursts_file}")

    # Regimes (one row per second, for playback/classifier slices)
    (output_dir / "regimes").mkdir(parents=True, exist_ok=True)
    regimes_file = output_dir / "regimes" / f"regimes_{underlying}_{date}.parquet"
    regimes_df = features_df.select(
        [
            "timestamp",
            "timestamp_ns",
            "underlying",
            "atm_strike",
            "dte_days",
            "regime",
            "regime_code",
            "in_burst",
            "burst_id",
            "fear_active",
            "flicker_30s",
            "chop_active",
        ]
    )
    regimes_df.write_parquet(regimes_file)
    print(f"  ✓ Saved regimes: {regimes_file}")
    
    print(f"\n✓ Processing complete for {underlying}/{date}")


# ============================================================================
# CLI
# ============================================================================

def main():
    base_dir = Path(__file__).resolve().parent
    default_data_dir = (base_dir / "../../data/options_date_packed_FULL_v3_SPOT_ENRICHED").resolve()
    default_output_dir = (base_dir / "../market_truth_data").resolve()

    parser = argparse.ArgumentParser(description='Market Truth Framework - Core Preprocessor')
    parser.add_argument('--date', required=True, help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--underlying', required=True, choices=['NIFTY', 'BANKNIFTY'],
                       help='Underlying to process')
    parser.add_argument('--data-dir', default=str(default_data_dir),
                       help='Path to raw data')
    parser.add_argument('--output-dir', default=str(default_output_dir),
                       help='Path for output files')
    parser.add_argument(
        "--round-output",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Round selected float outputs for readability (keeps returns/RV precise)",
    )
    parser.add_argument("--price-decimals", type=int, default=2, help="Decimals for price-like columns")
    parser.add_argument("--ratio-decimals", type=int, default=3, help="Decimals for ratio-like columns")
    parser.add_argument("--small-decimals", type=int, default=6, help="Decimals for small-magnitude columns (ret/RV)")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    
    preprocess_day(
        args.date,
        args.underlying,
        data_dir,
        output_dir,
        round_output=args.round_output,
        price_decimals=args.price_decimals,
        ratio_decimals=args.ratio_decimals,
        small_decimals=args.small_decimals,
    )


if __name__ == '__main__':
    main()
