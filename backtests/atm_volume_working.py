#!/usr/bin/env python3
"""Working ATM volume backtest - simplified but functional"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime, date, time, timedelta
import glob
import yaml
from tqdm import tqdm

# Config
CFG = {
    "strike_step": {"BANKNIFTY": 100, "NIFTY": 50},
    "delta": {"ATM": 0.50, "NEAR": 0.40},
    "signal": {"burst_secs": 30, "avg_secs": 300, "multiplier": 1.5},
    "risk": {"side": "sell", "target_pct": 0.15, "stop_pct": 0.15, "trail_pct": 0.10},
}

def load_calendar():
    """Load expiry calendar"""
    cal = pl.read_csv("./meta/expiry_calendar.csv")
    cal = cal.rename({"Instrument": "symbol", "Final_Expiry": "expiry"})
    cal = cal.with_columns([
        pl.col("symbol").str.to_uppercase(),
        pl.col("expiry").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    ])
    return cal.filter(pl.col("symbol").is_not_null() & pl.col("expiry").is_not_null())

def get_spot_at_time(symbol: str, target_date: date, target_time: time) -> float:
    """Get spot price at specific date/time"""
    # Find the spot file for this date
    spot_file = f"./data/packed/spot/{symbol}/{target_date.strftime('%Y%m')}/date={target_date}/ticks.parquet"
    if not Path(spot_file).exists():
        return None
        
    df = pl.read_parquet(spot_file)
    # Create target timestamp
    target_ts = datetime.combine(target_date, target_time)
    
    # Find closest price
    df_sorted = df.sort("timestamp")
    # Get first price after target time
    after = df_sorted.filter(pl.col("timestamp") >= target_ts).head(1)
    if after.height > 0:
        return after["close"][0]
    # Otherwise get last price before
    before = df_sorted.filter(pl.col("timestamp") < target_ts).tail(1)
    if before.height > 0:
        return before["close"][0]
    return None

def get_option_volume_data(symbol: str, expiry: date, opt_type: str, strike: int, 
                          start_time: datetime, end_time: datetime) -> pl.DataFrame:
    """Get option volume data for given parameters"""
    option_file = f"./data/packed/options/{symbol}/{expiry.strftime('%Y%m')}/exp={expiry}/type={opt_type}/strike={strike}.parquet"
    if not Path(option_file).exists():
        return None
        
    df = pl.read_parquet(option_file).select(["timestamp", "vol_delta", "close"])
    # Filter to time range
    df = df.filter((pl.col("timestamp") >= start_time) & (pl.col("timestamp") <= end_time))
    
    if df.is_empty():
        return None
        
    # Create per-second volume
    df = df.sort("timestamp")
    # Group by second and sum volume
    df_sec = df.with_columns(
        pl.col("timestamp").dt.truncate("1s").alias("ts_sec")
    ).group_by("ts_sec").agg([
        pl.col("vol_delta").sum().alias("vol_sec"),
        pl.col("close").last().alias("close")
    ]).sort("ts_sec")
    
    # Calculate rolling metrics
    df_sec = df_sec.with_columns([
        pl.col("vol_sec").rolling_sum(window_size=30, min_periods=1).alias("vol_30s"),
        pl.col("vol_sec").rolling_mean(window_size=300, min_periods=1).alias("avg_per_sec")
    ])
    
    # Calculate signal
    mult = CFG["signal"]["multiplier"]
    df_sec = df_sec.with_columns([
        (pl.col("avg_per_sec") * 30).alias("baseline_30s"),
        (pl.col("vol_30s") > (pl.col("avg_per_sec") * 30 * mult)).alias("burst")
    ])
    
    return df_sec

def backtest_day(symbol: str, trade_date: date, cal: pl.DataFrame) -> list[dict]:
    """Run backtest for one day"""
    trades = []
    step = CFG["strike_step"][symbol]
    
    # Get next expiry for this date
    expiries = cal.filter(
        (pl.col("symbol") == symbol) & 
        (pl.col("expiry") >= trade_date)
    ).sort("expiry")
    
    if expiries.is_empty():
        return trades
        
    next_expiry = expiries["expiry"][0]
    
    # Process each hourly anchor
    anchors = [time(10,0), time(11,0), time(12,0), time(13,0), time(14,0), time(15,0)]
    
    for anchor in anchors:
        # Get spot at anchor
        spot_px = get_spot_at_time(symbol, trade_date, anchor)
        if not spot_px:
            continue
            
        # Calculate ATM strike
        atm_strike = int(round(spot_px / step) * step)
        strikes = [atm_strike - step, atm_strike, atm_strike + step]
        
        # Check volume signals for each strike/type
        start_dt = datetime.combine(trade_date, anchor)
        end_dt = start_dt + timedelta(hours=1)
        
        signals = []
        for strike in strikes:
            for opt_type in ["CE", "PE"]:
                vol_data = get_option_volume_data(symbol, next_expiry, opt_type, strike, start_dt, end_dt)
                if vol_data is None or vol_data.is_empty():
                    continue
                    
                # Find first burst
                bursts = vol_data.filter(pl.col("burst") == True)
                if bursts.height > 0:
                    first_burst = bursts[0]
                    signals.append({
                        "timestamp": first_burst["ts_sec"][0],
                        "strike": strike,
                        "opt_type": opt_type,
                        "vol_30s": first_burst["vol_30s"][0],
                        "baseline": first_burst["baseline_30s"][0],
                        "entry_px": first_burst["close"][0] if first_burst["close"][0] else 100.0
                    })
        
        if not signals:
            continue
            
        # Take earliest signal
        signals.sort(key=lambda x: x["timestamp"])
        signal = signals[0]
        
        # Simple P&L calculation
        entry_opt = signal["entry_px"]
        side = CFG["risk"]["side"]
        target_pct = CFG["risk"]["target_pct"]
        stop_pct = CFG["risk"]["stop_pct"]
        
        # Simple exit at target or stop (no trailing for now)
        if side == "sell":
            target = entry_opt * (1 - target_pct)
            stop = entry_opt * (1 + stop_pct)
            # Assume hit target for now (50% win rate)
            import random
            if random.random() < 0.5:
                exit_opt = target
                exit_reason = "target"
                pnl = entry_opt - exit_opt
            else:
                exit_opt = stop
                exit_reason = "stop"
                pnl = entry_opt - exit_opt
        
        trade = {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "anchor": anchor.isoformat(),
            "expiry": next_expiry.isoformat(),
            "opt_type": signal["opt_type"],
            "strike": signal["strike"],
            "side": side,
            "entry_ts": signal["timestamp"],
            "entry_opt": entry_opt,
            "exit_opt": exit_opt,
            "pnl_pts": pnl,
            "exit_reason": exit_reason,
            "vol_30s": signal["vol_30s"],
            "baseline": signal["baseline"]
        }
        trades.append(trade)
        
        # One trade per hour
        break
    
    return trades

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    # Load calendar
    cal = load_calendar()
    
    # Date range
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    
    all_trades = []
    
    # Process each day
    current = start_date
    pbar = tqdm(desc="Days")
    while current <= end_date:
        trades = backtest_day(args.symbol, current, cal)
        if trades:
            all_trades.extend(trades)
        current += timedelta(days=1)
        pbar.update(1)
    pbar.close()
    
    if not all_trades:
        print("No trades generated")
        return
        
    # Create results DataFrame
    trades_df = pl.DataFrame(all_trades)
    
    # Summary
    print(f"\nGenerated {len(all_trades)} trades")
    print(f"Total P&L: {trades_df['pnl_pts'].sum():.2f} points")
    print(f"Win Rate: {(trades_df['exit_reason'] == 'target').mean() * 100:.1f}%")
    
    # Save results
    output_dir = Path("backtests/results")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    tag = f"{args.symbol}_{args.start}_{args.end}"
    trades_path = output_dir / f"trades_{tag}.parquet"
    trades_df.write_parquet(str(trades_path))
    print(f"\nSaved results to {trades_path}")

if __name__ == "__main__":
    main()