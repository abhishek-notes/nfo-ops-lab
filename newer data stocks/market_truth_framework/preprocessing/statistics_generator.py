#!/usr/bin/env python3
"""
Statistics Generator
====================

Generates truth tables and market statistics from processed data.
"""

from pathlib import Path
import polars as pl
import numpy as np
import json
from collections import defaultdict


def load_all_bursts(bursts_dir: Path):
    """Load all burst events from all days"""
    all_bursts = []
    
    for burst_file in sorted(bursts_dir.glob("bursts_*.parquet")):
        try:
            df = pl.read_parquet(burst_file)
            all_bursts.append(df)
        except:
            continue
    
    if all_bursts:
        return pl.concat(all_bursts)
    return None


def compute_burst_statistics(bursts_df: pl.DataFrame, dte_buckets=[0, 2, 4, 6]):
    """
    Compute comprehensive burst statistics by DTE
    
    Returns dict with statistics for each DTE bucket
    """
    stats = {}
    
    for dte in dte_buckets:
        # Filter to this DTE (±1 day tolerance)
        dte_bursts = bursts_df.filter(
            (pl.col('dte_at_start') >= dte - 1) &
            (pl.col('dte_at_start') <= dte + 1)
        )
        
        if len(dte_bursts) == 0:
            continue
        
        total = len(dte_bursts)
        
        stats[dte] = {
            'total_bursts': total,
            
            # Size statistics
            'avg_burst_size': float(dte_bursts['size_points'].mean()),
            'median_burst_size': float(dte_bursts['size_points'].median()),
            'p90_burst_size': float(dte_bursts['size_points'].quantile(0.9)),
            'p95_burst_size': float(dte_bursts['size_points'].quantile(0.95)),
            'max_burst_size': float(dte_bursts['size_points'].max()),
            
            # Duration statistics
            'avg_duration': float(dte_bursts['duration_seconds'].mean()),
            'median_duration': float(dte_bursts['duration_seconds'].median()),
            'p90_duration': float(dte_bursts['duration_seconds'].quantile(0.9)),
            
            # Option response (CE)
            'avg_ce_rel_delta': float(dte_bursts['ce_rel_delta'].mean()),
            'median_ce_rel_delta': float(dte_bursts['ce_rel_delta'].median()),
            'ce_rel_delta_std': float(dte_bursts['ce_rel_delta'].std()),
            
            # Option response (PE)
            'avg_pe_rel_delta': float(dte_bursts['pe_rel_delta'].mean()),
            'median_pe_rel_delta': float(dte_bursts['pe_rel_delta'].median()),
            'pe_rel_delta_std': float(dte_bursts['pe_rel_delta'].std()),
            
            # Direction breakdown
            'up_bursts': int((dte_bursts['direction'] == 1).sum()),
            'down_bursts': int((dte_bursts['direction'] == -1).sum()),
            
            # Time of day breakdown
            'morning_bursts': int((dte_bursts['time_of_day'] == 'morning').sum()),
            'midday_bursts': int((dte_bursts['time_of_day'] == 'midday').sum()),
            'afternoon_bursts': int((dte_bursts['time_of_day'] == 'afternoon').sum()),
        }
    
    return stats


def compute_microstructure_statistics(features_dir: Path):
    """
    Compute average microstructure metrics across all days
    """
    all_features = []
    
    for features_file in sorted(features_dir.glob("features_*.parquet")):
        try:
            df = pl.read_parquet(features_file, columns=[
                'ce_spread', 'pe_spread', 
                'ce_obi_5', 'pe_obi_5',
                'ce_bid_depth_5', 'ce_ask_depth_5',
                'pe_bid_depth_5', 'pe_ask_depth_5'
            ])
            all_features.append(df)
        except:
            continue
    
    if not all_features:
        return {}
    
    combined = pl.concat(all_features)
    
    stats = {
        'ce_spread': {
            'avg': float(combined['ce_spread'].mean()),
            'median': float(combined['ce_spread'].median()),
            'p95': float(combined['ce_spread'].quantile(0.95)),
        },
        'pe_spread': {
            'avg': float(combined['pe_spread'].mean()),
            'median': float(combined['pe_spread'].median()),
            'p95': float(combined['pe_spread'].quantile(0.95)),
        },
        'ce_obi': {
            'avg': float(combined['ce_obi_5'].mean()),
            'median': float(combined['ce_obi_5'].median()),
        },
        'pe_obi': {
            'avg': float(combined['pe_obi_5'].mean()),
            'median': float(combined['pe_obi_5'].median()),
        },
        'ce_avg_bid_depth': float(combined['ce_bid_depth_5'].mean()),
        'ce_avg_ask_depth': float(combined['ce_ask_depth_5'].mean()),
        'pe_avg_bid_depth': float(combined['pe_bid_depth_5'].mean()),
        'pe_avg_ask_depth': float(combined['pe_ask_depth_5'].mean()),
    }
    
    return stats


def generate_truth_tables(data_dir: Path, output_file: Path):
    """
    Generate complete truth tables for the market
    """
    print("="*80)
    print("GENERATING TRUTH TABLES")
    print("="*80)
    
    truth_tables = {}
    
    # Load burst data
    print("\nLoading burst data...")
    bursts_dir = data_dir / 'bursts'
    bursts_df = load_all_bursts(bursts_dir)
    
    if bursts_df is not None and len(bursts_df) > 0:
        print(f"  Loaded {len(bursts_df):,} burst events")
        
        # Compute burst statistics
        print("\nComputing burst statistics by DTE...")
        burst_stats = compute_burst_statistics(bursts_df)
        truth_tables['burst_statistics'] = burst_stats
        
        for dte, stats in burst_stats.items():
            print(f"  DTE {dte}: {stats['total_bursts']} bursts, "
                  f"avg size {stats['avg_burst_size']:.1f} points, "
                  f"avg duration {stats['avg_duration']:.1f}s")
    else:
        print("  ⚠️  No burst data found")
        truth_tables['burst_statistics'] = {}
    
    # Load microstructure data
    print("\nComputing microstructure statistics...")
    features_dir = data_dir / 'features'
    micro_stats = compute_microstructure_statistics(features_dir)
    truth_tables['microstructure'] = micro_stats
    
    if micro_stats:
        print(f"  CE spread avg: ₹{micro_stats['ce_spread']['avg']:.2f}")
        print(f"  PE spread avg: ₹{micro_stats['pe_spread']['avg']:.2f}")
        print(f"  CE avg OBI: {micro_stats['ce_obi']['avg']:.3f}")
        print(f"  PE avg OBI: {micro_stats['pe_obi']['avg']:.3f}")
    
    # Count available days
    features_count = len(list(features_dir.glob("features_*.parquet")))
    bursts_count = len(list(bursts_dir.glob("bursts_*.parquet")))
    
    truth_tables['metadata'] = {
        'days_processed': features_count,
        'days_with_bursts': bursts_count,
        'generated_at': str(pl.datetime.now()),
    }
    
    # Save
    print(f"\nSaving truth tables to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(truth_tables, f, indent=2)
    
    print(f"✓ Truth tables saved")
    
    return truth_tables


def main():
    data_dir = Path("../market_truth_data")
    output_file = data_dir / "statistics" / "truth_tables.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    truth_tables = generate_truth_tables(data_dir, output_file)
    
    print(f"\n{'='*80}")
    print(f"COMPLETE")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
