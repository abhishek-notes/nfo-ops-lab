#!/usr/bin/env python3
"""
Statistics Generator
====================

Generates truth tables and market statistics from processed data.
"""

from pathlib import Path
from datetime import datetime
import polars as pl
import numpy as np
import json
from collections import defaultdict


def _parse_underlying_and_date(stem: str, prefix: str):
    """
    Parse filenames like:
      - f"{prefix}_{UNDERLYING}_{YYYY-MM-DD}.parquet"
      - legacy: f"{prefix}_{YYYY-MM-DD}.parquet"
    """
    parts = stem.split("_")
    if len(parts) >= 3:
        return parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[1]
    return None, None


def _filter_bursts_by_dte_bucket(bursts_df: pl.DataFrame, bucket: int) -> pl.DataFrame:
    """
    Bucket mapping (consistent with the preprocessor's burst-param buckets):
      - 0: dte <= 0
      - 2: 1 <= dte <= 2
      - 4: 3 <= dte <= 4
      - 6: dte >= 5   (labeled "6+")
    """
    if bucket == 0:
        return bursts_df.filter(pl.col("dte_at_start") <= 0)
    if bucket == 2:
        return bursts_df.filter((pl.col("dte_at_start") >= 1) & (pl.col("dte_at_start") <= 2))
    if bucket == 4:
        return bursts_df.filter((pl.col("dte_at_start") >= 3) & (pl.col("dte_at_start") <= 4))
    if bucket == 6:
        return bursts_df.filter(pl.col("dte_at_start") >= 5)
    raise ValueError(f"Unsupported DTE bucket: {bucket}")


def _safe_float(x) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, float) and np.isnan(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _quantile_or_default(series: pl.Series, q: float, default: float = 0.0) -> float:
    try:
        if series.len() == 0:
            return default
        v = series.quantile(q)
        return _safe_float(v)
    except Exception:
        return default


def load_all_bursts(bursts_dir: Path):
    """Load all burst events from all days"""
    all_bursts = []
    
    for burst_file in sorted(bursts_dir.glob("bursts_*.parquet")):
        try:
            underlying, day = _parse_underlying_and_date(burst_file.stem, "bursts")
            df = pl.read_parquet(burst_file)
            if underlying and "underlying" not in df.columns:
                df = df.with_columns(pl.lit(underlying).alias("underlying"))
            if day and "date" not in df.columns:
                df = df.with_columns(pl.lit(day).alias("date"))
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
        dte_bursts = _filter_bursts_by_dte_bucket(bursts_df, dte)
        
        if len(dte_bursts) == 0:
            continue
        
        total = len(dte_bursts)

        ce_std = dte_bursts["ce_rel_delta"].std()
        pe_std = dte_bursts["pe_rel_delta"].std()
        if ce_std is None or np.isnan(ce_std):
            ce_std = 0.0
        if pe_std is None or np.isnan(pe_std):
            pe_std = 0.0
        
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
            'ce_rel_delta_std': float(ce_std),
            
            # Option response (PE)
            'avg_pe_rel_delta': float(dte_bursts['pe_rel_delta'].mean()),
            'median_pe_rel_delta': float(dte_bursts['pe_rel_delta'].median()),
            'pe_rel_delta_std': float(pe_std),
            
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
    Compute average microstructure metrics across all days (grouped by underlying when possible).
    """
    per_underlying = defaultdict(list)
    
    for features_file in sorted(features_dir.glob("features_*.parquet")):
        try:
            underlying, _day = _parse_underlying_and_date(features_file.stem, "features")
            df = pl.read_parquet(features_file, columns=[
                'ce_spread', 'pe_spread', 
                'ce_obi_5', 'pe_obi_5',
                'ce_bid_depth_5', 'ce_ask_depth_5',
                'pe_bid_depth_5', 'pe_ask_depth_5'
            ])
            per_underlying[underlying or "UNKNOWN"].append(df)
        except:
            continue
    
    if not per_underlying:
        return {}

    stats_by_underlying = {}
    for underlying, dfs in per_underlying.items():
        combined = pl.concat(dfs)
        stats_by_underlying[underlying] = {
            "ce_spread": {
                "avg": float(combined["ce_spread"].mean()),
                "median": float(combined["ce_spread"].median()),
                "p95": float(combined["ce_spread"].quantile(0.95)),
            },
            "pe_spread": {
                "avg": float(combined["pe_spread"].mean()),
                "median": float(combined["pe_spread"].median()),
                "p95": float(combined["pe_spread"].quantile(0.95)),
            },
            "ce_obi": {
                "avg": float(combined["ce_obi_5"].mean()),
                "median": float(combined["ce_obi_5"].median()),
            },
            "pe_obi": {
                "avg": float(combined["pe_obi_5"].mean()),
                "median": float(combined["pe_obi_5"].median()),
            },
            "ce_avg_bid_depth": float(combined["ce_bid_depth_5"].mean()),
            "ce_avg_ask_depth": float(combined["ce_ask_depth_5"].mean()),
            "pe_avg_bid_depth": float(combined["pe_bid_depth_5"].mean()),
            "pe_avg_ask_depth": float(combined["pe_ask_depth_5"].mean()),
        }

    return stats_by_underlying


def compute_liquidity_statistics(features_dir: Path):
    """
    Aggregate liquidity pull/replenish/flicker metrics across all days (by underlying).
    """
    per_underlying = defaultdict(list)

    cols = [
        "ce_pull_rate_30s",
        "ce_replenish_rate_30s",
        "ce_net_liquidity_30s",
        "ce_flicker_30s",
        "pe_pull_rate_30s",
        "pe_replenish_rate_30s",
        "pe_net_liquidity_30s",
        "pe_flicker_30s",
        "flicker_30s",
        "opt_vol_1s",
        "opt_active_1s",
        "rv_30s",
        "dte_days",
    ]

    for features_file in sorted(features_dir.glob("features_*.parquet")):
        try:
            underlying, _day = _parse_underlying_and_date(features_file.stem, "features")
            df = pl.read_parquet(features_file, columns=[c for c in cols if c in pl.read_parquet_schema(features_file)])
            per_underlying[underlying or "UNKNOWN"].append(df)
        except Exception:
            continue

    if not per_underlying:
        return {}

    out = {}
    for underlying, dfs in per_underlying.items():
        combined = pl.concat(dfs)

        def agg_rate(col: str):
            if col not in combined.columns:
                return {"avg": 0.0, "p95": 0.0}
            s = combined[col]
            return {
                "avg": _safe_float(s.mean()),
                "p95": _quantile_or_default(s, 0.95),
            }

        def agg_frac(col: str):
            if col not in combined.columns:
                return 0.0
            return _safe_float((combined[col].cast(pl.Int64) == 1).mean())

        out[underlying] = {
            "ce_pull_rate_30s": agg_rate("ce_pull_rate_30s"),
            "ce_replenish_rate_30s": agg_rate("ce_replenish_rate_30s"),
            "pe_pull_rate_30s": agg_rate("pe_pull_rate_30s"),
            "pe_replenish_rate_30s": agg_rate("pe_replenish_rate_30s"),
            "flicker_frac": agg_frac("flicker_30s"),
            "ce_flicker_frac": agg_frac("ce_flicker_30s"),
            "pe_flicker_frac": agg_frac("pe_flicker_30s"),
        }

    return out


def compute_regime_statistics(regimes_dir: Path):
    """
    Aggregate regime mix + fear/chop/burst seconds across all days (by underlying).
    """
    per_underlying = defaultdict(list)

    for f in sorted(regimes_dir.glob("regimes_*.parquet")):
        try:
            underlying, _day = _parse_underlying_and_date(f.stem, "regimes")
            df = pl.read_parquet(f, columns=["regime", "regime_code", "fear_active", "in_burst", "flicker_30s", "chop_active"])
            per_underlying[underlying or "UNKNOWN"].append(df)
        except Exception:
            continue

    if not per_underlying:
        return {}

    out = {}
    for underlying, dfs in per_underlying.items():
        combined = pl.concat(dfs)
        total = int(combined.height) if combined.height else 0
        if total == 0:
            out[underlying] = {}
            continue

        # Regime mix
        mix = (
            combined.group_by(["regime", "regime_code"])
            .len()
            .with_columns((pl.col("len") / total).alias("frac"))
            .sort("len", descending=True)
        )

        out[underlying] = {
            "total_seconds": total,
            "regime_mix": [
                {
                    "regime": r["regime"],
                    "regime_code": int(r["regime_code"]),
                    "seconds": int(r["len"]),
                    "frac": _safe_float(r["frac"]),
                }
                for r in mix.iter_rows(named=True)
            ],
            "fear_frac": _safe_float((combined["fear_active"].cast(pl.Int64) == 1).mean()) if "fear_active" in combined.columns else 0.0,
            "burst_frac": _safe_float((combined["in_burst"].cast(pl.Int64) == 1).mean()) if "in_burst" in combined.columns else 0.0,
            "flicker_frac": _safe_float((combined["flicker_30s"].cast(pl.Int64) == 1).mean()) if "flicker_30s" in combined.columns else 0.0,
            "chop_frac": _safe_float((combined["chop_active"].cast(pl.Int64) == 1).mean()) if "chop_active" in combined.columns else 0.0,
        }

    return out


def compute_vacuum_vs_trade_statistics(bursts_df: pl.DataFrame, dte_buckets=[0, 2, 4, 6]):
    """
    Decompose bursts into trade-driven vs vacuum-driven vs mixed using (TPS, vacuum_score).
    Classification thresholds are computed per underlying from global quantiles.
    """
    needed = {"tps", "vacuum_score"}
    if not needed.issubset(set(bursts_df.columns)):
        return {}

    out = {}
    for underlying in bursts_df["underlying"].unique().to_list() if "underlying" in bursts_df.columns else ["ALL"]:
        udf = bursts_df.filter(pl.col("underlying") == underlying) if underlying != "ALL" else bursts_df
        if udf.is_empty():
            continue

        tps = udf["tps"].cast(pl.Float64)
        vs = udf["vacuum_score"].cast(pl.Float64)
        tps_hi = _quantile_or_default(tps, 0.70)
        tps_lo = _quantile_or_default(tps, 0.30)
        vs_hi = _quantile_or_default(vs, 0.70)
        vs_lo = _quantile_or_default(vs, 0.30)

        def classify(df: pl.DataFrame) -> dict:
            if df.is_empty():
                return {"trade_driven": 0, "vacuum_driven": 0, "mixed": 0, "total": 0}
            cond_trade = (pl.col("tps") >= tps_hi) & (pl.col("vacuum_score") <= vs_lo)
            cond_vac = (pl.col("vacuum_score") >= vs_hi) & (pl.col("tps") <= tps_lo)
            total = int(df.height)
            trade_ct = int(df.select(cond_trade.sum()).item())
            vac_ct = int(df.select(cond_vac.sum()).item())
            mixed_ct = total - trade_ct - vac_ct
            return {
                "trade_driven": trade_ct,
                "vacuum_driven": vac_ct,
                "mixed": mixed_ct,
                "total": total,
            }

        overall = classify(udf)

        by_dte = {}
        for bucket in dte_buckets:
            by_dte[str(bucket)] = classify(_filter_bursts_by_dte_bucket(udf, bucket))

        by_tod = {}
        if "time_of_day" in udf.columns:
            for tod in ["morning", "midday", "afternoon"]:
                by_tod[tod] = classify(udf.filter(pl.col("time_of_day") == tod))

        out[underlying] = {
            "thresholds": {
                "tps_hi": tps_hi,
                "tps_lo": tps_lo,
                "vacuum_hi": vs_hi,
                "vacuum_lo": vs_lo,
            },
            "overall": overall,
            "by_dte_bucket": by_dte,
            "by_time_of_day": by_tod,
        }

    return out


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
        if "underlying" in bursts_df.columns:
            burst_stats = {}
            for underlying in bursts_df["underlying"].unique().to_list():
                burst_stats[underlying] = compute_burst_statistics(
                    bursts_df.filter(pl.col("underlying") == underlying)
                )
            truth_tables["burst_statistics"] = burst_stats

            for underlying, u_stats in burst_stats.items():
                for dte, stats in u_stats.items():
                    print(
                        f"  {underlying} DTE {dte}: {stats['total_bursts']} bursts, "
                        f"avg size {stats['avg_burst_size']:.1f} points, "
                        f"avg duration {stats['avg_duration']:.1f}s"
                    )
        else:
            burst_stats = compute_burst_statistics(bursts_df)
            truth_tables["burst_statistics"] = burst_stats

            for dte, stats in burst_stats.items():
                print(
                    f"  DTE {dte}: {stats['total_bursts']} bursts, "
                    f"avg size {stats['avg_burst_size']:.1f} points, "
                    f"avg duration {stats['avg_duration']:.1f}s"
                )
    else:
        print("  ⚠️  No burst data found")
        truth_tables['burst_statistics'] = {}
    
    # Load microstructure data
    print("\nComputing microstructure statistics...")
    features_dir = data_dir / 'features'
    micro_stats = compute_microstructure_statistics(features_dir)
    truth_tables['microstructure'] = micro_stats
    
    if micro_stats:
        # Print one-line summary for each underlying.
        for underlying, stats in micro_stats.items():
            print(f"  {underlying} CE spread avg: ₹{stats['ce_spread']['avg']:.2f}")
            print(f"  {underlying} PE spread avg: ₹{stats['pe_spread']['avg']:.2f}")

    # Liquidity stats (pull/replenish/flicker)
    print("\nComputing liquidity statistics...")
    liquidity_stats = compute_liquidity_statistics(features_dir)
    truth_tables["liquidity"] = liquidity_stats

    # Regime mix stats
    print("\nComputing regime statistics...")
    regimes_dir = data_dir / "regimes"
    regime_stats = compute_regime_statistics(regimes_dir)
    truth_tables["regimes"] = regime_stats

    # Vacuum vs trade decomposition (burst-level)
    if bursts_df is not None and len(bursts_df) > 0:
        print("\nComputing trade-vs-vacuum decomposition...")
        truth_tables["vacuum_vs_trade"] = compute_vacuum_vs_trade_statistics(bursts_df)
    
    # Count available days
    features_files = list(features_dir.glob("features_*.parquet"))
    bursts_files = list(bursts_dir.glob("bursts_*.parquet"))
    features_file_count = len(features_files)
    bursts_file_count = len(bursts_files)
    regimes_file_count = len(list((data_dir / "regimes").glob("regimes_*.parquet")))

    days_by_underlying = defaultdict(set)
    for f in features_files:
        underlying, day = _parse_underlying_and_date(f.stem, "features")
        if day:
            days_by_underlying[underlying or "UNKNOWN"].add(day)

    days_with_bursts_by_underlying = defaultdict(set)
    total_burst_events = 0
    if bursts_df is not None and len(bursts_df) > 0:
        total_burst_events = int(len(bursts_df))
        if "underlying" in bursts_df.columns and "date" in bursts_df.columns:
            for row in (
                bursts_df.select(["underlying", "date"]).unique().iter_rows(named=True)
            ):
                days_with_bursts_by_underlying[row["underlying"]].add(row["date"])
    
    truth_tables['metadata'] = {
        'feature_files': features_file_count,
        'burst_files': bursts_file_count,
        'regime_files': regimes_file_count,
        'days_by_underlying': {k: len(v) for k, v in days_by_underlying.items()},
        'days_with_bursts_by_underlying': {k: len(v) for k, v in days_with_bursts_by_underlying.items()},
        'total_burst_events': total_burst_events,
        'generated_at': datetime.now().isoformat(),
    }
    
    # Save
    print(f"\nSaving truth tables to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(truth_tables, f, indent=2)
    
    print(f"✓ Truth tables saved")
    
    return truth_tables


def main():
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "market_truth_data"
    output_file = data_dir / "statistics" / "truth_tables.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    truth_tables = generate_truth_tables(data_dir, output_file)
    
    print(f"\n{'='*80}")
    print(f"COMPLETE")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
