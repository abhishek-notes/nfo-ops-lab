#!/usr/bin/env python3
"""
Add spot-dependent columns to the OLD5Y 58-col packed options dataset (v2 sorted, NO_SPOT).

Adds 6 columns (same set as v3 spot-enriched pack):
  - spot_price
  - mid_price
  - distance_from_spot
  - moneyness_pct
  - intrinsic_value
  - time_value

Important:
- Keeps the original on-disk sort order (expiry → opt_type → strike → timestamp).
- Uses a backward as-of join (no lookahead).
"""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import polars as pl
import pyarrow.parquet as pq

try:
    import fcntl  # type: ignore[attr-defined]
except Exception:
    fcntl = None


def acquire_process_lock(lock_path: Path) -> int | None:
    if fcntl is None:
        return None
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                pid = f.read().strip() or "unknown"
        except Exception:
            pid = "unknown"
        raise SystemExit(f"Another enrich process is already running (lock: {lock_path}, pid: {pid}).")
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode("utf-8"))
    os.fsync(fd)
    return fd


def default_lock_path() -> Path:
    # newer data stocks/scripts/data_processing -> newer data stocks/logs
    return Path(__file__).resolve().parents[2] / "logs" / "old5y_spot_enrich.lock"


def iter_dates_in_input(root: Path) -> list[date]:
    dates: list[date] = []
    for p in root.iterdir():
        if not p.is_dir():
            continue
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name):
            continue
        dates.append(datetime.strptime(p.name, "%Y-%m-%d").date())
    return sorted(dates)


def load_spot_for_day(spot_parquet: Path, d: date) -> pl.DataFrame:
    lf = (
        pl.scan_parquet(str(spot_parquet))
        .filter(pl.col("timestamp").dt.date() == d)
        .select([pl.col("timestamp"), pl.col("price").cast(pl.Float32).alias("spot_price")])
    )
    spot = lf.collect(engine="streaming")
    if spot.is_empty():
        return spot
    spot = spot.sort("timestamp")
    # Defensive: if any spot ticks have null price, forward-fill so as-of joins don't inherit nulls.
    spot = spot.with_columns(pl.col("spot_price").fill_null(strategy="forward").alias("spot_price"))
    return spot


def read_parquet_safe(path: Path) -> pl.DataFrame:
    """
    Polars can occasionally throw a PanicException on some filesystems/files; fall back to PyArrow.
    """
    try:
        return pl.read_parquet(path, use_pyarrow=True, memory_map=False)
    except BaseException as e:
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        try:
            table = pq.read_table(path, memory_map=False)
            return pl.from_arrow(table)
        except BaseException as e2:
            if isinstance(e2, (KeyboardInterrupt, SystemExit)):
                raise
            # Retry with single-threaded reads (some external volumes can be flaky with concurrent reads).
            table = pq.read_table(path, memory_map=False, use_threads=False)
            return pl.from_arrow(table)


def enrich_one_file(options_path: Path, spot: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, object]]:
    options = read_parquet_safe(options_path)
    if options.is_empty():
        return options, {"rows": 0, "spot_joined": 0, "spot_join_rate": 0.0}

    options = options.with_row_index("__row_nr")

    # Build a mapping timestamp -> spot_price using an as-of join on a sorted (timestamp, row_nr) view,
    # then re-attach by row_nr to preserve the original order.
    ts_map = (
        options.select(["__row_nr", "timestamp"])
        .sort("timestamp")
        .join_asof(spot, on="timestamp", strategy="backward")
        .select(["__row_nr", "spot_price"])
    )
    raw_joined_rows = int(ts_map.filter(pl.col("spot_price").is_not_null()).height)
    # Fill any remaining nulls:
    # - forward fill (safe, no lookahead) handles mid-day holes if they occur
    # - backward fill handles only the leading nulls (pre-first spot tick)
    ts_map = ts_map.with_columns(
        pl.col("spot_price")
        .fill_null(strategy="forward")
        .fill_null(strategy="backward")
        .alias("spot_price")
    )

    options = (
        options.join(ts_map, on="__row_nr", how="left")
        .sort("__row_nr")
        .drop("__row_nr")
    )

    options = options.with_columns(
        [
            ((pl.col("bp0") + pl.col("sp0")) / 2.0).cast(pl.Float32).alias("mid_price"),
            (pl.col("strike") - pl.col("spot_price")).cast(pl.Float32).alias("distance_from_spot"),
            ((pl.col("strike") - pl.col("spot_price")) / pl.col("spot_price") * 100.0)
            .cast(pl.Float32)
            .alias("moneyness_pct"),
            pl.when(pl.col("opt_type") == "CE")
            .then((pl.col("spot_price") - pl.col("strike")).clip(lower_bound=0.0))
            .otherwise((pl.col("strike") - pl.col("spot_price")).clip(lower_bound=0.0))
            .cast(pl.Float32)
            .alias("intrinsic_value"),
        ]
    ).with_columns(
        [
            (pl.col("price") - pl.col("intrinsic_value"))
            .cast(pl.Float32)
            .alias("time_value"),
        ]
    )

    # Reduce float noise for these derived columns (readability + slightly smaller parquet deltas).
    options = options.with_columns(
        [
            pl.col("spot_price").round(2).cast(pl.Float32).alias("spot_price"),
            pl.col("mid_price").round(2).cast(pl.Float32).alias("mid_price"),
            pl.col("distance_from_spot").round(2).cast(pl.Float32).alias("distance_from_spot"),
            pl.col("moneyness_pct").round(3).cast(pl.Float32).alias("moneyness_pct"),
            pl.col("intrinsic_value").round(2).cast(pl.Float32).alias("intrinsic_value"),
            pl.col("time_value").round(2).cast(pl.Float32).alias("time_value"),
        ]
    )

    rows = int(options.height)
    stats = {
        "rows": rows,
        "spot_joined_raw": raw_joined_rows,
        "spot_filled": (rows - raw_joined_rows),
        "spot_join_rate_raw": (raw_joined_rows / rows) if rows else 0.0,
    }
    return options, stats


def write_parquet(df: pl.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    df.write_parquet(tmp_path, compression="zstd", statistics=True)
    tmp_path.replace(out_path)


def date_range(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d = d.fromordinal(d.toordinal() + 1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich OLD5Y packed options (NO_SPOT) with spot columns.")
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("newer data stocks/data/options_date_packed_OLD5Y_v3_SPOT_ENRICHED_TEST"),
    )
    parser.add_argument(
        "--spot-dir",
        type=Path,
        default=Path("newer data stocks/data/spot_data_OLD5Y"),
        help="Directory containing BANKNIFTY_all.parquet and NIFTY_all.parquet",
    )
    parser.add_argument("--date", type=str, default=None, help="Single day: YYYY-MM-DD")
    parser.add_argument("--start-date", type=str, default=None, help="Batch: YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="Batch: YYYY-MM-DD")
    parser.add_argument("--underlyings", type=str, default="BANKNIFTY,NIFTY")
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    _lock_fd = acquire_process_lock(default_lock_path())

    input_root: Path = args.input_root
    if not input_root.exists():
        raise FileNotFoundError(f"Input root not found: {input_root}")

    underlyings = [u.strip().upper() for u in args.underlyings.split(",") if u.strip()]

    spot_paths = {
        "BANKNIFTY": args.spot_dir / "BANKNIFTY_all.parquet",
        "NIFTY": args.spot_dir / "NIFTY_all.parquet",
    }
    for u in underlyings:
        if u not in spot_paths:
            raise SystemExit(f"Unknown underlying: {u}")
        if not spot_paths[u].exists():
            raise FileNotFoundError(f"Spot parquet not found: {spot_paths[u]}")

    single_mode = args.date is not None
    batch_mode = args.start_date is not None or args.end_date is not None
    if single_mode and batch_mode:
        raise SystemExit("Provide either --date OR (--start-date/--end-date), not both.")

    if single_mode:
        dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    elif batch_mode:
        if args.start_date is None or args.end_date is None:
            raise SystemExit("Batch mode requires both --start-date and --end-date.")
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        dates = list(date_range(start, end))
    else:
        dates = iter_dates_in_input(input_root)

    if not dates:
        print("No dates to process.")
        return 0

    for d in dates:
        for u in underlyings:
            in_path = input_root / d.isoformat() / u / f"part-{u.lower()}-0.parquet"
            if not in_path.exists():
                continue

            out_path = args.output_root / d.isoformat() / u / f"part-{u.lower()}-0.parquet"
            if out_path.exists() and not args.overwrite:
                continue

            print("=" * 80, flush=True)
            print(f"Enrich {u} {d.isoformat()}", flush=True)
            print("=" * 80, flush=True)

            spot = load_spot_for_day(spot_paths[u], d)
            if spot.is_empty():
                print(f"WARNING: No spot ticks for {u} on {d.isoformat()} (skipping)", flush=True)
                continue

            last_err: Exception | None = None
            for attempt in range(3):
                try:
                    enriched, stats = enrich_one_file(in_path, spot)
                    write_parquet(enriched, out_path)
                    print(
                        f"✓ Wrote {out_path} rows={stats['rows']:,} raw_joined={stats['spot_joined_raw']:,} filled={stats['spot_filled']:,} raw_rate={stats['spot_join_rate_raw']:.4f}",
                        flush=True,
                    )
                    last_err = None
                    break
                except BaseException as e:
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        raise
                    last_err = e
                    wait_s = 1.0 * (2**attempt)
                    print(
                        f"ERROR: {u} {d.isoformat()} attempt={attempt+1}/3 failed: {type(e).__name__}: {e} (retry in {wait_s:.0f}s)",
                        flush=True,
                    )
                    time.sleep(wait_s)

            if last_err is not None:
                print(f"FAILED: {u} {d.isoformat()} giving up after 3 attempts", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
