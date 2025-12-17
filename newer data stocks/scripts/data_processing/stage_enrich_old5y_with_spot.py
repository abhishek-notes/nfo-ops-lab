#!/usr/bin/env python3
"""
Stage OLD5Y packed NO_SPOT files from a slow/unreliable external drive to local SSD in batches,
then enrich with spot columns and write the final output.

Why:
- Reading Parquet directly from HDD can be extremely slow (random seeks) and may throw Errno 5 I/O errors.
- Copying the raw bytes to SSD first is sequential and then the Parquet read is fast/reliable from SSD.

Typical usage (10GB batches, output on Mac):
  python -u "newer data stocks/scripts/data_processing/stage_enrich_old5y_with_spot.py" \
    --input-root "/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT" \
    --output-root "newer data stocks/data/options_date_packed_OLD5Y_v3_SPOT_ENRICHED_LOCAL" \
    --spot-dir "newer data stocks/data/spot_data_OLD5Y" \
    --stage-root "newer data stocks/data/_stage_old5y_no_spot" \
    --batch-gb 10
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

try:
    import signal
except Exception:  # pragma: no cover
    signal = None


def _add_script_dir_to_path() -> None:
    script_dir = Path(__file__).resolve().parent
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))


_add_script_dir_to_path()
import enrich_old5y_packed_with_spot as enrich  # noqa: E402


BYTES_PER_GB = 1024**3


@dataclass(frozen=True)
class WorkItem:
    d: date
    underlying: str
    in_path: Path
    stage_path: Path
    out_path: Path
    size_bytes: int


def iter_dates_in_input(root: Path) -> list[date]:
    return enrich.iter_dates_in_input(root)


def file_path(root: Path, d: date, underlying: str) -> Path:
    return root / d.isoformat() / underlying / f"part-{underlying.lower()}-0.parquet"


def date_range(start: date, end: date) -> Iterable[date]:
    return enrich.date_range(start, end)


def atomic_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmpcopy")
    if tmp.exists():
        tmp.unlink()

    # Do not use shutil.copy2 directly: on flaky external volumes it can block indefinitely inside fcopyfile().
    # Instead, do a chunked userspace copy with a stall timeout so we can skip bad files and keep going.
    stall_s = float(os.environ.get("OLD5Y_COPY_STALL_S", "60"))

    old_handler = None
    if signal is not None:
        old_handler = signal.getsignal(signal.SIGALRM)

        def _alarm_handler(_signum, _frame) -> None:  # pragma: no cover
            raise TimeoutError(f"Copy stalled for {stall_s:.0f}s (src={src})")

        signal.signal(signal.SIGALRM, _alarm_handler)

    try:
        if signal is not None:
            signal.setitimer(signal.ITIMER_REAL, stall_s)
        with open(src, "rb") as fsrc:
            if signal is not None:
                signal.setitimer(signal.ITIMER_REAL, stall_s)
            with open(tmp, "wb") as fdst:
                while True:
                    if signal is not None:
                        signal.setitimer(signal.ITIMER_REAL, stall_s)
                    chunk = fsrc.read(16 * 1024 * 1024)
                    if signal is not None:
                        signal.setitimer(signal.ITIMER_REAL, 0)
                    if not chunk:
                        break
                    fdst.write(chunk)
        if signal is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
        try:
            shutil.copystat(src, tmp)
        except Exception:
            pass
    finally:
        if signal is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
            if old_handler is not None:
                signal.signal(signal.SIGALRM, old_handler)

    if tmp.stat().st_size != src.stat().st_size:
        tmp.unlink(missing_ok=True)
        raise OSError(f"Copy size mismatch for {src} -> {dst}")
    tmp.replace(dst)


def copy_with_retries(src: Path, dst: Path, attempts: int = 3) -> tuple[bool, str]:
    last_err: BaseException | None = None
    for attempt in range(attempts):
        try:
            atomic_copy(src, dst)
            return True, ""
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            last_err = e
            wait_s = 1.0 * (2**attempt)
            print(f"  COPY ERROR attempt={attempt+1}/{attempts}: {type(e).__name__}: {e} (retry in {wait_s:.0f}s)", flush=True)
            time.sleep(wait_s)
    assert last_err is not None
    return False, f"{type(last_err).__name__}: {last_err}"


def append_failure(csv_path: Path, *, stage: str, d: date, underlying: str, path: Path, error: str) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not csv_path.exists()
    line = f"{datetime.now().isoformat()},{stage},{d.isoformat()},{underlying},{path},{error}\n"
    with open(csv_path, "a", encoding="utf-8") as f:
        if is_new:
            f.write("ts,stage,date,underlying,path,error\n")
        f.write(line)


def build_work_list(
    input_root: Path,
    stage_root: Path,
    output_root: Path,
    underlyings: list[str],
    dates: list[date],
    overwrite: bool,
) -> list[WorkItem]:
    items: list[WorkItem] = []
    for d in dates:
        for u in underlyings:
            in_path = file_path(input_root, d, u)
            if not in_path.exists():
                continue
            out_path = file_path(output_root, d, u)
            if out_path.exists() and not overwrite:
                continue
            stage_path = file_path(stage_root, d, u)
            items.append(
                WorkItem(
                    d=d,
                    underlying=u,
                    in_path=in_path,
                    stage_path=stage_path,
                    out_path=out_path,
                    size_bytes=in_path.stat().st_size,
                )
            )
    return items


def pick_next_batch(items: list[WorkItem], batch_bytes: int) -> list[WorkItem]:
    # Preserve chronological order and try to keep a date together when possible.
    items_sorted = sorted(items, key=lambda x: (x.d, x.underlying))
    batch: list[WorkItem] = []
    total = 0
    current_day: date | None = None
    day_buffer: list[WorkItem] = []
    day_bytes = 0

    def flush_day() -> bool:
        nonlocal total, day_bytes, current_day, day_buffer
        if not day_buffer:
            return True
        if batch and total + day_bytes > batch_bytes:
            return False
        batch.extend(day_buffer)
        total += day_bytes
        current_day = None
        day_buffer = []
        day_bytes = 0
        return True

    for it in items_sorted:
        if current_day is None:
            current_day = it.d
        if it.d != current_day:
            if not flush_day():
                break
            current_day = it.d
        day_buffer.append(it)
        day_bytes += it.size_bytes
        if total >= batch_bytes:
            break

    flush_day()
    return batch


def maybe_cleanup_empty_dirs(path: Path, stop_at: Path) -> None:
    cur = path
    while True:
        if cur == stop_at:
            return
        try:
            cur.rmdir()
        except OSError:
            return
        cur = cur.parent


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage and enrich OLD5Y packed NO_SPOT in batches.")
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("newer data stocks/data/options_date_packed_OLD5Y_v3_SPOT_ENRICHED_LOCAL"),
    )
    parser.add_argument(
        "--stage-root",
        type=Path,
        default=Path("newer data stocks/data/_stage_old5y_no_spot"),
    )
    parser.add_argument(
        "--spot-dir",
        type=Path,
        default=Path("newer data stocks/data/spot_data_OLD5Y"),
        help="Directory containing BANKNIFTY_all.parquet and NIFTY_all.parquet",
    )
    parser.add_argument("--batch-gb", type=float, default=10.0)
    parser.add_argument("--underlyings", type=str, default="BANKNIFTY,NIFTY")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-batches", type=int, default=0, help="0 = run until complete")
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument(
        "--failures-csv",
        type=Path,
        default=Path("newer data stocks/logs/old5y_spot_enrich_stage_failures.csv"),
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on any copy/enrich error (default: continue and record failures).",
    )

    args = parser.parse_args()

    # Single-process safety (shared lock with the non-staged script).
    _lock_fd = enrich.acquire_process_lock(enrich.default_lock_path())

    underlyings = [u.strip().upper() for u in args.underlyings.split(",") if u.strip()]
    for u in underlyings:
        if u not in ("BANKNIFTY", "NIFTY"):
            raise SystemExit(f"Unknown underlying: {u}")

    spot_paths = {
        "BANKNIFTY": args.spot_dir / "BANKNIFTY_all.parquet",
        "NIFTY": args.spot_dir / "NIFTY_all.parquet",
    }
    for u in underlyings:
        if not spot_paths[u].exists():
            raise FileNotFoundError(f"Spot parquet not found: {spot_paths[u]}")

    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date must be provided together.")
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        dates = list(date_range(start, end))
    else:
        dates = iter_dates_in_input(args.input_root)

    batch_bytes = int(args.batch_gb * BYTES_PER_GB)
    batches_done = 0

    while True:
        remaining = build_work_list(
            input_root=args.input_root,
            stage_root=args.stage_root,
            output_root=args.output_root,
            underlyings=underlyings,
            dates=dates,
            overwrite=args.overwrite,
        )
        if not remaining:
            print("All done (no remaining work).", flush=True)
            return 0

        batch = pick_next_batch(remaining, batch_bytes=batch_bytes)
        if not batch:
            print("No batch selected (unexpected).", flush=True)
            return 2

        batch_sz = sum(it.size_bytes for it in batch)
        day_min = min(it.d for it in batch)
        day_max = max(it.d for it in batch)
        print("=" * 80, flush=True)
        print(
            f"Batch {batches_done+1}: {len(batch)} files, ~{batch_sz/1024/1024/1024:.2f} GB, days {day_min}..{day_max}",
            flush=True,
        )
        print("=" * 80, flush=True)

        # Stage
        print("Staging to SSD...", flush=True)
        for it in batch:
            if it.stage_path.exists() and it.stage_path.stat().st_size == it.size_bytes:
                continue
            print(f"  copy {it.underlying} {it.d} ({it.size_bytes/1024/1024:.1f} MB)", flush=True)
            ok, err = copy_with_retries(it.in_path, it.stage_path)
            if not ok:
                append_failure(
                    args.failures_csv,
                    stage="copy",
                    d=it.d,
                    underlying=it.underlying,
                    path=it.in_path,
                    error=err,
                )
                print(f"  SKIP (copy failed): {it.underlying} {it.d} {err}", flush=True)
                if args.fail_fast:
                    raise SystemExit(f"Copy failed (fail-fast): {it.in_path}: {err}")
                continue

        # Process (from staged files only)
        print("Processing staged batch (HDD idle)...", flush=True)
        for it in sorted(batch, key=lambda x: (x.d, x.underlying)):
            if not it.stage_path.exists():
                continue
            if it.out_path.exists() and not args.overwrite:
                it.stage_path.unlink(missing_ok=True)
                maybe_cleanup_empty_dirs(it.stage_path.parent, args.stage_root)
                continue

            spot = enrich.load_spot_for_day(spot_paths[it.underlying], it.d)
            if spot.is_empty():
                print(f"WARNING: No spot ticks for {it.underlying} on {it.d} (skipping)", flush=True)
                it.stage_path.unlink(missing_ok=True)
                maybe_cleanup_empty_dirs(it.stage_path.parent, args.stage_root)
                continue

            try:
                enriched, stats = enrich.enrich_one_file(it.stage_path, spot)
                enrich.write_parquet(enriched, it.out_path)
                print(
                    f"✓ Wrote {it.out_path} rows={stats['rows']:,} raw_joined={stats['spot_joined_raw']:,} filled={stats['spot_filled']:,} raw_rate={stats['spot_join_rate_raw']:.4f}",
                    flush=True,
                )
            except BaseException as e:
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
                append_failure(
                    args.failures_csv,
                    stage="enrich",
                    d=it.d,
                    underlying=it.underlying,
                    path=it.stage_path,
                    error=f"{type(e).__name__}: {e}",
                )
                print(f"  SKIP (enrich failed): {it.underlying} {it.d} {type(e).__name__}: {e}", flush=True)
                if args.fail_fast:
                    raise SystemExit(f"Enrich failed (fail-fast): {it.stage_path}: {type(e).__name__}: {e}")

            # Delete staged input as soon as output is safely written.
            it.stage_path.unlink(missing_ok=True)
            maybe_cleanup_empty_dirs(it.stage_path.parent, args.stage_root)

        batches_done += 1
        if args.max_batches and batches_done >= args.max_batches:
            print(f"Stopping after {batches_done} batch(es) due to --max-batches.", flush=True)
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
