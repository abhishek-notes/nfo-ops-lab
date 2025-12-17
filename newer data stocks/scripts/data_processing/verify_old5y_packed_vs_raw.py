#!/usr/bin/env python3
"""
Verify that a raw per-contract parquet (for one date) is represented 1:1 in the date-packed output.

This reuses the same transformation logic as the repacker by importing
`repack_raw_to_date_v2_SORTED_STREAMING.py` and calling `read_contract_rows_for_date(...)`.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class CheckResult:
    raw_file: Path
    date: str
    underlying: str
    expiry: str
    opt_type: str
    strike: float
    expected_rows: int
    actual_rows: int
    ok: bool
    message: str


def load_repacker_module(script_path: Path):
    import importlib.util

    spec = importlib.util.spec_from_file_location("old5y_repacker", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec from: {script_path}")
    mod = importlib.util.module_from_spec(spec)
    # dataclasses expects module to exist in sys.modules
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def row_hash(df: pl.DataFrame) -> pl.Series:
    return df.select(pl.struct(df.columns).hash().alias("h"))["h"]


def verify_one(
    *,
    repacker,
    raw_file: Path,
    packed_root: Path,
    calendar_path: Path,
    date_str: str,
) -> CheckResult:
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    parsed = repacker.parse_contract_filename(raw_file)
    if parsed is None:
        return CheckResult(
            raw_file=raw_file,
            date=date_str,
            underlying="",
            expiry="",
            opt_type="",
            strike=float("nan"),
            expected_rows=0,
            actual_rows=0,
            ok=False,
            message="Filename parse failed",
        )

    cal = repacker.load_expiry_calendar(calendar_path)
    resolved = repacker.resolve_expiry(parsed, cal)

    expected = repacker.read_contract_rows_for_date(raw_file, target_date, parsed, resolved)
    if expected.is_empty():
        return CheckResult(
            raw_file=raw_file,
            date=date_str,
            underlying=parsed.underlying,
            expiry=resolved.expiry.isoformat(),
            opt_type=parsed.opt_type,
            strike=float(parsed.strike),
            expected_rows=0,
            actual_rows=0,
            ok=False,
            message="No rows in raw file for that date (expected empty)",
        )

    packed_path = (
        packed_root
        / target_date.isoformat()
        / parsed.underlying
        / f"part-{parsed.underlying.lower()}-0.parquet"
    )
    if not packed_path.exists():
        return CheckResult(
            raw_file=raw_file,
            date=date_str,
            underlying=parsed.underlying,
            expiry=resolved.expiry.isoformat(),
            opt_type=parsed.opt_type,
            strike=float(parsed.strike),
            expected_rows=expected.height,
            actual_rows=0,
            ok=False,
            message=f"Packed file not found: {packed_path}",
        )

    lf = pl.scan_parquet(str(packed_path))
    actual = (
        lf.filter(
            (pl.col("expiry") == resolved.expiry)
            & (pl.col("opt_type") == parsed.opt_type)
            & (pl.col("strike") == float(parsed.strike))
        )
        .collect(engine="streaming")
        .select(expected.columns)
        .sort("timestamp")
    )

    expected_sorted = expected.sort("timestamp")

    if expected_sorted.height != actual.height:
        return CheckResult(
            raw_file=raw_file,
            date=date_str,
            underlying=parsed.underlying,
            expiry=resolved.expiry.isoformat(),
            opt_type=parsed.opt_type,
            strike=float(parsed.strike),
            expected_rows=expected_sorted.height,
            actual_rows=actual.height,
            ok=False,
            message="Row count mismatch",
        )

    # Compare row-hashes for a full-row equality check without materializing all columns for diff.
    h_exp = row_hash(expected_sorted)
    h_act = row_hash(actual)
    ok = bool((h_exp == h_act).all())

    return CheckResult(
        raw_file=raw_file,
        date=date_str,
        underlying=parsed.underlying,
        expiry=resolved.expiry.isoformat(),
        opt_type=parsed.opt_type,
        strike=float(parsed.strike),
        expected_rows=expected_sorted.height,
        actual_rows=actual.height,
        ok=bool(ok),
        message="OK" if ok else "Row hash mismatch (content differs)",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify raw per-contract data is present in packed output.")
    parser.add_argument("--repacker", type=Path, default=Path("newer data stocks/scripts/data_processing/repack_raw_to_date_v2_SORTED_STREAMING.py"))
    parser.add_argument("--expiry-calendar", type=Path, default=Path("newer data stocks/config/expiry_calendar.csv"))
    parser.add_argument(
        "--packed-root",
        type=Path,
        default=Path("/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT"),
    )
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw-file", type=Path, action="append", required=True, help="Path to raw per-contract parquet (repeatable)")

    args = parser.parse_args()

    repacker = load_repacker_module(args.repacker)

    results: list[CheckResult] = []
    for rf in args.raw_file:
        results.append(
            verify_one(
                repacker=repacker,
                raw_file=rf,
                packed_root=args.packed_root,
                calendar_path=args.expiry_calendar,
                date_str=args.date,
            )
        )

    ok_all = all(r.ok for r in results)
    for r in results:
        status = "PASS" if r.ok else "FAIL"
        print(f"{status} {r.underlying} {r.date} {r.raw_file.name} expiry={r.expiry} {r.opt_type} {r.strike:.0f} rows={r.actual_rows:,} :: {r.message}")

    return 0 if ok_all else 2


if __name__ == "__main__":
    raise SystemExit(main())
