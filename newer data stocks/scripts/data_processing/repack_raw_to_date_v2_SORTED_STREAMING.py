#!/usr/bin/env python3
"""
Streaming repack: RAW per-contract options parquet -> Date-partitioned (v2 sorted, NO SPOT).

Goal: Create the same 58-column schema as v2 (and v3 minus spot columns), optimized for fast backtests:
  - One output file per (date, underlying):
      <output>/<YYYY-MM-DD>/<UNDERLYING>/part-<underlying>-0.parquet
  - On-disk sort order: expiry → opt_type → strike → timestamp
  - Adds/derives:
      timestamp_ns, expiry, expiry_type, is_monthly, is_weekly, vol_delta

Designed for large datasets (85k+ files) without loading everything into RAM.
It writes incrementally using a ParquetWriter and preserves global sort order by
processing contracts in sorted contract-key order.
"""

from __future__ import annotations

import argparse
import re
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Optional

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


# =========================
# Filename parsing
# =========================

_RE_MONTHNAME = re.compile(r"^([a-z]+)(\d{2})([a-z]{3})(\d+)(ce|pe)$", re.IGNORECASE)
# Numeric format in this dataset encodes month as a single digit (1-9), and Oct/Nov/Dec use o/n/d.
_RE_NUMERIC = re.compile(r"^([a-z]+)(\d{2})(\d)(\d{2})(\d+)(ce|pe)$", re.IGNORECASE)
_RE_OND = re.compile(r"^([a-z]+)(\d{2})([ond])(\d{2})(\d+)(ce|pe)$", re.IGNORECASE)

_MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_OND_MONTH_MAP = {"o": 10, "n": 11, "d": 12}


@dataclass(frozen=True)
class ParsedContract:
    underlying: str  # BANKNIFTY/NIFTY
    expiry_year: int  # YYYY
    expiry_month: int  # 1-12
    expiry_day: Optional[int]  # 1-31 (None when monthname format)
    strike: int
    opt_type: str  # CE/PE

    @property
    def contract_month(self) -> str:
        return f"{self.expiry_year:04d}-{self.expiry_month:02d}"


def parse_contract_filename(path: Path) -> Optional[ParsedContract]:
    """
    Supported filename stems (case-insensitive):
      1) numeric:    banknifty2392148300ce  -> yy=23 m=9 dd=21 strike=48300 CE
      2) monthname:  banknifty25jul54600pe  -> yy=25 mon=jul strike=54600 PE (day unknown)
      3) ond-short:  banknifty22n1038700pe  -> yy=22 mon=nov(dd=10) strike=38700 PE
    """
    stem = path.stem.lower()

    m = _RE_MONTHNAME.match(stem)
    if m:
        underlying, yy, mon3, strike, opt = m.groups()
        month = _MONTH_MAP.get(mon3.lower())
        if month is None:
            return None
        return ParsedContract(
            underlying=underlying.upper(),
            expiry_year=2000 + int(yy),
            expiry_month=month,
            expiry_day=None,
            strike=int(strike),
            opt_type=opt.upper(),
        )

    m = _RE_NUMERIC.match(stem)
    if m:
        underlying, yy, mm, dd, strike, opt = m.groups()
        return ParsedContract(
            underlying=underlying.upper(),
            expiry_year=2000 + int(yy),
            expiry_month=int(mm),
            expiry_day=int(dd),
            strike=int(strike),
            opt_type=opt.upper(),
        )

    m = _RE_OND.match(stem)
    if m:
        underlying, yy, mon_letter, dd, strike, opt = m.groups()
        month = _OND_MONTH_MAP.get(mon_letter.lower())
        if month is None:
            return None
        return ParsedContract(
            underlying=underlying.upper(),
            expiry_year=2000 + int(yy),
            expiry_month=month,
            expiry_day=int(dd),
            strike=int(strike),
            opt_type=opt.upper(),
        )

    return None


# =========================
# Expiry calendar mapping
# =========================


@dataclass(frozen=True)
class ResolvedExpiry:
    expiry: date  # Final expiry date
    expiry_type: str  # "monthly" / "weekly"
    is_monthly: bool
    is_weekly: bool


@dataclass(frozen=True)
class ExpiryCalendar:
    monthly_final_by_month: dict[tuple[str, str], date]
    scheduled_to_final: dict[tuple[str, date], date]
    final_set: set[tuple[str, date]]


def load_expiry_calendar(calendar_path: Path) -> ExpiryCalendar:
    df = pl.read_csv(calendar_path)
    df = df.with_columns(
        [
            pl.col("Scheduled_Expiry").str.strptime(pl.Date, "%Y-%m-%d").alias("scheduled"),
            pl.col("Final_Expiry").str.strptime(pl.Date, "%Y-%m-%d").alias("final"),
        ]
    )

    monthly = df.filter(pl.col("Expiry_Type").str.to_lowercase() == "monthly")
    monthly_final_by_month = {
        (row["Instrument"], row["Contract_Month"]): row["final"]
        for row in monthly.select(["Instrument", "Contract_Month", "final"]).to_dicts()
    }

    scheduled_to_final: dict[tuple[str, date], date] = {}
    final_set: set[tuple[str, date]] = set()
    for row in df.select(["Instrument", "scheduled", "final"]).to_dicts():
        instrument = row["Instrument"]
        sched = row["scheduled"]
        fin = row["final"]
        if sched is not None and fin is not None:
            scheduled_to_final[(instrument, sched)] = fin
            final_set.add((instrument, fin))

    return ExpiryCalendar(
        monthly_final_by_month=monthly_final_by_month,
        scheduled_to_final=scheduled_to_final,
        final_set=final_set,
    )


def resolve_expiry(parsed: ParsedContract, cal: ExpiryCalendar) -> ResolvedExpiry:
    instrument = parsed.underlying
    contract_month = parsed.contract_month

    monthly_final = cal.monthly_final_by_month.get((instrument, contract_month))

    if parsed.expiry_day is None:
        if monthly_final is None:
            raise ValueError(f"Missing monthly expiry for {instrument} {contract_month}")
        return ResolvedExpiry(
            expiry=monthly_final,
            expiry_type="monthly",
            is_monthly=True,
            is_weekly=False,
        )

    guessed = date(parsed.expiry_year, parsed.expiry_month, parsed.expiry_day)

    # If filename contains scheduled date, map it to final. If it is already a final date, keep it.
    if (instrument, guessed) in cal.final_set:
        final_expiry = guessed
    else:
        final_expiry = cal.scheduled_to_final.get((instrument, guessed), guessed)

    is_monthly = monthly_final is not None and final_expiry == monthly_final
    return ResolvedExpiry(
        expiry=final_expiry,
        expiry_type="monthly" if is_monthly else "weekly",
        is_monthly=is_monthly,
        is_weekly=not is_monthly,
    )


# =========================
# Contract index (batch speed)
# =========================


@dataclass(frozen=True)
class CandidateContract:
    expiry: date
    opt_type: str
    strike: float
    path: Path
    parsed: ParsedContract
    resolved: ResolvedExpiry

    @property
    def sort_key(self) -> tuple[date, int, float, str]:
        return (self.expiry, 0 if self.opt_type == "CE" else 1, self.strike, self.path.name)


@dataclass(frozen=True)
class ContractIndex:
    candidates_by_underlying: dict[str, list[CandidateContract]]
    expiries_by_underlying: dict[str, list[date]]


def build_contract_index(
    *,
    input_dir: Path,
    calendar: ExpiryCalendar,
    underlyings: set[str],
) -> ContractIndex:
    """
    Build a reusable contract list once, so multi-date repacks don't re-scan 85k filenames per day.
    """
    by_underlying: dict[str, list[CandidateContract]] = {u: [] for u in underlyings}

    total_files = 0
    parse_failed = 0
    expiry_failed = 0

    for path in input_dir.glob("*.parquet"):
        total_files += 1
        parsed = parse_contract_filename(path)
        if parsed is None:
            parse_failed += 1
            continue
        if parsed.underlying not in underlyings:
            continue

        try:
            resolved = resolve_expiry(parsed, calendar)
        except Exception:
            expiry_failed += 1
            continue

        by_underlying[parsed.underlying].append(
            CandidateContract(
                expiry=resolved.expiry,
                opt_type=parsed.opt_type,
                strike=float(parsed.strike),
                path=path,
                parsed=parsed,
                resolved=resolved,
            )
        )

    for underlying in list(by_underlying.keys()):
        by_underlying[underlying].sort(key=lambda c: c.sort_key)

    expiries_by_underlying = {
        underlying: [c.expiry for c in candidates] for underlying, candidates in by_underlying.items()
    }

    print("Contract index:")
    print(f"  scanned_files={total_files:,}")
    print(f"  parse_failed={parse_failed:,} expiry_failed={expiry_failed:,}")
    for underlying in sorted(by_underlying.keys()):
        print(f"  {underlying}: {len(by_underlying[underlying]):,} contracts")

    return ContractIndex(
        candidates_by_underlying=by_underlying,
        expiries_by_underlying=expiries_by_underlying,
    )


def select_candidates_for_date(
    *,
    index: ContractIndex,
    underlying: str,
    target_date: date,
    max_expiry_days: Optional[int],
) -> list[CandidateContract]:
    candidates = index.candidates_by_underlying.get(underlying, [])
    if not candidates:
        return []

    expiries = index.expiries_by_underlying[underlying]
    start_idx = bisect_left(expiries, target_date)

    if max_expiry_days is None:
        end_idx = len(candidates)
    else:
        expiry_upper = target_date + timedelta(days=max_expiry_days)
        end_idx = bisect_right(expiries, expiry_upper)

    return candidates[start_idx:end_idx]


# =========================
# Parquet utilities
# =========================


def parquet_has_date(path: Path, target: date) -> bool:
    """
    Fast check using parquet row-group statistics on the 'ts' column.
    Returns True if file's [min_date, max_date] covers target date.
    """
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    if "ts" not in schema.names and "timestamp" not in schema.names:
        return False

    ts_col = "ts" if "ts" in schema.names else "timestamp"
    col_idx = schema.names.index(ts_col)

    min_ts: Optional[datetime] = None
    max_ts: Optional[datetime] = None

    for rg_idx in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(rg_idx).column(col_idx).statistics
        if stats is None:
            continue
        if stats.min is not None:
            min_ts = stats.min if min_ts is None else min(min_ts, stats.min)
        if stats.max is not None:
            max_ts = stats.max if max_ts is None else max(max_ts, stats.max)

    if min_ts is None or max_ts is None:
        return False

    return min_ts.date() <= target <= max_ts.date()


# =========================
# Repacking
# =========================


_TARGET_COLS: list[str] = [
    "timestamp",
    "price",
    "qty",
    "avgPrice",
    "volume",
    "bQty",
    "sQty",
    "open",
    "high",
    "low",
    "close",
    "changeper",
    "lastTradeTime",
    "oi",
    "oiHigh",
    "oiLow",
    "bq0",
    "bp0",
    "bo0",
    "bq1",
    "bp1",
    "bo1",
    "bq2",
    "bp2",
    "bo2",
    "bq3",
    "bp3",
    "bo3",
    "bq4",
    "bp4",
    "bo4",
    "sq0",
    "sp0",
    "so0",
    "sq1",
    "sp1",
    "so1",
    "sq2",
    "sp2",
    "so2",
    "sq3",
    "sp3",
    "so3",
    "sq4",
    "sp4",
    "so4",
    "symbol",
    "opt_type",
    "strike",
    "year",
    "month",
    "ts",
    "timestamp_ns",
    "expiry",
    "expiry_type",
    "is_monthly",
    "is_weekly",
    "vol_delta",
]

_PRICE_COLS = ["price", "avgPrice", "open", "high", "low", "close", "changeper"] + [
    f"bp{i}" for i in range(5)
] + [f"sp{i}" for i in range(5)]

_QTY_COLS = ["qty", "volume", "bQty", "sQty", "oi", "oiHigh", "oiLow"] + [
    f"bq{i}" for i in range(5)
] + [f"sq{i}" for i in range(5)] + [f"bo{i}" for i in range(5)] + [f"so{i}" for i in range(5)]

_TARGET_DTYPES: dict[str, pl.DataType] = {
    # Timestamps
    "timestamp": pl.Datetime(time_unit="us"),
    "ts": pl.Datetime(time_unit="us"),
    "lastTradeTime": pl.Datetime(time_unit="us"),
    "timestamp_ns": pl.Int64,
    # Prices
    **{c: pl.Float64 for c in _PRICE_COLS},
    # Quantities / counts
    **{c: pl.Int64 for c in _QTY_COLS},
    # Metadata
    "symbol": pl.Categorical,
    "opt_type": pl.Categorical,
    "strike": pl.Float32,
    "year": pl.Int16,
    "month": pl.Int16,
    "expiry": pl.Date,
    "expiry_type": pl.String,
    "is_monthly": pl.Boolean,
    "is_weekly": pl.Boolean,
    "vol_delta": pl.Int64,
}


def _date_bounds(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, time.min)
    end = start + timedelta(days=1)
    return start, end


def read_contract_rows_for_date(
    path: Path,
    target_date: date,
    parsed: ParsedContract,
    resolved: ResolvedExpiry,
) -> pl.DataFrame:
    start_dt, end_dt = _date_bounds(target_date)

    lf = pl.scan_parquet(str(path))

    # Filter by target date early
    lf = lf.filter((pl.col("timestamp") >= start_dt) & (pl.col("timestamp") < end_dt))

    df = lf.collect(engine="streaming")
    if df.is_empty():
        return df

    # Drop legacy column if present
    if "volactual" in df.columns:
        df = df.drop("volactual")

    # Ensure stable sort within contract
    if "timestamp" in df.columns:
        df = df.sort("timestamp")

    # Normalize metadata columns from filename/calendar (treat filename as truth)
    df = df.with_columns(
        [
            pl.lit(parsed.underlying).cast(pl.Categorical).alias("symbol"),
            pl.lit(parsed.opt_type).cast(pl.Categorical).alias("opt_type"),
            pl.lit(float(parsed.strike)).cast(pl.Float32).alias("strike"),
            pl.lit(resolved.expiry.year).cast(pl.Int16).alias("year"),
            pl.lit(resolved.expiry.month).cast(pl.Int16).alias("month"),
            pl.col("timestamp").alias("ts"),
            pl.col("timestamp").dt.epoch(time_unit="ns").alias("timestamp_ns"),
            pl.lit(resolved.expiry).cast(pl.Date).alias("expiry"),
            pl.lit(resolved.expiry_type).alias("expiry_type"),
            pl.lit(resolved.is_monthly).alias("is_monthly"),
            pl.lit(resolved.is_weekly).alias("is_weekly"),
        ]
    )

    # changeper: round to 2 decimals for consistency with v2/v3 outputs
    if "changeper" in df.columns:
        df = df.with_columns(pl.col("changeper").cast(pl.Float64).round(2).alias("changeper"))

    # Compute vol_delta within this contract (volume reset handling)
    if "volume" in df.columns:
        volume_i64 = pl.col("volume").cast(pl.Int64, strict=False)
        df = df.with_columns(
            [
                (volume_i64 - volume_i64.shift(1)).alias("__vdelta_raw")
            ]
        ).with_columns(
            [
                pl.when(pl.col("__vdelta_raw") < 0)
                .then(0)
                .otherwise(pl.col("__vdelta_raw"))
                .fill_null(0)
                .cast(pl.Int64)
                .alias("vol_delta")
            ]
        ).drop("__vdelta_raw")
    else:
        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias("vol_delta"))

    # Cast categoricals (if source was string) and strike type
    cast_exprs: list[pl.Expr] = []
    for col, dtype in _TARGET_DTYPES.items():
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
    if cast_exprs:
        df = df.with_columns(cast_exprs)

    # Ensure required columns exist (fill missing with nulls)
    missing = [c for c in _TARGET_COLS if c not in df.columns]
    if missing:
        df = df.with_columns(
            [pl.lit(None).cast(_TARGET_DTYPES.get(c, pl.Null)).alias(c) for c in missing]
        )

    return df.select(_TARGET_COLS)


def iter_contracts(input_dir: Path, underlyings: set[str]) -> Iterable[tuple[Path, ParsedContract]]:
    for p in input_dir.glob("*.parquet"):
        parsed = parse_contract_filename(p)
        if parsed is None:
            continue
        if parsed.underlying not in underlyings:
            continue
        yield p, parsed


def repack_one_day(
    *,
    input_dir: Path,
    output_dir: Path,
    calendar: ExpiryCalendar,
    target_date: date,
    underlyings: set[str],
    max_expiry_days: Optional[int],
    limit_files: Optional[int],
    overwrite: bool,
    skip_existing: bool,
    index: Optional[ContractIndex] = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    if index is None:
        index = build_contract_index(input_dir=input_dir, calendar=calendar, underlyings=underlyings)

    for underlying in sorted(underlyings):
        print("=" * 80)
        print(f"Repacking {underlying} for {target_date.isoformat()}")
        print("=" * 80)

        out_path = output_dir / target_date.isoformat() / underlying / f"part-{underlying.lower()}-0.parquet"
        tmp_path = out_path.with_name(out_path.name + ".tmp")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if tmp_path.exists():
            tmp_path.unlink()
        if out_path.exists():
            if overwrite:
                out_path.unlink()
            else:
                if skip_existing:
                    print(f"↷ Skip existing: {out_path}")
                    continue
                raise FileExistsError(f"Refusing to overwrite existing file: {out_path}")

        candidates = select_candidates_for_date(
            index=index,
            underlying=underlying,
            target_date=target_date,
            max_expiry_days=max_expiry_days,
        )
        print(f"Candidate contracts after expiry window: {len(candidates):,}")

        writer: Optional[pq.ParquetWriter] = None
        writer_schema: Optional[pa.Schema] = None
        total_rows = 0
        files_used = 0
        files_examined = 0
        success = False

        try:
            for c in candidates:
                files_examined += 1
                if limit_files is not None and files_examined > limit_files:
                    break

                # Fast range filter by parquet metadata (avoid reading full file)
                try:
                    if not parquet_has_date(c.path, target_date):
                        continue
                except Exception:
                    continue

                try:
                    df = read_contract_rows_for_date(c.path, target_date, c.parsed, c.resolved)
                except Exception as e:
                    print(f"  ! Failed to read {c.path.name}: {type(e).__name__}: {e}")
                    continue
                if df.is_empty():
                    continue

                table = df.to_arrow()
                if writer is None:
                    writer_schema = table.schema
                    writer = pq.ParquetWriter(
                        where=str(tmp_path),
                        schema=writer_schema,
                        compression="zstd",
                        use_dictionary=True,
                        write_statistics=True,
                    )
                else:
                    # Align schema if needed (e.g., nullability mismatches)
                    if writer_schema is not None and table.schema != writer_schema:
                        table = table.cast(writer_schema)

                writer.write_table(table, row_group_size=100_000)
                total_rows += df.height
                files_used += 1

                if files_used % 500 == 0:
                    print(f"  ...written {files_used:,} contracts, {total_rows:,} rows")

            success = True
        finally:
            if writer is not None:
                writer.close()
            if not success and tmp_path.exists():
                tmp_path.unlink()

        if files_used == 0:
            print(f"WARNING: No rows written for {underlying} on {target_date}")
            if out_path.exists():
                out_path.unlink()
            if tmp_path.exists():
                tmp_path.unlink()
            continue

        tmp_path.replace(out_path)
        print(f"✓ Wrote {total_rows:,} rows from {files_used:,} contracts → {out_path}")


def load_trading_days_from_parquet(
    *,
    parquet_path: Path,
    start_date: date,
    end_date: date,
    timestamp_col: str = "timestamp",
) -> list[date]:
    if not parquet_path.exists():
        raise FileNotFoundError(f"Trading-days parquet not found: {parquet_path}")

    lf = pl.scan_parquet(str(parquet_path)).select(pl.col(timestamp_col).dt.date().alias("date"))
    dates = lf.unique().sort("date").collect(engine="streaming")["date"].to_list()
    return [d for d in dates if start_date <= d <= end_date]


def iter_weekdays_in_range(start: date, end: date) -> list[date]:
    dates: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    return dates


def repack_date_range(
    *,
    input_dir: Path,
    output_dir: Path,
    calendar: ExpiryCalendar,
    start_date: date,
    end_date: date,
    underlyings: set[str],
    max_expiry_days: Optional[int],
    trading_days_parquet: Optional[Path],
    limit_files: Optional[int],
    overwrite: bool,
    skip_existing: bool,
) -> None:
    if trading_days_parquet is not None:
        dates = load_trading_days_from_parquet(
            parquet_path=trading_days_parquet,
            start_date=start_date,
            end_date=end_date,
        )
        print(f"Loaded {len(dates):,} trading days from {trading_days_parquet}")
    else:
        dates = iter_weekdays_in_range(start_date, end_date)
        print(f"Generated {len(dates):,} weekdays in range (no holiday filter)")

    if not dates:
        print("No dates to process.")
        return

    index = build_contract_index(input_dir=input_dir, calendar=calendar, underlyings=underlyings)

    for d in dates:
        repack_one_day(
            input_dir=input_dir,
            output_dir=output_dir,
            calendar=calendar,
            target_date=d,
            underlyings=underlyings,
            max_expiry_days=max_expiry_days,
            limit_files=limit_files,
            overwrite=overwrite,
            skip_existing=skip_existing,
            index=index,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Streaming repack to v2 sorted (58 cols, no spot enrichment)",
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw/options"))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("newer data stocks/data/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT_TEST"),
    )
    parser.add_argument("--expiry-calendar", type=Path, default=Path("newer data stocks/config/expiry_calendar.csv"))
    parser.add_argument("--date", type=str, default=None, help="Single day: YYYY-MM-DD")
    parser.add_argument("--start-date", type=str, default=None, help="Batch: start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="Batch: end date (YYYY-MM-DD)")
    parser.add_argument(
        "--trading-days-parquet",
        type=Path,
        default=None,
        help="Batch: parquet with a 'timestamp' column to derive trading days (skips weekends/holidays).",
    )
    parser.add_argument(
        "--underlyings",
        type=str,
        default="BANKNIFTY,NIFTY",
        help="Comma-separated list: BANKNIFTY,NIFTY",
    )
    parser.add_argument(
        "--max-expiry-days",
        type=int,
        default=70,
        help="Only include contracts with expiry in [date, date+N]. Use 0 to disable window.",
    )
    parser.add_argument("--limit-files", type=int, default=None, help="Debug: stop after examining N candidate files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")
    parser.add_argument("--skip-existing", action="store_true", help="Skip output files that already exist (resume)")

    args = parser.parse_args()

    input_dir: Path = args.input_dir
    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    cal_path: Path = args.expiry_calendar
    if not cal_path.exists():
        raise FileNotFoundError(f"Expiry calendar not found: {cal_path}")

    underlyings = {u.strip().upper() for u in args.underlyings.split(",") if u.strip()}

    max_expiry_days: Optional[int]
    if args.max_expiry_days == 0:
        max_expiry_days = None
    else:
        max_expiry_days = int(args.max_expiry_days)

    print("Loading expiry calendar...")
    cal = load_expiry_calendar(cal_path)
    print(f"  Loaded monthly expiries: {len(cal.monthly_final_by_month):,}")
    print(f"  Loaded scheduled mappings: {len(cal.scheduled_to_final):,}")

    single_mode = args.date is not None
    batch_mode = args.start_date is not None or args.end_date is not None

    if single_mode and batch_mode:
        raise SystemExit("Provide either --date OR (--start-date/--end-date), not both.")

    if not single_mode and not batch_mode:
        raise SystemExit("Provide --date for single-day mode, or --start-date/--end-date for batch mode.")

    if single_mode:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
        repack_one_day(
            input_dir=input_dir,
            output_dir=args.output_dir,
            calendar=cal,
            target_date=target,
            underlyings=underlyings,
            max_expiry_days=max_expiry_days,
            limit_files=args.limit_files,
            overwrite=args.overwrite,
            skip_existing=args.skip_existing,
        )
        return 0

    if args.start_date is None or args.end_date is None:
        raise SystemExit("Batch mode requires both --start-date and --end-date.")

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    if end_date < start_date:
        raise SystemExit("--end-date must be >= --start-date.")

    repack_date_range(
        input_dir=input_dir,
        output_dir=args.output_dir,
        calendar=cal,
        start_date=start_date,
        end_date=end_date,
        underlyings=underlyings,
        max_expiry_days=max_expiry_days,
        trading_days_parquet=args.trading_days_parquet,
        limit_files=args.limit_files,
        overwrite=args.overwrite,
        skip_existing=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
