#!/usr/bin/env python3
"""
Extract a (timestamp, price) series from SQL dumps that use REPLACE INTO.

Supports both plain `.sql` and gzipped `.sql.gz` inputs.

Typical use cases:
  - Index spot dumps: `REPLACE INTO `nifty` VALUES ('YYYY-MM-DD HH:MM:SS',11583.15);`
  - Futures dumps (many cols): `REPLACE INTO `niftyfut` VALUES ('YYYY-MM-DD HH:MM:SS',12091.00,75,...)`

Output parquet schema (minimal, compatible with v3 spot-enrichment loader):
  - timestamp (datetime[us])
  - price (float64)
"""

from __future__ import annotations

import argparse
import gzip
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import IO, Iterable, Optional

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class ExtractConfig:
    sql_path: Path
    table: str
    output_path: Path
    start_date: Optional[date]
    end_date: Optional[date]
    min_valid_year: int
    flush_rows: int


def _open_text(sql_path: Path) -> IO[str]:
    if sql_path.suffix.lower() == ".gz":
        return gzip.open(sql_path, "rt", errors="ignore")
    return sql_path.open("r", encoding="utf-8", errors="ignore")


def _iter_replace_lines(sql_path: Path, table: str) -> Iterable[str]:
    table = table.strip()
    prefix1 = f"REPLACE INTO `{table}` VALUES"
    prefix2 = f"REPLACE INTO `{table.lower()}` VALUES"
    prefix3 = f"REPLACE INTO `{table.upper()}` VALUES"

    with _open_text(sql_path) as f:
        for line in f:
            if not line.startswith("REPLACE INTO"):
                continue
            if line.startswith(prefix1) or line.startswith(prefix2) or line.startswith(prefix3):
                yield line


def _parse_ts_and_price(line: str) -> Optional[tuple[datetime, float]]:
    """
    Extract the first quoted timestamp and the first numeric value after it.
    """
    values_idx = line.find("VALUES")
    if values_idx == -1:
        return None

    q1 = line.find("'", values_idx)
    if q1 == -1:
        return None
    q2 = line.find("'", q1 + 1)
    if q2 == -1:
        return None

    ts_str = line[q1 + 1 : q2]
    try:
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    comma = line.find(",", q2)
    if comma == -1:
        return None

    j = comma + 1
    while j < len(line) and line[j] == " ":
        j += 1

    # Price ends at next comma or ')' (whichever comes first)
    next_comma = line.find(",", j)
    next_paren = line.find(")", j)
    if next_comma == -1 and next_paren == -1:
        return None
    if next_comma == -1:
        end = next_paren
    elif next_paren == -1:
        end = next_comma
    else:
        end = min(next_comma, next_paren)

    price_str = line[j:end].strip()
    if not price_str or price_str.upper() == "NULL":
        return None

    try:
        price = float(price_str)
    except ValueError:
        return None

    return ts, price


def extract_series(cfg: ExtractConfig) -> dict:
    cfg.output_path.parent.mkdir(parents=True, exist_ok=True)

    writer: Optional[pq.ParquetWriter] = None
    buf_ts: list[datetime] = []
    buf_price: list[float] = []

    stats = {
        "sql_path": str(cfg.sql_path),
        "table": cfg.table,
        "rows_written": 0,
        "lines_parsed": 0,
        "lines_skipped": 0,
        "min_ts": None,
        "max_ts": None,
    }

    def flush() -> None:
        nonlocal writer, buf_ts, buf_price
        if not buf_ts:
            return
        table = pa.table(
            {
                "timestamp": pa.array(buf_ts, type=pa.timestamp("us")),
                "price": pa.array(buf_price, type=pa.float64()),
            }
        )
        if writer is None:
            writer = pq.ParquetWriter(
                where=str(cfg.output_path),
                schema=table.schema,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
            )
        writer.write_table(table, row_group_size=100_000)
        stats["rows_written"] += len(buf_ts)
        buf_ts = []
        buf_price = []

    try:
        started_window = False
        out_of_order = False
        last_ts_seen: Optional[datetime] = None  # only for ts.year >= min_valid_year

        for line in _iter_replace_lines(cfg.sql_path, cfg.table):
            parsed = _parse_ts_and_price(line)
            if parsed is None:
                stats["lines_skipped"] += 1
                continue

            ts, price = parsed
            stats["lines_parsed"] += 1

            if ts.year < cfg.min_valid_year:
                stats["lines_skipped"] += 1
                continue

            if last_ts_seen is not None and ts < last_ts_seen:
                out_of_order = True
            last_ts_seen = ts

            d = ts.date()
            if cfg.start_date is not None and d < cfg.start_date:
                continue
            if cfg.end_date is not None and d > cfg.end_date:
                # If the dump is time-sorted, we can stop early once we pass the window.
                if started_window and not out_of_order:
                    break
                continue

            started_window = True

            if stats["min_ts"] is None or ts < stats["min_ts"]:
                stats["min_ts"] = ts
            if stats["max_ts"] is None or ts > stats["max_ts"]:
                stats["max_ts"] = ts

            buf_ts.append(ts)
            buf_price.append(price)

            if len(buf_ts) >= cfg.flush_rows:
                flush()

        flush()

    finally:
        if writer is not None:
            writer.close()

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract (timestamp,price) from REPLACE INTO SQL dumps")
    parser.add_argument("--sql", type=Path, required=True, help="Path to .sql or .sql.gz")
    parser.add_argument("--table", type=str, required=True, help="Table name in the SQL dump (e.g., nifty)")
    parser.add_argument("--output", type=Path, required=True, help="Output parquet path")
    parser.add_argument("--start-date", type=str, default=None, help="Filter start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="Filter end date (YYYY-MM-DD)")
    parser.add_argument("--min-valid-year", type=int, default=2000, help="Drop rows before this year (default: 2000)")
    parser.add_argument("--flush-rows", type=int, default=1_000_000, help="Rows per write batch (default: 1,000,000)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None
    if start and end and start > end:
        raise ValueError("--start-date must be <= --end-date")

    cfg = ExtractConfig(
        sql_path=args.sql,
        table=args.table,
        output_path=args.output,
        start_date=start,
        end_date=end,
        min_valid_year=args.min_valid_year,
        flush_rows=args.flush_rows,
    )

    stats = extract_series(cfg)
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
