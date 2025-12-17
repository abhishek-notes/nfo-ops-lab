#!/usr/bin/env python3
"""
Audit an OLD5Y date-packed repack run:
  - Parses the repack log to extract per-day status (written vs warning) and candidate counts.
  - Compares to the trading-day list (derived from a parquet with a 'timestamp' column, e.g. spot).
  - Compares to the output directory contents.

Outputs:
  - Markdown summary report
  - CSV of missing dates + reason
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class DayStatus:
    date: date
    underlying: str
    candidate_contracts: int | None
    status: str  # written|warning|unknown
    out_path: str | None


_RE_REPACK = re.compile(r"^Repacking\s+(BANKNIFTY|NIFTY)\s+for\s+(\d{4}-\d{2}-\d{2})\s*$")
_RE_CAND = re.compile(r"^Candidate contracts after expiry window:\s+([\d,]+)\s*$")
_RE_WRITTEN = re.compile(r"^✓ Wrote .*→\s+(.+/(BANKNIFTY|NIFTY)/part-[^/]+\.parquet)\s*$")
_RE_WARN = re.compile(r"^WARNING: No rows written for\s+(BANKNIFTY|NIFTY)\s+on\s+(\d{4}-\d{2}-\d{2})\s*$")


def load_trading_days(parquet_path: Path) -> list[date]:
    lf = pl.scan_parquet(str(parquet_path)).select(pl.col("timestamp").dt.date().alias("date"))
    return lf.unique().sort("date").collect(engine="streaming")["date"].to_list()


def parse_log(log_path: Path) -> dict[tuple[str, date], DayStatus]:
    statuses: dict[tuple[str, date], DayStatus] = {}

    cur_underlying: str | None = None
    cur_date: date | None = None
    cur_candidate: int | None = None

    for raw_line in log_path.read_text(errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        m = _RE_REPACK.match(line)
        if m:
            cur_underlying = m.group(1)
            cur_date = datetime.strptime(m.group(2), "%Y-%m-%d").date()
            cur_candidate = None
            key = (cur_underlying, cur_date)
            statuses[key] = DayStatus(
                date=cur_date,
                underlying=cur_underlying,
                candidate_contracts=None,
                status="unknown",
                out_path=None,
            )
            continue

        m = _RE_CAND.match(line)
        if m and cur_underlying and cur_date:
            cur_candidate = int(m.group(1).replace(",", ""))
            key = (cur_underlying, cur_date)
            prev = statuses.get(key)
            statuses[key] = DayStatus(
                date=cur_date,
                underlying=cur_underlying,
                candidate_contracts=cur_candidate,
                status=prev.status if prev else "unknown",
                out_path=prev.out_path if prev else None,
            )
            continue

        m = _RE_WRITTEN.match(line)
        if m:
            out_path = m.group(1)
            underlying = m.group(2)
            # date is embedded in path: .../<YYYY-MM-DD>/<UNDERLYING>/part-...
            parts = Path(out_path).parts
            # find the date segment by scanning backwards
            d = None
            for part in reversed(parts):
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part):
                    d = datetime.strptime(part, "%Y-%m-%d").date()
                    break
            if d is None:
                continue
            key = (underlying, d)
            prev = statuses.get(key)
            statuses[key] = DayStatus(
                date=d,
                underlying=underlying,
                candidate_contracts=prev.candidate_contracts if prev else None,
                status="written",
                out_path=out_path,
            )
            continue

        m = _RE_WARN.match(line)
        if m:
            underlying = m.group(1)
            d = datetime.strptime(m.group(2), "%Y-%m-%d").date()
            key = (underlying, d)
            prev = statuses.get(key)
            statuses[key] = DayStatus(
                date=d,
                underlying=underlying,
                candidate_contracts=prev.candidate_contracts if prev else None,
                status="warning",
                out_path=None,
            )

    return statuses


def find_written_dates(output_dir: Path) -> tuple[set[date], set[date]]:
    bn: set[date] = set()
    nf: set[date] = set()

    for p in output_dir.glob("*/BANKNIFTY/part-banknifty-0.parquet"):
        bn.add(datetime.strptime(p.parts[-3], "%Y-%m-%d").date())
    for p in output_dir.glob("*/NIFTY/part-nifty-0.parquet"):
        nf.add(datetime.strptime(p.parts[-3], "%Y-%m-%d").date())

    return bn, nf


def write_report(
    *,
    report_path: Path,
    missing_csv_path: Path,
    trading_days: list[date],
    output_dir: Path,
    statuses: dict[tuple[str, date], DayStatus],
) -> None:
    output_dir = output_dir.resolve()

    trading_set = set(trading_days)
    bn_written, nf_written = find_written_dates(output_dir)

    def _summary(underlying: str, written_dates: set[date]) -> dict[str, object]:
        missing = sorted(trading_set - written_dates)
        min_written = min(written_dates) if written_dates else None
        max_written = max(written_dates) if written_dates else None
        return {
            "underlying": underlying,
            "trading_days": len(trading_days),
            "written_days": len(written_dates),
            "missing_days": len(missing),
            "min_written": min_written,
            "max_written": max_written,
        }

    bn_sum = _summary("BANKNIFTY", bn_written)
    nf_sum = _summary("NIFTY", nf_written)

    rows: list[dict[str, object]] = []
    for underlying, written_dates, summary in [
        ("BANKNIFTY", bn_written, bn_sum),
        ("NIFTY", nf_written, nf_sum),
    ]:
        min_w = summary["min_written"]
        max_w = summary["max_written"]
        for d in sorted(trading_set - written_dates):
            key = (underlying, d)
            st = statuses.get(key)
            cand = st.candidate_contracts if st else None
            reason = "unknown"
            if min_w and d < min_w:
                reason = "before_first_written"
            elif max_w and d > max_w:
                reason = "after_last_written"
            else:
                reason = "internal_gap"
            if cand == 0:
                reason = "no_candidates"
            rows.append(
                {
                    "underlying": underlying,
                    "date": d.isoformat(),
                    "reason": reason,
                    "candidate_contracts": cand,
                    "log_status": st.status if st else None,
                }
            )

    missing_df = pl.DataFrame(rows).sort(["underlying", "date"])
    missing_csv_path.parent.mkdir(parents=True, exist_ok=True)
    missing_df.write_csv(missing_csv_path)

    # Markdown report
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# OLD5Y Repack — Run Audit\n")
    lines.append(f"- Output dir: `{output_dir}`")
    lines.append(f"- Trading days (from spot parquet): `{len(trading_days)}`")
    lines.append("")

    for s in [bn_sum, nf_sum]:
        lines.append(f"## {s['underlying']}\n")
        lines.append(f"- Written days: `{s['written_days']}` / `{s['trading_days']}`")
        lines.append(f"- Missing days: `{s['missing_days']}`")
        lines.append(f"- First written: `{s['min_written']}`")
        lines.append(f"- Last written: `{s['max_written']}`")
        lines.append("")

    # Missing reason counts
    lines.append("## Missing breakdown\n")
    reason_counts = (
        missing_df.group_by(["underlying", "reason"])
        .agg(pl.len().alias("n"))
        .sort(["underlying", "n"], descending=[False, True])
        .to_dicts()
    )
    for r in reason_counts:
        lines.append(f"- {r['underlying']}: {r['reason']} = `{r['n']}`")
    lines.append("")

    lines.append("## Files\n")
    lines.append(f"- Missing dates CSV: `{missing_csv_path}`")
    lines.append("")

    report_path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit an OLD5Y repack run (log + outputs).")
    parser.add_argument("--log", type=Path, default=Path("newer data stocks/logs/old5y_repack.log"))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/Volumes/Abhishek 5T/nfo_packed/options_date_packed_OLD5Y_v2_SORTED_NO_SPOT"),
    )
    parser.add_argument(
        "--trading-days-parquet",
        type=Path,
        default=Path("newer data stocks/data/spot_data_OLD5Y/NIFTY_all.parquet"),
    )
    parser.add_argument(
        "--report-md",
        type=Path,
        default=Path("newer data stocks/docs/reports/OLD5Y_REPACK_RUN_AUDIT.md"),
    )
    parser.add_argument(
        "--missing-csv",
        type=Path,
        default=Path("newer data stocks/docs/reports/old5y_repack_missing_dates.csv"),
    )

    args = parser.parse_args()

    if not args.log.exists():
        raise FileNotFoundError(f"Log not found: {args.log}")
    if not args.output_dir.exists():
        raise FileNotFoundError(f"Output dir not found: {args.output_dir}")
    if not args.trading_days_parquet.exists():
        raise FileNotFoundError(f"Trading-days parquet not found: {args.trading_days_parquet}")

    trading_days = load_trading_days(args.trading_days_parquet)
    statuses = parse_log(args.log)

    write_report(
        report_path=args.report_md,
        missing_csv_path=args.missing_csv,
        trading_days=trading_days,
        output_dir=args.output_dir,
        statuses=statuses,
    )

    print(f"✓ Wrote report: {args.report_md}")
    print(f"✓ Wrote missing CSV: {args.missing_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

