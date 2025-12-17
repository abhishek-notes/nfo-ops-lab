#!/usr/bin/env python3
"""
Audit old (5Y) raw options filenames and resolve expiries via expiry_calendar.csv.

Inputs:
  - Directory of per-contract parquets (default: data/raw/options)
  - Expiry calendar CSV (default: newer data stocks/config/expiry_calendar.csv)

Outputs:
  - CSV mapping (one row per file) with parsed tokens + resolved final expiry
  - Markdown summary (naming conventions, counts, examples, holiday-shift mappings)

This is meant for verification/spot-checking before running large repacks.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import polars as pl


_RE_MONTHNAME = re.compile(r"^([a-z]+)(\d{2})([a-z]{3})(\d+)(ce|pe)$", re.IGNORECASE)
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
    expiry_day: Optional[int]  # None for monthname format
    strike: int
    opt_type: str  # CE/PE
    pattern: str  # numeric / ond_short / monthname

    @property
    def contract_month(self) -> str:
        return f"{self.expiry_year:04d}-{self.expiry_month:02d}"

    @property
    def guessed_expiry(self) -> Optional[date]:
        if self.expiry_day is None:
            return None
        return date(self.expiry_year, self.expiry_month, self.expiry_day)


@dataclass(frozen=True)
class ResolvedExpiry:
    final_expiry: date
    expiry_type: str  # monthly/weekly
    is_monthly: bool
    is_weekly: bool
    mapping_source: str  # final_direct / scheduled_to_final / as_is / monthname_monthly


@dataclass(frozen=True)
class ExpiryCalendar:
    monthly_final_by_month: dict[tuple[str, str], date]
    scheduled_to_final: dict[tuple[str, date], date]
    final_set: set[tuple[str, date]]


def parse_contract_filename(path: Path) -> Optional[ParsedContract]:
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
            pattern="monthname",
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
            pattern="numeric",
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
            pattern="ond_short",
        )

    return None


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
            final_expiry=monthly_final,
            expiry_type="monthly",
            is_monthly=True,
            is_weekly=False,
            mapping_source="monthname_monthly",
        )

    guessed = parsed.guessed_expiry
    assert guessed is not None

    if (instrument, guessed) in cal.final_set:
        final_expiry = guessed
        mapping_source = "final_direct"
    else:
        mapped = cal.scheduled_to_final.get((instrument, guessed))
        if mapped is None:
            final_expiry = guessed
            mapping_source = "as_is"
        else:
            final_expiry = mapped
            mapping_source = "scheduled_to_final"

    is_monthly = monthly_final is not None and final_expiry == monthly_final
    return ResolvedExpiry(
        final_expiry=final_expiry,
        expiry_type="monthly" if is_monthly else "weekly",
        is_monthly=is_monthly,
        is_weekly=not is_monthly,
        mapping_source=mapping_source,
    )


def _fmt_date(d: Optional[date]) -> str:
    return "" if d is None else d.isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit old options filenames and resolve expiries via expiry_calendar.csv"
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw/options"))
    parser.add_argument(
        "--expiry-calendar",
        type=Path,
        default=Path("newer data stocks/config/expiry_calendar.csv"),
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("newer data stocks/docs/reports/old_options_filename_expiry_mapping.csv"),
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=Path("newer data stocks/docs/reports/OLD_OPTIONS_NAMING_AND_EXPIRY_MAPPING.md"),
    )
    parser.add_argument("--limit", type=int, default=None, help="Debug: limit number of files processed")
    args = parser.parse_args()

    if not args.input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {args.input_dir}")
    if not args.expiry_calendar.exists():
        raise FileNotFoundError(f"Expiry calendar not found: {args.expiry_calendar}")

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)

    cal = load_expiry_calendar(args.expiry_calendar)

    total_files = 0
    by_underlying = Counter()
    by_pattern = Counter()
    by_expiry_type = Counter()
    by_mapping_source = Counter()
    shift_examples: list[tuple[str, date, date, str]] = []  # filename, guessed, final, instrument
    pattern_examples: dict[str, list[str]] = defaultdict(list)
    expiry_counts: dict[tuple[str, date], int] = Counter()

    header = [
        "filename",
        "underlying",
        "pattern",
        "token_year",
        "token_month",
        "token_day",
        "strike",
        "opt_type",
        "contract_month",
        "guessed_expiry",
        "final_expiry",
        "expiry_type",
        "is_monthly",
        "is_weekly",
        "mapping_source",
        "mapped_from_scheduled",
    ]

    with args.output_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for idx, p in enumerate(sorted(args.input_dir.glob("*.parquet"), key=lambda x: x.name)):
            if args.limit is not None and idx >= args.limit:
                break

            parsed = parse_contract_filename(p)
            if parsed is None:
                # Shouldn't happen (we have full coverage), but keep file row for traceability.
                writer.writerow([p.name, "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""])
                continue

            resolved = resolve_expiry(parsed, cal)

            total_files += 1
            by_underlying[parsed.underlying] += 1
            by_pattern[parsed.pattern] += 1
            by_expiry_type[resolved.expiry_type] += 1
            by_mapping_source[resolved.mapping_source] += 1
            expiry_counts[(parsed.underlying, resolved.final_expiry)] += 1

            if len(pattern_examples[parsed.pattern]) < 12:
                pattern_examples[parsed.pattern].append(p.name)

            guessed = parsed.guessed_expiry
            mapped_from_scheduled = (
                guessed is not None
                and resolved.mapping_source == "scheduled_to_final"
                and guessed != resolved.final_expiry
            )
            if mapped_from_scheduled and len(shift_examples) < 60:
                shift_examples.append((p.name, guessed, resolved.final_expiry, parsed.underlying))

            writer.writerow(
                [
                    p.name,
                    parsed.underlying,
                    parsed.pattern,
                    parsed.expiry_year,
                    parsed.expiry_month,
                    "" if parsed.expiry_day is None else parsed.expiry_day,
                    parsed.strike,
                    parsed.opt_type,
                    parsed.contract_month,
                    _fmt_date(guessed),
                    resolved.final_expiry.isoformat(),
                    resolved.expiry_type,
                    str(resolved.is_monthly).lower(),
                    str(resolved.is_weekly).lower(),
                    resolved.mapping_source,
                    str(mapped_from_scheduled).lower(),
                ]
            )

    # Build markdown summary
    top_expiries = sorted(expiry_counts.items(), key=lambda kv: (-kv[1], kv[0][0], kv[0][1]))[:30]

    md_lines: list[str] = []
    md_lines.append("# Old Options (5Y) Filename Naming + Expiry Mapping Audit\n")
    md_lines.append(f"Generated: `{datetime.now().isoformat(timespec='seconds')}`\n")
    md_lines.append("## Inputs\n")
    md_lines.append(f"- Raw options dir: `{args.input_dir}`\n")
    md_lines.append(f"- Expiry calendar: `{args.expiry_calendar}`\n")
    md_lines.append("\n## Outputs\n")
    md_lines.append(f"- CSV mapping: `{args.output_csv}`\n")
    md_lines.append("\n## Naming conventions (observed)\n")
    md_lines.append("| Pattern | Example | Meaning |\n")
    md_lines.append("|---|---|---|\n")
    md_lines.append("| `numeric` | `banknifty2392148300ce` | `UNDERLYING` + `YY` + `M` + `DD` + `STRIKE` + `CE/PE` (month is 1 digit) |\n")
    md_lines.append("| `ond_short` | `banknifty22n1038700pe` | `UNDERLYING` + `YY` + (`o`/`n`/`d`) + `DD` + `STRIKE` + `CE/PE` (Oct/Nov/Dec) |\n")
    md_lines.append("| `monthname` | `banknifty24nov52100pe` | `UNDERLYING` + `YY` + `MON(3 letters)` + `STRIKE` + `CE/PE` (treated as monthly contract-month) |\n")
    md_lines.append("\n## Counts\n")
    md_lines.append(f"- Total files processed: **{total_files:,}**\n")
    md_lines.append(f"- By underlying: {dict(by_underlying)}\n")
    md_lines.append(f"- By pattern: {dict(by_pattern)}\n")
    md_lines.append(f"- By resolved expiry_type: {dict(by_expiry_type)}\n")
    md_lines.append(f"- By mapping_source: {dict(by_mapping_source)}\n")

    md_lines.append("\n## Examples (by pattern)\n")
    for pat in ["numeric", "ond_short", "monthname"]:
        ex = pattern_examples.get(pat, [])
        md_lines.append(f"- `{pat}`: " + (", ".join(f"`{x}`" for x in ex[:10]) if ex else "(none)") + "\n")

    md_lines.append("\n## Scheduled→Final shifts (holiday/week move cases)\n")
    if shift_examples:
        md_lines.append("These are files where the filename encodes a scheduled date, but the calendar maps it to a different final expiry date.\n\n")
        md_lines.append("| Underlying | Filename | Scheduled (from name) | Final (from calendar) |\n")
        md_lines.append("|---|---|---:|---:|\n")
        for fname, sched, fin, instr in shift_examples[:40]:
            md_lines.append(f"| {instr} | `{fname}` | {sched.isoformat()} | {fin.isoformat()} |\n")
    else:
        md_lines.append("- No scheduled→final shifts detected in filename-encoded expiries (unexpected; please double-check calendar).\n")

    md_lines.append("\n## Most common resolved expiries (top 30 by file count)\n")
    md_lines.append("| Underlying | Final expiry | Files |\n")
    md_lines.append("|---|---:|---:|\n")
    for (instr, fin), c in top_expiries:
        md_lines.append(f"| {instr} | {fin.isoformat()} | {c:,} |\n")

    md_lines.append("\n## Notes\n")
    md_lines.append("- `monthname` files do **not** include a day token; they are mapped to the **monthly** final expiry for that contract month using the calendar.\n")
    md_lines.append("- `numeric`/`ond_short` files include a day token which is treated as the scheduled expiry date; the calendar maps it to the final expiry date when shifted.\n")

    args.output_md.write_text("".join(md_lines), encoding="utf-8")

    print(f"Wrote CSV: {args.output_csv}")
    print(f"Wrote MD:  {args.output_md}")
    print(f"Total files: {total_files:,}")
    print(f"By underlying: {dict(by_underlying)}")
    print(f"By pattern: {dict(by_pattern)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
