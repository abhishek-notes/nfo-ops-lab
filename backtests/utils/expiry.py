from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import polars as pl

IST = "Asia/Kolkata"


@dataclass(frozen=True)
class ExpiryWindow:
    instrument: str
    prev_expiry: date | None
    expiry: date | None


def load_calendar(csv_path: str | Path) -> pl.DataFrame:
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"Calendar CSV not found: {p}")
    lf = pl.scan_csv(str(p))
    # Normalize column names we rely on
    # Expected columns include: Instrument, Expiry_Type, Final_Expiry
    # Use collect_schema() to avoid performance warning
    cols_list = lf.collect_schema().names()
    cols = {c.lower(): c for c in cols_list}
    # Collect with typed schema
    df = (
        lf.select([
            pl.col(cols.get("instrument", "Instrument")).alias("Instrument"),
            pl.col(cols.get("expiry_type", "Expiry_Type")).alias("Expiry_Type"),
            pl.col(cols.get("final_expiry", "Final_Expiry")).str.strptime(pl.Date, strict=False).alias("Final_Expiry"),
        ]).collect()
    )
    return df.sort(["Instrument", "Final_Expiry"])  # stable order


def select_immediate_expiry_window(calendar_df: pl.DataFrame, instrument: str, day: date) -> ExpiryWindow:
    c = calendar_df.filter(pl.col("Instrument") == instrument.upper())
    if c.is_empty():
        return ExpiryWindow(instrument=instrument.upper(), prev_expiry=None, expiry=None)
    # E = min{Final_Expiry >= day}
    c_future = c.filter(pl.col("Final_Expiry") >= pl.lit(day))
    if c_future.is_empty():
        return ExpiryWindow(instrument=instrument.upper(), prev_expiry=None, expiry=None)
    E = c_future[0, "Final_Expiry"]
    # prevE = max{Final_Expiry < E}
    c_past = c.filter(pl.col("Final_Expiry") < pl.lit(E))
    prevE = c_past[-1, "Final_Expiry"] if not c_past.is_empty() else None
    return ExpiryWindow(instrument=instrument.upper(), prev_expiry=prevE, expiry=E)
