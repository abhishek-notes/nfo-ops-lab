from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import polars as pl

IST = "Asia/Kolkata"


@dataclass(frozen=True)
class DayCache:
    symbol: str
    trade_date: date
    expiry: date
    df: pl.DataFrame  # wide: ts + close_*/vol_* columns


def load_day_cache(cache_dir: str | Path, symbol: str, trade_date: date) -> DayCache | None:
    base = Path(cache_dir) / symbol.upper() / f"date={trade_date:%Y-%m-%d}"
    # Find the single exp= directory under the date; immediate expiry by construction
    if not base.exists():
        return None
    exp_dirs = [p for p in base.iterdir() if p.is_dir() and p.name.startswith("exp=")]
    if not exp_dirs:
        return None
    exp_dir = exp_dirs[0]
    exp_str = exp_dir.name.split("=", 1)[1]
    parquet_path = exp_dir / "ladder_v1.parquet"
    if not parquet_path.exists():
        return None
    df = pl.read_parquet(str(parquet_path))
    return DayCache(symbol=symbol.upper(), trade_date=trade_date, expiry=date.fromisoformat(exp_str), df=df)

