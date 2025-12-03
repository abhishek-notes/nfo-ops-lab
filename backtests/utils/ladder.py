from __future__ import annotations
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable
import polars as pl

IST = "Asia/Kolkata"


def strike_step_for(symbol: str) -> int:
    return 100 if symbol.upper() == "BANKNIFTY" else 50


def round_to_step(price: float, step: int) -> int:
    if step <= 0:
        return int(round(price))
    return int(round(price / step) * step)


def anchors_from_strings(trade_day: date, anchors: list[str]) -> list[datetime]:
    out: list[datetime] = []
    for a in anchors:
        hh, mm = [int(x) for x in a.split(":", 1)]
        out.append(datetime.combine(trade_day, time(hh, mm)))
    return out


def full_second_grid(trade_day: date, t_open: str = "09:15:00", t_close: str = "15:30:00") -> pl.DataFrame:
    s = datetime.fromisoformat(f"{trade_day}T{t_open}")
    e = datetime.fromisoformat(f"{trade_day}T{t_close}")
    return pl.DataFrame({
        "ts": pl.datetime_range(s, e, interval="1s", time_unit="ns", time_zone=IST, eager=True)
    })


def spot_path_for_day(symbol: str, day: date) -> Path:
    yyyymm = f"{day:%Y%m}"
    return Path(f"data/packed/spot/{symbol.upper()}/{yyyymm}/date={day:%Y-%m-%d}/ticks.parquet")


def option_path(symbol: str, expiry: date, opt_type: str, strike: int) -> Path:
    yyyymm = f"{expiry:%Y%m}"
    return Path(
        f"data/packed/options/{symbol.upper()}/{yyyymm}/exp={expiry:%Y-%m-%d}/type={opt_type.upper()}/strike={strike}.parquet"
    )


def option_exists(symbol: str, expiry: date, opt_type: str, strike: int) -> bool:
    return option_path(symbol, expiry, opt_type, strike).exists()


def compute_anchor_atm_levels(spot_df: pl.DataFrame, anchors_ts: list[datetime], step: int, ladder_points: Iterable[int]) -> set[int]:
    # spot_df has columns [ts, close] with ts tz-aware IST ns
    if spot_df.is_empty():
        return set()
    # Prepare anchor DataFrame (naive datetimes) and attach IST
    a_df = pl.DataFrame({"anchor": anchors_ts}).with_columns(
        pl.col("anchor").dt.replace_time_zone(IST).dt.cast_time_unit("ns")
    )
    spot_sorted = spot_df.sort("ts")
    j = a_df.join_asof(spot_sorted.rename({"ts": "t"}), left_on="anchor", right_on="t", strategy="backward")
    levels: set[int] = set()
    for row in j.iter_rows(named=True):
        close = row.get("close")
        if close is None:
            continue
        atm = round_to_step(float(close), step)
        for p in ladder_points:
            # Include ATM and symmetric OTM strikes
            levels.add(atm + int(p))
            levels.add(atm - int(p))
    return {int(k) for k in levels if k > 0}
