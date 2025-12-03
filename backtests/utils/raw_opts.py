from __future__ import annotations
import re
from pathlib import Path
from functools import lru_cache
from datetime import datetime, date, time
import polars as pl
from dateutil import tz

IST = "Asia/Kolkata"
RAW_ROOT = Path("data/raw/options")


def _to_ist_ts(lf: pl.LazyFrame, col: str = "timestamp") -> pl.LazyFrame:
    lf2 = lf
    # If timestamp is string, parse; else assume already datetime
    if lf2.schema.get(col) == pl.Utf8:
        lf2 = lf2.with_columns(pl.col(col).str.strptime(pl.Datetime, strict=False))
    lf2 = lf2.with_columns(pl.col(col).dt.replace_time_zone(IST).dt.cast_time_unit("ns"))
    return lf2


def _min_max_date(path: Path) -> tuple[date | None, date | None]:
    try:
        lf = pl.scan_parquet(str(path)).select("timestamp", "ts").with_columns([
            pl.when(pl.col("timestamp").is_not_null()).then(pl.col("timestamp")).otherwise(pl.col("ts")).alias("t")
        ]).select("t")
        if lf.schema.get("t") == pl.Utf8:
            lf = lf.with_columns(pl.col("t").str.strptime(pl.Datetime, strict=False))
        lf = lf.with_columns(pl.col("t").dt.replace_time_zone(IST))
        mm = lf.select([pl.col("t").min().alias("tmin"), pl.col("t").max().alias("tmax")]).collect()
        tmin = mm[0, "tmin"]
        tmax = mm[0, "tmax"]
        return (tmin.date() if tmin else None, tmax.date() if tmax else None)
    except Exception:
        return (None, None)


def _extract_strike_from_name(p: Path) -> int | None:
    # Try to capture the last 4–7 digit number before ce/pe
    m = re.search(r"(\d{4,7})(?=c[ep]\.parquet$)", p.name, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    # Fallback: look for strike= groups (rare in raw)
    m2 = re.search(r"strike=(\d+)", p.name, re.IGNORECASE)
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            return None
    return None


def _read_meta(path: Path) -> tuple[str | None, str | None, int | None]:
    try:
        lf = pl.scan_parquet(str(path)).select(["symbol", "opt_type", "strike"]).head(1)
        df = lf.collect()
        if df.is_empty():
            return None, None, None
        sym = df[0, "symbol"] if "symbol" in df.columns else None
        typ = df[0, "opt_type"] if "opt_type" in df.columns else None
        try:
            k = int(df[0, "strike"]) if "strike" in df.columns and df[0, "strike"] is not None else None
        except Exception:
            k = None
        return sym, typ, k
    except Exception:
        return None, None, None


@lru_cache(maxsize=8)
def index_raw_files(symbol: str, opt_type: str) -> list[tuple[int | None, Path, date | None, date | None]]:
    sym_target = symbol.upper()
    t_target = opt_type.upper()
    files: list[Path] = []
    if not RAW_ROOT.exists():
        return []
    for p in RAW_ROOT.glob("*.parquet"):
        files.append(p)
    out = []
    for p in files:
        sym, typ, k_meta = _read_meta(p)
        if sym is None or typ is None:
            # fallback to filename heuristics for opt_type
            if sym is None:
                sym = symbol.upper() if p.name.lower().startswith(symbol.lower()) else None
            if typ is None:
                if p.name.lower().endswith("ce.parquet"):
                    typ = "CE"
                elif p.name.lower().endswith("pe.parquet"):
                    typ = "PE"
        if sym != sym_target or typ != t_target:
            continue
        k = k_meta if k_meta is not None else _extract_strike_from_name(p)
        tmin, tmax = _min_max_date(p)
        out.append((k, p, tmin, tmax))
    return out


def find_raw_file(symbol: str, opt_type: str, strike: int, day: date, step: int, band_steps: int = 5) -> Path | None:
    idx = index_raw_files(symbol, opt_type)
    # Candidates covering the day, exact strike preferred
    cands = [(k, p) for (k, p, tmin, tmax) in idx if tmin and tmax and tmin <= day <= tmax]
    if not cands:
        # As fallback, accept files whose range is within ±14 days
        cands = [(k, p) for (k, p, tmin, tmax) in idx if tmin and tmax and (tmin <= (day) <= (tmax + pl.duration(days=14).to_python()))]
    if not cands:
        return None
    # Try exact then nearest within band
    strikes = [k for (k, _) in cands if k is not None]
    if not strikes:
        return None
    if strike in strikes:
        return next(p for (k, p) in cands if k == strike)
    # nearest within band
    band = step * band_steps
    near = [k for k in strikes if abs(k - strike) <= band]
    if not near:
        # choose absolute nearest anyway
        k_use = min(strikes, key=lambda x: abs(x - strike))
    else:
        k_use = min(near, key=lambda x: abs(x - strike))
    return next(p for (k, p) in cands if k == k_use)


def load_option_seconds_raw(symbol: str, day: date, expiry: date, strike: int, opt_type: str) -> pl.DataFrame | None:
    step = 100 if symbol == "BANKNIFTY" else 50
    path = find_raw_file(symbol, opt_type, strike, day, step)
    if not path:
        return None
    try:
        lf = pl.scan_parquet(str(path)).select(["timestamp", "ts", "price", "qty", "volume"])  # columns may vary
    except Exception:
        return None
    # Build a normalized time column
    lf = lf.with_columns([
        pl.when(pl.col("timestamp").is_not_null()).then(pl.col("timestamp")).otherwise(pl.col("ts")).alias("t")
    ]).select(["t", "price", "qty", "volume"])
    if lf.schema.get("t") == pl.Utf8:
        lf = lf.with_columns(pl.col("t").str.strptime(pl.Datetime, strict=False))
    lf = lf.with_columns(pl.col("t").dt.replace_time_zone(IST).dt.cast_time_unit("ns").alias("ts"))
    t0 = datetime.combine(day, time(9, 15)).replace(tzinfo=tz.gettz(IST))
    t1 = datetime.combine(day, time(15, 30)).replace(tzinfo=tz.gettz(IST))
    s_l = pl.lit(t0).cast(pl.Datetime("ns", time_zone=IST))
    e_l = pl.lit(t1).cast(pl.Datetime("ns", time_zone=IST))
    df = lf.filter((pl.col("ts") >= s_l) & (pl.col("ts") <= e_l)).collect()
    if df.is_empty():
        return None
    # Per-second aggregate: last(price) and sum(qty) as vol
    sec = (
        df.with_columns(pl.col("ts").dt.truncate("1s").alias("sec"))
          .group_by("sec")
          .agg([
              pl.col("price").last().alias("close"),
              pl.when(pl.col("qty").is_not_null()).then(pl.col("qty")).otherwise(pl.lit(0)).sum().alias("vol"),
          ])
          .sort("sec")
    )
    return sec.rename({"sec": "ts"})
