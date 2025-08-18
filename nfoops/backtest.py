from __future__ import annotations
import operator, re
import polars as pl

OPS = {
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    "<": operator.lt,
}

def parse_rule(rule: str) -> list[tuple[str, str, float]]:
    # simple parser: "vol_ratio_15_over_30>=3 & r1s>0"
    parts = re.split(r"\s*&\s*|\s*\|\s*", rule)
    out = []
    for p in parts:
        m = re.match(r"([a-zA-Z0-9_]+)\s*(>=|<=|==|!=|>|<)\s*([-+]?[0-9]*\.?[0-9]+)", p)
        if not m:
            raise ValueError(f"Bad rule segment: {p}")
        out.append((m.group(1), m.group(2), float(m.group(3))))
    return out

def rule_mask(df: pl.DataFrame, rule: str) -> pl.Series:
    parts = parse_rule(rule)
    m = None
    for col, op, val in parts:
        if col not in df.columns:
            raise KeyError(f"Column {col} missing in features")
        expr = OPS[op](df[col], val)
        m = expr if m is None else (m & expr)
    return m

def vectorized_scalping_backtest(
    bars: pl.DataFrame,
    entry_rule: str = "vol_ratio_15_over_30>=3 & r1s>0",
    sl_pct: float = 0.5,   # percent
    tp_pct: float = 1.0,   # percent
    trail_pct: float = 0.4 # percent
) -> pl.DataFrame:
    """
    Vectorized long-only scalp on 1s bars.
    Entry at next bar close (or open) after rule is true.
    SL/TP/trailing computed with rolling extrema and groupwise scans (no Python loops).
    """
    df = bars.with_columns([
        pl.arange(0, pl.len()).alias("_row"),
    ])
    mask = rule_mask(df, entry_rule)
    df = df.with_columns(mask.alias("entry_signal"))

    # edge detection: rising edge (False->True)
    edge = (pl.col("entry_signal").cast(pl.Int8).diff().fill_null(0) == 1).alias("_edge")
    df = df.with_columns(edge)
    trade_id = pl.when(pl.col("_edge")).then(1).otherwise(0).cumsum().alias("trade_id")
    df = df.with_columns(trade_id)

    # entry at next bar close
    df = df.with_columns(
        pl.when(pl.col("_edge")).then(pl.col("close").shift(-1)).alias("_entry_price_raw")
    ).with_columns(
        pl.col("_entry_price_raw").forward_fill().alias("_ff_entry")
    ).with_columns(
        pl.when(pl.col("trade_id")>0).then(pl.col("_ff_entry")).otherwise(None).alias("entry_price")
    ).drop("_entry_price_raw","_ff_entry")

    # stops
    df = df.with_columns([
        (pl.col("entry_price") * (1 - sl_pct/100.0)).alias("stop_loss"),
        (pl.col("entry_price") * (1 + tp_pct/100.0)).alias("take_profit"),
    ])

    # trailing stop based on running high since entry (use close to be conservative intra-second)
    df = df.with_columns(
        pl.when(pl.col("trade_id")>0).then(pl.col("close")).otherwise(None).alias("_in_trade_close")
    ).with_columns(
        pl.col("_in_trade_close").cummax().over("trade_id").alias("run_max_close")
    ).with_columns(
        (pl.col("run_max_close") * (1 - trail_pct/100.0)).alias("trail_stop")
    ).drop("_in_trade_close")

    # Exit flags per bar
    exit_hit = (
        ((pl.col("low") <= pl.min_horizontal("stop_loss","trail_stop")) | (pl.col("high") >= pl.col("take_profit")))
        & (pl.col("trade_id")>0)
    ).alias("exit_hit")
    df = df.with_columns(exit_hit)

    # First exit row per trade
    exits = (
        df.filter(pl.col("exit_hit"))
          .group_by("trade_id")
          .agg(pl.col("_row").min().alias("exit_row"))
    )
    df = df.join(exits, on="trade_id", how="left")

    # in-trade mask
    df = df.with_columns((pl.col("trade_id")>0).alias("has_trade"))
    df = df.with_columns(
        (pl.col("_row") <= pl.col("exit_row")).fill_null(False).alias("in_trade")
    )

    # PnL only at exit row (close-to-close diff vs entry)
    df = df.with_columns(
        pl.when(pl.col("_row")==pl.col("exit_row"))
          .then(pl.col("close") - pl.col("entry_price"))
          .otherwise(0.0)
          .alias("pnl")
    )
    return df
