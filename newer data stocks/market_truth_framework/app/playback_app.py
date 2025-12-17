#!/usr/bin/env python3
"""
Market Truth Playback App (Streamlit)

Run:
  streamlit run app/playback_app.py
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import polars as pl
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "market_truth_data"
FEATURES_DIR = DATA_DIR / "features"
BURSTS_DIR = DATA_DIR / "bursts"
REGIMES_DIR = DATA_DIR / "regimes"


@dataclass(frozen=True)
class DayKey:
    underlying: str
    date: str  # YYYY-MM-DD


def _available_days() -> dict[str, list[str]]:
    days: dict[str, set[str]] = {}
    for f in FEATURES_DIR.glob("features_*.parquet"):
        parts = f.stem.split("_")
        if len(parts) < 3:
            continue
        underlying, day = parts[1], parts[2]
        days.setdefault(underlying, set()).add(day)
    return {k: sorted(v) for k, v in days.items()}


@st.cache_data(show_spinner=False)
def _load_features(key: DayKey) -> pd.DataFrame:
    path = FEATURES_DIR / f"features_{key.underlying}_{key.date}.parquet"
    cols = [
        "timestamp",
        "spot_price",
        "atm_strike",
        "dte_days",
        "ret_1s",
        "rv_10s",
        "rv_120s",
        "accel_10s",
        "ce_mid",
        "pe_mid",
        "ce_spread",
        "pe_spread",
        "ce_obi_5",
        "pe_obi_5",
        "ce_vol_delta",
        "pe_vol_delta",
        "opt_vol_1s",
    ]
    df = pl.read_parquet(path, columns=[c for c in cols if c in pl.read_parquet_schema(path)])
    return df.to_pandas()


@st.cache_data(show_spinner=False)
def _load_bursts(key: DayKey) -> pd.DataFrame:
    path = BURSTS_DIR / f"bursts_{key.underlying}_{key.date}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pl.read_parquet(path).to_pandas()


@st.cache_data(show_spinner=False)
def _load_regimes(key: DayKey) -> pd.DataFrame:
    path = REGIMES_DIR / f"regimes_{key.underlying}_{key.date}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pl.read_parquet(path).to_pandas()


def _regime_color(code: int) -> str:
    # Keep subtle: background overlays at low opacity.
    return {
        0: "rgba(128,128,128,0.10)",  # normal
        1: "rgba(30,144,255,0.12)",   # chop
        2: "rgba(255,140,0,0.14)",    # flicker
        3: "rgba(50,205,50,0.12)",    # burst
        4: "rgba(220,20,60,0.14)",    # fear
    }.get(int(code), "rgba(128,128,128,0.08)")


def _slice_by_time(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df
    ts = pd.to_datetime(df["timestamp"])
    mask = (ts >= start) & (ts <= end)
    out = df.loc[mask].copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"])
    return out


def _add_regime_overlays(fig: go.Figure, regimes: pd.DataFrame, row: int, col: int) -> None:
    if regimes.empty:
        return

    r = regimes[["timestamp", "regime_code"]].copy()
    r["timestamp"] = pd.to_datetime(r["timestamp"])
    r = r.sort_values("timestamp")

    codes = r["regime_code"].to_numpy()
    ts = r["timestamp"].to_numpy()
    if len(ts) == 0:
        return

    start_idx = 0
    for i in range(1, len(ts)):
        if codes[i] != codes[i - 1]:
            x0 = pd.Timestamp(ts[start_idx])
            x1 = pd.Timestamp(ts[i - 1])
            fig.add_vrect(
                x0=x0,
                x1=x1,
                fillcolor=_regime_color(int(codes[i - 1])),
                line_width=0,
                row=row,
                col=col,
            )
            start_idx = i

    x0 = pd.Timestamp(ts[start_idx])
    x1 = pd.Timestamp(ts[-1])
    fig.add_vrect(
        x0=x0,
        x1=x1,
        fillcolor=_regime_color(int(codes[-1])),
        line_width=0,
        row=row,
        col=col,
    )


def _add_burst_overlays(fig: go.Figure, bursts: pd.DataFrame, row: int, col: int) -> None:
    if bursts.empty:
        return

    b = bursts.copy()
    b["start_time"] = pd.to_datetime(b["start_time"])
    b["end_time"] = pd.to_datetime(b["end_time"])

    for _, r in b.iterrows():
        direction = int(r.get("direction", 0))
        fill = "rgba(50,205,50,0.12)" if direction >= 0 else "rgba(220,20,60,0.12)"
        fig.add_vrect(
            x0=r["start_time"],
            x1=r["end_time"],
            fillcolor=fill,
            line_width=0,
            row=row,
            col=col,
        )


def main():
    st.set_page_config(page_title="Market Truth Playback", layout="wide")
    st.title("Market Truth Playback")

    days = _available_days()
    underlyings = sorted(days.keys())
    if not underlyings:
        st.error(f"No processed feature files found in `{FEATURES_DIR}`")
        return

    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        underlying = st.selectbox("Underlying", underlyings, index=0)
    with col_b:
        available = days.get(underlying, [])
        if not available:
            st.error(f"No days found for {underlying}")
            return
        date = st.selectbox("Day", available, index=len(available) - 1)
    key = DayKey(underlying=underlying, date=date)

    features = _load_features(key)
    bursts = _load_bursts(key)
    regimes = _load_regimes(key)

    if features.empty:
        st.error("Empty features file.")
        return

    features["timestamp"] = pd.to_datetime(features["timestamp"])
    min_ts = features["timestamp"].iloc[0]
    max_ts = features["timestamp"].iloc[-1]

    with col_c:
        start_ts, end_ts = st.slider(
            "Time window",
            min_value=min_ts.to_pydatetime(),
            max_value=max_ts.to_pydatetime(),
            value=(min_ts.to_pydatetime(), max_ts.to_pydatetime()),
            format="HH:mm:ss",
        )

    start_ts = pd.Timestamp(start_ts)
    end_ts = pd.Timestamp(end_ts)

    f = _slice_by_time(features, start_ts, end_ts)
    r = _slice_by_time(regimes, start_ts, end_ts) if not regimes.empty else pd.DataFrame()
    b = bursts.copy()
    if not b.empty:
        b["start_time"] = pd.to_datetime(b["start_time"])
        b["end_time"] = pd.to_datetime(b["end_time"])
        b = b[(b["end_time"] >= start_ts) & (b["start_time"] <= end_ts)]

    # Summary
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Rows", f"{len(f):,}")
    with m2:
        st.metric("Bursts", int(len(b)) if not b.empty else 0)
    with m3:
        if not r.empty and "flicker_30s" in r.columns:
            st.metric("Flicker seconds", int((r["flicker_30s"] == 1).sum()))
        else:
            st.metric("Flicker seconds", 0)
    with m4:
        if not r.empty and "fear_active" in r.columns:
            st.metric("Fear seconds", int((r["fear_active"] == 1).sum()))
        else:
            st.metric("Fear seconds", 0)

    # Charts
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.45, 0.35, 0.20],
        subplot_titles=("Spot", "ATM Options", "Regime / Microstructure"),
    )

    fig.add_trace(
        go.Scatter(x=f["timestamp"], y=f["spot_price"], name="spot", mode="lines"),
        row=1,
        col=1,
    )

    if "ce_mid" in f.columns and "pe_mid" in f.columns:
        fig.add_trace(go.Scatter(x=f["timestamp"], y=f["ce_mid"], name="CE mid", mode="lines"), row=2, col=1)
        fig.add_trace(go.Scatter(x=f["timestamp"], y=f["pe_mid"], name="PE mid", mode="lines"), row=2, col=1)

    # Regime (step line)
    if not r.empty and "regime_code" in r.columns:
        fig.add_trace(
            go.Scatter(
                x=r["timestamp"],
                y=r["regime_code"],
                name="regime_code",
                mode="lines",
                line=dict(shape="hv"),
            ),
            row=3,
            col=1,
        )

    # Option spreads (overlay on row 3, secondary y not used to keep simple)
    if "ce_spread" in f.columns and "pe_spread" in f.columns:
        fig.add_trace(go.Scatter(x=f["timestamp"], y=f["ce_spread"], name="CE spread", mode="lines"), row=3, col=1)
        fig.add_trace(go.Scatter(x=f["timestamp"], y=f["pe_spread"], name="PE spread", mode="lines"), row=3, col=1)

    # Background overlays
    _add_regime_overlays(fig, r, row=1, col=1)
    _add_burst_overlays(fig, b, row=1, col=1)

    fig.update_layout(
        height=820,
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(rangeslider_visible=False)

    st.plotly_chart(fig, use_container_width=True)

    # Tables
    if not b.empty:
        st.subheader("Bursts")
        st.dataframe(b.sort_values("start_time")[[
            c
            for c in [
                "burst_id",
                "start_time",
                "end_time",
                "duration_seconds",
                "size_points",
                "direction",
                "ce_move",
                "pe_move",
                "ce_rel_delta",
                "pe_rel_delta",
                "tps",
                "vacuum_score",
                "fear_event",
            ]
            if c in b.columns
        ]], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

