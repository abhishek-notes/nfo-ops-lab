import os, glob
import polars as pl
import streamlit as st

st.set_page_config(page_title="NFO Scalper Lab", layout="wide")
st.title("NFO Scalper Lab ⚡")

ROOT = os.path.dirname(os.path.dirname(__file__))
features_root = os.path.join(ROOT, "data", "features")

symbol = st.selectbox("Symbol", ["BANKNIFTY","NIFTY"])
entry = st.text_input("Entry rule", "vol_ratio_15_over_30>=3 & r1s>0")
sl = st.number_input("SL %", 0.0, 10.0, 0.5, 0.05)
tp = st.number_input("TP %", 0.0, 10.0, 1.0, 0.05)
trail = st.number_input("Trail %", 0.0, 10.0, 0.4, 0.05)

@st.cache_data(show_spinner=False)
def load_features(symbol):
    fps = glob.glob(os.path.join(features_root, symbol, "*", "*", "*.parquet"))
    if not fps: return None
    dfs = [pl.read_parquet(f, use_pyarrow=True) for f in fps]
    X = pl.concat(dfs, how="vertical_relaxed").sort(["expiry","opt_type","strike","timestamp"])
    return X

X = load_features(symbol)
if X is None:
    st.warning("No features found. Run the CLI to generate first.")
else:
    from nfoops.backtest import vectorized_scalping_backtest
    st.write("Loaded rows:", len(X))
    res = vectorized_scalping_backtest(X, entry_rule=entry, sl_pct=sl, tp_pct=tp, trail_pct=trail)
    trades = res.filter(pl.col("pnl")!=0)
    st.write("Exits/trades:", len(trades))
    st.dataframe(trades.head(100).to_pandas())
