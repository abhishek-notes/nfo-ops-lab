"""NFO Options Data Viewer - Streamlit app to explore packed options data."""
import os
import glob
from pathlib import Path

import polars as pl
import streamlit as st

st.set_page_config(page_title="NFO Options Data Viewer", layout="wide")
st.title("NFO Options Data Viewer")

# Data folder configurations - default relative to this file, but allow override
_DATA_ROOT = Path(os.environ.get("NFO_DATA_ROOT", Path(__file__).resolve().parent.parent))
_NEW_DATA = _DATA_ROOT / "new 2025 data"

DATA_FOLDERS = {
    "main (nov+)": _NEW_DATA / "nov 4 to nov 18 new stocks data/processed_output/packed_options",
    "oct 20 to nov 3": _NEW_DATA / "oct 20 to nov 3 new stocks data/processed_output/packed_options",
    "oct 7 to oct 20": _NEW_DATA / "oct 7 to oct 20 new stocks data/processed_output/packed_options",
    "sep 23-oct 6": _NEW_DATA / "sep 23 to oct 6 new stocks data/processed_output/packed_options",
    "aug 29-sep 23": _NEW_DATA / "aug 29 to sep 23 new stocks data/processed_output/packed_options",
    "aug 14 to 10 sep": _NEW_DATA / "aug 14 to 10 sep new stocks data/processed_output/packed_options",
    "aug 13-29": _NEW_DATA / "aug 13 to aug 29 new stocks data/processed_output/packed_options",
    "aug 1 to aug 13": _NEW_DATA / "aug 1 to aug 13 new stocks data/processed_output/packed_options",
}

# Check which folders exist
available_folders = {}
for name, path in DATA_FOLDERS.items():
    if path.exists():
        available_folders[name] = path
    else:
        st.sidebar.warning(f"Folder not found: {name}")

if not available_folders:
    st.error("No data folders found! Please check paths.")
    st.stop()

# Sidebar controls
st.sidebar.header("Data Selection")
selected_folder = st.sidebar.selectbox("Data Folder", list(available_folders.keys()))
data_path = available_folders[selected_folder]

# Get available symbols
symbols = []
for d in data_path.iterdir():
    if d.is_dir() and d.name in ["BANKNIFTY", "NIFTY"]:
        symbols.append(d.name)

if not symbols:
    st.error(f"No symbol folders found in {data_path}")
    st.stop()

symbol = st.sidebar.selectbox("Symbol", sorted(symbols))
symbol_path = data_path / symbol

# Get available expiries
expiries = []
for month_dir in symbol_path.iterdir():
    if month_dir.is_dir():
        for exp_dir in month_dir.glob("exp=*"):
            expiry_date = exp_dir.name.replace("exp=", "")
            expiries.append(expiry_date)
expiries = sorted(set(expiries))

if not expiries:
    st.error(f"No expiry folders found for {symbol}")
    st.stop()

expiry = st.sidebar.selectbox("Expiry", expiries)

# Get option types (CE/PE)
opt_types = ["CE", "PE"]
opt_type = st.sidebar.selectbox("Option Type", opt_types)

# Get available strikes for this expiry/type
strikes = []
for month_dir in symbol_path.iterdir():
    if month_dir.is_dir():
        exp_dir = month_dir / f"exp={expiry}" / f"type={opt_type}"
        if exp_dir.exists():
            for pq in exp_dir.glob("strike=*.parquet"):
                strike = int(pq.stem.replace("strike=", ""))
                strikes.append(strike)
strikes = sorted(set(strikes))

if not strikes:
    st.warning(f"No strikes found for {symbol} {expiry} {opt_type}")
    st.stop()

strike = st.sidebar.selectbox("Strike", strikes)

# Load data
@st.cache_data(show_spinner="Loading data...")
def load_strike_data(folder_path, symbol, expiry, opt_type, strike):
    """Load parquet data for a specific strike."""
    pattern = f"{folder_path}/{symbol}/**/exp={expiry}/type={opt_type}/strike={strike}.parquet"
    files = glob.glob(str(pattern), recursive=True)
    if not files:
        return None
    dfs = [pl.read_parquet(f) for f in files]
    return pl.concat(dfs).sort("timestamp")

df = load_strike_data(data_path, symbol, expiry, opt_type, strike)

if df is None or len(df) == 0:
    st.error("No data found for selected options.")
    st.stop()

# Display stats
st.subheader(f"{symbol} {strike} {opt_type} exp={expiry}")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Rows", len(df))
with col2:
    min_ts = df["timestamp"].min()
    st.metric("First Timestamp", str(min_ts)[:19])
with col3:
    max_ts = df["timestamp"].max()
    st.metric("Last Timestamp", str(max_ts)[:19])
with col4:
    # Extract time component to check trading hours
    times = df.select(pl.col("timestamp").dt.time().alias("time"))
    unique_times = times.unique().sort("time")
    min_time = str(unique_times["time"][0])[:8]
    max_time = str(unique_times["time"][-1])[:8]
    st.metric("Time Range", f"{min_time} - {max_time}")

# Trading hours check
st.subheader("Trading Hours Analysis")
df_with_time = df.with_columns(
    pl.col("timestamp").dt.hour().alias("hour"),
    pl.col("timestamp").dt.minute().alias("minute"),
    pl.col("timestamp").dt.date().alias("date")
)

# Count ticks per hour
hourly = df_with_time.group_by("hour").agg(pl.count().alias("count")).sort("hour")
st.bar_chart(hourly.to_pandas().set_index("hour"))

# Show expected vs actual hours
st.write("**Expected Trading Hours:** 09:15 - 15:30 IST")
hours_present = hourly["hour"].to_list()
expected_hours = list(range(9, 16))  # 9 AM to 3 PM
missing_hours = [h for h in expected_hours if h not in hours_present]

if missing_hours:
    st.warning(f"Missing hours in data: {missing_hours}")
else:
    st.success("All trading hours (9-15) are present in the data!")

# Data preview
st.subheader("Data Preview")
st.dataframe(df.head(100).to_pandas())

# Price chart
st.subheader("Price Chart")
if "close" in df.columns:
    chart_df = df.select(["timestamp", "close"]).to_pandas().set_index("timestamp")
    st.line_chart(chart_df)

# Volume analysis
st.subheader("Volume Delta by Hour")
if "vol_delta" in df.columns:
    vol_by_hour = df_with_time.group_by("hour").agg(
        pl.col("vol_delta").sum().alias("total_vol_delta"),
        pl.count().alias("tick_count")
    ).sort("hour")
    st.dataframe(vol_by_hour.to_pandas())
