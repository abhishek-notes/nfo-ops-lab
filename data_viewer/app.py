#!/usr/bin/env python3
"""
NFO Options Data Viewer

A Streamlit app to view and compare data across:
- Raw parquet files (from SQL extraction)
- Packed parquet files (normalized format)
- Original SQL.gz data

Usage:
    streamlit run app.py
"""

import streamlit as st
import polars as pl
import gzip
import re
from pathlib import Path
from datetime import datetime, date
import os

# Data folder configurations - relative to this script's parent directory
_BASE = Path(__file__).resolve().parent.parent  # /Users/abhishek/workspace/nfo
_DATA_ROOT = _BASE / "newer data stocks"
_NEW_DATA = _DATA_ROOT / "new 2025 data"

DATA_FOLDERS = {
    "main (nov+)": _NEW_DATA / "nov 4 to nov 18 new stocks data",
    "oct 20 to nov 3": _NEW_DATA / "oct 20 to nov 3 new stocks data",
    "oct 7 to oct 20": _NEW_DATA / "oct 7 to oct 20 new stocks data",
    "sep 23-oct 6": _NEW_DATA / "sep 23 to oct 6 new stocks data",
    "aug 29-sep 23": _NEW_DATA / "aug 29 to sep 23 new stocks data",
    "aug 14 to 10 sep": _NEW_DATA / "aug 14 to 10 sep new stocks data",
    "aug 13-29": _NEW_DATA / "aug 13 to aug 29 new stocks data",
    "aug 1 to aug 13": _NEW_DATA / "aug 1 to aug 13 new stocks data",
}

MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}


def parse_filename(filename: str) -> dict | None:
    """Parse raw parquet filename into components."""
    # Pattern: banknifty25sep54000ce.parquet or nifty25aug22600pe.parquet
    pattern = r'^(banknifty|nifty)(\d{2})([a-z]{3})(\d+)(ce|pe)\.parquet$'
    m = re.match(pattern, filename.lower())
    if m:
        return {
            'symbol': m.group(1).upper(),
            'year': 2000 + int(m.group(2)),
            'month': MONTH_MAP.get(m.group(3)),
            'month_str': m.group(3),
            'strike': int(m.group(4)),
            'opt_type': m.group(5).upper(),
        }
    return None


def get_available_options(folder_path: str) -> list[dict]:
    """Get list of available options from raw_options folder."""
    raw_dir = Path(folder_path) / "processed_output" / "raw_options"
    if not raw_dir.exists():
        return []

    options = []
    for f in raw_dir.glob("*.parquet"):
        meta = parse_filename(f.name)
        if meta:
            meta['filename'] = f.name
            meta['path'] = str(f)
            options.append(meta)

    return sorted(options, key=lambda x: (x['symbol'], x['strike'], x['opt_type']))


def get_packed_path(folder_path: str, symbol: str, expiry: date, opt_type: str, strike: int) -> Path | None:
    """Find packed parquet file for given parameters."""
    packed_dir = Path(folder_path) / "processed_output" / "packed_options"
    if not packed_dir.exists():
        return None

    yyyymm = f"{expiry.year:04d}{expiry.month:02d}"
    exp_str = expiry.strftime("%Y-%m-%d")

    path = packed_dir / symbol / yyyymm / f"exp={exp_str}" / f"type={opt_type}" / f"strike={strike}.parquet"
    if path.exists():
        return path
    return None


def load_raw_data(path: str, filter_date: date = None) -> pl.DataFrame:
    """Load raw parquet file, optionally filtering by date."""
    df = pl.read_parquet(path)

    if filter_date and 'timestamp' in df.columns:
        df = df.filter(pl.col('timestamp').dt.date() == filter_date)

    return df


def load_packed_data(path: str, filter_date: date = None) -> pl.DataFrame:
    """Load packed parquet file, optionally filtering by date."""
    df = pl.read_parquet(str(path))

    if filter_date and 'timestamp' in df.columns:
        df = df.filter(pl.col('timestamp').dt.date() == filter_date)

    return df


def extract_sql_rows(sql_path: str, table_name: str, limit: int = 100) -> list[dict]:
    """Extract rows from SQL.gz file for a specific table."""
    rows = []

    # Column definitions matching SQL schema
    SQL_COLUMNS = [
        'timestamp', 'price', 'qty', 'avgPrice', 'volume', 'bQty', 'sQty',
        'open', 'high', 'low', 'close', 'changeper', 'lastTradeTime',
        'oi', 'oiHigh', 'oiLow',
        'bq0', 'bp0', 'bo0', 'bq1', 'bp1', 'bo1', 'bq2', 'bp2', 'bo2',
        'bq3', 'bp3', 'bo3', 'bq4', 'bp4', 'bo4',
        'sq0', 'sp0', 'so0', 'sq1', 'sp1', 'so1', 'sq2', 'sp2', 'so2',
        'sq3', 'sp3', 'so3', 'sq4', 'sp4', 'so4'
    ]

    try:
        with gzip.open(sql_path, 'rt', encoding='utf-8', errors='replace') as f:
            for line in f:
                if f'`{table_name}`' not in line:
                    continue
                if 'REPLACE INTO' not in line and 'INSERT INTO' not in line:
                    continue

                # Parse VALUES
                idx = line.find('VALUES')
                if idx == -1:
                    continue

                content = line[idx + 6:].strip()
                if content.endswith(';'):
                    content = content[:-1]

                # Simple parser
                in_row = False
                in_quote = False
                current_val = []
                current_row = []

                for char in content:
                    if not in_row:
                        if char == '(':
                            in_row = True
                            current_row = []
                            current_val = []
                    else:
                        if char == "'" and not in_quote:
                            in_quote = True
                        elif char == "'" and in_quote:
                            in_quote = False
                        elif char == ',' and not in_quote:
                            current_row.append(''.join(current_val).strip())
                            current_val = []
                        elif char == ')' and not in_quote:
                            current_row.append(''.join(current_val).strip())
                            if len(current_row) == len(SQL_COLUMNS):
                                row_dict = dict(zip(SQL_COLUMNS, current_row))
                                rows.append(row_dict)
                                if len(rows) >= limit:
                                    return rows
                            in_row = False
                        else:
                            current_val.append(char)

                if rows:
                    break
    except Exception as e:
        st.error(f"Error reading SQL: {e}")

    return rows


def get_sql_table_name(symbol: str, year: int, month_str: str, strike: int, opt_type: str) -> str:
    """Generate SQL table name from parameters."""
    yr = year % 100
    return f"{symbol}{yr}{month_str.upper()}{strike}{opt_type}"


# Streamlit App
st.set_page_config(page_title="NFO Data Viewer", layout="wide")
st.title("NFO Options Data Viewer")

# Sidebar for selection
st.sidebar.header("Data Selection")

# Folder selection
folder_name = st.sidebar.selectbox(
    "Data Folder",
    list(DATA_FOLDERS.keys()),
    index=0  # Default to "main (nov+)"
)
folder_path = DATA_FOLDERS[folder_name]

# Check if folder exists
if not Path(folder_path).exists():
    st.error(f"Folder not found: {folder_path}")
    st.stop()

# Get available options
options = get_available_options(folder_path)
if not options:
    st.warning("No raw options files found in this folder")
    st.stop()

# Symbol filter
symbols = sorted(list(set(o['symbol'] for o in options)))
selected_symbol = st.sidebar.selectbox("Symbol", symbols)

# Filter by symbol
filtered_options = [o for o in options if o['symbol'] == selected_symbol]

# Strike filter
strikes = sorted(list(set(o['strike'] for o in filtered_options)))
selected_strike = st.sidebar.selectbox("Strike", strikes)

# Option type filter
opt_types = sorted(list(set(o['opt_type'] for o in filtered_options if o['strike'] == selected_strike)))
selected_opt_type = st.sidebar.selectbox("Option Type", opt_types)

# Find matching file
matching = [o for o in filtered_options if o['strike'] == selected_strike and o['opt_type'] == selected_opt_type]
if not matching:
    st.warning("No matching option found")
    st.stop()

selected_option = matching[0]

# Load raw data to get available dates
raw_df = load_raw_data(selected_option['path'])

# Date filter (optional)
if 'timestamp' in raw_df.columns:
    available_dates = raw_df.select(pl.col('timestamp').dt.date().unique()).to_series().to_list()
    available_dates = sorted([d for d in available_dates if d is not None])

    if available_dates:
        selected_date = st.sidebar.selectbox(
            "Filter by Date (optional)",
            ["All Dates"] + available_dates,
            format_func=lambda x: str(x) if x != "All Dates" else x
        )
        if selected_date != "All Dates":
            raw_df = raw_df.filter(pl.col('timestamp').dt.date() == selected_date)

st.sidebar.markdown("---")
st.sidebar.markdown(f"**File:** `{selected_option['filename']}`")
st.sidebar.markdown(f"**Symbol:** {selected_symbol}")
st.sidebar.markdown(f"**Strike:** {selected_strike}")
st.sidebar.markdown(f"**Type:** {selected_opt_type}")

# Main content - tabs
tab1, tab2, tab3, tab4 = st.tabs(["Raw Parquet", "Packed Parquet", "SQL Data", "Compare"])

with tab1:
    st.header("Raw Parquet Data")
    st.markdown(f"**Source:** `{selected_option['path']}`")
    st.markdown(f"**Rows:** {raw_df.height:,}")
    st.markdown(f"**Columns:** {len(raw_df.columns)}")

    raw_start = st.number_input("Start row (raw)", min_value=0, value=0, step=100)
    raw_rows = st.number_input("Rows to show (raw)", min_value=10, value=500, step=100)

    # Show schema
    with st.expander("Schema"):
        schema_df = pl.DataFrame({
            "Column": raw_df.columns,
            "Type": [str(raw_df[c].dtype) for c in raw_df.columns]
        })
        st.dataframe(schema_df, use_container_width=True)

    # Show data
    st.dataframe(raw_df.slice(raw_start, raw_rows).to_pandas(), use_container_width=True)

with tab2:
    st.header("Packed Parquet Data")

    # Find packed file - need to determine expiry
    packed_dir = Path(folder_path) / "processed_output" / "packed_options" / selected_symbol
    if packed_dir.exists():
        # List available expiries
        expiries = []
        for ym_dir in packed_dir.iterdir():
            if ym_dir.is_dir():
                for exp_dir in ym_dir.iterdir():
                    if exp_dir.is_dir() and exp_dir.name.startswith("exp="):
                        exp_str = exp_dir.name.replace("exp=", "")
                        try:
                            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                            strike_file = exp_dir / f"type={selected_opt_type}" / f"strike={selected_strike}.parquet"
                            if strike_file.exists():
                                expiries.append((exp_date, strike_file))
                        except:
                            pass

        if expiries:
            expiries.sort(key=lambda x: x[0])
            selected_expiry = st.selectbox(
                "Select Expiry",
                expiries,
                format_func=lambda x: str(x[0])
            )

            packed_path = selected_expiry[1]
            packed_df = load_packed_data(packed_path)

            # Apply date filter if set
            if 'selected_date' in dir() and selected_date != "All Dates":
                packed_df = packed_df.filter(pl.col('timestamp').dt.date() == selected_date)

            packed_start = st.number_input("Start row (packed)", min_value=0, value=0, step=100)
            packed_rows = st.number_input("Rows to show (packed)", min_value=10, value=500, step=100)

            st.markdown(f"**Source:** `{packed_path}`")
            st.markdown(f"**Rows:** {packed_df.height:,}")
            st.markdown(f"**Columns:** {len(packed_df.columns)}")

            with st.expander("Schema"):
                schema_df = pl.DataFrame({
                    "Column": packed_df.columns,
                    "Type": [str(packed_df[c].dtype) for c in packed_df.columns]
                })
                st.dataframe(schema_df, use_container_width=True)

            st.dataframe(packed_df.slice(packed_start, packed_rows).to_pandas(), use_container_width=True)
        else:
            st.warning(f"No packed data found for {selected_symbol} {selected_strike} {selected_opt_type}")
    else:
        st.warning("Packed options directory not found")

with tab3:
    st.header("SQL Source Data")

    # Determine which SQL file to use
    if selected_symbol == "BANKNIFTY":
        sql_file = Path(folder_path) / "das_bankopt_mod.sql.gz"
    else:
        sql_file = Path(folder_path) / "das_niftyopt_mod.sql.gz"

    if sql_file.exists():
        table_name = get_sql_table_name(
            selected_symbol,
            selected_option['year'],
            selected_option['month_str'],
            selected_strike,
            selected_opt_type
        )

        st.markdown(f"**SQL File:** `{sql_file}`")
        st.markdown(f"**Table Name:** `{table_name}`")

        sql_start = st.number_input("Start row (SQL extract)", min_value=0, value=0, step=50)
        sql_rows = st.number_input("Rows to show (SQL extract)", min_value=10, value=100, step=50)

        if st.button("Extract SQL Rows"):
            with st.spinner("Extracting from SQL.gz..."):
                sql_records = extract_sql_rows(str(sql_file), table_name, limit=sql_start + sql_rows)
                if sql_records:
                    sql_df = pl.DataFrame(sql_records)
                    if "timestamp" in sql_df.columns:
                        sql_df = sql_df.with_columns(
                            pl.col("timestamp").cast(pl.Utf8).str.slice(0, 19).alias("_ts_norm")
                        )
                    if sql_start > 0:
                        sql_df = sql_df.slice(sql_start, sql_rows)
                    st.session_state["sql_df"] = sql_df
                    st.success(f"Extracted {len(sql_records)} rows (showing {len(sql_df)})")
                    st.dataframe(sql_df.to_pandas(), use_container_width=True)
                else:
                    st.warning(f"No data found for table `{table_name}`")
    else:
        st.warning(f"SQL file not found: {sql_file}")

with tab4:
    st.header("Compare Raw vs Packed")

    sql_df = st.session_state.get("sql_df")

    if 'packed_df' in dir() and packed_df is not None:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Raw Data Stats")
            st.metric("Total Rows", f"{raw_df.height:,}")
            st.metric("Columns", len(raw_df.columns))

            if 'open' in raw_df.columns:
                st.metric("Avg Close", f"{raw_df['close'].mean():.2f}" if raw_df['close'].mean() else "N/A")

            if 'timestamp' in raw_df.columns:
                dates = raw_df.select(pl.col('timestamp').dt.date().unique()).to_series().to_list()
                st.metric("Unique Dates", len([d for d in dates if d]))

        with col2:
            st.subheader("Packed Data Stats")
            st.metric("Total Rows", f"{packed_df.height:,}")
            st.metric("Columns", len(packed_df.columns))

            if 'close' in packed_df.columns:
                st.metric("Avg Close", f"{packed_df['close'].mean():.2f}" if packed_df['close'].mean() else "N/A")

            if 'timestamp' in packed_df.columns:
                dates = packed_df.select(pl.col('timestamp').dt.date().unique()).to_series().to_list()
                st.metric("Unique Dates", len([d for d in dates if d]))

        st.markdown("---")

        # Column comparison
        st.subheader("Column Comparison")
        raw_cols = set(raw_df.columns)
        packed_cols = set(packed_df.columns)
        sql_cols = set(sql_df.columns) if sql_df is not None else set()

        union_cols = sorted(raw_cols | packed_cols | sql_cols)
        presence = []
        for c in union_cols:
            presence.append({
                "Column": c,
                "Raw": c in raw_cols,
                "Packed": c in packed_cols,
                "SQL": c in sql_cols
            })
        st.dataframe(pl.DataFrame(presence).to_pandas(), use_container_width=True)

        # Sample row comparison
        st.markdown("---")
        st.subheader("Sample Row Comparison")

        def ts_strings(df):
            """Normalize timestamps to second-level strings for matching."""
            if df is None:
                return set()
            if "_ts_norm" in df.columns:
                return set(df["_ts_norm"].to_list())
            if "timestamp" not in df.columns:
                return set()
            return set(
                df.select(
                    pl.col("timestamp")
                    .cast(pl.Utf8)
                    .str.slice(0, 19)  # trim tz/offset if present
                    .alias("ts")
                )["ts"].to_list()
            )

        common_ts = ts_strings(raw_df) & ts_strings(packed_df)
        if sql_df is not None:
            common_ts = common_ts & ts_strings(sql_df)

        if common_ts:
            ts_choices = sorted(list(common_ts))[:200]
            chosen_ts = st.selectbox("Choose common timestamp", ts_choices)

            raw_row = raw_df.filter(pl.col('timestamp').cast(pl.Utf8).str.slice(0, 19) == chosen_ts).head(1)
            packed_row = packed_df.filter(pl.col('timestamp').cast(pl.Utf8).str.slice(0, 19) == chosen_ts).head(1)
            sql_row = None
            if sql_df is not None:
                ts_col = "_ts_norm" if "_ts_norm" in sql_df.columns else "timestamp"
                sql_row = sql_df.filter(pl.col(ts_col).cast(pl.Utf8).str.slice(0, 19) == chosen_ts).head(1)

            col_sections = []
            if raw_row.height > 0:
                col_sections.append(("Raw", raw_row))
            if packed_row.height > 0:
                col_sections.append(("Packed", packed_row))
            if sql_row is not None and sql_row.height > 0:
                col_sections.append(("SQL", sql_row))

            for name, df_ in col_sections:
                display_df = df_.drop("_ts_norm") if "_ts_norm" in df_.columns else df_
                st.subheader(f"{name} row @ {chosen_ts}")
                st.dataframe(display_df.to_pandas(), use_container_width=True)
                json_text = display_df.head(1).to_pandas().to_json(orient="records", date_format="iso")
                st.text_area(f"{name} row (copyable JSON)", json_text, height=120)
        else:
            raw_count = len(ts_strings(raw_df))
            packed_count = len(ts_strings(packed_df))
            sql_count = len(ts_strings(sql_df)) if sql_df is not None else 0
            st.info(f"No common timestamps across raw/packed/SQL for comparison "
                    f"(raw={raw_count}, packed={packed_count}, sql={sql_count})")
    else:
        st.info("Load packed data in the 'Packed Parquet' tab first")

# Footer
st.markdown("---")
st.markdown("*NFO Options Data Viewer - Compare raw, packed, and SQL data*")
