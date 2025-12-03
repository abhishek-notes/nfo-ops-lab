"""
Configuration for the backtesting framework.

All data folder paths and trading session parameters.
"""
from pathlib import Path
from dataclasses import dataclass, field
from datetime import time
from typing import List

IST = "Asia/Kolkata"

# Trading session times (IST)
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

# Data root paths
DATA_ROOT = Path("/workspace/newer data stocks")
YESTERDAY_DATA = DATA_ROOT / "new stocks yesterday data"

# All packed options data folders (in chronological order)
PACKED_OPTIONS_FOLDERS = {
    "till 13 august": YESTERDAY_DATA / "till 13 august new stocks data/processed_output/packed_options",
    "aug 13-29": YESTERDAY_DATA / "aug 13 to aug 29 new stocks data/processed_output/packed_options",
    "aug 29-sep 23": YESTERDAY_DATA / "aug 29 to sep 23 new stocks data/processed_output/packed_options",
    "till 10 sep": YESTERDAY_DATA / "till 10 sep new stocks data/processed_output/packed_options",
    "sep 23-oct 6": YESTERDAY_DATA / "sep 23 to oct 6 new stocks data/processed_output/packed_options",
    "oct 7-20": YESTERDAY_DATA / "oct 7-20 new stocks data/processed_output/packed_options",
    "oct-nov": YESTERDAY_DATA / "oct-nov new stocks data/processed_output/packed_options",
    "main (nov+)": DATA_ROOT / "processed_output/packed_options",
}

# Raw options data folders
RAW_OPTIONS_FOLDERS = {
    "till 13 august": YESTERDAY_DATA / "till 13 august new stocks data/processed_output/raw_options",
    "aug 13-29": YESTERDAY_DATA / "aug 13 to aug 29 new stocks data/processed_output/raw_options",
    "aug 29-sep 23": YESTERDAY_DATA / "aug 29 to sep 23 new stocks data/processed_output/raw_options",
    "till 10 sep": YESTERDAY_DATA / "till 10 sep new stocks data/processed_output/raw_options",
    "sep 23-oct 6": YESTERDAY_DATA / "sep 23 to oct 6 new stocks data/processed_output/raw_options",
    "oct 7-20": YESTERDAY_DATA / "oct 7-20 new stocks data/processed_output/raw_options",
    "oct-nov": YESTERDAY_DATA / "oct-nov new stocks data/processed_output/raw_options",
    "main (nov+)": DATA_ROOT / "processed_output/raw_options",
}

# Sample packed data (older BANKNIFTY samples from 2019)
SAMPLE_PACKED_OPTIONS = DATA_ROOT / "Banknifty packed samples"

# Strike step sizes
STRIKE_STEP = {
    "BANKNIFTY": 100,
    "NIFTY": 50,
}


@dataclass
class BacktestConfig:
    """Configuration for a backtest run."""

    # Symbols to backtest
    symbols: List[str] = field(default_factory=lambda: ["BANKNIFTY", "NIFTY"])

    # Session times
    session_start: time = MARKET_OPEN
    session_end: time = MARKET_CLOSE

    # Data source: "packed" or "raw"
    data_source: str = "packed"

    # Use all available data folders
    use_all_folders: bool = True

    # Specific folders to use (if use_all_folders is False)
    folders: List[str] = field(default_factory=list)

    # Risk parameters
    sl_pct: float = 15.0  # Stop loss percentage
    tp_pct: float = 15.0  # Take profit percentage
    trail_pct: float = 10.0  # Trailing stop percentage

    # Signal parameters
    burst_secs: int = 30  # Volume burst window
    avg_secs: int = 300  # Average volume window
    multiplier: float = 1.5  # Burst multiplier threshold

    # Output
    results_dir: Path = Path("/workspace/backtests/framework/results")

    def get_packed_folders(self) -> dict:
        """Get packed options folders based on config."""
        if self.use_all_folders:
            return {k: v for k, v in PACKED_OPTIONS_FOLDERS.items() if v.exists()}
        return {k: PACKED_OPTIONS_FOLDERS[k] for k in self.folders if k in PACKED_OPTIONS_FOLDERS}

    def get_raw_folders(self) -> dict:
        """Get raw options folders based on config."""
        if self.use_all_folders:
            return {k: v for k, v in RAW_OPTIONS_FOLDERS.items() if v.exists()}
        return {k: RAW_OPTIONS_FOLDERS[k] for k in self.folders if k in RAW_OPTIONS_FOLDERS}
