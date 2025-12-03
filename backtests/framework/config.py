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
NEW_2025_DATA = DATA_ROOT / "new 2025 data"

# All packed options data folders (in chronological order)
PACKED_OPTIONS_FOLDERS = {
    "aug 1 to aug 13": NEW_2025_DATA / "aug 1 to aug 13 new stocks data/processed_output/packed_options",
    "aug 13-29": NEW_2025_DATA / "aug 13 to aug 29 new stocks data/processed_output/packed_options",
    "aug 29-sep 23": NEW_2025_DATA / "aug 29 to sep 23 new stocks data/processed_output/packed_options",
    "aug 14 to 10 sep": NEW_2025_DATA / "aug 14 to 10 sep new stocks data/processed_output/packed_options",
    "sep 23-oct 6": NEW_2025_DATA / "sep 23 to oct 6 new stocks data/processed_output/packed_options",
    "oct 7 to oct 20": NEW_2025_DATA / "oct 7 to oct 20 new stocks data/processed_output/packed_options",
    "oct 20 to nov 3": NEW_2025_DATA / "oct 20 to nov 3 new stocks data/processed_output/packed_options",
    "main (nov+)": NEW_2025_DATA / "nov 4 to nov 18 new stocks data/processed_output/packed_options",
}

# Raw options data folders
RAW_OPTIONS_FOLDERS = {
    "aug 1 to aug 13": NEW_2025_DATA / "aug 1 to aug 13 new stocks data/processed_output/raw_options",
    "aug 13-29": NEW_2025_DATA / "aug 13 to aug 29 new stocks data/processed_output/raw_options",
    "aug 29-sep 23": NEW_2025_DATA / "aug 29 to sep 23 new stocks data/processed_output/raw_options",
    "aug 14 to 10 sep": NEW_2025_DATA / "aug 14 to 10 sep new stocks data/processed_output/raw_options",
    "sep 23-oct 6": NEW_2025_DATA / "sep 23 to oct 6 new stocks data/processed_output/raw_options",
    "oct 7 to oct 20": NEW_2025_DATA / "oct 7 to oct 20 new stocks data/processed_output/raw_options",
    "oct 20 to nov 3": NEW_2025_DATA / "oct 20 to nov 3 new stocks data/processed_output/raw_options",
    "main (nov+)": NEW_2025_DATA / "nov 4 to nov 18 new stocks data/processed_output/raw_options",
}

# Sample packed data (older BANKNIFTY samples from 2019)
SAMPLE_PACKED_OPTIONS = NEW_2025_DATA / "Processed Samples/Banknifty packed samples"

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
