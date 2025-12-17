# Backtesting / Strategy Scripts - Path Conventions

**Last Verified**: December 13, 2025  
**Status**: ✅ Documented (align code to this)

---

## Summary (what “correct” means)

- **Packed data input (primary)**: `data/options_date_packed_FULL_v3_SPOT_ENRICHED/<YYYY-MM-DD>/<UNDERLYING>/part-*.parquet`
- **Canonical strategy outputs**: `strategies/strategy_results/**`
- **Exported/flattened CSVs** (optional): `results/` (flat CSVs)

This repo has multiple runner scripts; the safest and most portable approach is:
- resolve paths via `Path(__file__).resolve()` (script location), not the current working directory.

---

## Current layout notes

### Strategy scripts

- Buying runners live in: `strategies/buying/*.py`
- Selling runners live in: `strategies/selling/**.py`
- Most runners write outputs into: `strategies/strategy_results/**`

### Data processing scripts

- SQL extraction: `scripts/sql_extraction/*.py`
- Spot extraction: `scripts/spot_extraction/*.py`
- Repacking: `scripts/data_processing/*.py`
- Verification: `scripts/verification/*.py`

---

## Root directory status (note)

The repo root is intentionally folder-organized, but a few large reference files exist (e.g., `Market Truth Framework Fixes.md`).

---

## Recommendation: make paths execution-proof

If a script uses `Path("../data/...")`, it depends on where you run it from (CWD). Prefer:

```python
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]   # adjust per folder depth
data_dir = ROOT / "data" / "options_date_packed_FULL_v3_SPOT_ENRICHED"
results_dir = ROOT / "strategies" / "strategy_results" / "..."
```

---

## Verification commands

```bash
# Verify no hardcoded absolute paths
rg -n 'Path\\(\"/Users' strategies scripts market_truth_framework

# Find any '../results' usage (prefer strategies/strategy_results)
rg -n '\\.\\./results' strategies

# Confirm packed-data root references
rg -n 'options_date_packed_FULL_v3_SPOT_ENRICHED' strategies scripts market_truth_framework
```
