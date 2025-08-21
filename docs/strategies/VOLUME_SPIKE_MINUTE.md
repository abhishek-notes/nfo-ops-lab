# Volume Spike (Minute) Strategy

- Module: `backtests/strategies/volume_spike_minute.py`
- Purpose: Capture impulsive order-flow bursts by buying the ATM option in the direction of 1-minute spot move when per-minute volume spikes.

Logic
- Aggregate ATM CE and PE to per-second (`close`, `vol` from `vol_delta`), then to per-minute sums.
- Spike when `vol_m > mult * avg(vol_m previous lookback_min minutes)`, defaults: `mult=4.0`, `lookback_min=5`.
- Direction via 1-minute spot change at the spike minute: up -> CE, down -> PE.
- Entry at first second of next minute, or same minute if at session end.
- Risk: `target_pts=5`, `stop_pts=3`, trail by `trail_pts=1` after target.

Params
- `--symbol`: `BANKNIFTY|NIFTY`
- `--start`, `--end`: inclusive dates
- `--mult`: spike multiple (default 4.0)
- `--lookback-min`: averaging window (default 5)
- `--target-pts`, `--stop-pts`, `--trail-pts`

Run
- Direct: `python3 backtests/strategies/volume_spike_minute.py --symbol BANKNIFTY --start 2023-11-15 --end 2023-11-21`
- Orchestrated: `python3 backtests/run_all_strategies.py --symbol BANKNIFTY --start 2023-11-15 --end 2023-11-21 --include vol_spike_minute`

Output
- `backtests/results/volume_spike_minute_{SYMBOL}_{START}_{END}.parquet` with one row per trade.

Notes
- Uses per-second option closes (aggregated from ticks) to avoid vendor OHLC artifacts.
- Expiry/strike discovery: nearest weekly expiry within +4 weeks; nearest strike present in both CE and PE.

