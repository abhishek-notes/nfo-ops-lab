# ATM Volume-Burst Backtest (Spot-routed, Options-executed)

This strategy routes by **spot** each hour to pick the **ATM** option (plus ±1 strikes),
then watches **per-second option volume** for a 30-second burst vs a 5-minute baseline.
On first qualified burst (with a trend filter vs the hour open), it opens a trade
on that option series and manages exits with target/stop/trailing.

## Why this form
- Your packed options contain `vol_delta` per tick; that's perfect for a burst signal.
- Option OHLC can be noisy. Default **PnL = delta-proxy** using spot×delta, which is robust and fast.
- We still record the option 'close' at entry (`entry_opt`) for intuitive % risk sizing.

## Layout assumptions

```
data/
  packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet
meta/
  expiry_calendar.csv   # Instrument, Final_Expiry, Expiry_Type
```

Spot parquet files can be anywhere; pass a glob like `--spot-glob "./data/spot/{symbol}/*.parquet"`.

## Install
```bash
pip install -r backtests/requirements.txt
```

## Run examples

```bash
# Quick 1-2 weeks
python backtests/atm_volume.py --symbol BANKNIFTY --start 2023-11-01 --end 2023-11-10

# Full year with different threshold
python backtests/atm_volume.py --symbol BANKNIFTY --start 2023-01-01 --end 2023-12-31 --multiplier 2.0

# Using custom spot glob
python backtests/atm_volume.py --symbol NIFTY --start 2024-02-01 --end 2024-02-28 --spot-glob "./my_spot/NIFTY/*.parquet"
```

## Outputs
- `backtests/results/trades_{symbol}_{start}_{end}.parquet`
- `backtests/results/summary_{symbol}_{start}_{end}.parquet`

### Trades schema
- symbol, trade_date, anchor (HH:MM:SS), expiry, opt_type, strike, side
- entry_ts, exit_ts, entry_spot, exit_spot, delta_used
- entry_opt, exit_opt, pnl_pts, exit_reason

### Summary schema (per trade_date)
- n_trades, pnl_pts, avg_pnl, std_pnl

## Config (backtests/config.yaml)
- **session**: anchors 10:00..15:00 IST
- **signal**: 30s burst over 5m average × multiplier
- **strike_step**: BN=100, NIFTY=50 (override if needed)
- **delta**: ATM=0.50, NEAR=0.40
- **risk**: side=sell by default, with target/stop/trailing in option %
- **pnl_mode**: delta_proxy (default) or option_close

## Notes
- We forward-map trade_date → next expiry using the calendar CSV.
- Seconds are materialized + cached in ./cache/seconds/... for speed; subsequent runs reuse.
- We choose the first burst across ATM ±1 strikes (CE/PE) within the hour to avoid overlap.
- Set risk.side = "long" to invert and buy options on bursts with matching trend filter.