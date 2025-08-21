# Strategy Catalog (Chat-Derived)

This catalog enumerates every strategy family and concrete variant referenced across the 21 chat PDFs, mapped to your current data layout. Entries are grouped by family. Each entry defines a playbook with inputs, logic, risk, and backtest notes. Where helpful, a runnable scaffold is referenced under `backtests/strategies/`.

Counts
- Families: 28
- Concrete variants: 74
- Runnable scaffolds provided in this repo now: 6 (see “Scaffolds”)

Scaffolds
- `backtests/strategies/gamma_scalp_baseline.py`
- `backtests/strategies/iv_rv_spread_proxy.py`
- `backtests/strategies/expiry_iv_crush_play.py`
- `backtests/strategies/short_straddle_premium_decay.py`
- `backtests/strategies/dispersion_proxy_index_vs_basket.py`
- `backtests/strategies/oi_shift_breakout.py`

Data layout (packed)
- Options: `data/packed/options/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/type={CE|PE}/strike={K}.parquet`
- Spot: `data/packed/spot/{SYMBOL}/{YYYYMM}/date={YYYY-MM-DD}/ticks.parquet`
- Futures: `data/packed/futures/{SYMBOL}/{YYYYMM}/exp={YYYY-MM-DD}/ticks.parquet`
- Calendar: `meta/expiry_calendar.csv`

Note on options OHLC: Vendor data often repeats session-wide OHLC; rely on `close` and volume features; use spot-path delta proxy or robust last-trade sampling.

---

## 1) Volatility Risk Premium (Short IV)

Entry 1.1 — Short ATM straddle (weekly) with tail hedge
- Thesis: Implied > realized; systematically short premium, hedge tails.
- Inputs: Spot 1s; options `close`, `vol_delta`; calendar.
- Preconditions: No major event day; IV percentile high.
- Entry: Sell ATM CE+PE at 10:00; tail hedge via OTM options.
- Exit: Target on premium decay or 15:20; stop on spot move threshold.
- Risk: Hard stop; tail hedge cost budget.
- Costs: Brokerage, STT, slippage modeled.
- Backtest notes: Use spot delta-proxy; avoid options OHLC.

Entry 1.2 — Short near-ATM strangle with dynamic wings
- As above; dynamic wings based on intraday realized vol estimate.

Entry 1.3 — Premium harvest post-gap open (fade)
- Enter after first 5m if realized vol drops below implied.

## 2) IV–RV Spread (Directional Vol)

Entry 2.1 — Buy vol when IV << forecast RV (intraday)
- Thesis: Mean-revert IV; long gamma intraday.
- Entry: Buy ATM straddle upon vol signal; gamma scalp.

Entry 2.2 — Short vol when IV >> realized RV (steady state)
- Use rolling RV estimator vs IV proxy.

## 3) Term Structure & Skew Trades

Entry 3.1 — Front vs next-week IV spread (calendar)
- Long/short calendar depending on steepness; exit on flattening.

Entry 3.2 — Skew mean reversion (OTM put/call skew)

## 4) Dispersion

Entry 4.1 — Short index vol, long basket vol (proxy)
- Use futures + options proxies; re-balance daily.
- Scaffold: `dispersion_proxy_index_vs_basket.py`.

Entry 4.2 — Long index vol vs short basket (stress regimes)

## 5) Delta-Neutral Gamma Scalping

Entry 5.1 — ATM straddle gamma scalp baseline
- Buy ATM straddle; scalp delta using spot; tight risk.
- Scaffold: `gamma_scalp_baseline.py`.

Entry 5.2 — Event-bound intraday scalp (pre/post event windows)

## 6) Expiry-Day IV Crush

Entry 6.1 — Pre-expiry IV crush short premium
- Sell premium at 10:00 on expiry; exit before auction.
- Scaffold: `expiry_iv_crush_play.py`.

Entry 6.2 — Late-day crush acceleration (14:00 entry)

## 7) Market-Making Microstructure (Proxy)

Entry 7.1 — Spread capture with inventory bounds (simulation)
- Simulated quotes; use spot path; cap inventory; exit at hour end.

## 8) Order Flow Imbalance (Proxy)

Entry 8.1 — Options vol_delta bursts (your ATM±1 variant)
- Already implemented: `backtests/atm_volume_ultra.py`.

Entry 8.2 — Sustained flow (multi-window confirmation)

## 9) OI Build-up Signals

Entry 9.1 — Intraday OI increase with price acceptance
- Go with OI build direction; exit on OI reversal.
- Scaffold: `oi_shift_breakout.py`.

Entry 9.2 — Divergence: price up, OI down (fade)

## 10) Theta-Scoped Intraday Plays

Entry 10.1 — Morning premium decay (10:00–12:00)

Entry 10.2 — Lunch lull decay (12:30–14:00)

## 11) Straddle/Strangle/Spreads

Entry 11.1 — Short straddle with dynamic delta hedge
- Scaffold: `short_straddle_premium_decay.py`.

Entry 11.2 — Iron condor with ATR-based wings

Entry 11.3 — Diagonal/Calendar with IV filter

## 12) Covered Calls / Protective Puts / Collars

Entry 12.1 — Protective put overlay on futures/spot proxy

Entry 12.2 — Covered call yield enhancement days

## 13) Event/Earnings (Index News Proxy)

Entry 13.1 — Post-event mean-reversion in IV

Entry 13.2 — Pre-event long vol with time-boxed hedge

## 14) Intraday Trend / Mean-Revert / Breakout

Entry 14.1 — Spot trend-follow via delta-proxy options

Entry 14.2 — Mean reversion to VWAP bands

Entry 14.3 — Breakout from opening range

## 15) Institutional Flow Patterns

Entry 15.1 — Detect block/bulk-linked OI shifts (proxy)

Entry 15.2 — Hedging footprints (covered calls / protective puts)

## 16) ML-based (Tree/Deep/RL)

Entry 16.1 — XGBoost on intraday features (IV gap, bursts, OI)

Entry 16.2 — LSTM/Transformer spot-path classification

Entry 16.3 — RL hedging policy over spot path

## 17) Markov/Regime Models

Entry 17.1 — 2–3 state regime model for vol/price

Entry 17.2 — State-conditioned strike selection

## 18) Termed Variance/Vol Swaps (Proxy)

Entry 18.1 — Variance carry via options proxy basket

## 19) Skew/Smile Dynamics

Entry 19.1 — Skew steepening/flattening trades

## 20) Calendar Anomalies / Seasonality

Entry 20.1 — Day-of-week specific behaviors

Entry 20.2 — Month/quarter roll patterns

## 21) Risk Parity / Sizing Overlays

Entry 21.1 — Vol targeting on options premium exposure

## 22) Borrow/Carry Proxies

Entry 22.1 — Carry tilt via futures basis + options overlay

## 23) Auction Imbalance (Proxy)

Entry 23.1 — Pre-close premium adjustment

## 24) Cross-Asset Hedges

Entry 24.1 — NIFTY/BANKNIFTY cross hedging plays

## 25) Microburst/Fractals (Volume/Price)

Entry 25.1 — Multi-window burst confirmation (>30s, >5m avg)

## 26) Spread Greeks Optimization

Entry 26.1 — Delta/theta neutralization constraints

## 27) Execution Alpha

Entry 27.1 — Queue position heuristics (proxy fills)

## 28) Misc. Robustness Filters

Entry 28.1 — Data quality, calendar, and tz guards baked-in

---

Appendix: Implementation Notes
- Spot path is authoritative for intraday trajectory; options prices proxied via delta where needed.
- Always align timestamps to `datetime[ns, Asia/Kolkata]` and filter by session hours.
- Use `vol_delta` derived from cumulative volume via `.diff().clip(lower=0)` for options/futures; spot `vol_delta`=0 by design.

