# Theta-Positive Intraday Option Selling Strategies
## For ₹20L Capital | Weekly Expiry | 1-Second Data | Greeks Available

---

## **Portfolio Parameters**

**Capital**: ₹20,00,000  
**Max Drawdown**: 10% (₹2,00,000)  
**Max Loss Per Day**: 10% (₹2,00,000)  
**Max Concurrent Strategies**: 10  
**Instruments**: NIFTY (₹1.75L/lot, 75 qty) & BANKNIFTY (₹2.5L/lot, 35 qty)  
**Data**: 1-second orderbook (5 levels bid/ask) + Greeks  
**Trading Hours**: 09:15 - 15:20 (all positions squared off by 15:15)

---

## **STRATEGY 1: Morning Theta Harvest - Pure Decay Play**

### A. Concept Summary
Sell ATM straddle at market open when IV is elevated from overnight uncertainty, targeting rapid theta decay in first 2 hours as uncertainty resolves. Pure non-directional play on time decay with tight risk management.

### B. Market and Instruments
- **Index**: BANKNIFTY preferred (higher premiums), NIFTY as alternative
- **Expiry**: Current week (0-4 DTE), preferably Thursday options on Thursday/Friday
- **Time Window**: Enter 09:25-09:35, exit by 11:30 or earlier if targets hit
- **Avoid**: Monday (weekend gap risk priced in), event days

### C. Entry Conditions
**Time Filter**:
- Enter between 09:25:00 and 09:35:00 only
- Skip entry if first 10 minutes show >1.5% underlying move

**Volatility Conditions**:
- IV percentile of ATM options > 40th percentile (elevated but not extreme)
- Bid-ask spread on ATM < 2% of mid price (ensure liquidity)

**Strike Selection**:
- CE Strike = Closest strike ≥ underlying LTP
- PE Strike = Closest strike ≤ underlying LTP  
- Both legs must have delta absolute value between 0.45-0.55

**Position Size**:
- 1 lot only per entry
- Premium collected target: ₹45,000-60,000 (BANKNIFTY) or ₹35,000-45,000 (NIFTY)

**Entry Confirmation**:
```python
entry_signal = (
    time >= 09:25:00 AND time <= 09:35:00
    AND abs(underlying_change_10min) < 1.5%
    AND atm_iv_percentile > 40
    AND (ce_ask - ce_bid) / ce_mid < 0.02
    AND (pe_ask - pe_bid) / pe_mid < 0.02
    AND abs(ce_delta) BETWEEN 0.45 AND 0.55
    AND abs(pe_delta) BETWEEN 0.45 AND 0.55
)
```

### D. Management and Adjustment Logic

**Premium Imbalance (Breakout Response)**:
- If one leg premium increases >40% from entry while other decreases >20%:
  - Close the losing leg immediately
  - Trail stop on winning leg: lock profit when it decays to 50% of entry premium
  - This converts to directional bet with protected profit

**Time-Based Management**:
- 10:00: If combined premium < 85% of entry, consider partial exit (50%)
- 10:30: If combined premium < 75% of entry, exit remaining 50%
- 11:30: Mandatory exit regardless of P&L

**Delta Imbalance**:
- If net delta exceeds ±0.25 (position becoming directional):
  - Underlying moved significantly
  - Close the ATM leg that's now OTM
  - Let the ITM leg decay or trail stop

### E. Stop Loss, Profit Target, and Risk Management

**Hard Stop Loss**:
- Max loss per trade: 150% of premium collected
- If combined premium to buy back > 2.5x entry credit, exit immediately
- Max underlying move tolerance: ±1.2% from entry

**Profit Targets**:
- Primary target: 40% decay (combined premium = 60% of entry)
- Stretch target: 60% decay (combined premium = 40% of entry)
- Trailing stop: If 50% decay achieved, trail at 40% decay level

**Time-Based Exit**:
- 11:30 mandatory exit (avoid afternoon volatility)
- If in profit at 11:00, trail stop at 30% decay

**Risk Management**:
- Maximum 1 straddle open at a time for this strategy
- Daily loss limit: ₹50,000 - pause strategy for the day if hit
- If 2 consecutive losses, skip next day

### F. Position Sizing and Scaling

**Base Size**:
- Start with 1 lot (₹2.5L margin BANKNIFTY or ₹1.75L NIFTY)
- Do NOT scale up intraday for this strategy

**Account Scaling**:
- With ₹20L: Max 1 lot
- With ₹40L: Max 2 lots (enter separately if multiple signals)
- With ₹60L: Max 3 lots

**Concurrent Positions**:
- Max 1 active position for this specific strategy
- Can run alongside other strategies (respect portfolio-wide 10 strategy limit)

### G. Implementation Notes for Algos

**Data Requirements**:
```python
# At 1-second frequency
underlying_ltp = get_underlying_price()  # NIFTY/BANKNIFTY spot
atm_ce_bid, atm_ce_ask = get_orderbook_L1(ce_strike, side='bid/ask')
atm_pe_bid, atm_pe_ask = get_orderbook_L1(pe_strike, side='bid/ask')
ce_delta, pe_delta, combined_theta = get_greeks(ce_strike, pe_strike)

# Historical for IV percentile
iv_30d_history = get_iv_history(30_days)
current_iv_percentile = percentile_rank(current_iv, iv_30d_history)

# Entry premium
entry_credit = (ce_bid + pe_bid) / 2  # Conservative: use bid prices
```

**Execution Logic**:
- Use **LIMIT orders** at mid price for entry, adjust to ask if not filled in 5 seconds
- Use **MARKET orders** for stop loss (priority is exit speed)
- Monitor every second, not every tick (avoid noise)

**Slippage Assumptions**:
- Entry: ₹5-10 slippage per leg (use mid to conservative ask)
- Exit: ₹10-20 slippage on stop loss
- Normal profit exit: ₹5 slippage per leg

### H. Failure Modes and Robustness

**Failure Scenarios**:
1. **Large gap opening** (>1.5%): Premiums already adjusted, less decay opportunity
2. **Trending day**: Underlying breaks out strongly by 10:00, one leg loses heavily
3. **IV spike mid-morning**: News event or panic, premiums expand instead of decay

**Robustness Enhancements**:
1. **Gap Filter**: Skip if absolute gap > 1.5% or if pre-market futures indicate strong trend
2. **Volatility Regime Filter**: 
   - Calculate 5-day realized volatility
   - Only enter if realized vol < 20 annualized (calm regime)
3. **Volume Confirmation**: Skip if first 15 minutes show 2x normal volume (unusual activity)
4. **Event Calendar**: Disable on RBI days, Budget, election results, US Fed days

**Backtesting Considerations**:
- Test across regime changes (high IV periods, low IV periods)
- Validate that theta decay actually dominates in first 2 hours historically
- Check for slippage sensitivity with 5-level orderbook data

---

## **STRATEGY 2: Afternoon Calm Strangle - Low-Delta Decay**

### A. Concept Summary
Sell 1% OTM strangle in afternoon when intraday volatility has settled, exploiting final 2-3 hours of theta decay with lower delta risk. Regime-adaptive: works best on range-bound days. Win rate >65%.

### B. Market and Instruments
- **Index**: BANKNIFTY (12:30-15:15), NIFTY (13:00-15:15)
- **Expiry**: Current week, optimal on Wednesday-Friday (high theta)
- **Time Window**: Enter 12:30-14:00, exit 15:10-15:15
- **Exploit**: Post-lunch calm, avoid morning volatility

### C. Entry Conditions
**Time Filter**:
- Enter between 12:30 and 14:00
- Preferred: 13:00-13:30 (lunch lull)

**Range Condition** (Key Filter):
- Calculate intraday range from 09:15 to current time:
  ```python
  intraday_range_pct = (high - low) / open * 100
  ```
- Only enter if `intraday_range_pct < 1.2%` (range-bound day)
- Underlying has not moved >0.5% in last 30 minutes

**Volatility Conditions**:
- Realized volatility (5-minute) in last hour < 15% annualized
- IV still elevated (percentile > 30) but not spiking

**Strike Selection**:
- CE Strike: 1st strike where `distance_from_spot ≈ underlying * 1.0%` (positive)
- PE Strike: 1st strike where `distance_from_spot ≈ underlying * 1.0%` (negative)
- Target delta: |delta| between 0.20-0.30 per leg

**Position Size**:
- 1 lot BANKNIFTY or 1 lot NIFTY
- Target premium: ₹25,000-35,000 (lower than ATM, but safer)

**Entry Signal**:
```python
entry = (
    time BETWEEN 12:30 AND 14:00
    AND intraday_range < 1.2%
    AND realized_vol_1h < 15%
    AND underlying_move_30min < 0.5%
    AND ce_distance ≈ 1.0% AND pe_distance ≈ 1.0%
    AND abs(ce_delta) BETWEEN 0.20 AND 0.30
    AND abs(pe_delta) BETWEEN 0.20 AND 0.30
)
```

### D. Management and Adjustment Logic

**Breakout Handling** (Rare but Critical):
- If underlying breaks daily range (moves >1.5% from entry):
  - Close loser leg immediately (limit loss to 2x premium on that leg)
  - Hold winner leg with trailing stop at 50% decay

**Premium Monitoring**:
- Every 5 minutes, check combined premium
- If premium decays to 60% of entry by 14:30, exit 50% and trail rest

**Time Decay Acceleration**:
- After 14:30, theta accelerates
- If combined premium < 50% of entry, exit fully (take profit)

**Delta Shift**:
- If net portfolio delta exceeds ±0.15:
  - Market is drifting
  - Close the losing side, trail the winner

### E. Stop Loss, Profit Target, and Risk Management

**Hard Stop**:
- Max loss: 200% of credit received (less aggressive than Strategy 1)
- Underlying move > 1.5% from entry point = immediate exit

**Profit Targets**:
- Target 1: 50% decay (exit 50% of position)
- Target 2: 70% decay (exit remaining 50%)
- If neither target by 15:10, exit market

**Time Stop**:
- 15:10: Begin forced exit
- 15:15: All positions closed

**Risk Limits**:
- Daily max loss for this strategy: ₹40,000
- After 2 stop losses in a day, pause until next day

### F. Position Sizing and Scaling

**Base**: 1 lot per entry
**Maximum**: 2 concurrent strangles if spacing >1 hour and different setups
**Scaling**: Not recommended for this strategy (keep it simple)

### G. Implementation Notes

**Critical Data Points**:
```python
# Range calculation
intraday_high = max(underlying_prices[9:15:00 to now])
intraday_low = min(underlying_prices[9:15:00 to now])
range_pct = (intraday_high - intraday_low) / underlying_open * 100

# Realized vol (5-min returns, 1-hour window)
returns_5min = log(price_t / price_t-5min)
realized_vol_1h = std(returns_5min) * sqrt(252 * 78)  # Annualized

# OTM strike selection
target_distance = underlying * 0.01
ce_strike = find_closest_strike(ce_strikes, target=underlying + target_distance)
pe_strike = find_closest_strike(pe_strikes, target=underlying - target_distance)
```

**Execution**:
- Entry: Limit orders at mid, aggressive to ask if needed (low urgency)
- Exit: Limit at mid for profit-taking, market for stops

### H. Failure Modes and Robustness

**Failures**:
1. Sudden afternoon rally/selloff (breaks range assumption)
2. IV spike from late news (premiums expand)
3. Thursday/Friday expiry: gamma risk increases significantly

**Enhancements**:
1. **Expiry Day Filter**: On Thursday (weekly expiry), reduce position to 0.5 lots or skip entirely
2. **News Filter**: Disable from 14:00-14:30 if US market opens with gap >1%
3. **Trend Filter**: Skip if underlying showing clear directional bias (9 of last 10 5-min candles same direction)
4. **Liquidity Check**: Ensure bid-ask spread < 3% on both strikes

---

## **STRATEGY 3: Breakout Adaptive Straddle - Premium Rebalancing**

### A. Concept Summary
Start with ATM straddle, actively manage based on premium differential. If market breaks out, close loser early to limit damage, trail winner to capture continued move. Convert theta play into delta play when regime shifts. Mildly directional adaptation.

### B. Market and Instruments
- **Index**: BANKNIFTY (more volatile, better for this)
- **Expiry**: Current week
- **Time Window**: Enter 09:45-10:15 (after initial volatility settles)
- **Best Days**: Trending days (ironically, but we adapt)

### C. Entry Conditions
**Time**: 09:45-10:15 (post-opening noise)

**Volatility Setup**:
- IV percentile > 50 (elevated uncertainty)
- First 30 minutes showed movement (not dead calm - we want some action)

**Strike Selection**:
- ATM straddle (delta ≈ ±0.50 each leg)

**Entry Premium Balance**:
- Verify CE premium and PE premium are within 15% of each other
- Combined premium target: ₹50,000+

```python
entry = (
    time BETWEEN 09:45 AND 10:15
    AND iv_percentile > 50
    AND abs(ce_premium - pe_premium) / max(ce_premium, pe_premium) < 0.15
    AND abs(ce_delta) > 0.45 AND abs(pe_delta) > 0.45
    AND combined_premium > 50000
)
```

### D. Management and Adjustment Logic

**Core Logic - Premium Differential Monitoring** (Check every 10 seconds):

```python
premium_ratio = max(current_ce_premium, current_pe_premium) / min(current_ce_premium, current_pe_premium)

if premium_ratio > 1.8:  # Strong imbalance
    # One leg is losing badly
    loser_leg = ce if current_ce_premium > entry_ce_premium * 1.4 else pe
    
    # Close loser
    close_position(loser_leg, order_type='MARKET')
    
    # Winner side (decay side)
    winner_leg = pe if loser_leg == ce else ce
    
    # Trail winner with these rules:
    winner_trailing_stop = winner_current_premium * 1.3  # Allow 30% adverse move
    
    # If winner decays to 40% of entry, book profit
    if winner_current_premium < entry_winner_premium * 0.4:
        close_position(winner_leg, order_type='LIMIT at mid')
```

**Delta Imbalance**:
- Monitor net delta every 30 seconds
- If `abs(net_delta) > 0.35` for 2 minutes straight:
  - Someone's winning/losing significantly
  - Apply premium ratio logic above

**Time Progression**:
- 12:00: If straddle still balanced (ratio < 1.5), tighten stops (move to 175% loss per leg)
- 14:00: If still open, evaluate: exit if profit < ₹5,000
- 15:00: Mandatory close

### E. Stop Loss, Profit Target, and Risk Management

**Stop Loss (Dynamic)**:
- **Initial**: 200% of premium per leg (₹100K loss on ₹50K credit)
- **After 1 hour**: Tighten to 175% if still balanced
- **If converted to single leg**: 130% trailing stop on that leg

**Profit Targets**:
- Balanced decay: 50% combined premium decay = close 50%, trail rest
- Single leg post-breakout: 60% decay = close

**Risk**:
- Daily max loss: ₹75,000 (higher because we're adapting to breaks)
- Max 2 concurrent positions

### F. Position Sizing
- 1 lot BANKNIFTY per trade
- Max 2 straddles running (if entered at different times)

### G. Implementation

**Premium Monitoring** (Critical):
```python
def monitor_premium_balance(ce_current, pe_current, ce_entry, pe_entry):
    ce_change_pct = (ce_current - ce_entry) / ce_entry
    pe_change_pct = (pe_current - pe_entry) / pe_entry
    
    # Loser: premium increased significantly
    # Winner: premium decreased
    
    if ce_change_pct > 0.4 and pe_change_pct < -0.2:
        return 'CLOSE_CE', 'TRAIL_PE'
    elif pe_change_pct > 0.4 and ce_change_pct < -0.2:
        return 'CLOSE_PE', 'TRAIL_CE'
    else:
        return 'HOLD', 'HOLD'
```

**Execution**:
- Premium checks every 10 seconds
- Delta checks every 30 seconds
- Fast execution on breakout leg closure (market orders acceptable)

### H. Failure Modes
**Failures**:
1. **Whipsaw**: Market breaks, we close loser, then reverses (lost on both)
2. **Low volatility**: Straddle just bleeds theta on both sides, premium differential never develops
3. **Gap risk**: Sudden move catches us

**Robustness**:
1. **Whipsaw Protection**: After closing one leg, don't re-enter opposite leg for 30 minutes
2. **Low Vol Filter**: If realized vol < 12%, skip this strategy (use Strategy 1 instead)
3. **Position Limit**: Never more than 2 of these running (limits whipsaw exposure)

---

## **STRATEGY 4: Iron Condor - Range Binding on Calm Days**

### A. Concept Summary
Sell wider strangle (2% OTM), buy far OTM options as protection (3.5% OTM). Defined risk, lower margin. Works on days with established range. Pure non-directional, lower theta but capped loss.

### B. Market and Instruments
- **Index**: NIFTY (less volatile, better for defined risk)
- **Expiry**: Current week
- **Time**: Enter 10:30-12:00 (after morning volatility)
- **Days**: Range-bound days only

### C. Entry Conditions

**Range Detection** (Mandatory):
```python
# Calculate pre-entry range
range_9am_to_now = (high - low) / underlying
morning_trend_strength = abs(close_now - open) / (high - low)  # < 0.6 means choppy

enter_if = (
    time BETWEEN 10:30 AND 12:00
    AND range_9am_to_now < 1.5%  # Not too volatile
    AND morning_trend_strength < 0.6  # Choppy, not trending
    AND realized_vol_2h < 18%
)
```

**Strike Selection** (4 legs):
- **Sell CE**: 2% above spot, delta ≈ -0.15
- **Sell PE**: 2% below spot, delta ≈ +0.15
- **Buy CE**: 3.5% above spot (protection)
- **Buy PE**: 3.5% below spot (protection)

**Premium Structure**:
- Net credit: ₹15,000 - ₹25,000 (after buying wings)
- Max risk: Width of strikes - net credit ≈ ₹35,000 - ₹50,000

### D. Management

**Breakout**: 
- If underlying touches either short strike:
  - Close entire IC immediately (don't fight breakouts)
  - Max loss: Predefined (₹35-50K)

**Profit Taking**:
- If net premium decays to 70% (₹5-7K profit), close 50%
- If 85% decay, close all

**Time Management**:
- 14:30: Evaluate, if minimal profit, consider early exit
- 15:05: Close all

### E. Risk Management

**Max Loss**: Defined by structure (₹35-50K per IC)
**Profit Target**: 50-70% of max profit (₹7-12K)
**Daily Limit**: 2 IC max, ₹70K daily loss limit

### F. Position Sizing
- 1 lot NIFTY per IC
- Max 2 concurrent ICs

### G. Implementation
**Greeks Check**:
```python
short_ce_delta + short_pe_delta ≈ 0  # Balanced
combined_theta > 500 per day  # Sufficient decay
```

**Orderbook**: Ensure all 4 legs have reasonable liquidity (spread < 5%)

### H. Failure Modes
**Failures**: Breakouts (by design, accept defined loss)

**Robustness**:
1. Only trade on low realized vol days (< 18%)
2. Avoid expiry day (gamma spike)
3. Event filter active

---

## **STRATEGY 5: High-Frequency Theta Scalp - Multiple Small Bets**

### A. Concept Summary
Sell ATM or near-ATM options for 15-30 minute holds, targeting 15-25% premium decay in short bursts. High frequency (4-6 trades/day), small size, relies on rapid theta and mean reversion.

### B. Market and Instruments
- **Index**: BANKNIFTY (liquid, tight spreads)
- **Expiry**: Current week, Wed-Fri (high theta)
- **Time**: 9:30, 10:30, 11:30, 13:30, 14:30 (hourly entries)

### C. Entry Conditions

**Entry Windows** (Hourly):
- 9:30 ±5 min, 10:30 ±5 min, etc.
- Each window can trigger 1 trade

**Market State**:
- Last 15 minutes: underlying moved < 0.4% (micro-calm)
- Orderbook balanced: `(bid_qty - ask_qty) / (bid_qty + ask_qty) < 0.15`

**Strike**:
- ATM or ±50 points from ATM (delta 0.40-0.60)
- Sell straddle or single side if delta > 0.55

**Size**: 1 lot, hold 15-30 min max

```python
if time in [9:30, 10:30, 11:30, 13:30, 14:30] (±5 min):
    if underlying_range_15min < 0.4% AND orderbook_imbalance < 0.15:
        enter_atm_straddle(lots=1, max_hold_minutes=30)
```

### D. Management

**Quick Exit**:
- Target: 20% decay in 20 minutes
- If achieved, exit immediately
- If not achieved by 30 min, exit anyway (prevent overnight risk)

**Stop**: 80% loss (₹40K premium → ₹32K loss)

### E. Risk Management
- Max 2 concurrent positions
- Daily max loss: ₹60K (after 3 stop losses, pause)

### F. Implementation
**Speed**: Critical - need fast execution
**Slippage**: ₹5-10 per leg (tight)

### G. Failure Modes
**Failure**: Volatile days (all trades stop out)
**Fix**: Disable if realized vol > 20%

---

## **STRATEGY 6-8: Additional Strategies** *(Brief Outlines)*

### **STRATEGY 6: IV Spike Fade - Sell After Panic**
- Enter when IV jumps >30% in 10 minutes (news event)
- Sell OTM options expecting IV crush
- 60 min hold max
- High risk/reward

### **STRATEGY 7: Weekly Expiry Day Gamma Scalp**
- Thursday PM only
- Sell very close ATM (delta 0.50±0.05)
- Exit within 1 hour
- Capture theta acceleration near expiry
- Higher risk, capped size (0.5 lots)

### **STRATEGY 8: Delta-Neutral Strangle with Auto-Rebalancing**
- Sell 1.5% OTM strangle
- Monitor delta every 5 min
- If net delta > ±0.20, adjust by closing imbalanced side
- Continuous rebalancing
- Longer hold (2-4 hours)

---

## **Portfolio-Level Risk Management**

**Daily Limits**:
- Max loss across ALL strategies: ₹2,00,000 (10% of capital)
- If hit, close all positions and stop trading for the day

**Correlation Management**:
- Max 3 short option positions (any strategy) on same underlying
- Diversify across NIFTY and BANKNIFTY if possible

**Weekly Review**:
- Disable worst performing strategy if 3-week losing streak
- Scale up best performer (but respect position limits)

**Event Blacklist**:
- RBI policy days: No trading
- Budget day: No trading
- Election results: No trading  
- US Fed days: Reduce size by 50%

---

## **Implementation Priority**

**Start with these 3**:
1. **Strategy 1** (Morning Theta Harvest) - Simplest, highest win rate
2. **Strategy 2** (Afternoon Calm Strangle) - Complementary time window
3. **Strategy 3** (Breakout Adaptive) - Learn premium management

**Add later**:
4. Strategy 5 (High-Frequency) - Requires faster execution infrastructure
5. Strategy 4 (Iron Condor) - After mastering range detection
6-8. Advanced variations once core strategies profitable

---

**This strategy suite is designed for systematic, rule-based execution with your 1-second orderbook data and Greeks. All conditions are quantifiable and backtestable.**
