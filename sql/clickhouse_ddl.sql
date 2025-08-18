-- Optional ClickHouse mirror (if you want)
CREATE TABLE IF NOT EXISTS options_1s (
  symbol LowCardinality(String),
  opt_type LowCardinality(String),
  strike UInt32,
  expiry Date,
  ts DateTime('Asia/Kolkata'),
  open Float32, high Float32, low Float32, close Float32,
  vol UInt64,
  vol_15s UInt64, vol_30s UInt64,
  vol_ratio_15_over_30 Float32,
  r1s Float32,
  ema_15s Float32, ema_30s Float32
) ENGINE=MergeTree()
PARTITION BY (symbol, toYYYYMM(expiry))
ORDER BY (symbol, opt_type, strike, expiry, ts);
