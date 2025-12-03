#!/usr/bin/env bash
set -euo pipefail

# Summarize EOD gamma scalp monthly net (fees applied) for BANKNIFTY 2025.
# Usage:
#   bash backtests/scripts/summarize_gamma_bnf_2025.sh

if [ -f backtests/scripts/.python_bin ]; then
  PYTHON_BIN="$(cat backtests/scripts/.python_bin)"
fi
PY=${PYTHON_BIN:-python3}
$PY - << 'PY'
import polars as pl, glob
FEE={'stt_rate':0.0010,'exch_rate':0.00035,'ipft_rate':0.000005,'sebi_rate':0.000001,'stamp_rate':0.00003,'gst_rate':0.18}
lot=30
files=sorted(glob.glob('backtests/results/gamma_scalp_eod_BANKNIFTY_2025-*_2025-*_2025-*.parquet'))
if not files:
    print('No gamma EOD files found.'); raise SystemExit
df=pl.concat([pl.read_parquet(f) for f in files], how='vertical_relaxed')
df=df.with_columns([pl.col('date').str.strptime(pl.Date, strict=False).alias('date_d')])
buy=pl.col('entry_prem')*lot; sell=pl.col('exit_prem')*lot
turn=buy+sell; stt=sell*FEE['stt_rate']; exch=turn*FEE['exch_rate']; ipft=turn*FEE['ipft_rate']; sebi=turn*FEE['sebi_rate']; stamp=buy*FEE['stamp_rate']; gst=(exch+ipft+sebi)*FEE['gst_rate']
fees=(stt+exch+ipft+sebi+stamp+gst)
out=df.with_columns([(pl.col('pnl')*lot).alias('gross_rupees'), fees.alias('fees_rupees'), (pl.col('pnl')*lot-fees).alias('net_rupees'), pl.col('date_d').dt.strftime('%Y-%m').alias('month')])
print(out.group_by('month').agg([pl.len().alias('trades'), pl.col('net_rupees').sum().alias('net')]).sort('month'))
PY
