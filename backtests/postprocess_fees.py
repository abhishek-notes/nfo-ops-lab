#!/usr/bin/env python3
from __future__ import annotations
import argparse, glob, json
from pathlib import Path
import polars as pl

# Fee model (India F&O; no brokerage)
FEE = {
    'stt_rate': 0.0010,      # 0.10% on sell value
    'exch_rate': 0.00035,    # 0.035% on turnover
    'ipft_rate': 0.000005,   # 0.0005% on turnover
    'sebi_rate': 0.000001,   # 0.0001% on turnover
    'stamp_rate': 0.00003,   # 0.003% on buy value
    'gst_rate': 0.18,        # 18% of (exch+ipft+sebi+clearing+brokerage)
}

def compute_fees(df: pl.DataFrame, symbol: str, lot_size: int) -> pl.DataFrame:
    cols = df.columns
    buy_col = None; sell_col = None
    for a,b in [("buy_premium","sell_premium"),("entry_opt","exit_opt"),("buy","sell"),("prem_in","prem_out")]:
        if a in cols and b in cols:
            buy_col, sell_col = a, b; break
    if not buy_col:
        return df.with_columns([
            pl.lit(None).alias("turnover"),
            pl.lit(None).alias("fees_total"),
            pl.lit(None).alias("net_rupees"),
            pl.lit(None).alias("net_points"),
        ])
    qty = lot_size
    buyv = pl.col(buy_col).abs() * qty
    sellv = pl.col(sell_col).abs() * qty
    turnover = buyv + sellv
    stt = sellv * FEE['stt_rate']
    exch = turnover * FEE['exch_rate']
    ipft = turnover * FEE['ipft_rate']
    sebi = turnover * FEE['sebi_rate']
    stamp = buyv * FEE['stamp_rate']
    gst = (exch + ipft + sebi) * FEE['gst_rate']
    fees = stt + exch + ipft + sebi + stamp + gst
    if 'pnl' in cols:
        gross_rupees = pl.col('pnl')
    elif 'pnl_pts' in cols:
        gross_rupees = pl.col('pnl_pts') * qty
    else:
        gross_rupees = (pl.col(sell_col) - pl.col(buy_col)) * qty
    net_rupees = gross_rupees - fees
    net_points = pl.when((buyv+sellv) > 0).then(net_rupees / qty).otherwise(None)
    return df.with_columns([
        turnover.alias("turnover"),
        fees.alias("fees_total"),
        net_rupees.alias("net_rupees"),
        net_points.alias("net_points"),
    ])

def summarize_dir(symbol: str, lot_size: int, out_csv: Path):
    base = Path('backtests/results')
    patterns = [
        f"gamma_scalp_{symbol}_*.parquet",
        f"iv_rv_proxy_{symbol}_*.parquet",
        f"expiry_iv_crush_{symbol}_*.parquet",
        f"short_straddle_{symbol}_*.parquet",
        f"dispersion_proxy_{symbol}_*.parquet",
        f"oi_shift_{symbol}_*.parquet",
        f"orb_{symbol}_*.parquet",
        f"vwap_mr_{symbol}_*.parquet",
        f"iron_condor_{symbol}_*.parquet",
        f"momentum_scalp_{symbol}_*.parquet",
        f"momentum_short_strangle_ride_{symbol}_*.parquet",
    ]
    rows = []
    for pat in patterns:
        files = glob.glob(str(base/pat))
        if not files:
            continue
        for fp in files:
            try:
                df = pl.read_parquet(fp)
            except Exception:
                continue
            df2 = compute_fees(df, symbol, lot_size)
            year = None
            for c in ['trade_date','date','entry_ts','timestamp']:
                if c in df2.columns:
                    s = df2[c]
                    if s.dtype == pl.Utf8:
                        try:
                            s = pl.Series(s).str.strptime(pl.Date, strict=False)
                        except:
                            try:
                                s = pl.Series(s).str.strptime(pl.Datetime, strict=False)
                            except:
                                continue
                    if s.dtype == pl.Date:
                        year = s.dt.year(); break
                    if isinstance(s.dtype, pl.Datetime):
                        year = s.dt.year(); break
            if year is None:
                continue
            part = pl.DataFrame({
                'strategy': Path(fp).name.split('_')[0],
                'year': year,
                'gross_pts': df2.get('pnl_pts') if 'pnl_pts' in df2.columns else None,
                'gross_rupees': df2.get('pnl') if 'pnl' in df2.columns else None,
                'net_rupees': df2.get('net_rupees'),
                'net_points': df2.get('net_points'),
            })
            agg = part.group_by(['strategy','year']).agg([
                pl.len().alias('trades'),
                pl.col('gross_pts').sum().alias('gross_pts'),
                pl.col('gross_rupees').sum().alias('gross_rupees'),
                pl.col('net_rupees').sum().alias('net_rupees'),
                pl.col('net_points').sum().alias('net_points'),
            ])
            rows.append(agg)
    if not rows:
        return None
    out = pl.concat(rows, how='vertical_relaxed').group_by(['strategy','year']).agg([
        pl.col('trades').sum().alias('trades'),
        pl.col('gross_pts').sum().alias('gross_pts'),
        pl.col('gross_rupees').sum().alias('gross_rupees'),
        pl.col('net_rupees').sum().alias('net_rupees'),
        pl.col('net_points').sum().alias('net_points'),
    ]).sort(['strategy','year'])
    out.write_csv(str(out_csv))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--symbol', required=True, choices=['BANKNIFTY','NIFTY'])
    ap.add_argument('--lot-size', type=int, default=None)
    args = ap.parse_args()
    lot = args.lot_size if args.lot_size else (30 if args.symbol=='BANKNIFTY' else 75)
    out = Path('backtests/results')/f'strategy_summary_{args.symbol}.csv'
    res = summarize_dir(args.symbol, lot, out)
    if res is None:
        print('No result files found to summarize.')
    else:
        print('Wrote', out)

if __name__ == '__main__':
    main()
