from __future__ import annotations
import argparse, os, glob, sys
import polars as pl
from datetime import datetime
from .io import read_option_file
from .calendar import read_calendar, attach_expiry
from .features import seconds_bars_ohlc, add_micro_features, gate_market_hours
from .futures import read_futures_parquet
from .spot import read_spot_glob
import duckdb, yaml

def cmd_pack(args):
    os.makedirs(args.out, exist_ok=True)
    cal = read_calendar(args.calendar) if args.calendar else None
    files = sorted(glob.glob(os.path.join(args.in_dir, "*.parquet")))
    if not files:
        print(f"No files in {args.in_dir}")
        return
    for i, fp in enumerate(files, 1):
        df = read_option_file(fp)
        if cal is not None:
            df = attach_expiry(df, cal)
        # partition path: symbol/YYYYMM/expiry/opt_type
        df = df.sort("timestamp")
        if "expiry" in df.columns:
            yyyymm = df["timestamp"].dt.strftime("%Y%m").to_list()[0]
            exp = df["expiry"].dt.strftime("%Y-%m-%d").to_list()[0] if df["expiry"].null_count()==0 else "unknown"
        else:
            yyyymm = df["timestamp"].dt.strftime("%Y%m").to_list()[0]
            exp="unknown"
        sym = df["symbol"].to_list()[0]
        typ = df["opt_type"].to_list()[0]
        strike = df["strike"].to_list()[0]
        out_dir = os.path.join(args.out, sym, yyyymm, f"exp={exp}", f"type={typ}")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"strike={strike}.parquet")
        df.write_parquet(out_path, compression="zstd", compression_level=args.zstd, statistics=True, use_pyarrow=True)
        if i % 100 == 0:
            print(f"Packed {i}/{len(files)}...")

def cmd_make_duckdb(args):
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    con = duckdb.connect(args.db)
    # map packed parquet (if exists)
    base = "./data/packed/options"
    if os.path.exists(base):
        con.execute(f"CREATE OR REPLACE VIEW options_packed AS SELECT * FROM read_parquet('{base}/**/*.parquet');")
    # futures
    if os.path.exists("./data/raw/futures/banknifty_futures.parquet"):
        con.execute("CREATE OR REPLACE VIEW fut_bank AS SELECT * FROM read_parquet('./data/raw/futures/banknifty_futures.parquet');")
    if os.path.exists("./data/raw/futures/nifty_futures.parquet"):
        con.execute("CREATE OR REPLACE VIEW fut_nifty AS SELECT * FROM read_parquet('./data/raw/futures/nifty_futures.parquet');")
    # spot
    if os.path.exists("./data/raw/spot"):
        con.execute("CREATE OR REPLACE VIEW spot_all AS SELECT * FROM read_parquet('./data/raw/spot/*.parquet');")
    con.close()
    print(f"DuckDB at {args.db} ready.")

def _iter_packed_strikes(packed_root: str, symbol: str | None):
    pat = os.path.join(packed_root, symbol if symbol else "*", "*", "exp=*", "type=*", "strike=*.parquet")
    for fp in glob.glob(pat):
        yield fp

def cmd_features(args):
    cal = read_calendar(args.calendar) if args.calendar else None
    os.makedirs(args.out, exist_ok=True)
    count=0
    for fp in _iter_packed_strikes(args.packed, args.symbol):
        df = pl.read_parquet(fp, use_pyarrow=True)
        if cal is not None and "expiry" not in df.columns:
            df = attach_expiry(df, cal)
        # keep only market hours (optional drop pre-open seconds)
        df = df.sort("timestamp")
        df = gate_market_hours(df, "09:15:00", "15:30:00")
        bars = seconds_bars_ohlc(df)
        feats = add_micro_features(bars)
        # attach identifiers from path
        parts = fp.split(os.sep)
        symbol = parts[-5]
        expiry = parts[-3].split("=",1)[1]
        opt_type = parts[-2].split("=",1)[1]
        strike = int(parts[-1].split("=",1)[1].split(".")[0])
        feats = feats.with_columns([
            pl.lit(symbol).alias("symbol"),
            pl.lit(opt_type).alias("opt_type"),
            pl.lit(strike).alias("strike"),
            pl.lit(expiry).str.strptime(pl.Date).alias("expiry")
        ]).select(["symbol","opt_type","strike","expiry","timestamp","open","high","low","close","vol","vol_15s","vol_30s","vol_ratio_15_over_30","r1s","ema_15s","ema_30s"])
        # write
        out_dir = os.path.join(args.out, symbol, expiry, opt_type)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{strike}.parquet")
        feats.write_parquet(out_path, compression="zstd", compression_level=args.zstd, statistics=True, use_pyarrow=True)
        count+=1
        if count % 200 == 0:
            print(f"Features for {count} strikes...")
    print(f"Features written for {count} strikes.")

def cmd_backtest(args):
    from .backtest import vectorized_scalping_backtest
    # load features for symbol & date range
    paths = glob.glob(os.path.join(args.features, args.symbol, "*", "*", "*.parquet"))
    frames=[]
    for p in paths:
        df = pl.read_parquet(p, use_pyarrow=True)
        if args.from_date or args.to_date:
            if args.from_date:
                df = df.filter(pl.col("timestamp")>=pl.datetime(int(args.from_date[:4]), int(args.from_date[5:7]), int(args.from_date[8:10]), 0,0,0))
            if args.to_date:
                df = df.filter(pl.col("timestamp")<=pl.datetime(int(args.to_date[:4]), int(args.to_date[5:7]), int(args.to_date[8:10]), 23,59,59))
        frames.append(df)
    if not frames:
        print("No features found. Did you run 'features'?")
        return
    X = pl.concat(frames, how="vertical_relaxed").sort(["expiry","opt_type","strike","timestamp"])
    res = vectorized_scalping_backtest(
        X,
        entry_rule=args.entry,
        sl_pct=args.sl, tp_pct=args.tp, trail_pct=args.trail
    )
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    res.write_csv(args.out)
    print(f"Backtest rows: {len(res):,}. Saved to {args.out}")

def build_parser():
    p = argparse.ArgumentParser(prog="nfoops")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("pack", help="pack raw options parquet -> standardized partitioned parquet")
    sp.add_argument("--in", dest="in_dir", required=True)
    sp.add_argument("--out", dest="out", required=True)
    sp.add_argument("--calendar", required=False)
    sp.add_argument("--zstd", type=int, default=3)
    sp.set_defaults(func=cmd_pack)

    sd = sub.add_parser("make-duckdb", help="create hot DuckDB cache and external parquet views")
    sd.add_argument("--db", required=True)
    sd.set_defaults(func=cmd_make_duckdb)

    sf = sub.add_parser("features", help="build 1s bars and micro features")
    sf.add_argument("--packed", required=True)
    sf.add_argument("--out", required=True)
    sf.add_argument("--calendar", required=False)
    sf.add_argument("--symbol", required=False)
    sf.add_argument("--zstd", type=int, default=3)
    sf.add_argument("--from", dest="from_date", required=False)
    sf.add_argument("--to", dest="to_date", required=False)
    sf.set_defaults(func=cmd_features)

    sb = sub.add_parser("backtest", help="vectorized 1s scalper backtest")
    sb.add_argument("--features", required=True)
    sb.add_argument("--symbol", required=True, choices=["BANKNIFTY","NIFTY"])
    sb.add_argument("--entry", required=True)
    sb.add_argument("--sl", type=float, default=0.5)
    sb.add_argument("--tp", type=float, default=1.0)
    sb.add_argument("--trail", type=float, default=0.4)
    sb.add_argument("--from", dest="from_date", required=False)
    sb.add_argument("--to", dest="to_date", required=False)
    sb.add_argument("--out", required=True)
    sb.set_defaults(func=cmd_backtest)

    return p

def main():
    args = build_parser().parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
