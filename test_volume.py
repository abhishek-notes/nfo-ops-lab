import polars as pl

# Check an options file for volume data
df = pl.read_parquet('data/packed/options/BANKNIFTY/202311/exp=2023-11-22/type=CE/strike=44400.parquet')
print(f'Shape: {df.shape}')
print('Vol delta stats:')
non_zero = (df['vol_delta'] > 0).sum()
print(f'  Non-zero count: {non_zero}')
print(f'  Max: {df["vol_delta"].max()}')
print(f'  Sum: {df["vol_delta"].sum()}')
print()
print('Sample with non-zero volume:')
print(df.filter(pl.col('vol_delta') > 0).head(5))