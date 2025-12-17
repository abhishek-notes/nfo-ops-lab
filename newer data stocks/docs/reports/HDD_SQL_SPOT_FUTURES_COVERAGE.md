# HDD SQL Coverage Check (Spot + Futures)

This is a quick verification of the SQL dumps you mounted under `/Volumes/*` for **spot** and **futures**.

All of these dumps use `REPLACE INTO` statements (not `INSERT INTO`).

## Inputs

| Series | Path | SQL Table |
|---|---|---|
| BANKNIFTY spot | `/Volumes/Abhishek-HD/BNF NF/BNF/banknifty.sql.gz` | `banknifty` |
| NIFTY spot | `/Volumes/Abhishek 5T/Raw SQL Stocks Rajesh Data/NF/nifty.sql.gz` | `nifty` |
| BANKNIFTY futures | `/Volumes/Abhishek-HD/BNF NF/BNF/bankniftyfut.sql.gz` | `bankniftyfut` |
| NIFTY futures | `/Volumes/Abhishek 5T/Raw SQL Stocks Rajesh Data/NF/niftyfut.sql.gz` | `niftyfut` |

## Coverage results (validated by scanning the full files)

All results below exclude the small number of bogus timestamps `< year 2000` (these appear as `1970-01-01 05:30:00`).

| Series | Rows (valid) | Unique dates | Min timestamp | Max timestamp | Invalid rows (<2000) |
|---|---:|---:|---:|---:|---:|
| BANKNIFTY spot | 28,578,942 | 1,496 | 2019-04-08 09:15:00 | 2025-07-31 15:34:59 | 1 |
| NIFTY spot | 28,096,358 | 1,495 | 2019-04-08 09:15:00 | 2025-07-31 15:34:59 | 1 |
| BANKNIFTY futures | 22,580,621 | 1,342 | 2019-03-07 09:15:00 | 2025-07-30 15:30:00 | 2 |
| NIFTY futures | 23,266,764 | 1,349 | 2019-03-07 09:15:00 | 2025-07-31 15:30:01 | 2 |

## Notes

- Since these are `REPLACE INTO` dumps, the existing `INSERT`-based extractor `newer data stocks/scripts/spot_extraction/extract_spot_data.py` is not applicable to these files.
- A compatible extractor for these dumps was added at `newer data stocks/scripts/spot_extraction/extract_price_series_from_replace_sql.py`.

