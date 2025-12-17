# Old Options (5Y) Filename Naming + Expiry Mapping Audit
Generated: `2025-12-14T16:04:00`
## Inputs
- Raw options dir: `data/raw/options`
- Expiry calendar: `newer data stocks/config/expiry_calendar.csv`

## Outputs
- CSV mapping: `newer data stocks/docs/reports/old_options_filename_expiry_mapping.csv`

## Naming conventions (observed)
| Pattern | Example | Meaning |
|---|---|---|
| `numeric` | `banknifty2392148300ce` | `UNDERLYING` + `YY` + `M` + `DD` + `STRIKE` + `CE/PE` (month is 1 digit) |
| `ond_short` | `banknifty22n1038700pe` | `UNDERLYING` + `YY` + (`o`/`n`/`d`) + `DD` + `STRIKE` + `CE/PE` (Oct/Nov/Dec) |
| `monthname` | `banknifty24nov52100pe` | `UNDERLYING` + `YY` + `MON(3 letters)` + `STRIKE` + `CE/PE` (treated as monthly contract-month) |

## Counts
- Total files processed: **85,278**
- By underlying: {'BANKNIFTY': 44260, 'NIFTY': 41018}
- By pattern: {'numeric': 48171, 'monthname': 22122, 'ond_short': 14985}
- By resolved expiry_type: {'weekly': 62688, 'monthly': 22590}
- By mapping_source: {'final_direct': 62480, 'monthname_monthly': 22122, 'scheduled_to_final': 278, 'as_is': 398}

## Examples (by pattern)
- `numeric`: `banknifty1941128500ce.parquet`, `banknifty1941128500pe.parquet`, `banknifty1941128600ce.parquet`, `banknifty1941128600pe.parquet`, `banknifty1941128700ce.parquet`, `banknifty1941128700pe.parquet`, `banknifty1941128800ce.parquet`, `banknifty1941128800pe.parquet`, `banknifty1941128900ce.parquet`, `banknifty1941128900pe.parquet`
- `ond_short`: `banknifty19d0530100ce.parquet`, `banknifty19d0530100pe.parquet`, `banknifty19d0530200ce.parquet`, `banknifty19d0530200pe.parquet`, `banknifty19d0530300ce.parquet`, `banknifty19d0530300pe.parquet`, `banknifty19d0530400ce.parquet`, `banknifty19d0530400pe.parquet`, `banknifty19d0530500ce.parquet`, `banknifty19d0530500pe.parquet`
- `monthname`: `banknifty19apr28300ce.parquet`, `banknifty19apr28300pe.parquet`, `banknifty19apr28400ce.parquet`, `banknifty19apr28400pe.parquet`, `banknifty19apr28500ce.parquet`, `banknifty19apr28500pe.parquet`, `banknifty19apr28600ce.parquet`, `banknifty19apr28600pe.parquet`, `banknifty19apr28700ce.parquet`, `banknifty19apr28700pe.parquet`

## Scheduled→Final shifts (holiday/week move cases)
These are files where the filename encodes a scheduled date, but the calendar maps it to a different final expiry date.

| Underlying | Filename | Scheduled (from name) | Final (from calendar) |
|---|---|---:|---:|
| BANKNIFTY | `banknifty21n0436300ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436300pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436400ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436400pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436500ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436500pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436600ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436600pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436700ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436700pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436800ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436800pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436900ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0436900pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437000ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437000pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437100ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437100pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437200ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437200pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437300ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437300pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437400ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437400pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437500ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437500pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437600ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437600pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437700ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437700pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437800ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437800pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437900ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0437900pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0438000ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0438000pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0438100ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0438100pe.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0438200ce.parquet` | 2021-11-04 | 2021-11-03 |
| BANKNIFTY | `banknifty21n0438200pe.parquet` | 2021-11-04 | 2021-11-03 |

## Most common resolved expiries (top 30 by file count)
| Underlying | Final expiry | Files |
|---|---:|---:|
| BANKNIFTY | 2025-04-30 | 376 |
| BANKNIFTY | 2025-05-28 | 376 |
| BANKNIFTY | 2025-06-25 | 366 |
| BANKNIFTY | 2025-01-29 | 302 |
| BANKNIFTY | 2025-02-25 | 302 |
| BANKNIFTY | 2025-03-26 | 302 |
| BANKNIFTY | 2025-07-30 | 302 |
| BANKNIFTY | 2024-07-31 | 282 |
| BANKNIFTY | 2024-08-28 | 282 |
| NIFTY | 2024-12-26 | 281 |
| BANKNIFTY | 2024-07-03 | 280 |
| NIFTY | 2025-06-26 | 280 |
| NIFTY | 2024-08-29 | 266 |
| BANKNIFTY | 2024-06-26 | 264 |
| NIFTY | 2025-05-29 | 260 |
| NIFTY | 2024-07-25 | 258 |
| BANKNIFTY | 2024-10-30 | 256 |
| NIFTY | 2024-11-28 | 256 |
| BANKNIFTY | 2024-09-25 | 250 |
| BANKNIFTY | 2024-12-24 | 250 |
| BANKNIFTY | 2024-11-27 | 248 |
| NIFTY | 2025-03-27 | 243 |
| NIFTY | 2025-05-15 | 242 |
| BANKNIFTY | 2024-04-24 | 240 |
| BANKNIFTY | 2024-06-12 | 240 |
| BANKNIFTY | 2024-06-19 | 240 |
| BANKNIFTY | 2024-07-10 | 240 |
| BANKNIFTY | 2024-10-01 | 238 |
| BANKNIFTY | 2024-10-09 | 238 |
| BANKNIFTY | 2024-10-16 | 238 |

## Notes
- `monthname` files do **not** include a day token; they are mapped to the **monthly** final expiry for that contract month using the calendar.
- `numeric`/`ond_short` files include a day token which is treated as the scheduled expiry date; the calendar maps it to the final expiry date when shifted.
