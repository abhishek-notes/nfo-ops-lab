# Optimization Guide (Chat-Derived)

This guide consolidates all performance and robustness optimizations referenced across the chats and codebase. Each item includes purpose, implementation notes, and where to apply it in this repo.

Counts
- Optimizations cataloged: 36

Storage & I/O
1) Columnar storage (Parquet + ZSTD/Snappy)
- Purpose: 3–10x smaller than CSV/row; fast column reads.
- Apply: All packers write Parquet with `compression="zstd"`.

2) Predicate pushdown (Polars/DuckDB)
- Purpose: Load only the day/hour range and needed columns.
- Apply: `pl.scan_parquet(...).filter(...).select(...)` in backtests.

3) Column projection
- Purpose: Read `timestamp, close, vol_delta` only for options.
- Apply: `.select(["timestamp","close","vol_delta"])`.

4) Manifest-driven pruning
- Purpose: Skip nonexistent strikes/expiries quickly.
- Apply: Use `meta/packed_manifest.csv` for pre-checks.

5) Per-second pre-aggregation cache
- Purpose: Compute per-second once per (strike,type,day).
- Apply: `option_day_seconds(...)` reused across anchors; write cache if repeat runs.

6) Streaming archives (7z -so / ratarmount)
- Purpose: Process >100 GB datasets with limited disk.
- Apply: In ingestion/conversion jobs; see chat notes.

7) RAM disk staging (optional)
- Purpose: 1.3–1.8x speedup for transient intermediates.
- Apply: On SSD-limited systems while converting.

8) Memory-mapped arrays (numpy.memmap)
- Purpose: Treat huge arrays as memory without loading fully.
- Apply: Indicator-heavy pipelines outside Polars.

Compute
9) Polars over pandas for large scans/joins
- Purpose: Columnar, parallel, predictable memory profile.
- Apply: Backtests and packers already use Polars.

10) Multiprocessing per partition/day
- Purpose: Scale across CPU cores; isolate memory.
- Apply: Strategy runners that iterate many days.

11) Numba JIT for hot loops
- Purpose: 10–100x speedups for numeric loops.
- Apply: PnL loops, rolling calcs where not trivial in Polars.

12) Avoid Python in hot path
- Purpose: Reduce GIL contention.
- Apply: Move math to Polars/NumPy/Numba; keep Python as orchestration.

13) Vectorization / window ops
- Purpose: Eliminate Python loops.
- Apply: Rolling sums/means for burst features.

14) Precomputed indices (dense 1s grid)
- Purpose: O(1) joins, exact-second matches for anchors.
- Apply: `dense_1s_index` in backtests.

15) Timezone/timeunit consistency
- Purpose: Prevent filter/join errors; enable pushdown.
- Apply: Cast literals to `Datetime("ns", Asia/Kolkata)` consistently.

16) Minimal allocations
- Purpose: Reduce GC and mem spikes.
- Apply: Reuse frames; avoid unnecessary `.collect()`.

Infra / Latency
17) AWS Mumbai + placement groups + ENA SR-IOV
- Purpose: 1–2 ms savings in network stack.
- Apply: Live execution infra.

18) Broker colo / exchange colo
- Purpose: Reduce RTT from 5–7 ms → 0.5–1 ms / <1 ms.
- Apply: Only for latency-critical strategies.

19) TLS session ticket reuse & warm pools
- Purpose: Faster order handshakes.
- Apply: Execution layer.

20) Lock-free queues / ring buffers
- Purpose: Lower intra-process latency.
- Apply: Feed → strategy pipeline in live.

21) Kernel-bypass (Onload/DPDK) [advanced]
- Purpose: Shave 1–2 ms network stack.
- Apply: C/Rust feed handlers.

Robustness / Data Hygiene
22) Calendar: final expiry mapping
- Purpose: Avoid misaligned trades.
- Apply: Packers + backtests use `join_asof(..., strategy="forward")`.

23) OHLC repairs
- Purpose: Vendor zeros/NaNs fixed; enforce bounds.
- Apply: `ensure_ohlc` in packers.

24) Volume deltas via diff().clip(0)
- Purpose: Cumulative volume resets; ensure non-negative deltas.
- Apply: Packers compute `vol_delta` correctly.

25) Timestamps: IST attach (no shift)
- Purpose: Preserve wall times; align datasets.
- Apply: `replace_time_zone("Asia/Kolkata")` in packers.

26) Session-hour filtering
- Purpose: Exclude off-market noise.
- Apply: 09:15–15:30 windows enforced.

27) Unique-by-timestamp de-dup
- Purpose: Remove duplicates post-merge.
- Apply: `.unique(["timestamp"]).sort("timestamp")` before write.

28) Partitioning standards
- Purpose: Stable paths by symbol/date/expiry/strike.
- Apply: As documented in the wiki.

Backtest-Specific
29) Candidate reduction (ATM ±1 / anchor-based)
- Purpose: Load only relevant strikes per hour.
- Apply: `pick_strikes_for_day` in `atm_volume_ultra.py`.

30) One-time per-day spot grid
- Purpose: Reuse across anchors and series.
- Apply: `SpotStore.load_day_seconds` + `dense_1s_index`.

31) Early casting for pushdown
- Purpose: Ensure predicates push to scan level.
- Apply: Cast `timestamp` and literals before `.filter(...)`.

32) Burst baseline as rate × window
- Purpose: Comparable units; avoid bias.
- Apply: `base_30s = rolling_mean(avg_secs) * burst_secs`.

33) Anchor price caching
- Purpose: Avoid re-filtering for each entry.
- Apply: `entry_px_at[(strike,type,anchor)]` map.

Conversion / ETL
34) Pre-extract gz once; avoid archive seeks
- Purpose: Remove repeated I/O.
- Apply: Use include lists; `7z -mmt=on`.

35) Stream parse → DuckDB → Parquet
- Purpose: Bypass DB deadlocks; faster than SQL import.
- Apply: See chat-derived pipeline in notes.

36) Multi-worker execution (6–8)
- Purpose: Near-linear speedup up to SSD/CPU limits.
- Apply: GNU parallel / Python multiprocessing.

