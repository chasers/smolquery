# `bench/planner.exs` — scan DuckLake, or plan around it?

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `890b5a1` |
| Command | `mix run bench/planner.exs` (defaults: `SEGMENTS=300`, `ROWS=20000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |

## Headline

**Planning around DuckLake only pays once the snapshot's metadata is cached, and
only when the query is selective.** Cold, the planner loses to a native scan at
every selectivity. Cached, it wins 6× on a one-file query and 1.8× at 10%, and
falls back to par on a full scan — where there was nothing to prune anyway.

## Fixture

300 segments × 20,000 rows = 6,000,000 rows, snapshot 3.

## The metadata side, priced

```
  ducklake_list_files (300 files) : 3.4 ms
  min-max for every file from metadata   : 11.8 ms (300 files)
```

~15 ms to learn everything needed to prune 300 files. That is the number the
snapshot cache amortizes, and it is why the cold planner loses.

## Native scan vs planner, by selectivity

```
selectivity: 1 of 300
  A native                   min     5.2 ms  median     5.6 ms     1 files
  B planner, cold metadata   min     6.8 ms  median     7.3 ms     1 files
  C planner, snapshot-cached min     0.9 ms  median     0.9 ms     1 files
  of 300 segments, and all three agree: true

selectivity: 10%
  A native                   min     7.4 ms  median     8.1 ms    30 files
  B planner, cold metadata   min    11.7 ms  median    12.1 ms    30 files
  C planner, snapshot-cached min     4.0 ms  median     4.4 ms    30 files
  of 300 segments, and all three agree: true

selectivity: all
  A native                   min    25.3 ms  median    27.4 ms   300 files
  B planner, cold metadata   min    38.2 ms  median    40.4 ms   300 files
  C planner, snapshot-cached min    28.3 ms  median    29.8 ms   300 files
  of 300 segments, and all three agree: true
```

`files` is from `EXPLAIN ANALYZE`'s `Total Files Read`, not wall clock — all three
paths prune to the same file count at every selectivity, so the comparison is
honest about work done rather than just time taken.

## What this settles

- **The planner needs a snapshot-scoped metadata cache to be worth having.**
  Cold, it adds 30–50% to every query; cached, it is a 6× win where pruning
  matters most. The cache is not an optimization on top of the planner — it is
  the planner's precondition.
- **DuckDB prunes correctly on its own.** Native and planned agree on file counts
  at all three selectivities, so the planner is not buying *correctness* of
  pruning. It buys knowing the file list before the scan — which is what a
  distributed fan-out needs and a local scan does not.
- **Full scans should skip the planner.** At `selectivity: all`, planning is pure
  overhead (cached path ~2 ms slower than native). Cheap enough not to special-case
  yet, but the shape to remember when the union planner lands (T-27).
