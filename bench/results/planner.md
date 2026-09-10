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

## Follow-up: the preview over a deep hot tier (T-449, 2026-09-10)

The sandbox's query pods OOMKilled in a loop, about fifteen seconds after a
browser opened the table page of `supabase_staging.system_logs`, whose buffer
nodes held a wedged seal backlog of thousands of micro-segments. The page's
preview is `SELECT * FROM t LIMIT 50`. Two costs scaled with the hot file
count: the planner fetched every entry with its 63-column stats block (T-328:
~7.6 KB per entry against ~0.4 KB without) and decoded the lot in the BEAM,
and the hot read unioned every file by name, so DuckDB opened all of them
before the LIMIT could stop it.

Measured locally through `Smolquery.QueryService.Client` against a real
DuckLake (sqlite) and a canned manifest server holding 8,000 micro-segments of
20 rows × 10 columns each, every entry padded to 63 columns of stats (49 MB of
manifest JSON, served pre-encoded so the server costs the VM nothing). Peaks
sampled every 20 ms; Linux aarch64, Elixir 1.20 / OTP 29, DuckDB via ADBC.

```
SELECT * FROM analytics.events LIMIT 50       wall ms   BEAM peak   RSS peak   hot files read
  before  plan only                             2,518    +792 MiB     +98 MiB   8,000 listed
  before  full query                            6,375    +674 MiB    +662 MiB   8,000
  after   plan only                               314     +17 MiB     +14 MiB   3 listed
  after   full query                              166      +8 MiB      +0 MiB   3
```

Two changes, both in the planner: a plan whose statement has no WHERE
conjunct and no Top-N bound for a table fetches that table's manifest as
`GET …?stats=false` (`Smolquery.BufferService.HotClient`, honoured by
`HotServer`'s GET route); and an unordered, unfiltered constant `LIMIT n`
(`Smolquery.QueryService.AnyN`) hands DuckDB only the newest micro-segments
whose row counts cover what the sealed tier's row count at the snapshot does
not. At 3,000 files the same preview took 3.7 s before; at 500, 0.9 s.

What this does not cover: a `WHERE` still fetches every entry with stats and
hands DuckDB every surviving file, which is T-328's remaining scope — scoping
the planner's fetch by ids or by the pruner's needs.
