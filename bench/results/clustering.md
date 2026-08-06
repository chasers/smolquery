# `bench/clustering.exs` — does the ORDER BY analog work, and what does it cost

| | |
|---|---|
| Run | 2026-08-06 |
| Commit | `4d4e1aa` (branch `obk/order-by-key`) |
| Command | `mix run bench/clustering.exs` (defaults: `P=1000`, `R=500000`, `MICRO=16`, `BATCH=10000`, `REPS=7`) |
| Machine | Apple M1 Pro · 10 cores · 16 GiB · macOS 26.5.1 |
| Runtime | Elixir 1.20.2 / OTP 28.3 · 10 online schedulers · DuckDB threads 10 · `memory_limit=2GB` |

> **OTP 28.3.** These numbers are not comparable to published OTP 29 bench
> results in this directory.

Same row stream on both arms: zipf-ish `project_id` over 1000 projects, 500 000
rows, ~200 B payload, 16 micro-segments × 31 250 rows. `clustered` has
`clustering: ["project_id", "ts"]`; `plain` has `[]`.

## Headline

**Correctness PASS.** Flushed micro-segments and sealed segments are
`(project_id, ts)` nondecreasing with nulls last.

**Write cost +28.4%** on flush rows/s (clustered slower). Seal merge is *not*
slower — clustered sealed faster here (61.7 ms vs 91.5 ms) and slightly smaller
(0.983× plain bytes).

**RAM:** BEAM heap of the writer process is **~0%** over plain for the flush
sort (`Enum.sort_by` reorders existing row-map references; Explorer holds the
frame off-heap). OS RSS peak during seal is **+190.4 MiB** for clustered vs
plain (1205.7 vs 1015.3 MiB), under `memory_limit=2GB`.

**Read win 1.8×** on sealed `WHERE project_id = 0` (selecting payload): 15.8 ms
→ 8.8 ms p50. Row-group evidence: clustered seals into 31 groups and only **1**
matches the predicate; plain has 5 groups and all 5 match.

## A1. Correctness

```
  PASS  flushed micro-segment is (project_id, ts) nondecreasing, nulls last
  PASS  sealed segment is (project_id, ts) nondecreasing, nulls last
```

## C. RAM — BEAM heap delta on Writer.write

50 000-row flush-sized batch, peak sampled on the calling process during
`Writer.write`:

```
  arm            rows   peak heap Δ (MiB)
  plain           50000               0.0
  clustered       50000               0.0

  clustered heap over plain: 0.0%
```

## B. Write-path cost — flush through buffer commit

```
  16 micro-segments × 31250 rows (one write_batch = one flush)

  arm         flushes     rows      rows/s    p50 ms    p95 ms
  plain            16    500000    453352.7      62.8     113.5
  clustered        16    500000    324577.5      94.7     118.1

  flush rows/s delta (clustered vs plain): 28.4%
```

## A2. Pruning effect — hot tier

```
  predicate: WHERE project_id = 0
  arm         files   match RG   rows ret   p50 ms
  plain          16        16     15780      4.8
  clustered      16        16     15780      4.9
```

File-level min/max is identical for the same row sets, so both arms read all 16
files. Sorting inside a micro-segment does not tighten per-file stats.

## B+C. Seal merge + OS RSS; A3 sealed pruning

```
  arm         merge ms     rows/s   sealed MiB   RSS Δ MiB   RSS peak MiB
  plain           91.5   5465197.6         3.9       224.5        1015.3
  clustered       61.7   8101233.0         3.9       188.9        1205.7

  sealed bytes clustered/plain = 0.983
  memory_limit in effect: 2GB

  sealed WHERE project_id = 0 (select payload — forces page reads)
  arm         row groups   match RG   explain rows   p50 ms   last100 p50
  plain                5         5         15780     15.8          3.9
  clustered           31         1         15780      8.8          2.9
```

**EXPLAIN ANALYZE caveat:** DuckDB's plan reports TABLE_SCAN output cardinality
(post-filter), not rows scanned before prune — both arms show `15780` explain
rows. Pruning evidence is the parquet_metadata `match RG` column (1 vs 5) plus
latency. Fallback is intentional, not faked precision.

## What this settles

- **Correctness proven** — sort at flush and `ORDER BY` at seal both keep
  `(project_id, ts)` order with nulls last (A1).
- **Write cost ~28%** on the buffer flush path; seal merge pays for itself in
  wall time and bytes on this fixture.
- **RAM cost** — BEAM ~0% for the flush sort; seal `ORDER BY` shows up as
  **~190 MiB** higher OS RSS peak than plain (DuckDB, under a 2 GB
  `memory_limit`).
- **Read win ~1.8×** on sealed point-lookup of a hot project; row-group
  pruning is real (`match RG` 1 vs 5). Hot-tier file pruning is a no-op when
  both arms hold the same per-segment row sets — the sealed tier is where the
  clustering key earns its keep.
