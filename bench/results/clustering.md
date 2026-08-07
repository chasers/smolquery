# `bench/clustering.exs` — does the ORDER BY analog work, and what does it cost

| | |
|---|---|
| Run | 2026-08-06 |
| Commit | `feat/order-by-review` (stacked on PR #96 `c52e7db`) |
| Command | `mix run bench/clustering.exs` (defaults: `P=1000`, `R=500000`, `MICRO=16`, `BATCH=10000`, `REPS=7`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29.0.2 · 10 online schedulers · DuckDB threads 10 · `memory_limit=2GB` |

> **Supersedes the PR #96 run (OTP 28.3).** Two things changed. The flush sort
> moved from `Enum.sort_by` over row maps to Polars sorting the built frame —
> Erlang term order compares `NaiveDateTime` structs field-alphabetically
> (`:day` before `:month`), so the old sort was not chronological across month
> boundaries or with nonzero microseconds. And the bench's own `assert_sorted!`
> comparator shared that term ordering, so A1 was verifying the wrong
> invariant; it now compares with `NaiveDateTime.compare/2` and the fixture
> crosses the Jan→Feb boundary with microsecond jitter. The Polars sort is also
> why the write cost fell from the previously reported +28.4% to +6.6%.

Same row stream on both arms: zipf-ish `project_id` over 1000 projects, 500 000
rows, ~200 B payload, 16 micro-segments × 31 250 rows. `clustered` has
`clustering: ["project_id", "ts"]`; `plain` has `[]`.

## Headline

**Correctness PASS.** Flushed micro-segments and sealed segments are
`(project_id, ts)` nondecreasing with nulls last — now checked chronologically,
across a month boundary, with sub-second timestamps.

**Write cost +6.6%** on flush rows/s (clustered slower). Seal merge is *not*
slower — clustered sealed faster here (79.2 ms vs 105.6 ms) and slightly
smaller (0.957× plain bytes).

**RAM:** BEAM heap of the writer process is **~0%** over plain for the flush
sort (Polars sorts the frame off-heap; row maps are never reordered on the
BEAM). OS RSS peak during seal is **+179.1 MiB** for clustered vs plain
(1329.0 vs 1149.9 MiB), under `memory_limit=2GB`.

**Read win 1.67×** on sealed `WHERE project_id = 0` (selecting payload):
14.0 ms → 8.4 ms p50. Row-group evidence: clustered seals into 31 groups and
only **1** matches the predicate; plain has 5 groups and all 5 match.

## A1. Correctness

```
  PASS  flushed micro-segment is (project_id, ts) nondecreasing, nulls last
  PASS  sealed segment is (project_id, ts) nondecreasing, nulls last
```

## C. RAM — BEAM heap delta on Writer.write

50 000-row flush-sized batch, peak sampled on the calling process during
`Writer.write`:

```
  arm            rows   peak heap Δ (MiB)
  plain           50000               0.0
  clustered       50000               0.0

  clustered heap over plain: -4.2%
```

## B. Write-path cost — flush through buffer commit

```
  16 micro-segments × 31250 rows (one write_batch = one flush)

  arm         flushes     rows      rows/s    p50 ms    p95 ms
  plain            16    500000    482058.7      61.4      94.2
  clustered        16    500000    450426.5      59.9     126.4

  flush rows/s delta (clustered vs plain): 6.6%
```

## A2. Pruning effect — hot tier

```
  predicate: WHERE project_id = 0
  arm         files   match RG   rows ret   p50 ms
  plain          16        16     15780      5.4
  clustered      16        16     15780      5.4
```

File-level min/max is identical for the same row sets, so both arms read all 16
files. Sorting inside a micro-segment does not tighten per-file stats.

## B+C. Seal merge + OS RSS; A3 sealed pruning

```
  arm         merge ms     rows/s   sealed MiB   RSS Δ MiB   RSS peak MiB
  plain          105.6   4737001.7         4.2       219.6        1149.9
  clustered       79.2   6312095.2         4.1       178.4        1329.0

  sealed bytes clustered/plain = 0.957
  memory_limit in effect: 2GB

  sealed WHERE project_id = 0 (select payload — forces page reads)
  arm         row groups   match RG   explain rows   p50 ms   last100 p50
  plain                5         5         15780     14.0          3.6
  clustered           31         1         15780      8.4          2.9
```

**EXPLAIN ANALYZE caveat:** DuckDB's plan reports TABLE_SCAN output cardinality
(post-filter), not rows scanned before prune — both arms show `15780` explain
rows. Pruning evidence is the parquet_metadata `match RG` column (1 vs 5) plus
latency. Fallback is intentional, not faked precision.

## What this settles

- **Correctness proven, chronologically** — sort at flush and `ORDER BY` at
  seal both keep `(project_id, ts)` order with nulls last, across month
  boundaries and microsecond-resolution timestamps (A1).
- **Write cost ~7%** on the buffer flush path once Polars does the sorting;
  seal merge pays for itself in wall time and bytes on this fixture.
- **RAM cost** — BEAM ~0% for the flush sort; seal `ORDER BY` shows up as
  **~180 MiB** higher OS RSS peak than plain (DuckDB, under a 2 GB
  `memory_limit`).
- **Read win ~1.7×** on sealed point-lookup of a hot project; row-group
  pruning is real (`match RG` 1 vs 5). Hot-tier file pruning is a no-op when
  both arms hold the same per-segment row sets — the sealed tier is where the
  clustering key earns its keep.
