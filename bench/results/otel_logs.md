# `bench/otel_logs.exs` — OpenTelemetry logs, end to end

| | |
|---|---|
| Run | 2026-08-04 |
| Commit | `29bb13d` (PL-20: one-pass validation T-151, pipelined commit T-152) |
| Command | `mix run bench/otel_logs.exs 2>/dev/null` (defaults: `WRITERS=4,8,16,32`, `BATCH=2000`, `TABLES=1`, `SECONDS=10`, `TAIL_SECONDS=10`, `SUSTAINED_SECONDS=30`, `TAIL_INTERVAL_MS=1000`, `REPS=5`, buffer `flush_interval_ms: 1000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |
| Parallelism | 10 logical processors · 10 schedulers online · 10 dirty-CPU · 10 dirty-IO · 10 DuckDB threads per engine |

Same workload as the 2026-08-03 baseline (`d3b12cb`): 61-column OTel log
records (~2.1 KB of JSON each, 4.2 MiB per 2,000-row batch) through
`POST /v1/datasets/logs/tables/otel_logs/insert`, tailed with
`SELECT * … ORDER BY timestamp DESC LIMIT 100` through `POST /v1/queries`.
The driver shares the machine with the node it measures.

## Headline, and the delta against 2026-08-03

**One table now takes ~42.5k wide log records/s through the HTTP API (baseline
38.4k), no longer degrades at 32 writers (41.7k against the baseline's 33.3k
droop), and spreading 16 writers over 4 tables reaches 66–73k rows/s on 8.7 of
10 cores — the node ceiling the baseline could not buy at any table count
(57.1k, 4.8 cores).** Zero 429s anywhere; p95 down ~17% at the peak.

| | baseline `d3b12cb` | PL-20 `29bb13d` | delta |
|---|---|---|---|
| 1 table, 16 writers | 38,426 rows/s | 42,476 | +11% |
| 1 table, 32 writers | 33,298 (falling) | 41,676 (flat) | +25% |
| 16 writers over 4 tables | 55,130 | 73,109 | +33% |
| node peak, any tables | 57,149 (8 tables, 4.8 cores) | 73,109 (4 tables, 8.7 cores) | +28% |
| validate stage, 2,000 rows | 54.9 ms | 39.1 ms | 1.40× |
| p95 at 16 writers | 959.1 ms | 798.4 ms | −17% |

Two changes did this (PL-20 steps 1–2):

- **T-151** — the validator coerces in one pass (it ran `value_from_json/2`
  twice per valid value) and checks unknown columns against a `MapSet`
  (it was O(columns²) list membership per row).
- **T-152** — `TableBuffer` no longer encodes inline. A linked `Committer`
  owns the encode, manifest log, and replication round, and every flush
  trigger hands off immediately, so `flush_max_bytes`-sized commits run
  back to back while the buffer keeps accepting. What used to wait
  uncounted in the buffer's mailbox now waits in the committer's FIFO —
  the same rows one hop later, but the table keeps accumulating through
  every one of them.

Two designs were tried and rejected on this bench before the shipped one, and
the 429-reason readout added to the driver is what caught both: counting
in-flight bytes against admission halved effective capacity (429 storms at 16
writers, and every refused request had already paid decode + validate);
depth-1 pipelining acked writers in convoys of one huge commit (one huge
handoff copy, one long single-threaded encode — 32k rows/s ceiling).

## Phase 0 — stage profile: one 2,000-row batch (4.2 MiB of JSON), median of 5

```
  stage                                    ms   krows/s   share
  JSON decode (Phoenix parser)           25.7      77.8     26%
  validate + coerce (per row)            39.1      51.2     39%
  rows → Arrow → Parquet + fsync         35.6      56.2     35%
  insert path, one core                 100.4      19.9    100%

  Parquet decode → Arrow                  0.5    3766.5      1%
  Arrow → rows (DataFrame.to_rows)       29.0      69.0     29%
```

Validation dropped from 54.9 to 39.1 ms (across this session's runs it
measured 38.6–46.0; the write stage is the noisy one, 18.7–35.6 ms
run-to-run). Validation is still the largest per-row stage — the remaining
cost is rows × columns of `Map.get` + dispatch + rebuilding each row map —
which is the T-139 columnar-validator argument, unchanged.

## Phase 1 — ingest ceiling (writers sweep, 10s each, no reader)

```
    writers    rows/s   MiB/s   req/s      p50      p95      p99      max   429s    prep   cores   sched%
          4     29120    61.9      15    230.6    286.0    315.0    315.6      0    36.5    3.15     18.0
          8     40360    85.7      20    355.8    398.2    416.3    489.1      0    39.1    4.33     24.4
         16     42476    90.2      21    689.6    798.4    877.1   1011.3      0    42.6    4.61     26.4
         32     41676    88.5      21   1397.3   1591.0   1827.1   1896.2      0    59.7     4.6     27.2
```

The knee moved from 8 to ~16 writers and the curve no longer bends down:
baseline lost 13% going 16 → 32 writers; now it loses 2% (noise). The
single-table ceiling is still a serialization ceiling — 4.6 of 10 cores —
but it sits ~11% higher because the buffer accepts while the committer
encodes, and the encode itself is pipelined against the fsync it follows.

## Phase 1b — one table or the node? (`TABLES` sweep)

Separate invocations, offered load held constant at 16 writers:
`TABLES=n WRITERS=16 SECONDS=8 REPS=1 mix run bench/otel_logs.exs 2>/dev/null`

```
  tables    rows/s   MiB/s   req/s      p50      p95      p99      max   429s    prep   cores   sched%
       1     43584    92.6      22    634.9    930.4   1045.4   1080.9      0    45.1    4.68     27.4
       2     63069   134.0      32    436.9    549.7    709.4    764.1      0    49.3    7.51     43.5
       4     73109   155.3      37    371.5    423.5    581.7    663.6      0    59.3    8.71     57.0
       8     66000   140.2      33    409.1    535.3    554.3    581.6      0    63.2    8.29     58.7
```

This is the run that shows what the pipeline unlocked. Baseline: 8 tables
bought 1.46× and the node never used more than 4.8 cores. Now: 2 tables buy
1.45×, 4 buy 1.68×, and the machine runs at 8.7 of 10 cores — the committer
processes encode concurrently across tables, so partitioning finally
parallelizes the stage that used to serialize inside each buffer. The dip at
8 tables is the driver and node competing for the same 10 cores (`prep`
63 ms, sched 59%). PL-6's partitioned writes should now be re-benched: with
the encode off the buffer's critical path, partitions multiply a bigger
number than they did on 2026-08-03.

## Phase 2 — tail floor (no ingest, 10s, last 100)

```
  1626000 rows × 61 columns in otel_logs, 23 unsealed micro-segments a tail reads (407 in the manifest, the rest retired inside their grace period)
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            27    189.5    206.6    243.4    243.4   100-100     4104.4
  filtered       23    217.0    234.5    242.1    242.1   100-100     9207.1
```

189 ms against the baseline's 234, mostly because this run's tail read 23
unsealed micro-segments where the baseline's read 54 — sealing kept up
better behind the faster commits. Same structural findings as baseline: the
filtered tail is slower and staler, and tail latency is a readout of hot
depth.

## Phase 3 — sustained: 21,238 rows/s offered, tail every 1000 ms, 30s

```
     offered  achieved      p50      p95      p99      max   429s   late   cores   sched%
       21238     21226    358.9    515.0    566.8    629.0      0      0    3.34     15.3
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            30    294.6    401.3    414.6    414.6   100-100     1014.9
  2266000 rows × 61 columns in otel_logs, 55 unsealed micro-segments a tail reads (567 in the manifest, the rest retired inside their grace period)
```

Half the (higher) ceiling sustained with a live tail: insert p50 359 ms
(baseline 419 at a lower rate), freshness 1.01 s p50 (baseline 1.12), no
drift across 30 seconds, nothing shed, no tick late. The freshness floor is
still the flush interval plus the query, as it should be.

## Node counters (`GET /metrics`)

```
  buffer_commits_total                     567
  buffer_rows_committed_total              2266000
  buffer_admission_refused_rows_total      -
  seal_attempts_total                      8
  ingest_rows_rejected_total               -
  query_jobs_total                         82
```

2.27M rows in 567 commits — 4,000 rows a commit, the same group-commit
merging as baseline, now without stopping the accumulator to run each one.

## What this settles

- **Pipelining the commit was worth +11% on one table and +28% on the node,
  and it un-bent the concurrency curve.** The moduledoc's "inline until a
  benchmark says otherwise" clause is retired; `bench/results/otel_logs.md`
  (2026-08-03) was that benchmark, and this run is the answer.
- **The `TABLES` multiplier roughly doubled** (1.46× → 1.68×, and reached at
  4 tables instead of 8), because the encode now runs in per-table committer
  processes instead of serializing inside each buffer. PL-6/T-57 partitioned
  writes attack a bigger prize than the baseline priced.
- **Validation is still the largest per-row stage (39%)** even after the
  one-pass rewrite. The remaining cost is structural — rows × columns of
  Elixir term work — and lands with T-139's frames-end-to-end path, which
  these numbers re-price but do not change.
- **Admission semantics are preserved exactly**: the byte bound measures the
  accumulator, outstanding rows wait in the committer FIFO the way they used
  to wait in the mailbox, and nothing was shed at any writer count.
- **Two rejected designs are documented in the git history** (in-flight
  admission accounting; depth-1 convoys) — both looked plausible and both
  lost to this bench. The driver now prints distinct 429 reasons so the next
  regression names itself.
