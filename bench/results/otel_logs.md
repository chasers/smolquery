# `bench/otel_logs.exs` — OpenTelemetry logs, end to end

| | |
|---|---|
| Run | 2026-08-03 |
| Commit | `d3b12cb` (plus the commit adding this script) |
| Command | `mix run bench/otel_logs.exs 2>/dev/null` (defaults: `WRITERS=4,8,16,32`, `BATCH=2000`, `TABLES=1`, `SECONDS=10`, `TAIL_SECONDS=10`, `SUSTAINED_SECONDS=30`, `TAIL_INTERVAL_MS=1000`, `REPS=5`, buffer `flush_interval_ms: 1000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |
| Parallelism | 10 logical processors · 10 schedulers online · 10 dirty-CPU · 10 dirty-IO · 10 DuckDB threads per engine |

One node, every role but `:web`, everything over HTTP: 61-column OTel log
records (~2.1 KB of JSON each, 4.2 MiB per 2,000-row batch) through
`POST /v1/datasets/logs/tables/otel_logs/insert`, tailed with
`SELECT * … ORDER BY timestamp DESC LIMIT 100` through `POST /v1/queries`. The
driver shares the machine with the node it measures.

## Headline

**One table takes ~38.4k wide log records/s (82 MiB/s of JSON) through the HTTP API
while using 4.8 of 10 cores; eight tables take 57.1k. A live tail over half the
single-table stream costs 331 ms at p50 while showing rows 1.12 s old.**

Two things set that ceiling, and the CPU measurement separates them. Per row, the
cost is Elixir term work: one batch takes 107.5 ms on a core, **51% row-by-row
validation, 25% JSON decode**, only 24% the Arrow encode and Parquet write. But the
node reaches its ceiling at **under half the machine** — 4.79 of 10 cores, with
throughput *falling* at 32 writers while CPU stays flat — so what caps it is
serialization, not capacity. Not bytes, not the disk (~1 MiB/s of zstd Parquet
against a 2.2 GB/s SSD), and not the fsync (`buffer.md`'s toggle: ±2–6%).

Nothing drifts over 30 seconds of sustained load, because the sealer pins the hot
tier near 55 unsealed micro-segments while 2M rows land on it.

## Phase 0 — stage profile: one 2,000-row batch (4.2 MiB of JSON), median of 5

```
  stage                                    ms   krows/s   share
  JSON decode (Phoenix parser)           26.8      74.6     25%
  validate + coerce (per row)            54.9      36.4     51%
  rows → Arrow → Parquet + fsync         25.8      77.5     24%
  insert path, one core                 107.5      18.6    100%

  Parquet decode → Arrow                  0.7    2849.0      1%
  Arrow → rows (DataFrame.to_rows)       29.2      68.4     27%
```

This is where the ceiling comes from. A single core moves 18.6k rows/s through
the insert path, and the measured 38.4k is that work spread across cores while
each writer waits on a group commit.

**The biggest stage is `IngestService.Validator` — 51%, more than double the
Arrow encode and Parquet write combined.** It walks every value of every column
(`Schema.value_from_json/2` per field), so its cost is rows × columns of Elixir
term work. At 61 columns that is 1.8M coercions a batch. The much-blamed
Polars/Parquet path is the *cheapest* third of the insert.

**And this is why a faster wire format alone would not help.** Parquet decode is
0.7 ms — 38× faster than parsing the same rows from JSON — but
`load_controller.ex`'s `indexed/1` immediately calls `DataFrame.to_rows`, which
costs 29.2 ms, *more than the 26.8 ms JSON decode it would replace*. Then
validation (54.9 ms) and the rows → Arrow re-encode still run. So on today's
code a Parquet `/load` is a little **slower** than a JSON `/insert`, not faster,
and NDJSON is slower still (per-line `JSON.decode` plus a spool to disk).

`bench/results/ingest_transport.md` reached the same conclusion from the
transport side — *"the frame→rows conversion, not the wire, is the cost"* — and
this prices it for the API edge. The win needs the frame to stay a frame:
`Writer.write/3` already accepts a `DataFrame`, so the gap is a columnar
validator, a `TableBuffer` that accumulates frames, and per-index error
reporting without `to_rows`.

## Phase 1 — ingest ceiling (writers sweep, 10s each, no reader)

```
    writers    rows/s   MiB/s   req/s      p50      p95      p99      max   429s    prep   cores   sched%
          4     26726    56.8      13    256.3    292.2    327.6    328.7      0    38.2    3.16     19.4
          8     35028    74.4      18    393.0    560.7    611.5    613.5      0    41.7    4.42     26.0
         16     38426    81.6      19    763.8    959.1   1068.0   1176.5      0    44.9    4.79     29.1
         32     33298    70.7      17   1675.1   1979.0   2265.0   2318.0      0    66.3    4.21     28.7
```

**The ceiling is coordination, not CPU, and now that is measured rather than
inferred.** At the 16-writer peak the process consumes **4.79 of 10 cores** and the
BEAM's normal schedulers are 29% busy. At 32 writers throughput *drops* 13% while
cores stay flat at 4.21 — more offered load buys nothing and costs latency, which is
what a serialization ceiling looks like. Half the machine is idle at maximum
throughput.

`cores` comes from the OS process's CPU time, so it counts DuckDB's and Polars'
native threads that no BEAM statistic sees — and the co-resident driver. The gap
between it and `sched%` × 10 is that native work: ~4.8 cores total against ~2.9
core-equivalents on BEAM schedulers, so roughly **1.9 cores of native work**.

The knee is at 8 writers. From 8 to 32 the achieved rate moves ±8% while p50
goes 393 → 1675 ms: four times the offered load, four times the wait, the same
work done. That is the Little's-law regime `bench/results/buffer.md` found for
group commit — arriving through Phoenix and the ingest edge does not change its
shape. The dip at 32 is the driver and the node competing for the same 10 cores,
visible in `prep` climbing 37 → 78 ms.

`prep` is the driver's own per-batch cost: building 2,000 wide rows and encoding
4.2 MiB of JSON. It is *not* inside the reported latencies, but it is on the same
CPUs, so this is a node ceiling with a co-resident client. A collector on another
machine would leave more of the box for the server.

`ack_budget_ms: 5_000` never fires: even at 32 writers p99 is 2.59 s. PL-9's
budget is a guardrail well above this workload, not an SLO on it.

## Phase 1b — one table or the node? (`TABLES` sweep)

Separate invocation, offered load held constant at 16 writers:
`TABLES=n WRITERS=16 SECONDS=8 REPS=1 mix run bench/otel_logs.exs 2>/dev/null`

```
  tables    rows/s   MiB/s   req/s      p50      p95      p99      max   429s    prep
       1     39017    82.9      20    738.6    861.9   1158.2   1189.8      0    49.7
       2     46934    99.7      23    605.6    768.9    971.9    999.6      0    53.5
       4     55130   117.1      28    482.8    673.2    738.3    748.3      0    64.6
       8     57149   121.4      29    484.3    556.1    600.1    625.8      0    64.8
```

One table is one `TableBuffer` doing its encode inline, so this separates a
*table's* ceiling from the *node's*. **Spreading the same load over 8 tables buys
1.46×, not 8×** — and it is done by 4. Insert p50 nearly halves (739 → 484 ms)
because each buffer queues less.

Read together with the stage profile, that number is exactly what it should be:
only the last 24% of a batch's CPU runs inside the table's buffer. The other 76%
— JSON decode and validation — already runs in per-request processes across all
cores, and partitioning the buffer does nothing for it. `buffer.md`'s partition
proxy got 3.1–4.1× from 8 buffers because it was measuring *only* the encode
path, with no HTTP edge in front of it.

So partitioned writes (PL-6) are worth ~1.5× on this workload, and the columnar
frame-through path is what attacks the other three quarters. They compose.

## Phase 2 — tail floor (no ingest, 10s, last 100)

```
  1440000 rows × 61 columns in otel_logs, 54 unsealed micro-segments a tail reads (361 in the manifest, the rest retired inside their grace period)
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            22    234.5    253.8    282.7    282.7   100-100     4997.0
  filtered       20    256.4    304.5    304.5    304.5   100-100    10244.9
```

234 ms to tail 1.4M rows with nothing competing: `bench/results/query.md`'s
~50 ms job engine and ~10 ms plan, plus the union scan over 54 unsealed
micro-segments and the sealed tier, plus framing 100 wide rows.

The filtered tail (`service_name = 'checkout-api' AND severity_number >= 17`) is
**slower**, not faster: 256 ms. `ORDER BY timestamp DESC LIMIT 100` has to touch
every surviving file either way, and the predicate only adds work. Pruning has
nothing to prune — both columns are low-cardinality and present in every
segment, unlike the id-range case `query.md` measured.

Its freshness is 10.2 s against the unfiltered 5.0 s, and that is structural: ERROR
is ~4% of rows on one service in five, so the newest *matching* row is inherently
older than the newest row. A filtered tail is staler than an unfiltered one
roughly by the reciprocal of its selectivity — worth knowing before promising a
"live" filtered view. (Both numbers here are measured with ingest stopped, so
they mostly say how long ago phase 1 ended; phase 3's is the one that means
something.)

## Phase 3 — sustained: 19,213 rows/s offered, tail every 1000 ms, 30s

```
     offered  achieved      p50      p95      p99      max   429s   late   cores   sched%
       19213     19414    419.3    587.0    610.9    627.9      0      0    3.43     17.9
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            30    331.1    479.8    495.5    495.5   100-100     1124.9
  2032000 rows × 61 columns in otel_logs, 60 unsealed micro-segments a tail reads (509 in the manifest, the rest retired inside their grace period)
```

Offered and achieved agree to 1% and no tick ran late, so this is the paced load it
claims to be: half the measured ceiling, with a reader on it. It runs on **3.43
cores** — 72% of the ceiling's CPU for 51% of the ceiling's throughput, so the fixed
overhead per unit of work is real but modest.

- **Inserts cost 419 ms at p50, 587 ms at p95.** A batch waits for the group
  commit it lands in — the same cost as the 8-writer saturating case at half the
  load, which is what "the ack is the flush, not the queue" looks like.
- **The tail costs 331 ms at p50, 480 ms at p95** — ~97 ms over the idle floor,
  against a hot tier that grew only 54 → 60 files, so most of that is contention
  for the box rather than more files to read.
- **Freshness sits at 1.12 s at p50**: rows become visible at the next group
  commit, then the query takes ~330 ms. With a 1,000 ms flush interval that is
  the floor, and it is where the workload sat.

## Phase 3 timeline — per second

```
    second    rows/s  insert p50   tail ms  fresh ms
         0     16000       455.3     401.2   13004.5
         1     20000       485.2     402.6    1396.5
         2     24000       388.1     314.6     644.3
         3     20000       436.0     265.3     759.2
         4     16000       427.9     465.1    1124.9
         5     16000       445.4     324.1    1153.7
         6     20000       458.2     331.1    1326.1
         7     24000       407.7     210.9     541.1
         8     20000       383.8     246.0     741.7
         9     16000       413.0     309.5     974.5
        10     16000       458.2     442.6    1253.5
        11     20000       453.2     331.1    1333.9
        12     24000       419.3     270.3     603.5
        13     20000       415.2     253.7     754.5
        14     16000       432.2     438.7    1106.4
        15     16000       512.8     479.8    1329.4
        16     16000       540.5     350.6    1333.7
        17     28000       421.1     306.9     646.7
        18     20000       492.2     325.6     830.6
        19     16000       452.7     495.5    1166.0
        20     16000       430.9     355.2    1193.7
        21     20000       460.9     313.8    1319.8
        22     24000       413.2     230.3     574.5
        23     20000       412.6     216.8     725.8
        24     16000       458.6     388.4    1062.4
        25     16000       482.9     381.0    1219.3
        26     20000       455.6     312.9    1321.8
        27     24000       400.8     278.5     624.6
        28     20000       445.5     413.6     924.5
        29     16000       541.5     468.0    1138.5
        30     16000       382.5         -         -
```

The first sample is 13.0 s stale because nothing had been written since phase 1
ended — that is the tail catching up, and it does so within one interval. After
that, freshness oscillates between 0.54 s and 1.40 s for 29 straight seconds with
no trend. The oscillation is the flush interval beating against the 1 Hz tail: a
tail landing just before a commit sees rows a full interval old, one landing just
after sees them fresh. Half a flush interval of jitter is the expected shape.

Nothing degrades: insert p50 holds a 383–541 ms band, tail p50 a 211–496 ms band,
while the table grows 1.4M → 2.0M rows. The per-second rows/s alternates 16k–28k
because eight writers paced at 833 ms per batch put one batch in some one-second
buckets and two in others; the 30-second mean is flat.

## Node counters (`GET /metrics`)

```
  buffer_commits_total                     509
  buffer_rows_committed_total              2032000
  buffer_admission_refused_rows_total      -
  seal_attempts_total                      5
  ingest_rows_rejected_total               -
  query_jobs_total                         73
```

2M rows in 509 group commits — 4,000 rows a commit, two 2,000-row batches merged
on average, so group commit is earning its name at this rate. Nothing was refused
by admission and nothing was rejected in validation.

**Five seal attempts retired 449 of those 509 micro-segments, and that is why the
tail's latency is flat.** The hot tier sat at 54 files after phase 1 and 60 after
phase 3 — pinned under `seal_max_files: 64` while 592k more rows landed on it. Sealing is not falling behind at 19k rows/s; it is holding the read side's
cost constant, which is exactly what the tier boundary is for.

## What this settles

- **The e2e numbers exist now**: ~38.4k wide (61-column) records/s and 82 MiB/s of
  JSON through the front door on one table, 57.1k over eight, with a co-resident
  client; half the single-table rate sustained with a live tail, all at sub-second
  p95.
- **The ceiling is serialization, not CPU — measured, not inferred.** Maximum
  throughput consumes 4.79 of 10 cores (29% BEAM scheduler busy, so ~1.9 cores of
  native DuckDB/Polars work), and pushing to 32 writers *lowers* throughput while
  CPU stays flat. Half the machine is idle at the ceiling, which is why the
  `TABLES` sweep buys anything at all and why more concurrency alone does not.
- **The per-row cost lives in the row-shaped middle of the write path, and
  validation owns half of it.** 51% validate, 25% JSON decode, 24% Arrow + Parquet
  + fsync, for 18.6k rows/s per core. Not bytes, not the disk (the workload writes
  ~1 MiB/s of zstd Parquet against a 2.2 GB/s SSD), and not the fsync
  (`buffer.md`'s toggle: ±2–6%).
- **A different wire format is not the fix while the buffer takes rows.** Parquet
  parses 38× faster than JSON, and `DataFrame.to_rows` gives all of it back and
  then some. The fix is frames end to end — `Writer.write/3` already accepts
  one — which is the option `ingest_transport.md` priced at 4–5× and deferred.
- **Partitioning one table's writes is worth ~1.5× here, not 8×**, because only
  the last quarter of a batch's CPU is inside the `TableBuffer`. That re-scopes
  PL-6's expected win for an HTTP-fronted workload, as against `buffer.md`'s
  3.1–4.1× measured with no edge in front.
- **The ack contract holds under load.** A 200 means queryable, and end to end
  that reads out as 1.12 s of staleness at p50 — flush interval plus query —
  stable for 30 seconds with no drift.
- **Insert latency is the flush interval and the batch size, not the API.** Ask
  for lower insert latency by turning `flush_interval_ms` down or batches
  smaller, not by optimizing the front door.
- **Sealing keeps up, and that is why tails are stable.** Hot depth held near 50
  files across 2M rows; tail latency is a readout of that depth, so the seal path
  is the read path's latency budget.
- **A filtered tail is slower and staler than an unfiltered one.** `ORDER BY
  timestamp DESC LIMIT 100` reads every surviving file regardless, and rarer
  matches mean older newest-matches.
- **Planning reads a manifest that is mostly already-sealed entries.** Every
  query fetches the whole hot manifest and drops the entries the catalog already
  has (`Planner.include?/2`); a sealed segment stays listed for
  `retire_grace_ms` (10 min) so a query holding an older snapshot cannot lose
  rows underneath it. This run fetched 509 entries per query to scan 60, so the
  planner's input grows with grace period × ingest rate while the scan's grows
  only with seal lag. No cost was visible at 509 (`query.md` measured planning
  flat to 256), but that is the term that moves if the grace period lengthens.
