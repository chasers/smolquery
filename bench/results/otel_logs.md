# `bench/otel_logs.exs` — OpenTelemetry logs, end to end

| | |
|---|---|
| Run | 2026-08-03 |
| Commit | `d3b12cb` (plus the commit adding this script) |
| Command | `mix run bench/otel_logs.exs 2>/dev/null` (defaults: `WRITERS=4,8,16,32`, `BATCH=2000`, `TABLES=1`, `SECONDS=10`, `TAIL_SECONDS=10`, `SUSTAINED_SECONDS=30`, `TAIL_INTERVAL_MS=1000`, `REPS=5`, buffer `flush_interval_ms: 1000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |

One node, every role but `:web`, everything over HTTP: 61-column OTel log
records (~2.1 KB of JSON each, 4.2 MiB per 2,000-row batch) through
`POST /v1/datasets/logs/tables/otel_logs/insert`, tailed with
`SELECT * … ORDER BY timestamp DESC LIMIT 100` through `POST /v1/queries`. The
driver shares the machine with the node it measures.

## Headline

**One table takes ~37.7k wide log records/s (80 MiB/s of JSON) through the HTTP
API; eight tables take 57.1k. A live tail over half the single-table stream
costs 331 ms at p50 while showing rows 1.15 s old.** The ceiling is per-row
Elixir CPU, not bytes and not disk: one batch costs 107.5 ms on a core, of which
**51% is row-by-row validation and 25% is JSON decode** — only 24% is the Arrow
encode and Parquet write. Nothing drifts over 30 seconds of sustained load,
because the sealer pins the hot tier at ~50 unsealed micro-segments while 2M
rows land on it.

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
the insert path, and the measured 37.7k is that work spread across cores while
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
    writers    rows/s   MiB/s   req/s      p50      p95      p99      max   429s    prep
          4     26292    55.9      13    258.4    313.7    394.3    395.6      0    37.0
          8     35946    76.4      18    387.5    496.6    564.2    566.3      0    42.4
         16     37665    80.0      19    786.9    916.5   1058.2   1062.4      0    48.2
         32     36371    77.3      18   1635.4   2145.4   2588.7   2610.1      0    77.5
```

The knee is at 8 writers. From 8 to 32 the achieved rate moves ±4% while p50
goes 388 → 1635 ms: four times the offered load, four times the wait, the same
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
  1438000 rows × 61 columns in otel_logs, 55 unsealed micro-segments a tail reads (360 in the manifest, the rest retired inside their grace period)
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            21    231.3    285.9    366.7    366.7   100-100     3957.4
  filtered       20    250.8    301.6    301.6    301.6   100-100     9194.4
```

231 ms to tail 1.4M rows with nothing competing: `bench/results/query.md`'s
~50 ms job engine and ~10 ms plan, plus the union scan over 55 unsealed
micro-segments and the sealed tier, plus framing 100 wide rows.

The filtered tail (`service_name = 'checkout-api' AND severity_number >= 17`) is
**slower**, not faster: 251 ms. `ORDER BY timestamp DESC LIMIT 100` has to touch
every surviving file either way, and the predicate only adds work. Pruning has
nothing to prune — both columns are low-cardinality and present in every
segment, unlike the id-range case `query.md` measured.

Its freshness is 9.2 s against the unfiltered 4.0 s, and that is structural: ERROR
is ~4% of rows on one service in five, so the newest *matching* row is inherently
older than the newest row. A filtered tail is staler than an unfiltered one
roughly by the reciprocal of its selectivity — worth knowing before promising a
"live" filtered view. (Both numbers here are measured with ingest stopped, so
they mostly say how long ago phase 1 ended; phase 3's is the one that means
something.)

## Phase 3 — sustained: 18,832 rows/s offered, tail every 1000 ms, 30s

```
     offered  achieved      p50      p95      p99      max   429s   late
       18832     19010    407.8    571.5    644.1    647.9      0      0
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            30    331.0    470.5    513.9    513.9   100-100     1153.0
  2014000 rows × 61 columns in otel_logs, 48 unsealed micro-segments a tail reads (504 in the manifest, the rest retired inside their grace period)
```

Offered and achieved agree to 0.9% and no tick ran late, so this is the paced
load it claims to be: half the measured ceiling, with a reader on it.

- **Inserts cost 408 ms at p50, 572 ms at p95.** A batch waits for the group
  commit it lands in — the same cost as the 8-writer saturating case at half the
  load, which is what "the ack is the flush, not the queue" looks like.
- **The tail costs 331 ms at p50, 471 ms at p95** — 100 ms over the idle floor.
  Hot depth is *lower* than in phase 2 (48 vs 55 files), so that is contention
  for the box, not more files to read.
- **Freshness sits at 1.15 s at p50**: rows become visible at the next group
  commit, then the query takes ~330 ms. With a 1,000 ms flush interval that is
  the floor, and it is where the workload sat.

## Phase 3 timeline — per second

```
    second    rows/s  insert p50   tail ms  fresh ms
         0     16000       443.7     385.6   11904.1
         1     16000       461.3     323.5    1319.2
         2     24000       377.8     290.9    1433.2
         3     24000       424.4     275.9     721.4
         4     16000       391.6     358.7     952.3
         5     16000       493.8     470.5    1203.3
         6     16000       413.8     256.1    1153.0
         7     16000       460.2     216.7    1262.8
         8     28000       377.1     238.9     585.5
         9     20000       400.2     199.4     695.6
        10     16000       423.0     232.2     880.4
        11     16000       414.8     331.0    1124.0
        12     16000       465.7     306.0    1249.0
        13     20000       369.9     245.8    1344.8
        14     28000       392.9     299.8     698.9
        15     16000       392.2     268.6     817.4
        16     16000       446.4     435.6    1136.3
        17     16000       458.2     396.3    1247.3
        18     16000       503.1     341.4    1343.5
        19     24000       424.9     335.7    1486.9
        20     20000       435.5     360.5     811.7
        21     20000       501.7     513.9    1116.3
        22     16000       485.9     370.6    1104.2
        23     16000       490.3     339.7    1240.7
        24     20000       433.9     275.5    1332.8
        25     24000       451.4     275.0     631.2
        26     20000       450.1     228.7     734.9
        27     16000       481.4     390.3    1054.2
        28     16000       464.9     391.5    1205.1
        29     16000       542.1     405.8    1359.5
        30     16000       423.8         -         -
```

The first sample is 11.9 s stale because nothing had been written since phase 1
ended — that is the tail catching up, and it does so within one interval. After
that, freshness oscillates between 0.59 s and 1.49 s for 29 straight seconds with
no trend. The oscillation is the flush interval beating against the 1 Hz tail: a
tail landing just before a commit sees rows a full interval old, one landing just
after sees them fresh. Half a flush interval of jitter is the expected shape.

Nothing degrades: insert p50 holds a 370–542 ms band, tail p50 a 199–514 ms band,
while the table grows 1.4M → 2.0M rows. The per-second rows/s alternates 16k/24k
because eight writers paced at 850 ms per batch put one batch in some one-second
buckets and two in others; the 30-second mean is flat.

## Node counters (`GET /metrics`)

```
  buffer_commits_total                     504
  buffer_rows_committed_total              2014000
  buffer_admission_refused_rows_total      -
  seal_attempts_total                      5
  ingest_rows_rejected_total               -
  query_jobs_total                         73
```

2M rows in 504 group commits — 4,000 rows a commit, two 2,000-row batches merged
on average, so group commit is earning its name at this rate. Nothing was refused
by admission and nothing was rejected in validation.

**Five seal attempts retired 456 of those 504 micro-segments, and that is why the
tail's latency is flat.** The hot tier sat at 55 files after phase 1 and 48 after
phase 3 — pinned well under `seal_max_files: 64` while 576k more rows landed on
it. Sealing is not falling behind at 19k rows/s; it is holding the read side's
cost constant, which is exactly what the tier boundary is for.

## What this settles

- **The e2e numbers exist now**: ~37.7k wide (61-column) records/s and 80 MiB/s
  of JSON through the front door on one table, 57.1k over eight, with a
  co-resident client; half the single-table rate sustained with a live tail, all
  at sub-second p95.
- **The bottleneck is per-row Elixir CPU in the row-shaped middle of the write
  path, and validation owns half of it.** 51% validate, 25% JSON decode, 24%
  Arrow + Parquet + fsync, for 18.6k rows/s per core. Not bytes, not the disk
  (the workload writes ~1 MiB/s of zstd Parquet against a 2.2 GB/s SSD), and not
  the fsync (`buffer.md`'s toggle: ±2–6%).
- **A different wire format is not the fix while the buffer takes rows.** Parquet
  parses 38× faster than JSON, and `DataFrame.to_rows` gives all of it back and
  then some. The fix is frames end to end — `Writer.write/3` already accepts
  one — which is the option `ingest_transport.md` priced at 4–5× and deferred.
- **Partitioning one table's writes is worth ~1.5× here, not 8×**, because only
  the last quarter of a batch's CPU is inside the `TableBuffer`. That re-scopes
  PL-6's expected win for an HTTP-fronted workload, as against `buffer.md`'s
  3.1–4.1× measured with no edge in front.
- **The ack contract holds under load.** A 200 means queryable, and end to end
  that reads out as 1.15 s of staleness at p50 — flush interval plus query —
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
  rows underneath it. This run fetched 504 entries per query to scan 48, so the
  planner's input grows with grace period × ingest rate while the scan's grows
  only with seal lag. No cost was visible at 504 (`query.md` measured planning
  flat to 256), but that is the term that moves if the grace period lengthens.
