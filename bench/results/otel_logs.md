# `bench/otel_logs.exs` — OpenTelemetry logs, end to end

| | |
|---|---|
| Run | 2026-08-03 |
| Commit | `d3b12cb` (plus the commit adding this script) |
| Command | `mix run bench/otel_logs.exs 2>/dev/null` (defaults: `WRITERS=4,8,16,32`, `BATCH=2000`, `SECONDS=10`, `TAIL_SECONDS=10`, `SUSTAINED_SECONDS=30`, `TAIL_INTERVAL_MS=1000`, buffer `flush_interval_ms: 1000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |

One node, every role but `:web`, everything over HTTP: 61-column OTel log
records (~2.4 MB of JSON per 2,000-row batch) through
`POST /v1/datasets/logs/tables/otel_logs/insert`, tailed with
`SELECT * … ORDER BY timestamp DESC LIMIT 100` through `POST /v1/queries`. The
driver shares the machine with the node it measures.

## Headline

**A single node takes ~38.9k wide log records/s (82.6 MiB/s of JSON) through the
HTTP API, and a live tail over half that stream costs 297 ms at p50 while
showing rows 1.16 s old.** Ingest saturates at 8 writers — past that, throughput
is flat and only latency grows, proportionally (16 writers double p50, 32
quadruple it), which is queueing and nothing else. The tail under load costs
63 ms more than tailing an idle node, and neither insert latency, tail latency,
nor freshness drifts over 30 seconds, because the sealer pins the hot tier at
~60 unsealed micro-segments while 2.1M rows land on it. The ack contract — a 200
means queryable — reads out end to end as flush interval plus query time.

## Phase 1 — ingest ceiling (writers sweep, 10s each, no reader)

```
    writers    rows/s   MiB/s   req/s      p50      p95      p99      max   429s    prep
          4     27336    58.1      14    252.2    274.6    350.1    350.8      0    36.2
          8     37626    79.9      19    376.6    473.6    545.7    546.9      0    39.7
         16     38870    82.6      19    759.1    881.4    982.6   1149.3      0    45.6
         32     35079    74.5      18   1518.4   1885.9   2166.6   2376.8      0    67.9
```

The knee is at 8 writers. From 8 to 32 the achieved rate moves ±4% while p50
goes 377 → 1518 ms: four times the offered load, four times the wait, the same
work done. That is the Little's-law regime `bench/results/buffer.md` found for
group commit — arriving through Phoenix and the ingest edge does not change its
shape. The small regression at 32 is the driver and the node competing for the
same 10 cores, visible in the `prep` column climbing 36 → 68 ms.

`prep` is the driver's own per-batch cost: building 2,000 wide rows and encoding
2.4 MB of JSON. It is *not* in the reported latencies, but it is on the same
CPUs, so this ceiling is a node's ceiling with a co-resident client. A collector
on another machine would leave more of the box for the server.

`ack_budget_ms: 5_000` never fires: even at 32 writers p99 is 2.17 s. PL-9's
budget is a guardrail well above this workload, not an SLO on it.

## Phase 2 — tail floor (no ingest, 10s, last 100)

```
  1508000 rows × 61 columns, 61 unsealed micro-segments a tail reads (378 in the manifest, the rest retired inside their grace period)
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            22    233.7    256.1    264.8    264.8   100-100     5172.8
  filtered       19    277.0    319.5    319.5    319.5   100-100    10201.3
```

234 ms to tail 1.5M rows, with nothing competing: `bench/results/query.md`'s
~50 ms job engine and ~10 ms plan, plus the union scan over 61 unsealed
micro-segments and the sealed tier, plus framing 100 wide rows. Tight
distribution — p99 is 13% over p50.

The manifest the planner fetches holds 378 entries even though only 61 are
readable; the rest are sealed and retired, waiting out `retire_grace_ms`. So
planning's *input* grows with the grace period while the scan's does not.
`query.md` measured planning flat to 256 hot entries; 378 (and 526 by the end of
phase 3) in front of it produced no visible cost here.

The filtered tail (`service_name = 'checkout-api' AND severity_number >= 17`) is
**slower**, not faster: 277 ms. `ORDER BY timestamp DESC LIMIT 100` has to touch
every surviving file either way, and the predicate only adds work. Pruning has
nothing to prune — both columns are low-cardinality and present in every
segment, unlike the id-range case `query.md` measured.

Its freshness is 10.2 s against the unfiltered 5.2 s, and that is structural,
not noise: ERROR is ~4% of rows on one service in five, so the newest *matching*
row is inherently older than the newest row. A filtered tail is staler than an
unfiltered one roughly by the reciprocal of its selectivity — worth knowing
before promising a "live" filtered view. (Both numbers here are measured with
ingest stopped, so they mostly say how long ago phase 1 ended; phase 3's is the
freshness that means something.)

## Phase 3 — sustained: 19,435 rows/s offered, tail every 1000 ms, 30s

```
     offered  achieved      p50      p95      p99      max   429s   late
       19435     19432    405.5    576.8    634.4    640.5      0      0
  shape       tails      p50      p95      p99      max      rows  fresh p50
  all            30    296.6    467.7    578.5    578.5   100-100     1158.9
  2100000 rows × 61 columns, 60 unsealed micro-segments a tail reads (526 in the manifest, the rest retired inside their grace period)
```

Offered and achieved agree to 0.02% and no tick ran late, so this is the paced
load it claims to be: half the measured ceiling, with a reader on it.

- **Inserts cost 406 ms at p50, 577 ms at p95.** A batch waits for the group
  commit it lands in — the same cost as the 8-writer saturating case, at half
  the load, which is what "the ack is the flush, not the queue" looks like.
- **The tail costs 297 ms at p50, 468 ms at p95** — 63 ms over the idle floor.
  The hot depth is the same (60 vs 61 files), so that 63 ms is contention for
  the box, not more files to read.
- **Freshness sits at 1.16 s at p50**: rows become visible at the next group
  commit, then the query takes ~300 ms. With a 1,000 ms flush interval that is
  the floor, and it is where the workload sat.

## Phase 3 timeline — per second

```
    second    rows/s  insert p50   tail ms  fresh ms
         0     16000       427.9     404.1   13219.2
         1     20000       449.9     365.6    1359.3
         2     28000       375.6     285.8     635.0
         3     16000       500.2     275.3     796.4
         4     16000       568.8     578.5    1276.6
         5     16000       449.6     279.7    1154.7
         6     24000       413.0     252.8    1305.2
         7     24000       436.1     204.5     611.7
         8     16000       451.3     217.5     800.4
         9     16000       479.4     399.8    1158.9
        10     20000       468.7     386.1    1328.9
        11     24000       373.6     232.3     521.5
        12     20000       389.2     245.1     712.1
        13     16000       435.8     441.1    1083.9
        14     16000       517.8     453.7    1272.8
        15     24000       358.3     296.6    1293.6
        16     24000       412.7     277.7     629.8
        17     16000       400.7     306.0     834.0
        18     16000       542.1     467.7    1170.6
        19     16000       460.3     295.8    1176.8
        20     24000       354.0     213.7    1271.7
        21     24000       417.9     196.5     606.5
        22     16000       404.6     204.3     791.3
        23     16000       405.8     340.4    1103.4
        24     16000       462.2     325.6    1266.7
        25     28000       398.0     256.5    1368.2
        26     20000       437.6     262.7     732.7
        27     16000       405.5     349.5     997.5
        28     16000       489.7     412.3    1236.4
        29     20000       459.5     346.8    1348.8
        30     12000       396.7         -         -
```

The first sample is 13.2 s stale because nothing had been written since phase 1
ended — that is the tail catching up, and it does so within one interval. After
that, freshness oscillates between 0.52 s and 1.37 s for 29 straight seconds
with no trend. The oscillation is the flush interval beating against the 1 Hz
tail: a tail landing just before a commit sees rows a full interval old, one
landing just after sees them fresh. Half a flush interval of jitter is the
expected shape, and it is what the run shows.

Nothing degrades: insert p50 holds a 354–569 ms band, tail p50 a 196–579 ms
band, while the table grows 1.5M → 2.1M rows. The per-second rows/s alternates
16k/24k because eight writers paced at 822 ms per batch put one batch in some
one-second buckets and two in others; the 30-second mean is flat.

## Node counters (`GET /metrics`)

```
  buffer_commits_total                     526
  buffer_rows_committed_total              2100000
  buffer_admission_refused_rows_total      -
  seal_attempts_total                      5
  ingest_rows_rejected_total               -
  query_jobs_total                         73
```

2.1M rows in 526 group commits — 4,000 rows a commit, two 2,000-row batches
merged on average, so group commit is earning its name at this rate. Nothing was
refused by admission and nothing was rejected in validation.

**Five seal attempts retired 466 of those 526 micro-segments, and that is the
reason the tail's latency is flat.** The hot tier sat at 61 files after phase 1
and 60 after phase 3 — pinned just under `seal_max_files: 64` while 592k more
rows landed on it. Sealing is not falling behind at 19k rows/s; it is holding
the read side's cost constant, which is exactly what the tier boundary is for.

## What this settles

- **The e2e number exists now**: ~38.9k wide (61-column) records/s and
  82.6 MiB/s of JSON through the front door on one node, with a co-resident
  client; half that sustained with a live tail, everything at sub-second p95.
- **The ack contract holds under load.** A 200 means queryable, and end to end
  that reads out as 1.16 s of staleness at p50 — flush interval plus query —
  stable for 30 seconds with no drift.
- **Insert latency is the flush interval and the batch size, not the API.**
  Phoenix, auth, JSON parsing, and validation are invisible next to waiting for
  a group commit, and throughput going flat past 8 writers says the write path,
  not the edge, is the bottleneck. Anyone wanting lower insert latency should
  turn `flush_interval_ms` down or batches smaller, not optimize the front door.
- **Sealing keeps up, and that is why tails are stable.** Hot depth held at ~60
  files across 2.1M rows. Tail latency is a readout of that depth, so the
  seal path is the read path's latency budget — `bench/results/sealer.md`'s
  concern, seen from the other end.
- **A filtered tail is slower and staler than an unfiltered one.** `ORDER BY
  timestamp DESC LIMIT 100` reads every surviving file regardless, and rarer
  matches mean older newest-matches. Both halves of that matter to anyone
  promising a live filtered view.
- **Planning reads a manifest that is mostly already-sealed entries.** Every
  query fetches the whole hot manifest over HTTP and then drops the entries
  whose files the catalog already has (`Planner.include?/2`). A sealed
  micro-segment is not deleted at seal time — it stays in the manifest for
  `retire_grace_ms` (10 min) so a query holding an older snapshot cannot have
  rows deleted out from under it. So this run's planner fetched 526 entries per
  query to scan 60: **the planner's input grows with grace period × ingest rate,
  while the scan's grows only with seal lag.** No cost was visible at 526
  (`query.md` measured planning flat to 256), but that is the term that moves if
  the grace period lengthens or the rate climbs.
