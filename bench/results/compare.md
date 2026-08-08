# `bench/compare.exs` — smolquery vs ClickHouse

| | |
|---|---|
| Run | 2026-08-07 |
| Commit | `aeb273b (dirty working tree)` |
| Command | `ARMS=smolquery CH_VARIANT=identical ROWS=30000000 PROJECTS=1000 POOL=100000 BATCH=2000 WRITERS=4,8,16,32 REPS=3 BURST_SECONDS=10 QUERY_REPS=3 mix run bench/compare.exs 2>/dev/null` |
| Machine | Apple M1 Pro · 10 cores · 16.0 GiB · Darwin 25.5.0 |
| Runtime | Elixir 1.20.2 / OTP 29 · 10 schedulers online |
| ClickHouse | 25.8 · config hash `bab5cabce68a` |
| ClickHouse variant | `identical` — plain String, ClickHouse's stock LZ4 |
| Fixture | POOL=100000 row templates, PROJECTS=1000 tenants |
| Arms | smolquery |

Same rows, same sort key `(project_id, timestamp)`, same
machine, one arm at a time. The driver shares the machine with whichever server it
is measuring, on both arms.

## Reading the write phase

Phase W reports three kinds of number and they answer different questions.
smolquery `:http` is what a client experiences. smolquery `:storage` is the same
storage engine with Phoenix, the JSON decode and the per-row validator removed —
65% of the write path by the stage profile in `bench/results/otel_logs.md`.
**`:storage` is not like-for-like either**: ClickHouse's `JSONEachRow` parser
validates types as it reads, so `:storage` is the lower bound on our side and
`:http` the upper. The honest read is the pair, never one alone.

Phase L is a separate number again — one mode, highest writer count, the bulk
load that puts identical data on both arms so phases D and R compare the same
corpus.

## Workload

```
  ROWS=30000000 PROJECTS=1000 BATCH=2000
  WRITERS=4,8,16,32 REPS=3 BURST_SECONDS=10 QUERY_REPS=3
  POOL=100000 row templates
  arms: smolquery
  CH_VARIANT=identical (plain String, ClickHouse's stock LZ4)
  sort key: (project_id, timestamp) on both arms
  commit: aeb273b (dirty working tree)

  fixture is 2263 B/row on the wire → 64744.9 MiB per arm
  Phase W ~4.0 min of bursts across all arms; Phase L ~11.8 min per arm at 42500 rows/s (bench/results/otel_logs.md, 2026-08-04)
  settle and the query phase are on top of that and are not estimated here
```

## Query set

```
  id   query                                           hypothesis
  Q1   count(*) whole table                            metadata-only in ClickHouse; we must pay I/O — expect to lose outright
  Q2   count() for one project                         the tenant point lookup; sort key should make both cheap
  Q3   last 100 rows of one project by timestamp desc  the real product query (log tail UI)
  Q4   count() grouped by project_id, all projects     the tenant fan-out; the whole point of the exercise
  Q5   count grouped by service_name, one project, 1h windownarrow group-by inside one tenant
  Q6   p95 duration_ms grouped by http_route, one projectheavier aggregate function
  Q7   approx distinct trace_id, one project           approx-distinct: uniq vs approx_count_distinct
  Q8   per-minute error count, top 10 projects         time bucket + top-N across tenants
  Q9   substring match in body, one project            needle in haystack; forces a scan inside the tenant range
  Q10  SELECT * limit 100, one project                 cost of the wide 62-column projection

  heavy tenant = project_ref(0), the most frequent under the log-uniform skew:
  a ceiling on per-tenant latency, not a typical tenant.
  Q2, Q3, Q5 also run against the emptiest tenant, chosen from the corpus rather than assumed.
```

## Arm smolquery — setup

```
  smolquery up; server pid 99230
  corpus table created: 62 columns, sorted by (project_id, timestamp)
```

## Phase W — smolquery: write throughput

```
  10s bursts on scratch table `otel_logs_w`, 2000 rows/batch, 3 rep(s) per cell
  nothing written here is read by Phase D or Phase R
  running 4 writers, mode http …
    36280 rows/s median (35395–37303), p95 252.1 ms, 0 error(s), driver took 18% of the burst
  running 8 writers, mode http …
    45780 rows/s median (44956–46112), p95 432.9 ms, 0 error(s), driver took 13% of the burst
  running 16 writers, mode http …
    45773 rows/s median (45317–46173), p95 843.6 ms, 0 error(s), driver took 8% of the burst
  running 32 writers, mode http …
    44678 rows/s median (44000–46047), p95 1679.5 ms, 0 error(s), driver took 5% of the burst
  running 4 writers, mode storage …
    51940 rows/s median (44993–52494), p95 202.4 ms, 0 error(s), driver took 29% of the burst
  running 8 writers, mode storage …
    43717 rows/s median (40940–43765), p95 460.4 ms, 0 error(s), driver took 15% of the burst
  running 16 writers, mode storage …
    40059 rows/s median (40047–41603), p95 926.2 ms, 0 error(s), driver took 8% of the burst
  running 32 writers, mode storage …
    41939 rows/s median (40522–43529), p95 1747.8 ms, 0 error(s), driver took 8% of the burst
```

## Phase L — smolquery: corpus load

```
  30000000 rows into `otel_logs` via 32 writers, mode http — this is the table Phase D and Phase R measure
  loaded 30000000 rows in 750.4s wall — 41295 rows/s over the measured window, 0 error(s)
```

## Phase D — smolquery: settle, then bytes on disk

```
  settling (seal + compaction / OPTIMIZE TABLE FINAL) — this can take a while …
  settled
  4722519343 bytes over 30000000 acked rows
```

## Tenants — smolquery

```
  sampled every 75th of 30000000 row indices; emptiest tenant seen 42 time(s) in the sample
  heavy tenant aaaaaphloieywwvhfyby, empty tenant aabjtugqkdpgbefylard — both verified populated on this arm
```

## Phase R — smolquery: read

```
  10 queries, cold + 2 hot, caches dropped before each cold run
  Q1/all …
  Q2/heavy …
  Q2/empty …
  Q3/heavy …
  Q3/empty …
  Q4/all …
  Q5/heavy …
  Q5/empty …
  Q6/heavy …
  Q7/heavy …
  Q8/all …
  Q9/heavy …
  Q10/heavy …
```

## W1 — write throughput, rows/s (median repetition)

```
  arm         mode                    w=4         w=8        w=16        w=32
  smolquery   http                  36280       45780       45773       44678
  smolquery   storage               51940       43717       40059       41939
  rows acked ÷ the longest writer's time inside the timer — the same window W2's
  percentiles describe. Not burst wall clock: that also contains this driver's own
  row generation and format conversion, which costs a different amount per arm and
  would understate ClickHouse on the headline pair.
  That denominator is a lower bound on the true concurrent window, so these rates
  are upper bounds — never understated, possibly overstated, and overstated more
  on the arm whose writers do more driver work between batches, which is
  ClickHouse. The residual bias therefore runs against smolquery, not for it.
  smolquery `:http` is what a client experiences; `:storage` is the same engine
  with Phoenix, the JSON decode and the validator taken out — 65% of the path by
  the stage profile in bench/results/otel_logs.md. ClickHouse has no `:storage`
  counterpart: its JSONEachRow parser validates types too, so `:storage` is our
  lower bound, not a like-for-like number. The honest read is the pair.
  The like-for-like ClickHouse row against smolquery `:http` is `:durable_async`:
  async_insert with wait plus table-level fsync. Our ack amortizes — one
  TableBuffer group commit covers many client batches and pays one segment fsync
  plus one manifest-log fsync for the whole commit — and `:durable_async` is the
  ClickHouse shape that does the same. `:durable` alone fsyncs every INSERT;
  `:async` alone batches without fsync; both are context, not the headline.
```

## W2 — write latency per batch, ms (all repetitions pooled)

```
  arm         mode             writers    rows/s   spread       p50       p95       p99       max       acked   errors
  smolquery   http                   4     36280       5%     214.6     252.1     322.8     606.4      900000        0
  smolquery   http                   8     45780       3%     339.0     432.9     500.6     529.4     1224000        0
  smolquery   http                  16     45773       2%     680.8     843.6     914.2    1092.0     1324000        0
  smolquery   http                  32     44678       5%    1382.9    1679.5    1769.0    1896.1     1404000        0
  smolquery   storage                4     51940      14%     157.6     202.4     252.4     287.8     1084000        0
  smolquery   storage                8     43717       6%     365.8     460.4     490.0     525.1     1120000        0
  smolquery   storage               16     40059       4%     777.9     926.2     999.8    1046.1     1180000        0
  smolquery   storage               32     41939       7%    1550.8    1747.8    1777.4    1817.0     1276000        0
  latencies cover acked batches only — a shed request returns in microseconds
  and would pull p50 down while the shedding it signals disappeared from view.
```

## W3 — write errors by reason (Phase W and Phase L)

```
  none on any arm — nothing was shed, refused, or failed, and nothing was retried
```

## L — corpus load: the same ROWS into the table Phase D and Phase R measure

```
  arm         mode             writers    rows/s   spread       p50       p95       p99       max       acked   errors   seconds
  smolquery   http                  32     41295        —    1539.1    1759.6    1928.7    2538.0    30000000        0     750.4
  one mode per arm, at the highest writer count in WRITERS: this phase exists to
  put identical data on both arms, and the bulk-load rate is what that cost.
  rows/s is over the measured window, as in W1; `seconds` is wall clock, so the
  gap between them is what the driver spent generating and encoding rows.
```

## W4 — server process resources, per arm per phase

```
  arm         phase      RSS peak    RSS p95   RSS mean  cores mean  cores peak  samples
  smolquery   W            5777.4     5724.0     4207.1         2.6        9.59     1182
  smolquery   L            5785.2     4966.6     4198.9        3.81        9.54     3520
  smolquery   D            5029.1     5028.1     4751.2        2.26        7.84       36
  smolquery   R            4948.2     4937.3     4829.9        5.47        6.95       24
  RSS in MiB, sampled every 200 ms from the server's OS pid only.
  Phase W aggregates the per-cell windows: cores mean is Σcpu ÷ Σwall over the
  measured windows, so idle time between cells is excluded rather than averaged in.
  `—` means the arm could not name a pid; it is an absence, not a zero.
```

## D — bytes on disk after settle

```
  arm                   rows           bytes        MiB     B/row  status                        
  smolquery         30000000      4722519343     4503.7     157.4  measured after settle         

  Phase W's scratch table is still on the machine during Phase D and Phase R.
  Bytes are unaffected: both arms scope disk_bytes/1 to the corpus table by name.
  Time is not. ClickHouse scopes settle/1 to the corpus, so scratch merges are
  neither triggered nor waited for and run through D and R on the server's CPU.
  smolquery is asymmetric the other way: Compactor.sweep/2 and GC.sweep/2 are
  global, so scratch compaction lands inside D's sampler window, while
  force_seal/2 is corpus-scoped, so scratch micro-segments stay resident in the
  buffer and inflate RSS through R. Treat D's and R's cores and RSS as upper
  bounds, differently inflated per arm.
```

## R1 — query latency, ms (cold / hot-min / hot-median)

```
  id   tenant       sm:cold     sm:hmin     sm:hmed
  Q1   all             78.1        74.1        76.2
  Q2   heavy          111.8       113.4       115.9
  Q2   empty          111.6       109.5       110.5
  Q3   heavy          135.0       130.9       136.7
  Q3   empty          155.1       145.5       147.8
  Q4   all            165.6       131.6       132.1
  Q5   heavy          119.2       113.1       115.2
  Q5   empty          108.9       110.9       115.8
  Q6   heavy          140.8       124.4       128.0
  Q7   heavy          130.5       119.6       122.8
  Q8   all            247.7       203.0       206.6
  Q9   heavy          149.3       142.2       148.0
  Q10  heavy           89.4        89.2        89.8
  cold is the run right after drop_caches; hot excludes it. They are never averaged
  together — the average of a cold and a hot run describes neither. `heavy` is the
  most populated tenant and a ceiling; `empty` is the least populated one that
  still has rows, chosen from the corpus rather than assumed.
  Phase W's scratch table is still resident — see the note under D. On ClickHouse
  its merges may still be running here; on smolquery its micro-segments are still
  in the buffer. Both inflate these latencies, and not equally.
```

## R2 — what each query touched

```
  id   tenant       sm:rows       sm:read      sm:MiB
  Q1   all                1             —           —
  Q2   heavy              1             —           —
  Q2   empty              1             —           —
  Q3   heavy            100             —           —
  Q3   empty            100             —           —
  Q4   all             1000             —           —
  Q5   heavy              5             —           —
  Q5   empty              5             —           —
  Q6   heavy              5             —           —
  Q7   heavy              1             —           —
  Q8   all              140             —           —
  Q9   heavy              1             —           —
  Q10  heavy            100             —           —
  `read` is rows scanned, `MiB` bytes scanned, both as the arm reported them;
  `—` means that arm does not expose the figure, not that it read nothing.
```

## R3 — row-count agreement between arms

```
  only one arm ran — there is nothing to agree or disagree with.
```

## R4 — ClickHouse ÷ smolquery (below 1.0 means ClickHouse is faster)

```
  skipped: the clickhouse arm did not run (ARMS=smolquery).
```

## Fairness ledger

Everything this benchmark did *not* equalise, who it helps, and why it was left
unequal. A comparison without this table is an advertisement.

| what differs | which arm it favours | why it was not equalised |
|---|---|---|
| fsync on ack — ClickHouse `:default` does not fsync, smolquery always does | ClickHouse | It is what ClickHouse is out of the box. `:durable_async` is reported beside it as the like-for-like amortized promise, with `:durable` and `:async` as context, so the reader gets the spectrum rather than a choice already made for them. |
| client-format work — 65% of smolquery's `:http` path is JSON decode and per-row validation, done in Elixir; ClickHouse parses JSONEachRow in C++ | ClickHouse | Not equalised because it is real: it is what a client pays. `:storage` is reported beside it as our lower bound, and it is *not* like-for-like either — ClickHouse's parser validates types as it reads, so no single number on either side is the answer. |
| `LowCardinality` column encodings | neither — identical model | This run used `CH_VARIANT=identical` — `low_cardinality: false`. A single run measures one variant; the side-by-side the contract wants needs one run per variant, so nothing here should be read as having covered both. |
| compression codec — smolquery writes Parquet zstd | depends: ClickHouse on write, smolquery on size | This run used `codec: :lz4`. LZ4 writes faster and reads bigger, zstd the reverse, so the sign of this row flips with the setting — and again, one run is one variant, not both. |
| smolquery reads its own hot tier over HTTP even on one machine | ClickHouse | Architecture rather than a benchmark artifact: the hot tier is a service so a query node can read a buffer node it shares no memory with. Removing the hop here would measure a system that does not exist. |
| smolquery pins a snapshot and applies a membership rule on every read | ClickHouse | It is what we pay for read consistency across seal and compaction. ClickHouse does not offer that guarantee, so there is nothing to equalise against — only a cost to name. |
| process model — ClickHouse is one process; smolquery is a BEAM plus a DuckDB thread pool plus a store | neither, but RSS is not naively comparable | The RSS columns measure the single OS process each arm names. On smolquery that is one of several cooperating pieces, so the figure is a floor for the deployment, not the whole of it. |
| the driver shares the machine with the server | neither | True on both arms, which is what makes it tolerable. It caps both throughput figures; the resource columns exclude the driver so the server's own cost stays readable. |
| the per-tenant queries use the most and least populated tenants, not a median one | neither, but both are extremes | `heavy` is a ceiling on per-tenant latency and `empty` a floor. Neither is a typical tenant, and the log-uniform skew means most tenants sit far closer to `empty` than to `heavy`. |
| the ingest format is JSON — nobody ships OTel logs as 2.1 KB of JSON per record at volume; real pipelines send OTLP over protobuf | neither arm, but it flatters neither either | Both arms are measured on a format neither is optimised for, so the write numbers are a floor for both. It was not equalised because smolquery's ingest API takes JSON and adding an OTLP path to compare against ClickHouse's protobuf input would be building the thing under test. It matters most to us: the stage profile puts 26% of our write path in the JSON decode alone, and ClickHouse parses its JSON in C++. |
| fixture cardinality is bounded by `POOL` — this run used 100000 row templates | unknown, and that is the problem | 100000 distinct templates is enough that compression sees realistic entropy, but it is still a bounded corpus: real log traffic has effectively unbounded body cardinality. Bytes-on-disk and read latency are both better than a production corpus would give, on both arms — and not necessarily by the same factor, since Parquet and MergeTree do not compress repetition identically. |
| both arms run on local disk; smolquery's production sealed tier is an S3-compatible object store (MinIO) | smolquery — production pays object-store latency this run does not measure | Putting both on MinIO would make object-store round-trip latency dominate both arms and turn the result into a measurement of MinIO rather than of either engine. The object-store shape is a separate experiment with its own table. |
| Phase W's scratch table is still on the machine during Phase D and Phase R | neither consistently — the two arms are contaminated differently | There is no drop seam in the frozen `Backend` behaviour. Bytes are unaffected: both arms scope `disk_bytes/1` to the corpus table by name. Time is not. ClickHouse scopes `settle/1` to the corpus, so scratch merges run through D and R unwaited; smolquery's compaction sweep is global and lands inside D's sampler, while its seal is corpus-scoped so scratch micro-segments inflate RSS through R. Different contamination per arm is worse than a shared bias, and D's and R's cores and RSS should be read as upper bounds. |
| W1 and L divide by the longest writer's timed total, a lower bound on the true concurrent window | ClickHouse — the residual runs against smolquery | Dividing by a lower bound makes every rate an upper bound: never understated, possibly overstated. The gap grows with the driver work between a writer's batches, which is larger on the ClickHouse arm, so ClickHouse's rate is overstated by more than smolquery's. It was not equalised because closing it needs the union of every writer's timed intervals, and only durations are recorded, not start times. Left deliberately in this direction: an estimator that flatters the arm this benchmark's author does not own is the safe one. |

## What this settles

- **Write throughput at the like-for-like durability promise.** Compare
  smolquery `:http` against ClickHouse `:durable_async`, not `:default` or
  unamortized `:durable`, in W1.
  _(fill in: the ratio, and whether the gap is a serialization ceiling or a
  capacity one — W4's cores columns are the tell.)_
- **How much of that gap is ours to fix cheaply.** The `:http` to `:storage`
  distance is client-format work, not storage. _(fill in: the ratio, and
  whether closing it would change the headline at all.)_
- **What the sort key buys per tenant.** Q2/Q3/Q5 run against the heaviest
  and the emptiest populated tenant. _(fill in: whether the empty tenant is
  cheap on both arms; if it is not cheap on ours, the clustering key is not
  pruning.)_
- **Where we lose outright.** Q1 is metadata-only in ClickHouse and I/O for
  us; Q4 is the tenant fan-out the whole exercise exists for.
  _(fill in: the two ratios, and whether Q4 justifies the rest.)_
- **What a row costs on disk.** _(fill in: the D table's B/row on each arm,
  and whether the difference is the codec or the layout.)_
- **What this run cannot say.** Only one arm ran (smolquery), so nothing here is a comparison — these are one system's numbers on the comparison harness.

