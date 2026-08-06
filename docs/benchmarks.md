# Benchmarks

`bench/` holds the measurements architectural decisions were made on, so they
can be re-run rather than re-argued — after a DuckDB or DuckLake upgrade, or
before revisiting the decision they settled.

```sh
mix run bench/planner.exs                         # scan DuckLake, or plan around it?
mix run bench/adbc.exs                            # what ADBC costs to connect, fetch, and share
mix run bench/buffer.exs                          # what group commit costs, and where it bends
mix run bench/sealer.exs                          # what a seal costs, and how far behind it runs
mix run bench/query.exs                           # what a query job costs, and the hot tier's read path
mix run bench/ingest_transport.exs                # ingest→buffer: gen_rpc terms vs Arrow IPC over HTTP
mix run bench/ack_budget.exs                      # does the ack budget bound overload latency?
mix run bench/clustering.exs                      # does the ORDER BY analog work, and what does it cost?
mix run bench/cluster_ingest.exs                  # does aggregate ingest scale with buffer-node count?
mix run bench/otel_logs.exs 2>/dev/null           # OTel logs over HTTP: wide ingest with a live tail
mix run bench/load.exs 2>/dev/null                # batch loads: which format, and what a file costs

SEGMENTS=1500 ROWS=2000 mix run bench/planner.exs   # bigger catalog, smaller segments
ROWS=10000000 CLIENTS=16 mix run bench/adbc.exs     # push the fetch and concurrency sizes
CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs   # more samples, more concurrency
BENCH_SECTION=replication_delta mix run bench/buffer.exs   # one section by name
INPUTS=64 ROWS=20000 mix run bench/sealer.exs       # bigger claims, bigger merges
NODES=4 WRITERS=16 mix run bench/cluster_ingest.exs # a wider fleet, more load per node
WRITERS=8,32 RATE=40000 mix run bench/otel_logs.exs 2>/dev/null   # a different sweep and offered rate
POOL=20000 ROWS=100000 mix run bench/load.exs 2>/dev/null         # higher-cardinality rows, bigger files
```

Each script's `@moduledoc` records what it measures and what it concluded.
[`bench/results/`](../bench/results/) holds the last recorded run of each — the
tables verbatim, the machine they came from, and the decisions they settled.
Re-run a script after a DuckDB, DuckLake, Explorer, or OTP upgrade and overwrite
its file there, so the next comparison has something to diff against.

## Results worth knowing before writing a read path

- **A large result must not come back as Elixir terms.** `Smolquery.Engine.Result`
  converts Arrow row by row, which costs roughly a kilobyte and 2 µs per row —
  5M rows take 11.2 s and 3.4 GiB, against 390 ms and 11.5 MiB left in Arrow. It
  is the right shape for catalog and control-plane queries, and a trap for user
  results, which is what `Engine.frame/3` and the `:max_result_rows` ceiling are
  for.
- **One connection serializes.** `Engine.Connection` is a per-query mutex, so
  query throughput is flat in client count. Eight connections serve eight
  concurrent clients about 3.5× faster than one does.

## Group commit — `bench/buffer.exs`

Reports batches/s, rows/s, MB/s, and p50/p95/p99 ack latency across batch size ×
writers × tables, sweeps `flush_interval_ms`, prices the two fsyncs behind an
ack, and locates the one-table inline-flush ceiling in five parts — a writer
sweep to 1024 against a light and a heavy schema, the Polars encode timed in
isolation, the fsync toggle re-run at the top of the sweep, a `flush_max_bytes`
sweep, and a partition proxy running P independent buffers over one workload.
Its other knobs: `MAX_BATCH`, `MAX_TABLES`, `WRITERS`, `BATCH`,
`WRITERS_PER_BUFFER`. Sealing is disabled throughout — this script measures
group commit, `bench/sealer.exs` measures sealing.

Every ceiling section runs three schemas, because the encode is what it
investigates and cost-per-row is the variable that moves everything else:
`light` (2 columns, 165 B/row), `heavy` (4 columns with a `Decimal`, 254 B/row),
and `huge` (20 columns spanning all seven logical types, 867 B/row — the shape
of a real event table). `huge` is 5.3× `light`'s bytes but 12× its encode time,
since per-column overhead dominates payload size.

One table saturates at **2.19M rows/s light, 1.08M heavy, 280K huge**, set by
the encode plus the write path around it. None of the configured bounds sets it:
`flush_max_bytes` from 8 MB to 512 MB moves rows-per-flush 8–27× and throughput
5–12%, non-monotonically — it decides how rows are *packed* into flushes, not
how many get through.

The number to design against is ack latency, and it has two regimes. Below
saturation, `p50 = flush_interval_ms + ~5 ms` regardless of load — group
commit's whole promise. At saturation, `p50 = outstanding rows ÷ throughput`
(Little's Law) and **`flush_interval_ms` drops out entirely**. So at the default
config a 20-column table returns a **2.01 second p50 ack**. Eight independent
buffers reach 6.68M rows/s — 3.11× light, 4.09× huge, ordered by how
encode-bound each schema is — which is the case for partitioned writes.
Partitioning divides the overload factor but does not define the cliff;
`ack_budget_ms` now defines it (`bench/results/ack_budget.md`).

## Scaling across nodes — `bench/cluster_ingest.exs`

Answers the cluster milestone's exit criterion in the other direction — not "is
it correct across nodes" but "is it faster across nodes". It stands up a fleet
of peer BEAMs running the `:buffer` role, joined into one ring through the same
`:pg` membership production uses, and sweeps node count against two driver
topologies: `edge` (one node fans out to the fleet, the kind cluster's api ×1 /
buffer ×3 shape) and `fleet` (a driver on every buffer node, writing only what
it owns). `WRITERS` is per node, so offered load grows with the fleet and flat
per-node throughput is what scaling looks like.

**Aggregate ingest goes 215 → 420 → 603 krows/s over one, two, and three buffer
nodes** — 1.95× and 2.80× — with per-node throughput holding at 215 / 210 / 201,
so near-linear but not free. Fan-out costs ~11%, and the edge is not the
bottleneck at three nodes (`bench/results/cluster_ingest.md`).

Throughput is measured off peer BEAMs rather than against the local kind
cluster: a 4 GB Docker VM with every pod on a shrunken request measures VM
contention, not the fleet.

## The workload end to end — `bench/otel_logs.exs`

The only script that measures a *workload* rather than a component: 61-column
OpenTelemetry log records streaming in through `POST …/insert` while a tailer
asks for the last 100 rows through `POST /v1/queries`, everything over the real
HTTP front door. Because `SmolqueryApi.Endpoint` is a singleton Phoenix endpoint,
the script takes over the node's boot — it terminates the role subtrees, points
the catalog, buffer, and sealed dirs at a scratch directory, and restarts all but
`:web`. Four sections: a stage profile of one batch, an ingest ceiling (writers
sweep, no reader), a tail floor (no ingest), and the sustained case (paced ingest
at half the ceiling plus a 1 Hz tail, with a per-second timeline). Knobs:
`WRITERS` (the sweep, comma-separated), `TABLES`, `SUSTAINED_WRITERS`, `BATCH`,
`SECONDS`, `TAIL_SECONDS`, `SUSTAINED_SECONDS`, `TAIL_INTERVAL_MS`, `REPS`, `POOL`,
`RATE`, `FLUSH_MS`. Redirect stderr — ADBC warns once per query. Both e2e scripts
open with a **Runtime parallelism** block — logical processors, the three scheduler
classes and how many are online, and DuckDB's threads per engine — because a
throughput number means nothing without it.

**One table takes ~38.4k wide records/s (82 MiB/s of JSON) using 4.8 of 10 cores,
eight tables 57.1k, and a live tail over half the single-table stream costs 331 ms
at p50 while showing rows 1.12 s old** — stable for 30 seconds with no drift in
insert latency, tail latency, or freshness, because the sealer pins the hot tier
near 55 unsealed micro-segments while 2M rows land.

**The ceiling is serialization, not capacity.** Every phase reports `cores` (whole
CPUs from the OS process's CPU time, so DuckDB's native threads count) and `sched%`
(normal-scheduler busy). Maximum throughput consumes **4.79 of 10 cores** at 29%
scheduler busy — so ~1.9 cores of native work — and pushing from 16 to 32 writers
*lowers* throughput 13% while CPU stays flat. Half the machine is idle at the
ceiling, which is why more concurrency alone buys nothing.

**The ceiling is per-row Elixir CPU, and validation owns half of it.** One
2,000-row batch costs 107.5 ms on a core: 51% `IngestService.Validator` (it
coerces every value of every column — 1.8M coercions at 61 columns), 25% JSON
decode, and only 24% the Arrow encode plus Parquet write. Two consequences worth
knowing before optimizing anything here:

- **A faster wire format alone does not help.** Parquet decodes 38× faster than
  the same rows parse from JSON (0.7 vs 26.8 ms), but `/load` immediately calls
  `DataFrame.to_rows` at 29.2 ms — more than the JSON decode it replaces — and
  then validation and the re-encode still run. `bench/results/ingest_transport.md`
  said the same from the transport side: the frame→rows conversion, not the wire,
  is the cost. The win needs frames end to end; `Writer.write/3` already takes one.
- **Partitioning one table's writes is worth ~1.5×, not 8×.** Holding offered load
  at 16 writers, spreading it over 1/2/4/8 tables gives 39.0k → 46.9k → 55.1k →
  57.1k rows/s, because only the last quarter of a batch's CPU runs inside the
  `TableBuffer`. `buffer.md`'s 3.1–4.1× from 8 buffers was measured with no HTTP
  edge in front of the encode.

A *filtered* tail is slower (251 vs 231 ms) and staler than an unfiltered one:
`ORDER BY timestamp DESC LIMIT 100` reads every surviving file regardless, and
rarer matches mean an older newest match (`bench/results/otel_logs.md`).

## Batch loads — `bench/load.exs`

`POST /…/load` takes the file as the body — NDJSON, CSV, or Parquet by content
type — spools it to disk, parses it, and pushes 10,000-row chunks through the same
insert path a streaming write uses. Same 61-column OTel fixture as
`bench/otel_logs.exs`, so the two compare. Knobs: `ROWS`, `SCALE` (the size sweep),
`BATCH`, `POOL` (fixture cardinality), `FLUSH_MS`.

**Format choice is worth at most 1.6×, and Parquet's 531× size advantage buys
almost none of it**: NDJSON 9.0k rows/s, CSV 14.3k, Parquet 14.6k, from files of
106 MiB, 50 MiB, and 0.2 MiB. Parsing is ~42 µs of a row's ~111 µs; the remaining
~69 µs is format-independent, because all three formats converge on
`DataFrame.to_rows` → validate → re-encode. That is the same floor the insert
bench's stage profile found, and it is the argument for frames end to end (T-139) —
a new content type on today's `parse/3` cannot remove it.

Three more findings, none of them visible from inside the code:

- **A load costs ~10× the file in peak BEAM heap**, essentially all process memory:
  a 106 MiB NDJSON load peaks 1.06 GiB above baseline, while *binary* memory grows
  only 1.04× the file. The disk spool bounds the request body; `parse/3`
  materializes every row anyway. At the 256 MiB `load_max_bytes` default that
  extrapolates to ~2.5 GiB for one request.
- **`/load` is not the fast path, and the CPU number says why**: a load consumes
  **1.0 of 10 cores** — one request is one process. It beats *serial* inserts 7.1×
  by amortizing group commits, but it is **2.6× slower than concurrent `/insert`**
  (14.6k vs 38.4k rows/s, 1.0 core vs 4.8). Fan out `/insert` for throughput; use
  `/load` for convenience and format support.
- **`load_max_bytes` is a byte cap, so it is a different row limit per format**:
  ~120k rows for 61-column NDJSON, ~254k for CSV (`bench/results/load.md`).

## Sealing — `bench/sealer.exs`

Compares the two merge implementations, measures merge throughput against input
count and rows, times the whole handoff, and reports the sealed-to-hot size
ratio. That last number is why it exists: DuckDB's `COPY` defaults to snappy
while segments are written with zstd, so sealing silently made data 2.85× larger
until the codec was matched. No correctness test could catch that.

## Clustering — `bench/clustering.exs`

Does the ORDER BY analog work, and what does it cost — the same zipf-ish row
stream on two tables (`clustered` with `["project_id", "ts"]` vs `plain` with
`[]`), measuring correctness, flush and seal write throughput, BEAM heap and OS
RSS around the sort, and sealed-tier pruning under a point lookup. Knobs: `P`,
`R`, `MICRO`, `BATCH`, `REPS`.

**Correctness holds (chronological, verified across a month boundary with
microsecond timestamps); flush costs ~7% rows/s at saturation; seal RSS peaks
~+180 MiB higher under `memory_limit=2GB`; sealed point lookups win ~1.7×**
(`bench/results/clustering.md`).
