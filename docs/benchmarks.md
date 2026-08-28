# Benchmarks

`bench/` holds the measurements behind the architectural decisions. Re-run a
measurement instead of re-arguing a decision. Re-run after a DuckDB or DuckLake
upgrade, or before you revisit the decision the measurement settled.

```sh
mix run bench/planner.exs                         # scan DuckLake, or plan around it?
mix run bench/adbc.exs                            # what ADBC costs to connect, fetch, and share
mix run bench/buffer.exs                          # what group commit costs, and where it bends
mix run bench/sealer.exs                          # what a seal costs, and how far behind it runs
mix run bench/hot_manifest.exs                    # can HotServer serve manifests as fast as the buffer commits?
mix run bench/query.exs                           # what a query job costs, and the hot tier's read path
mix run bench/ingest_transport.exs                # ingest→buffer: gen_rpc terms vs Arrow IPC over HTTP
mix run bench/ack_budget.exs                      # does the ack budget bound overload latency?
mix run bench/clustering.exs                      # does the ORDER BY analog work, and what does it cost?
mix run bench/cluster_ingest.exs                  # does aggregate ingest scale with buffer-node count?
mix run bench/otel_logs.exs 2>/dev/null           # OTel logs over HTTP: wide ingest with a live tail
mix run bench/profile.exs 2>/dev/null             # where BEAM CPU goes under ingest: threads, processes, microstates

SEGMENTS=1500 ROWS=2000 mix run bench/planner.exs   # bigger catalog, smaller segments
ROWS=10000000 CLIENTS=16 mix run bench/adbc.exs     # push the fetch and concurrency sizes
CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs   # more samples, more concurrency
BENCH_SECTION=replication_delta mix run bench/buffer.exs   # one section by name
INPUTS=64 ROWS=20000 mix run bench/sealer.exs       # bigger claims, bigger merges
COLUMNS=63 BACKLOGS=64,4096 mix run bench/hot_manifest.exs   # a wide table, a deep backlog
NODES=4 WRITERS=16 mix run bench/cluster_ingest.exs # a wider fleet, more load per node
WRITERS=8,32 RATE=40000 mix run bench/otel_logs.exs 2>/dev/null   # a different sweep and offered rate
WRITERS=32 SECONDS=20 mix run bench/profile.exs 2>/dev/null       # profile at a saturating writer count
```

Each script's `@moduledoc` records what the script measures and what it
concluded. [`bench/results/`](../bench/results/) holds the last recorded run of
each script: the tables verbatim, the machine the run came from, and the
decisions the run settled.

Re-run a script after a DuckDB, DuckLake, Explorer, or OTP upgrade. Then
overwrite the script's file in `bench/results/`. The next comparison then has
data to diff against.

## Results worth knowing before writing a read path

- **Do not return a large result as Elixir terms.** `Smolquery.Engine.Result`
  converts Arrow row by row. The conversion costs roughly a kilobyte and 2 µs
  per row. 5M rows take 11.2 s and 3.4 GiB as terms, against 390 ms and
  11.5 MiB left in Arrow. Terms are the right shape for catalog and
  control-plane queries. Terms are a trap for user results. `Engine.frame/3`
  and the `:max_result_rows` ceiling exist for that reason.
- One connection serializes. `Engine.Connection` is a per-query mutex, so query
  throughput is flat in client count. Eight connections serve eight concurrent
  clients about 3.5× faster than one connection does.

## Group commit — `bench/buffer.exs`

This script measures group commit only. Sealing is disabled throughout;
`bench/sealer.exs` measures sealing.

The script reports batches/s, rows/s, MB/s, and p50/p95/p99 ack latency across
batch size × writers × tables. It sweeps `flush_interval_ms`. It prices the two
fsyncs behind an ack. It locates the one-table inline-flush ceiling in five
parts:

1. A writer sweep to 1024, against a light schema and a heavy schema.
2. The encode, timed in isolation. Since PL-57 this arm times the fixture
   writer (`Writer.write(rows)`), not the DuckDB `COPY` a deployment runs;
   see T-401 before reading it as a production number.
3. The fsync toggle, re-run at the top of the sweep.
4. A `flush_max_bytes` sweep.
5. A partition proxy that runs P independent buffers over one workload.

Its other knobs: `MAX_BATCH`, `MAX_TABLES`, `WRITERS`, `BATCH`,
`WRITERS_PER_BUFFER`.

### The three schemas

Every ceiling section runs three schemas. The encode is what the script
investigates. Cost per row is the variable that moves everything else.

- `light`: 2 columns, 165 B/row.
- `heavy`: 4 columns with a `Decimal`, 254 B/row.
- `huge`: 20 columns that span all seven logical types, 867 B/row — the shape
  of a real event table.

`huge` is 5.3× `light`'s bytes but 12× its encode time. Per-column overhead
dominates payload size.

### The throughput ceiling

One table saturates at **2.19M rows/s light, 1.08M heavy, 280K huge**. The
encode plus the write path around it set this ceiling. No configured bound sets
it. `flush_max_bytes` from 8 MB to 512 MB moves rows-per-flush 8–27× and
throughput 5–12%, non-monotonically. That knob decides how the buffer packs
rows into flushes, not how many rows get through.

### Ack latency has two regimes

Design against ack latency. It has two regimes:

- Below saturation: `p50 = flush_interval_ms + ~5 ms`, regardless of load.
  That is the whole promise of group commit.
- At saturation: `p50 = outstanding rows ÷ throughput` (Little's Law).
  `flush_interval_ms` drops out entirely.

The below-saturation number predates the adaptive wait (T-202). Below
`commit_siblings` in-flight inserts, the window is now
`flush_idle_interval_ms`. A lone writer's p50 is therefore the commit itself,
not the interval.

At the default config, a 20-column table returns a **2.01 second p50 ack**.

Eight independent buffers reach 6.68M rows/s — 3.11× light, 4.09× huge. The
order follows how encode-bound each schema is. That is the case for partitioned
writes. Partitioning divides the overload factor. It does not define the cliff;
`ack_budget_ms` now defines the cliff (`bench/results/ack_budget.md`).

## Scaling across nodes — `bench/cluster_ingest.exs`

**Aggregate ingest goes 215 → 420 → 603 krows/s over one, two, and three
buffer nodes** — 1.95× and 2.80×. Per-node throughput holds at 215 / 210 /
201, so the scaling is near-linear. Fan-out costs ~11%. The edge is not the
bottleneck at three nodes (`bench/results/cluster_ingest.md`).

The script answers the cluster milestone's exit criterion in the other
direction. The question is not "is it correct across nodes" but "is it faster
across nodes". The script stands up a fleet of peer BEAMs that run the
`:buffer` role. It joins them into one ring through the same `:pg` membership
production uses.

The script sweeps node count against two driver topologies:

- `edge`: one node fans out to the fleet — the api ×1 / buffer ×3 shape of the
  kind cluster.
- `fleet`: a driver on every buffer node; each driver writes only what its node
  owns.

`WRITERS` is per node, so offered load grows with the fleet. Flat per-node
throughput is the sign of scaling.

The script measures throughput off peer BEAMs, not against the local kind
cluster. A 4 GB Docker VM, with every pod on a shrunken request, measures VM
contention, not the fleet.

## The workload end to end — `bench/otel_logs.exs`

**One table takes ~38.4k wide records/s** (82 MiB/s of JSON) with 4.8 of 10
cores. Eight tables take 57.1k. A live tail over half the single-table stream
costs 331 ms at p50. The tail shows rows 1.12 s old. All of this is stable for
30 seconds, with no drift in insert latency, tail latency, or freshness. The
sealer pins the hot tier near 55 unsealed micro-segments while 2M rows land.

### What the script runs

This is the only script that measures a *workload* rather than a component.
61-column OpenTelemetry log records stream in through `POST …/insert`. A
tailer asks for the last 100 rows through `POST /v1/queries`. Everything runs
over the real HTTP front door.

`SmolqueryApi.Endpoint` is a singleton Phoenix endpoint, so the script takes
over the node's boot. It terminates the role subtrees. It points the catalog,
buffer, and sealed dirs at a scratch directory. It restarts all roles but
`:web`.

The script has four sections:

1. A stage profile of one batch.
2. An ingest ceiling: a writer sweep, with no reader.
3. A tail floor: no ingest.
4. The sustained case: paced ingest at half the ceiling plus a 1 Hz tail, with
   a per-second timeline.

Knobs: `WRITERS` (the sweep, comma-separated), `TABLES`, `SUSTAINED_WRITERS`,
`BATCH`, `SECONDS`, `TAIL_SECONDS`, `SUSTAINED_SECONDS`, `TAIL_INTERVAL_MS`,
`REPS`, `POOL`, `RATE`, `FLUSH_MS`. Redirect stderr — Arrow Database
Connectivity (ADBC) warns once per query.

Both end-to-end scripts open with a **Runtime parallelism** block: the logical
processors, the three scheduler classes with how many are online, and DuckDB's
threads per engine. A throughput number has no meaning without that context.

### The ceiling is serialization, not capacity

Every phase reports `cores` and `sched%`. `cores` is whole CPUs from the OS
process's CPU time, so it counts DuckDB's native threads. `sched%` is
normal-scheduler busy.

Maximum throughput consumes **4.79 of 10 cores** at 29% scheduler busy — so
~1.9 cores of native work. A push from 16 to 32 writers *lowers* throughput
13% while CPU stays flat. Half the machine is idle at the ceiling. That is why
more concurrency alone does not help.

### The ceiling is per-row Elixir CPU

Validation owns half of the per-row CPU. One 2,000-row batch costs **107.5 ms**
on a core:

- 51% `IngestService.Validator` — it coerces every value of every column, 1.8M
  coercions at 61 columns.
- 25% the JSON decode.
- 24% the Arrow encode plus the Parquet write.

Know two consequences before you optimize anything here:

- A faster wire format alone does not help. Parquet decodes 38× faster than
  the same rows parse from JSON (0.7 vs 26.8 ms). But the removed `/load`
  route then called `DataFrame.to_rows` at 29.2 ms — more than the JSON
  decode it replaced. Validation and the re-encode still ran after that.
  `bench/results/ingest_transport.md` said the same from the transport side:
  the frame→rows conversion, not the wire, is the cost. The frame path that
  argument led to (T-139) was removed in PL-57 once the passthrough made it
  unreachable.
- Partitioning one table's writes is worth ~1.5×, not 8×. Hold offered load at
  16 writers, then spread it over 1/2/4/8 tables: 39.0k → 46.9k → 55.1k →
  57.1k rows/s. Only the last quarter of a batch's CPU runs inside the
  `TableBuffer`. `buffer.md`'s 3.1–4.1× from 8 buffers was measured with no
  HTTP edge in front of the encode.

A *filtered* tail is slower (251 vs 231 ms) and staler than an unfiltered
tail. `ORDER BY timestamp DESC LIMIT 100` reads every surviving file
regardless of the filter. Rarer matches mean an older newest match
(`bench/results/otel_logs.md`).

## Batch loads — removed (T-413)

The `POST /…/load` route and `bench/load.exs` are gone. The bench that
measured them (PL-18) is why: a load cost ~10× the file in peak BEAM heap,
ran on one core, and was 2.6× slower than concurrent `/insert` (14.6k vs
38.4k rows/s). Format choice was worth at most 1.6×, because NDJSON, CSV,
and Parquet all converged on `DataFrame.to_rows` → validate → re-encode.

To load a file, split it into NDJSON bodies under
`SMOLQUERY_INSERT_MAX_NDJSON_BYTES`. Fan them out over concurrent `/insert`
requests. That is the measured fast path.

## Sealing — `bench/sealer.exs`

The sealed-to-hot size ratio is the reason this script exists. DuckDB's `COPY`
defaults to snappy, while segments use zstd. The codec mismatch silently made
sealed data **2.85×** larger until the codec was matched. No correctness test
could catch that.

The script also does three things:

- It compares the two merge implementations.
- It measures merge throughput against input count and rows.
- It times the whole handoff.

## Hot-tier reads — `bench/hot_manifest.exs`

`HotServer` runs on the pod that commits. A manifest read that costs more than
a commit steals the throughput it exists to serve (PL-45). This script asks
whether the route keeps up.

It answers four questions:

- what a manifest read costs against unsealed backlog depth, `GET` whole
  against `POST` claim-scoped,
- how many reads a node serves per second, and the seal rate that buys — a seal
  attempt is two scoped reads (T-316),
- what the reads cost the commits: commit throughput and ack latency with no
  readers, under `GET` load, and under `POST` load,
- where an entry's bytes go, against table width. The flush-time stats block is
  per column, so a wide table is what makes an entry expensive.

Column count is the variable a 4-column fixture hides — the soak behind PL-45
ran 63 columns. Knobs: `COLUMNS`, `WIDTHS`, `BACKLOGS`, `CONTENDED_BACKLOG`,
`READERS`, `WRITERS`, `SECONDS`, `ROWS`.

## Clustering — `bench/clustering.exs`

**Correctness holds**: the order is chronological, verified across a month
boundary with microsecond timestamps. Flush costs ~7% rows/s at saturation.
Seal resident set size (RSS) peaks ~+180 MiB higher under `memory_limit=2GB`.
Sealed point lookups win ~1.7× (`bench/results/clustering.md`).

The script asks two questions: does the ORDER BY analog work, and what does it
cost? It runs the same Zipf-like row stream on two tables: `clustered` with
`["project_id", "ts"]` vs `plain` with `[]`. It measures:

- correctness,
- flush and seal write throughput,
- BEAM heap and OS RSS around the sort,
- sealed-tier pruning under a point lookup.

Knobs: `P`, `R`, `MICRO`, `BATCH`, `REPS`.

## Profiling — `bench/profile.exs`

This script is a **diagnostic**, not a benchmark. It drives the same ingest
load as `bench/otel_logs.exs`. Over one measured window, it reports where the
VM spends that CPU:

- per-thread scheduler utilization,
- the top processes by reductions,
- `:msacc` microstates per thread class.

It answers "why is the beam hot" from inside the VM. It shows whether the time
is on the normal schedulers (Bandit, JSON decode, validation), on the dirty
CPU schedulers (the DuckDB encode, offloaded major GCs), or on dirty IO — and in
which processes. Reach for perf or eBPF in a Linux VM only after this script
says the time is inside the `emulator` state. That is the one bucket no in-VM
view can open. Knobs: `WRITERS`, `BATCH`, `SECONDS`, `WARMUP_MS`.

The script settles nothing by itself, so it keeps no `bench/results/` file.
Read its output at the commit it ran against, next to the change under
diagnosis.
