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
mix run bench/cluster_ingest.exs                  # does aggregate ingest scale with buffer-node count?

SEGMENTS=1500 ROWS=2000 mix run bench/planner.exs   # bigger catalog, smaller segments
ROWS=10000000 CLIENTS=16 mix run bench/adbc.exs     # push the fetch and concurrency sizes
CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs   # more samples, more concurrency
INPUTS=64 ROWS=20000 mix run bench/sealer.exs       # bigger claims, bigger merges
NODES=4 WRITERS=16 mix run bench/cluster_ingest.exs # a wider fleet, more load per node
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

## Sealing — `bench/sealer.exs`

Compares the two merge implementations, measures merge throughput against input
count and rows, times the whole handoff, and reports the sealed-to-hot size
ratio. That last number is why it exists: DuckDB's `COPY` defaults to snappy
while segments are written with zstd, so sealing silently made data 2.85× larger
until the codec was matched. No correctness test could catch that.
