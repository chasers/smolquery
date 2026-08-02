# bench/cluster_ingest.exs — aggregate ingest vs buffer-node count

| | |
|---|---|
| Run | 2026-08-02 |
| Commit | 7b09204 |
| Command | `mix run bench/cluster_ingest.exs` (defaults: `NODES=3 WRITERS=8 BATCHES=200 ROWS=1000 TABLES=4 FLUSH_MS=25`) |
| Machine | Apple M1 Max, 10 cores, 64 GiB, macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 (erts 17.0.2), 10 schedulers |

`WRITERS` is per buffer node, so offered load grows with the fleet: the aggregate
column is what the fleet absorbed, the per-node column is what each node
absorbed. Flat per-node throughput as nodes are added is what linear scaling
looks like.

## Aggregate ingest — 8 writers/node × 200 batches × 1000 rows, 4 tables/node

```
topology    nodes        rows     krows/s    per node   scale
edge            1     1600000       215.4       215.4     1.0
fleet           1     1600000       241.3       241.3     1.0
edge            2     3200000       420.2       210.1    1.95
fleet           2     3200000       414.2       207.1    1.72
edge            3     4800000       603.1       201.0     2.8
fleet           3     4800000       628.7       209.6    2.61
```

`edge` — one driver node fans out to the whole fleet over gen_rpc, the kind
cluster's shape (api ×1, buffer ×3). `fleet` — a driver on every buffer node
writing only to tables that node owns, all local, no remote transport.

Every timed window here is 7–8 s (1.6 M rows at 215 krows/s, 4.8 M at 603). An
earlier run at `BATCHES=20` finished each window in under a second and read
*higher* at 3 nodes (606.5 / 657.0 krows/s) — noise, not signal, which is why
`BATCHES` now defaults to 200.

## What this settles

**PL-11's exit criterion, the throughput clause: aggregate ingest scales with
buffer-node count, and is not flat.** The `edge` row is the deployment-shaped
number: 215.4 → 420.2 → 603.1 krows/s, or 1.95× and 2.80× of one node against a
theoretical 2× and 3×. Per-node throughput holds at 215.4 / 210.1 / 201.0 — a
~7% decline at three nodes, so scaling is near-linear but not free.

**The ingest edge is not the bottleneck at three nodes.** If the single driver
node were saturating, `edge` per-node throughput would collapse as the fleet
grew rather than drift down 7%, and `edge` would fall further behind `fleet` at
each step; instead the two stay within ~4% of each other at 3 nodes (603.1 vs
628.7). Fan-out costs ~11% at one node (215.4 vs 241.3), consistent with
gen_rpc term transfer in `bench/results/ingest_transport.md`.

**Nothing in ownership resolution serializes the fleet.** Ownership is read per
write through `Smolquery.BufferService.Routing` off live `:pg` membership;
`Smolquery.Cluster.RingCache` keys the built ring on the member list so 128
points per node are hashed once per ring change, not once per write. A missing
cache there would show up as per-node throughput falling with node count, and it
does not.

**`fleet` is the noisier column and should not be read as a prediction.** Its
scale factors (1.72×, 2.61×) trail `edge`'s despite every write being local,
because all ten schedulers on one host are shared by the drivers and the buffers
both — driver work that the `edge` topology places on a separate node. It is a
ceiling, not a forecast.

**One host is the honest limit of this measurement.** Three peer BEAMs on ten
cores still have headroom, but a fourth or fifth node would be measuring the
host, not the fleet — the per-node decline already visible at three nodes is
partly that. Where scaling actually stops is not settled here and needs real
distinct hosts; the local kind cluster cannot answer it either (4 GB Docker VM,
every pod on a shrunken request), which is why L8's throughput claim lives here
and its correctness clauses live in `scripts/kind-smoke.sh`.

**No `StorageService` runs, so nothing seals.** The run logs `seal_ready … no
node is running Smolquery.StorageService — the hot tier is accumulating
unsealed` for every table that crosses the threshold, which is the intended
shape: this measures the write path into a durable, queryable hot tier, not the
seal path (`bench/results/sealer.md` measures that). The numbers carry no merge
or catalog cost, and a fleet under sustained ingest with sealing enabled will
differ.
