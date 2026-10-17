# Burst spike: FLAME runners on AWS Lambda (T-673, PL-77)

| | |
|---|---|
| Run | 2026-10-16 |
| Branch | `t673-lambda-spike` at `652b6f0` |
| Parent | one pod in the `smolquery-dev` EKS cluster (eu-central-1), `SMOLQUERY_ROLES=none`, node `smolquery@<pod ip>`, dist port pinned to 9100 |
| Runner | Lambda `smolquery-burst-runner`: container image (release + `rel/lambda/bootstrap`), arm64, 10,240 MB (6 vCPU), 10 GB ephemeral, 900 s, in the cluster VPC |
| Query pod | c6a.4xlarge, 16 vCPU, 32 DuckDB threads |

## The question

Can a query fan its sealed scan out to many short-lived Lambdas and come
back faster than the cluster's own machines? Two things to measure: how
fast a runner boots and connects, and how fast 100 of them scan a dataset
too big for one pod to scan quickly.

## The inputs

Three inputs were used. Only one of them appears in the head-to-head.

| input | what it is | files | bytes | rows | used for |
|---|---|---:|---:|---:|---|
| `bench/hits_v2_x10/` | a loose S3 copy: the `bench/hits_v2/` prefix copied ten times. That prefix holds partition 0 of the catalog table (97 live files plus 17 retired ones); the `__p1` and `__p2` partitions are not in it. Not a catalog table | 1,140 | 64.3 GB | not comparable | **the head-to-head**: Lambda runners against one query pod, same file list |
| `bench.hits_v2` | the DuckLake table (ClickBench hits, 105 columns), 3 write partitions | 240 (245 on the day) | 9.3 GB | 99,997,497 | the cluster's normal distributed path, for reference only |
| `bench/onebrc_v6/` | the One Billion Row Challenge as Parquet | 6 | 1.24 GB | 987M | one runner's CPU-bound rate |

The hits query is the same everywhere:

    SELECT RegionID, sum(AdvEngineID), count(*), sum(ResolutionWidth),
           count(ResolutionWidth) ... GROUP BY RegionID

It reads 3 of 105 columns, so "scans 64 GB" means 64 GB of Parquet files
are the input; the bytes pulled from S3 are those three columns' pages.

## Head-to-head: the same 64 GB, the same query

| who | wall | what the time includes |
|---|---:|---|
| 100 Lambda runners, cold | 4.6 s | leasing 100 runners in parallel (1.3 to 2.9 s each) plus the scan |
| 100 Lambda runners, warm | **1.75 s** | runners already connected: scan plus merge |
| 50 Lambda runners, warm | 3.69 s | |
| one query pod | 33 to 35 s | one engine, 32 threads, over the same 1,140 files |

Shards were cut greedily by bytes, about 640 MB per runner at 100. A
runner scanned about 470 MB/s of Parquet for this query. 100 runners
pulled about 35 GB/s from one prefix with no sign of S3 throttling.

| run | runners | leases | wall | scan per shard p50 / p90 / max |
|---|---:|---|---:|---|
| x10, run 1 | 100 (88 distinct environments) | cold, p50 1.81 s | 5.36 s | 2,435 / 2,584 / 2,730 ms |
| x10, run 2 | 100 | cold, p50 1.33 s | 4.57 s | 1,704 / 1,872 / 2,059 ms |
| x10 | 50 | warm, 2 ms | 3.69 s | 2,706 / 2,881 / 3,061 ms |
| x10 | 100 | warm, 2 ms | 1.75 s | 1,363 / 1,530 / 1,644 ms |
| one query pod, run 1 / run 2 | 1 | | 35.2 s / 32.7 s | |

## Reference: what the cluster does today

Not the same input. This is `POST /v1/queries` on the real `bench.hits_v2`
table (9.3 GB, 240 files) through the three query pods, which the planner
split into 3 shards. The x10 copy is not a catalog table, so the cluster
path could not run on it.

| run | wall |
|---|---:|
| three runs | 3.5 / 2.6 / 2.7 s |

To compare across sizes, use throughput over input bytes:

| path | input | GB/s |
|---|---|---:|
| one query pod | 64 GB copy | about 1.9 |
| cluster, 3 query pods | 9.3 GB table | about 3 |
| burst, 100 runners, cold | 64 GB copy | about 14 |
| burst, 100 runners, warm | 64 GB copy | about 37 |

At the cluster's rate, 64 GB would take about 20 s. Registering the copy as
a catalog table would make this a measured number; T-683 should do that.

A partial run also exists: 41 runners over the first 41 of the table's 240
files (3.2 GB, a truncated listing, see "What held, what broke"), cold
leases p50 2.15 s, wall 3.14 s, scan per shard 271 / 656 / 716 ms. It is a
per-runner throughput sample, not a table scan.

## Boot: how fast a runner is ready

| what | measured |
|---|---:|
| Lambda init, first-ever invocation after a deploy (image caching) | 4,159 ms |
| Lambda init, fresh environment, image cached (3 samples) | 49, 458, 50 ms |
| Lambda init, average over 119 inits | 118 ms |
| lease on a fresh environment, first-ever for the image (init + BEAM boot + connect) | 11,937 ms |
| lease on a fresh environment, image cached | 6,448 ms |
| lease on a reused environment (bootstrap waiting, files in page cache) | 1,360 ms |
| call on a connected runner | 1 to 2 ms |
| 24 concurrent leases, each held 4 s: lease p50 / p90 / max | 2,067 / 2,620 / 2,886 ms; 24 distinct runners; batch wall 6.9 s |
| 100 concurrent leases: lease p50 / p90 / max, run 1 | 1,807 / 2,317 / 2,426 ms; 88 distinct runners |
| 100 concurrent leases: lease p50 / p90 / max, run 2 | 1,327 / 1,707 / 2,026 ms; 100 distinct runners |

A cold lease is 1.3 to 2.9 s, not sub-second. Lambda's own init is 50 to
460 ms once the image is cached; the rest is the BEAM booting from an image
Lambda streams on demand. A fresh environment (6.4 s) against a reused one
(1.36 s) is that streaming; the 100-runner batches saw 1.3 to 1.8 s medians
because those environments had the image cached. The BEAM boot itself, with
files cached, is about 1.2 s.

## One runner's rate on a CPU-bound scan

onebrc, `count(*), max(length(station)), round(sum(temperature))`, both
columns read, one runner, 6 threads, `memory_limit` 7 GB:

| files | rows | wall | rate |
|---:|---:|---:|---:|
| 1 (277 MB) | 220.8M | 10.0 s | 22M rows/s, 28 MB/s |
| 6 (1.24 GB) | 987.0M | 43.8 s | 22.5M rows/s |
| 6 files on 6 runners, cold | 987.0M | 14.9 s wall | includes 2 to 5 s leases |

This scan is CPU-bound on 6 vCPUs, not network-bound.

## Memory and cost

| | |
|---|---:|
| peak runner memory (Lambda report), all runs | 497 MB of 10,240 MB |
| typical billed duration per lease | 64 to 73 s (60 s of it `idle_shutdown_after`) |
| invocations this afternoon | 243 |
| billed | 168,836 GB-s |
| cost (arm64, $0.0000133334 per GB-s) | $2.25 |

Memory is irrelevant: 10 GB buys 6 vCPUs, which is what the scan uses. The
idle window is the whole bill. A query's own work was 2 to 5 s per runner
and the runner then sat 60 s before shutting down.

## Lifetime: the 15-minute cap

The runner held past the cap ended at exactly 900,000 ms with
`Status: timeout` in Lambda's report. On the parent the runner's
Terminator went `:noconnection`, the `FLAME.Runner` stopped with
`no connection`, and the pool killed the caller checked out on it with
`:kill` (so a `catch :exit` around `FLAME.call` does not see it; the
caller just dies). The pool itself survived and leased again.

That next lease took 52.5 s, and not because of Lambda: the backend's
HTTP pool held about 50 keep-alive connections to the Lambda API from the
100-runner burst, the API had closed them during the quiet quarter hour,
and each boot attempt took one dead connection, failed with
`Req.TransportError{reason: :closed}` before sending, and FLAME retried a
second later. Fixed on the branch: the invoke sends `Connection: close` and
retries only a `:closed` transport error. A `max_lifetime` under 15
minutes on the runner (D8) turns the hard kill into a graceful shutdown.

## What held, what broke

- `-dist_listen false` on the runner: connects outbound, hidden, no epmd
  registration, work spawned on it runs. Proven locally first, then on
  Lambda.
- The bootstrap loop: one invocation = one BEAM; idle shutdown exits the
  BEAM, the invocation completes, the next event on the same environment
  boots a fresh BEAM in 1.36 s.
- The parent's node name had to be `smolquery@<pod ip>`: the runner
  resolves it through epmd on the pod. A production API pod's
  headless-service name would not resolve from a Lambda (T-689 stands).
- Kubernetes injected `SMOLQUERY_API_PORT=tcp://…` from the `smolquery-api`
  Service into the parent pod; `enableServiceLinks: false` fixed it.
- `bin/smolquery rpc` inherited the pinned dist port and died with
  `eaddrinuse`; the rpc client runs with `ELIXIR_ERL_OPTIONS=`.
- A `kubectl exec` stream that prints nothing for about four minutes is
  cut, which killed a long `FLAME.call`'s caller; long measurements run
  detached on the parent.
- Lambda refuses an OCI image index; `crane copy --platform linux/arm64`
  flattens it. The sandbox nodes are x86, so the parent needs an amd64
  image; the spike workflow builds each on a native runner.
- The rpc helper printed a catalog listing through `IO.inspect` without
  `printable_limit: :infinity`, which cuts a string at 4,096 characters, so
  `bench.hits_v2` looked like 41 files instead of 240. That is why one run
  above is a partial scan and why the 64 GB copy was built from the S3
  prefix rather than from the catalog.

## What this decides for PL-77

- Lambda is a go. Build the backend (T-682) on this spike's code.
- A cold lease is 1.3 to 2.9 s. Sub-second leases need a connected runner:
  either a warm pool (`min` > 0, billed while idle) or a runner whose BEAM
  survives the invocation and is re-parented on the next one (a follow-up
  design; FLAME has no re-parenting).
- `idle_shutdown_after` is the cost lever, not memory. 10 to 15 s fits a
  dashboard's burst of queries; 60 s cost ten times the work.
- 100 runners against one S3 prefix showed no throttling at 64 GB in
  under 2 s. Measure again at 500 and 1,000 (T-683), and register the x10
  copy as a catalog table first so the cluster path is measured, not
  extrapolated.
- The x10 copy (`bench/hits_v2_x10/`) stays in the bucket for the L4 bench.
