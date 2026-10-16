# Burst spike: FLAME runners on AWS Lambda (T-673, PL-77)

| | |
|---|---|
| Run | 2026-10-16 |
| Branch | `t673-lambda-spike` at `652b6f0` |
| Parent | one pod in the `smolquery-dev` EKS cluster (eu-central-1), `SMOLQUERY_ROLES=none`, node `smolquery@<pod ip>`, dist port pinned to 9100 |
| Runner | Lambda `smolquery-burst-runner`: container image (release + `rel/lambda/bootstrap`), arm64, 10,240 MB (6 vCPU), 10 GB ephemeral, 900 s, in the cluster VPC |
| Data | `bench/onebrc_v6` (6 files, 1.24 GB, 987M rows); the first 41 of `bench.hits_v2`'s 240 catalog files (3.2 GB, a truncated listing, see below); `bench/hits_v2_x10` (a tenfold S3 copy of the `bench/hits_v2/` prefix: 1,140 files, 64.3 GB; partition 0's live and retired files, not the `__p1` and `__p2` partitions) |
| Cluster baseline | `POST /v1/queries` on `bench.hits_v2` through `smolquery-query` (3 query pods, c6a.4xlarge, 32 threads each); one query pod's engine over the x10 files |

## Headline

**A Lambda can host a FLAME runner.** The runner boots the release with
`-dist_listen false`, connects back to the parent over distribution as a
hidden node, takes `FLAME.call` work, idles out, and the bootstrap loop
hands the same environment to the next lease. Nothing in FLAME needed a
change.

**100 runners scan 64 GB in 4.6 s cold and 1.75 s warm.** One production
query pod (16 vCPU) needs 33 to 35 s for the same scan. The cluster's own
distributed path answers the same query over the 9.3 GB catalog table in
2.6 to 3.5 s.

**A cold lease is 1.3 to 2.9 s, not sub-second.** Lambda's own init is 50
to 460 ms once the image is cached; the rest is the BEAM booting from a
lazily loaded image. A warm runner answers in 1 to 2 ms.

## Boot

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
| 100 concurrent leases (x10 scan): lease p50 / p90 / max, run 1 | 1,807 / 2,317 / 2,426 ms; 88 distinct runners |
| 100 concurrent leases (x10 scan): lease p50 / p90 / max, run 2 | 1,327 / 1,707 / 2,026 ms; 100 distinct runners |

The gap between a fresh environment (6.4 s) and a reused one (1.36 s) is
the BEAM reading its files from an image Lambda streams on demand; the
later 100-runner batches saw 1.3 to 1.8 s medians because those workers had
the image cached. The BEAM boot itself, with files cached, is about 1.2 s.

## Scan

onebrc (`count(*), max(length(station)), round(sum(temperature))`, both
columns read), one runner, 6 threads, `memory_limit` 7 GB:

| files | rows | wall | rate |
|---:|---:|---:|---:|
| 1 (277 MB) | 220.8M | 10.0 s | 22M rows/s, 28 MB/s |
| 6 (1.24 GB) | 987.0M | 43.8 s | 22.5M rows/s |
| 6 files on 6 runners, cold | 987.0M | 14.9 s wall | includes 2 to 5 s leases |

The onebrc scan is CPU-bound on 6 vCPUs, not network-bound.

hits (`SELECT RegionID, sum(AdvEngineID), count(*), sum(ResolutionWidth), count(ResolutionWidth) ... GROUP BY RegionID`, 3 of 105 columns read):

| data | runners | leases | wall | scan p50 / p90 / max |
|---|---:|---|---:|---|
| 41 of the 240 `bench.hits_v2` catalog files, 3.2 GB | 41 (38 distinct) | cold, p50 2.15 s | 3.14 s | 271 / 656 / 716 ms |
| x10, 1,140 files, 64.3 GB | 100 (88 distinct) | cold, p50 1.81 s | 5.36 s | 2,435 / 2,584 / 2,730 ms |
| x10 | 50 | warm, 2 ms | 3.69 s | 2,706 / 2,881 / 3,061 ms |
| x10 | 100 | cold, p50 1.33 s | 4.57 s | 1,704 / 1,872 / 2,059 ms |
| x10 | 100 | warm, 2 ms | **1.75 s** | 1,363 / 1,530 / 1,644 ms |

Shards were cut greedily by bytes (about 640 MB per runner at 100). A
runner scanned about 470 MB/s of parquet for this three-column query.

Baselines for the same query and files:

| where | wall |
|---|---:|
| one production query pod, 16 vCPU, 32 threads, x10 files, run 1 / run 2 | 35.2 s / 32.7 s |
| the cluster's distributed path, `bench.hits_v2` (245 files, 9.3 GB, 100M rows), 3 shards, 3 runs | 3.5 / 2.6 / 2.7 s |

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
- A `Req` pool full of keep-alive connections the Lambda API had closed cost one lease 52 s of `:closed` retries; the invoke now closes its connection.
- The parent's catalog listing showed 41 files for `bench.hits_v2` while the
  API reported 245. The catalog was right: the rpc helper printed the list
  through `IO.inspect` without `printable_limit: :infinity`, which cuts a
  string at 4,096 characters, and the saved file is 4,104 bytes ending in
  `<> ...`. A query pod lists 240 live files, 9.29 GB, 99,997,497 rows
  (245 earlier, before `merge_adjacent` compactions), the same as the API.
  The 41-file run above is therefore a partial scan, still valid as a
  per-runner throughput sample. The x10 copy was built from the S3 prefix,
  not the catalog, so it is 114 files times ten: the 97 live files of
  partition 0 plus 17 retired ones, without the two `__p` partitions. Both
  the Lambda and the single-pod baselines read that same list.

## Lifetime

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

## What this decides for PL-77

- Lambda is a go. Build the backend (T-682) on this spike's code.
- A cold lease is 1.3 to 2.9 s. Sub-second leases need a connected runner:
  either a warm pool (`min` > 0, billed while idle) or a runner whose BEAM
  survives the invocation and is re-parented on the next one (a follow-up
  design; FLAME has no re-parenting).
- `idle_shutdown_after` is the cost lever, not memory. 10 to 15 s fits a
  dashboard's burst of queries; 60 s cost ten times the work.
- 100 runners against one S3 prefix showed no throttling at 64 GB in
  under 2 s (about 35 GB/s aggregate). Measure again at 500 and 1,000.
- The x10 copy (`bench/hits_v2_x10/`) stays in the bucket for the L4 bench.
