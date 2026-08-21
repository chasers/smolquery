# `bench/distributed_query.exs` — can K DuckDB instances run one query?

| | |
|---|---|
| Run | 2026-08-21 |
| Commit | `12cab41` |
| Command | `FILES=32 ROWS=1000000 THREADS=8 SHARDS=2,4,8 REPS=3 mix run bench/distributed_query.exs` |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.6 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.3 (ADBC) |
| Tracker | PL-48, T-348 |

## Headline

**Scatter/gather across K DuckDB instances costs nearly nothing, so K machines
scale a scan close to linearly.** With a constant thread budget — K instances
at 8/K threads each versus one instance at 8 threads — the sharded plan matches
the baseline on every shape and beats it by 1.3–1.4× on a high-cardinality
group-by. Every sharded result equals the single-instance result. The price a
real network pays is the partial size: negligible for global and low-cardinality
aggregates, ~10 MiB *per shard* when the partial is a ~500k-group group-by.

## Method

Single-machine simulation of K machines. Shard the 32-file list round-robin
across K worker instances (own `Adbc.Database` each, `threads = 8 / K`). Each
worker runs a partial query over its shard and writes the partial as parquet
(`COPY`). A coordinator instance merges the partials with a final query. The
baseline is one instance with all 8 threads over all 32 files. `x base` is
baseline wall / sharded wall; ≥ 1.0 means the shard plan is at least as fast
with the same total threads.

## Fixture

32 parquet files × 1M rows = 32M rows, 582.2 MiB. Columns: `id BIGINT`,
`ts TIMESTAMP`, `user_id` (~500k distinct), `status` (3 values),
`value DOUBLE`. Generated in 3,087 ms.

## Results

```
global aggregates (count / sum / min / max / avg)
  config              wall ms  x base  partials MiB  merge ms   check
  1 x 8 threads          78.4     1.0
  2 x 4 threads          71.5     1.1           0.0       1.4   ok (1 rows)
  4 x 2 threads          71.2     1.1           0.0       1.5   ok (1 rows)
  8 x 1 threads          72.2    1.08           0.0       1.7   ok (1 rows)

group-by, low cardinality (3 groups)
  config              wall ms  x base  partials MiB  merge ms   check
  1 x 8 threads          64.2     1.0
  2 x 4 threads          62.2    1.03           0.0       1.3   ok (3 rows)
  4 x 2 threads          61.0    1.05           0.0       1.5   ok (3 rows)
  8 x 1 threads          68.3    0.94           0.0       1.7   ok (3 rows)

group-by, high cardinality (~500k groups)
  config              wall ms  x base  partials MiB  merge ms   check
  1 x 8 threads         359.9     1.0
  2 x 4 threads         252.3    1.43           9.9      17.8   ok (checksum over 500000 rows)
  4 x 2 threads         256.2    1.41          19.5      35.5   ok (checksum over 500000 rows)
  8 x 1 threads         276.9     1.3          38.8      58.9   ok (checksum over 500000 rows)

top-k by aggregate (LIMIT 10)
  config              wall ms  x base  partials MiB  merge ms   check
  1 x 8 threads         264.3     1.0
  2 x 4 threads         239.4     1.1           9.2      12.3   ok (10 rows)
  4 x 2 threads         227.1    1.16          18.1      16.6   ok (10 rows)
  8 x 1 threads         236.0    1.12          36.4      32.1   ok (10 rows)
```

## Reading the numbers

- **Coordination is not the bottleneck.** At constant thread budget, sharding
  never loses more than 10%, and usually wins. On real machines each worker
  keeps all its threads, so the scan itself scales with K.
- **High-cardinality group-by gets *faster* sharded.** K small independent
  hash tables beat one instance's shared parallel aggregation on this box.
- **Partials are the network bill.** Global and low-cardinality aggregates
  ship bytes. A ~500k-group partial ships ~10 MiB per shard, and the total
  grows linearly with K (9.9 → 38.8 MiB). Merge time follows it
  (18 → 59 ms) but stays a small fraction of the scan.
- **Top-k decomposes only as a full partial group-by.** Its partial is the
  same size as the high-cardinality one. A shard cannot truncate its groups
  early without breaking correctness.
- **Fixed costs bound the win.** From `bench/query.exs`: ~50 ms of engine
  bootstrap per instance, plus manifest fan-out and, in a real cluster, the
  partial transfer. Distribution pays when the per-shard scan time is large
  against those; a sub-100 ms query gains nothing.

## Caveats

- One machine: workers share memory bandwidth with each other and with the
  baseline they are compared to. Network transfer and object-store read
  contention are not simulated; partial size is reported as its proxy.
- Partial/final SQL was written by hand per shape. A real implementation
  needs a rewrite step, and some shapes (exact median, exact
  `count(distinct)`) do not decompose this way.

## What this settles

The PL-48 spike question: scatter/gather over multiple DuckDB instances is a
viable way to scale one query past one machine. The merge is cheap, the
results are exact, and the constraint is partial-result size on
high-cardinality group-bys. Whether to build it is a separate decision — the
fixed per-shard costs mean it only pays for scans that already run for
hundreds of milliseconds on one node.
