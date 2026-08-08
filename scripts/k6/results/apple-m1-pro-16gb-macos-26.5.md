# Insert load: smolquery vs ClickHouse — Apple M1 Pro, 16 GB, macOS 26.5.1

Measured 2026-08-08 with `scripts/k6/insert.js` and `scripts/k6/watch.go`.

Every number below is one 60-second run against a server in its own OS process,
on an otherwise idle machine. Only one database ran at a time.

## Machine and software

| item | value |
|---|---|
| CPU | Apple M1 Pro, 10 logical cores |
| memory | 16 GiB |
| OS | macOS 26.5.1 (darwin 25.5.0) |
| disk | APFS, 235 GiB free at the end of the series |
| k6 | v2.1.0 (go1.26.4, darwin/arm64) |
| ClickHouse | 25.8.29.51 LTS, repo-local binary, `scripts/clickhouse/up.sh` |
| smolquery | 0.5.0, `MIX_ENV=prod`, Elixir 1.20.2 / OTP 29, branch `bench/clickhouse-compare` at `55316ec` plus uncommitted working-tree changes |
| payload | 3062 rows x 62 OTel columns, `SEED=42`, `PROJECTS=1000` |

`%CPU` is percent of one core. **1000 is the whole machine.**

## Method

What was held equal:

* **Same rows.** All three body files come out of one `generate.js` run, so the
  arms differ by wire format, not by data.
* **Same clock.** 60 s measured, `gracefulStop=5s`, on every run.
* **Same sort key.** ClickHouse `ORDER BY (project_id, timestamp)`; the smolquery
  table gets the same pair as its clustering key.
* **Cold table every run.** Each run wipes the data directory, starts the server,
  runs a 20 s warmup at the same settings, waits 15 s, then measures. Without
  this the third run of a series was 20% slower than the first purely because
  the table had grown.
* **One server at a time.** Starting either arm stops the other first.
* **No compression** on either side. The body goes on the wire as it sits on disk.

What was measured on both sides: server CPU and RSS, **and k6's own CPU and RSS**,
sampled every 500 ms from the process table.

Row counts were checked after every run. **All 13 runs matched exactly**: rows at
rest always equalled `accepted requests x 3062`. Neither engine lost or
duplicated a row.

## smolquery

Columnar body (3.28 MiB) unless stated. `200` means the rows are durable
(manifest fsynced) and queryable.

| run | mode | rows/s | p50 | p95 | p99 | max |
|---|---|---:|---:|---:|---:|---:|
| `sq-col-vus1` | 1 VU closed | 30,887 | 97.5 | 106.0 | 140.8 | 149.3 |
| `sq-col-vus4` | 4 VU closed | **74,803** | 156.1 | 207.6 | 240.4 | 294.2 |
| `sq-col-vus8` | 8 VU closed | 76,298 | 315.5 | 459.7 | 499.6 | 550.9 |
| `sq-row-vus4` | 4 VU closed, row-major 6.42 MiB | 60,289 | 199.1 | 234.6 | 261.2 | 308.2 |
| `sq-col-rate20` | 20/s open | 61,156 | 135.4 | 180.9 | 213.7 | 269.9 |
| `sq-col-rate30` | 30/s open | 82,900 | 1761.5 | 3561.8 | 4240.2 | 4345.0 |

Latency is milliseconds.

| run | server avg %CPU | peak %CPU | mean RSS | peak RSS | k6 avg %CPU | k6 peak RSS | k6 share of CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sq-col-vus1` | 110 | 339 | 2199 MiB | 2463 MiB | 2.3 | 68 MiB | 1.8% |
| `sq-col-vus4` | 384 | 642 | 3091 MiB | 3558 MiB | 5.4 | 108 MiB | 1.2% |
| `sq-col-vus8` | 416 | 716 | 3469 MiB | 4001 MiB | 6.2 | 160 MiB | 1.3% |
| `sq-row-vus4` | 395 | 674 | 2913 MiB | 3275 MiB | 7.4 | 165 MiB | 1.6% |
| `sq-col-rate20` | 297 | 586 | 3157 MiB | 3741 MiB | 4.6 | 301 MiB | 1.3% |
| `sq-col-rate30` | 455 | 860 | 5350 MiB | 7770 MiB | 7.0 | 936 MiB | 1.4% |

`sq-col-rate30` dropped 59 iterations and the drain was 4.3 s of a 64.3 s window.
It is past saturation and is a saturation probe, not a throughput result.

## ClickHouse

`JSONEachRow` body (6.41 MiB). `async_insert` off throughout.

| run | mode | fsync | rows/s | p50 | p95 | p99 | max |
|---|---|---|---:|---:|---:|---:|---:|
| `ch-vus1` | 1 VU closed | no | 120,189 | 24.9 | 26.3 | 28.7 | 50.0 |
| `ch-vus4` | 4 VU closed | no | 359,141 | 32.0 | 44.3 | 52.0 | 90.6 |
| `ch-vus8` | 8 VU closed | no | **368,347** | 61.4 | 107.6 | 132.9 | 186.5 |
| `ch-rate20` | 20/s open | no | 61,238 | 25.2 | 26.8 | 28.0 | 50.3 |
| `ch-rate150` | 150/s open | no | 234,840 | 302.9 | 1081.6 | 2809.4 | 8234.2 |
| `ch-vus1-fsync` | 1 VU closed | yes | 77,493 | 38.7 | 43.3 | 46.8 | 51.3 |
| `ch-vus4-fsync` | 4 VU closed | yes | 165,814 | 73.4 | 91.9 | 100.6 | 121.0 |

| run | server avg %CPU | peak %CPU | mean RSS | peak RSS | k6 avg %CPU | k6 peak RSS | k6 share of CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ch-vus1` | 217 | 336 | 1325 MiB | 2073 MiB | 12.6 | 99 MiB | 4.8% |
| `ch-vus4` | 666 | 827 | 2030 MiB | 2909 MiB | 40.9 | 189 MiB | 5.0% |
| `ch-vus8` | 715 | 859 | 2222 MiB | 3239 MiB | 38.8 | 312 MiB | 4.5% |
| `ch-rate20` | 127 | 244 | 1545 MiB | 2069 MiB | 5.9 | 491 MiB | 3.8% |
| `ch-rate150` | 558 | 874 | 2366 MiB | 3298 MiB | 84.6 | 2557 MiB | 12.0% |
| `ch-vus1-fsync` | 148 | 252 | 1362 MiB | 1942 MiB | 8.3 | 100 MiB | 4.6% |
| `ch-vus4-fsync` | 262 | 464 | 1650 MiB | 2406 MiB | 15.2 | 171 MiB | 4.8% |

`ch-rate150` dropped 4207 iterations. It is a saturation probe.

## The comparison

The only pair where a `200` means close to the same thing on both sides is
**4 VUs closed loop, fsync on**:

| | smolquery | ClickHouse (fsync) | ratio |
|---|---:|---:|---:|
| rows/s | 74,803 | 165,814 | **2.2x** |
| p50 latency | 156.1 ms | 73.4 ms | 2.1x |
| p99 latency | 240.4 ms | 100.6 ms | 2.4x |
| server avg %CPU | 384 | 262 | 1.5x |
| server peak RSS | 3558 MiB | 2406 MiB | 1.5x |
| CPU-seconds per million rows | 59.4 | 18.3 | **3.2x** |

ClickHouse moves 2.2x the rows while using less CPU and less memory. Per row it
costs 3.2x less CPU.

Against ClickHouse's own default (no fsync), the gap at 4 VUs is 4.8x.

### CPU cost per million rows

This is the ratio that does not depend on how many clients were used:

| arm | 1 VU | 4 VU | 8 VU |
|---|---:|---:|---:|
| smolquery, columnar | 41.2 | 59.4 | 62.9 |
| ClickHouse, no fsync | 20.9 | 21.5 | 22.5 |
| ClickHouse, fsync | 22.1 | 18.3 | — |

ClickHouse holds near 20 CPU-seconds per million rows across the whole range.
smolquery starts at 41 and gets worse as concurrency rises — it spends more CPU
per row when more clients are writing, which is what a contention cost looks
like.

**fsync costs ClickHouse wall time, not CPU.** Throughput falls 2.2x with fsync
on, but CPU per million rows barely moves (21.5 to 18.3). The server is waiting
on the disk, not computing.

## Findings

1. **smolquery saturates at 4 concurrent writers.** Going from 4 to 8 VUs buys
   2% more throughput (74,803 to 76,298) and doubles p50 latency (156 ms to
   316 ms). The queue is behind the server, not in front of it.

2. **ClickHouse saturates at about the same point, much higher up.** 4 to 8 VUs
   gives 3% more throughput (359k to 368k) at double the latency. Its ceiling is
   4.8x higher.

3. **Neither ceiling is k6's fault.** k6 used 1.2–1.6% of total CPU on the
   smolquery runs and 3.8–5.0% on the ClickHouse runs. The one exception is the
   150/s overload probe, where k6 reached 12% of total CPU and 2.5 GiB RSS —
   that run is generator-contaminated and is reported as a probe only.

4. **The columnar body is worth 24% on the smolquery arm.** Same rows, same
   count, 4 VUs: columnar 74,803 rows/s against row-major 60,289. It is also
   half the bytes (3.28 MiB against 6.42 MiB), so part of that is wire and part
   is parsing.

5. **smolquery's memory is its sharpest problem.** At 4 VUs it holds 3.5 GiB
   peak against ClickHouse's 2.4 GiB for 2.2x fewer rows. Pushed past saturation
   (`sq-col-rate30`) it reached **7.8 GiB peak on a 16 GiB machine** — inside
   swap distance while doing 83k rows/s. ClickHouse's peak never left 3.3 GiB in
   any run, including its own overload probe.

6. **Overload is destructive on both sides, and worse on ClickHouse.** At 150/s
   offered, ClickHouse delivered 234,840 rows/s — *less* than the 368,347 it
   sustained in a closed loop — with p99 at 2.8 s and max at 8.2 s. Its own 20 s
   warmup at the same rate held 454,740 rows/s with nothing dropped, so the
   collapse is what happens as parts accumulate, not an instant limit.

## END-TO-END: the DuckDB flush writer, integrated and measured through k6

Everything from "The storage floor" onwards is a **component** measurement. This
section is the whole database: HTTP, k6, a 200 the client waited for, and a
`SELECT count(*)` afterwards that had to match k6's accepted rows. It did, on
every run below.

`flush_writer: :duckdb` on `Smolquery.BufferService` switches the accumulator and
the flush:

* `application/x-ndjson` bodies spool to disk unparsed
  (`SmolqueryApi.InsertController`), counted by newline as they stream;
* `TableBuffer` accumulates **paths**, not rows — one small binary per request;
* the flush is one `COPY (SELECT * FROM read_json([paths…]) ORDER BY clustering)
  TO staged` through the buffer's own DuckDB instance
  (`Smolquery.Segments.Writer.write({:ndjson, paths}, …)`);
* manifest fsync, replication and the withheld ack are untouched.

One table, 60 s each:

| writer | VUS | `flush_max_bytes` | `encode_concurrency` | rows/s | server %CPU | p50 |
|---|---:|---:|---:|---:|---:|---:|
| polars | 8 | 4.5 MB | 1 | 77,246 | 410 | 308 ms |
| duckdb | 8 | 4.5 MB | 1 | 72,745 | **213** | 300 ms |
| duckdb | 16 | 4.5 MB | 1 | 73,289 | 217 | 611 ms |
| duckdb | 32 | 4.5 MB | 1 | 81,841 | 203 | 1152 ms |
| polars | 16 | 32 MB | 1 | 83,239 | 460 | 562 ms |
| duckdb | 16 | 32 MB | 1 | 207,279 | 289 | 217 ms |
| **duckdb** | 16 | 32 MB | 4 | **231,898** | 318 | **193 ms** |

**231,898 rows/s end to end against 83,239 for the Polars path — 2.8x, at 69% of
the CPU and a third of the latency.**

Three things the component bench could not say:

1. **At the default flush threshold the swap is worth nothing in throughput** —
   72,745 against 77,246 — and everything in CPU: 213% against 410%. Every
   request is its own flush there, and one `COPY` per 3062 rows is latency the
   client waits for.
2. **The gain appears only when bodies group.** At 32 MB several spooled bodies
   enter one `COPY`, which costs barely more than one did. Polars gains nothing
   from the same change (83,239) because its cost is per row, not per flush.
3. **The server is not CPU-bound on this path.** At 8, 16 and 32 VUs the DuckDB
   arm sits near 210% CPU and throughput does not move — only latency does. That
   is the serial per-table flush pipeline, not the machine.

### `encode_concurrency` stops paying at 4

Swept at VUS=16, `flush_max_bytes` 32 MB, one table:

| `encode_concurrency` | duckdb rows/s | server %CPU | p50 |
|---:|---:|---:|---:|
| 1 | 207,279 | 289 | 217 ms |
| 4 | 231,898 | 318 | 193 ms |
| 6 | 232,279 | 313 | 192 ms |
| 8 | 231,548 | 314 | 194 ms |
| 10 | 232,736 | 319 | 193 ms |

Flat from 4 onwards, within noise — 231.5k to 232.7k across 6, 8 and 10. That is
the shape of a mutex, and the mutex is named: the buffer's `Engine.Connection`
wraps **one** ADBC connection, so however many encode slots exist, their `COPY`s
queue for it. A connection per slot is the next lever and is not implemented.

The same knob **hurts** the Polars path: at VUS=16 and 32 MB it goes 83,239
(`enc=1`) → 58,602 (`enc=4`), a 30% loss, which is what `Committer`'s moduledoc
predicts — above 1 the rows are copied again into the encode task, and for a
term-shaped batch that copy costs more than the parallelism returns.

So the headline comparison uses each writer at its own best setting: Polars at
`enc=1`, DuckDB at `enc=4`. Comparing both at `enc=1` gives 207,279 against
83,239, which is 2.5x rather than 2.8x.

### A connection pool for the flush

`write_pool_size` starts N DuckDB instances for the buffer and picks one per
segment with `:erlang.phash2(segment_id, n)`. Hashed on the **segment**, not the
table: a table has one buffer and one committer, so hashing on the table would
send all of its flushes to one connection and the pool would do nothing for the
single-table case it exists for.

VUS=16, `flush_max_bytes` 32 MB, `encode_concurrency: 4`:

| `write_pool_size` | rows/s | server %CPU | p50 |
|---:|---:|---:|---:|
| 1 | 231,898 | 318 | 193 ms |
| 2 | 269,248 | 381 | 174 ms |
| **4** | **306,733** | 389 | 150 ms |
| 8 | 304,993 | 456 | 148 ms |

Four is the knee; eight buys nothing and costs 17% more CPU.

At p50 150 ms, sixteen closed-loop VUs can only offer ~326k rows/s, so that run
was near the *generator's* limit rather than the server's. Raising the load:

| VUS | pool | enc | rows/s | server %CPU | p50 |
|---:|---:|---:|---:|---:|---:|
| 16 | 4 | 4 | 306,733 | 389 | 150 ms |
| **32** | **4** | **4** | **383,157** | 464 | 242 ms |
| 32 | 8 | 8 | 345,219 | 488 | 274 ms |

**383,157 rows/s end to end, one table, verified** — at 464% CPU, so still less
than half the machine.

### It is not "just more DuckDB writers"

Three independent changes, and the pool is the smallest:

| change | rows/s | what it removed |
|---|---:|---|
| baseline, Polars | 83,239 | — |
| spool + DuckDB flush, same threshold | 72,745 | the Elixir term path: **CPU 410% → 213%** at equal throughput |
| + group bodies per flush (32 MB) | 207,279 | one flush per request |
| + `encode_concurrency: 4` | 231,898 | the committer blocking on one encode |
| + `write_pool_size: 4` | 306,733 | the single ADBC connection |
| + more offered load (VUS 32) | 383,157 | the generator's own ceiling |

The first change bought **no throughput at all** and halved CPU per row — it is
what made every later change able to pay. Without it more writers buy little, and
the Polars arm proves it: the same `encode_concurrency: 4` makes Polars 30%
*slower*.

What is still single, and untouched: one `TableBuffer`, one `Committer`, one
manifest log with its fsync, one replication round per flush.

### Against ClickHouse, same harness, end to end

| | rows/s | what a 200 means |
|---|---:|---|
| smolquery, polars | 83,239 | manifest fsynced, queryable, **every row validated** |
| **smolquery, duckdb** | **383,157** | manifest fsynced, queryable, **no per-row validation** |
| ClickHouse, `fsync_after_insert=1` | 165,814 | part fsynced |
| ClickHouse, default | 368,347 | part in the page cache |

Durability-matched, smolquery with the DuckDB writer is **2.3x ClickHouse**, and
it now edges past ClickHouse's unsynced default as well.

Read the middle row with its caveat attached: it is a full end-to-end measurement
of a **weaker contract**, not the same contract made faster. The Polars row runs
`Validator` on every value and returns per-index `insertErrors`; the DuckDB row
runs no validation and returns none.

### What this path gives up

The losses are in the contract, not the numbers:

* **No per-row validation.** `insertErrors` is always `[]`, and a value the
  schema cannot take fails the whole flush — every waiter in it — instead of one
  index. `docs/api.md` promises BigQuery-style partial failure for `/insert`;
  this route does not keep that promise. The JSON route is unchanged and does.
* **Row-major on the wire.** An unparsed body is NDJSON, so this arm moves
  6.41 MiB where the columnar JSON moves 3.28 MiB for the same rows. It wins
  despite twice the bytes.
* **Local owner only.** The batch is a path on this node's disk, so
  `IngestService.Client.insert_file/5` refuses with `:spooled_batch_not_local`
  rather than let `:gen_rpc` hand a remote owner a filename it cannot open.
* **Local stores only.** `COPY` writes to a filesystem path.
* The 8 MB body cap bounds an NDJSON request to about 3600 rows, against about
  7100 for the columnar shape.

## The storage floor

**Component measurements from here down.** The HTTP numbers above cannot be read
without knowing what the layers under them can do. Two benchmarks measure that, on the same rows, on the same machine,
with no server in the way:

* `scripts/duckdb` — Go with DuckDB linked in, writing to a `.duckdb` file.
* `bench/parquet_write.exs` — `Smolquery.Segments.Writer` alone, which is
  `Explorer.DataFrame` and Polars encoding Parquet in Rust. This is the terminal
  step of every smolquery write.

| what | rows/s | CPU | CPU-s per Mrow |
|---|---:|---:|---:|
| DuckDB, 1 writer, 1 batch per commit | 38,783 | 216% | 55.7 |
| DuckDB, 1 writer, 64 batches per commit | 142,170 | 162% | 11.4 |
| **DuckDB, 4 writers** | **361,123** | 427% | 11.8 |
| Elixir Parquet writer, 3062-row batch | 181,063 | 147% | 8.1 |
| **Elixir Parquet writer, 12k-row batch** | **258,489** | 145% | 5.6 |
| Elixir Parquet writer, 49k-row batch | 221,959 | 134% | 6.0 |
| Elixir Parquet writer, 196k-row batch | 119,442 | 123% | 10.3 |
| **smolquery over HTTP, 4 VUs** | **74,803** | 384% | 59.4 |

Three things follow.

1. **The bottleneck is above the writer, not in it.** Polars encodes Parquet at
   181,000 rows/s on the same batch size the HTTP arm posts, using 147% CPU.
   The HTTP arm delivers 74,803 rows/s at 384% CPU. Roughly **59% of the
   throughput and most of the CPU is spent before the rows reach the writer** —
   in the socket, the JSON decode, the validator and the buffer.
2. **DuckDB and ClickHouse are level on raw ingest.** 361,123 against 368,347.
   The single-writer figure of 38,783 is not DuckDB's ceiling; it is one
   goroutine's. Concurrent appends do not conflict, and four writers reach
   parity with ClickHouse's eight connections. Verified: every arm counts the
   table afterwards, and the advantage survives a forced checkpoint per batch.
3. **Batch size beats everything else on both floors.** DuckDB gains 3.7x from
   64 batches under one commit; the Elixir writer gains 1.4x from a 12k-row
   batch over a 3062-row one, then loses it again at 196k as the frame stops
   fitting comfortably.

Two costs turned out to be near-free on the Elixir side, measured at a
49k-row batch:

| | rows/s | vs baseline |
|---|---:|---|
| zstd, sorted by clustering key | 221,959 | — |
| zstd, unsorted | 226,038 | the sort costs 1.8% |
| snappy, sorted | 228,378 | zstd costs 2.8% over snappy |
| no compression, sorted | 228,548 | zstd costs 2.9% |

The clustering sort and zstd together cost under 3% of write throughput. Neither
is where the write path's budget goes.

Memory is the exception. The Elixir bench reached **9.7 GiB peak RSS**, almost
all of it in the 196k-row arm building one DataFrame. That is the same failure
mode `sq-col-rate30` showed over HTTP, reached from a different direction.

Caveat on the sizes: both benchmarks insert the same 3062-row batch repeatedly,
so bytes/row (5.7 for Parquet+zstd) is what a thousand copies of 3062 distinct
rows compress to, not what real data would. Ratios hold; absolutes do not.

## The ceiling is per table, not per node

Every closed-loop run above posts to one table. That turns out to be the
measurement, not a detail of it.

`TableBuffer`'s own moduledoc calls the process "the serialization point for a
table's writes", and `Committer` defaults `encode_concurrency` to `1`. So a
table's write path is: many request processes, one accumulator, one committer,
one Polars encode. Spreading the same eight writers over four tables:

| run | rows/s | server avg %CPU | peak %CPU | peak RSS |
|---|---:|---:|---:|---:|
| 8 VUs, 1 table | 76,298 | 416 | 716 | 4001 MiB |
| 8 VUs, 4 tables (2 each) | **133,251** | 664 | 958 | 3929 MiB |

**1.75x the throughput from the same number of writers**, and the server finally
reaches the machine — 958% peak against 716%. Rows at rest matched per table.

This reframes the single-table numbers. smolquery was never CPU-bound on this
box at 8 VUs; it was waiting on one GenServer per table. Going 4 to 8 VUs on one
table bought 2% because the queue was already behind the accumulator.

It also says where the remaining gap to ClickHouse is. ClickHouse has no
per-table serialization point: every `INSERT` writes an independent part, which
is why eight connections use seven cores. smolquery's segments are already
immutable and merged later — the same model — so nothing in the design forbids
several accumulators per table. There is simply one.

### How far more buffers go

Eight writers, held constant, spread over more buffers:

| buffers (tables) | rows/s | server avg %CPU |
|---:|---:|---:|
| 1 | 76,874 | 408 |
| 2 | 120,529 | 661 |
| **4** | **133,251** | 664 |
| 8 | 127,551 | 661 |

The first extra buffer is worth +57%. Four is the knee; eight is worse. CPU stops
at ~661% either way, so ~133,000 rows/s is not the machine running out of cores —
what limits it there is not identified.

### Micro-segments carry no min/max statistics

`bench/segment_encoder.exs` writes segments with the real `Writer` and then reads
them back with `parquet_metadata`. Of 62 columns:

| writer | columns | with min | with max | with null count |
|---|---:|---:|---:|---:|
| Polars, via `Writer` | 62 | **0** | **0** | 62 |
| DuckDB `COPY` | 62 | 58 | 58 | 62 |

Polars writes null counts and no bounds. DuckDB writes bounds for every column
that has a non-null value — the four missing ones are `error_type`,
`exception_type`, `exception_message` and `exception_stacktrace`, which the
generator emits as all-null.

Source-verified, not inferred:

* `deps/explorer/native/explorer/src/dataframe/io.rs:259` — the eager writer is
  `ParquetWriter::new(…).with_compression(…).finish(…)`; `with_statistics` is
  never called.
* `deps/explorer/native/explorer/src/lazyframe/io.rs:87` and `:132` — the
  streaming writer sets `statistics: StatisticsOptions::empty()` outright.
* `Explorer.DataFrame.to_parquet/3` validates `compression`, `streaming` and
  `config` only. **There is no `:statistics` option**, so this cannot be turned on
  from Elixir.

#### But the footer is not where the hot tier prunes

Missing footer statistics matter less than they look, and the reason is worth
being precise about.

`Writer.stats/2` computes min/max/null_count from the in-memory DataFrame and puts
them in the `Segment`, which becomes a `HotManifest.Entry`.
`Smolquery.QueryService.Pruner` then drops micro-segments from *those* stats
"before their URLs are built, which is before DuckDB pays an HTTP footer read for
each". So hot-tier file-level pruning never consulted the Parquet footer, and for
timestamps — which PL-1 names as "the pruning that matters" — it works today.

The gap is narrower and sharper than "no statistics":

* `Writer`'s `@orderable` is `[:int64, :float64, :timestamp, :date]`, so
  **string columns get `min: nil, max: nil`** in the manifest.
* That is not a choice. `Explorer.Series.min/1` raises for `:string` — verified:
  *"not implemented for dtype :string"*, and the valid list is numerics, dates and
  durations only.
* `Pruner`'s own rule: "a column without stats prunes nothing".

So for `project_id` — the **first column of the clustering key** — nothing can
prune anywhere. Not the manifest, because Explorer cannot compute a string bound.
Not the footer, because Polars writes none. A tenant-filtered query over the hot
tier reads every micro-segment's data.

That is what the clustering sort at flush was for, and it is the one case where it
currently delivers nothing.

A second, smaller point: at 3062 rows a segment is **one row group** on both
sides, well under `ROW_GROUP_SIZE 16384`. So that knob does nothing at flush size
and only bites on sealed files, and within-segment pruning is impossible at flush
size regardless of statistics.

### Replacing Polars with DuckDB

Same rows, same sort, same codec, 20 s per arm:

| arm | rows/s | avg %CPU | CPU-s per Mrow | bytes/row |
|---|---:|---:|---:|---:|
| `polars` — `Writer.write/3` today | 181,568 | 147 | 8.1 | 183.5 |
| **`duckdb encode`** — `COPY` from a resident table | **220,125** | 109 | **4.9** | 179.0 |
| `duckdb parse+encode` — `COPY` from `read_json` | 150,575 | 152 | 10.1 | 179.0 |

Swapping the encoder is worth **+21% throughput, −40% CPU per row, −2.5% bytes**.

It also closes the string-bounds gap as a side effect, which is the more valuable
half: the DuckDB-written segments carry `project_id` bounds
(`aanrqjjrtwokvqpwrwdw..zyttytvhqqrfjrtuhcna` in the run above), so DuckDB can skip
a micro-segment on a tenant predicate without decompressing it. Neither the
manifest nor Polars can supply that today, and Explorer cannot be made to.

The two changes are independent, though. Adding string bounds to the manifest would
need a min/max Explorer does not implement for `:string`, so it means computing it
in Elixir over the column or reaching past `Series`. Switching the encoder gets it
for free, in the footer, where `Pruner` does not currently look.

Run twice, the second time with nothing else resident on the machine; the two
agreed to within 0.5% on throughput. The first run had an idle smolquery node left
over holding ~2.5 GiB, which is why the `duckdb encode` CPU figure moved from 116%
to 109% between them — the throughput numbers did not move.

The third arm is the other reading of "drop Polars" — spool the request body and
let DuckDB parse it too, so no row ever becomes an Elixir term. At the segment
level that is *slower* than today, because it re-parses JSON from disk. Its value
would be upstream, in the 63% of the write path that `bench/results/pathprof.md`
attributes to HTTP, decode and validate — which this bench does not measure.

It is also not a drop-in: it performs no per-row validation and returns no
per-index errors, which `Validator` does and the API contract promises. Read it as
a ceiling for a redesign, not as a swap.

### The accumulator as a DuckDB table

`bench/buffer_duckdb.exs` keeps the GenServer and replaces `state.chunks`: each
request becomes `INSERT INTO buf SELECT * FROM read_json(body)`, and the flush is
`COPY (SELECT * FROM buf ORDER BY project_id, timestamp) TO segment` followed by
`DELETE FROM buf`. No row becomes an Elixir term at any point.

`k` is requests per flush — the axis `flush_max_bytes` controls. Both fair arms
start from the same bytes on disk:

| arm | k=1 | k=4 | k=16 | CPU-s per Mrow |
|---|---:|---:|---:|---:|
| Elixir: decode + coerce + Polars | 22,642 | 38,875 | 30,304 | 45.5 |
| **DuckDB: `read_json` + `COPY`** | **89,927** | **110,459** | **98,684** | **12.4** |
| _(reference)_ Polars from ready-made columns | 161,244 | 237,168 | 188,756 | 8.9 |

**4.0x the throughput and 3.7x less CPU per row**, single-threaded on both sides.
The third row is the same Polars call handed pre-parsed columns, i.e. with the
decode excluded — it is there to show that the difference is the parse, not the
encoder.

The Elixir arm's 22,642 rows/s is consistent with `bench/results/pathprof.md`'s
15,800–18,900 for one writer end to end, which additionally pays for HTTP.

Memory is the sharper result. Holding 48,992 rows in the accumulator without
flushing:

| accumulator | RSS delta | per row |
|---|---:|---:|
| Elixir terms | 620 MiB | 13,279 bytes |
| DuckDB table | 0 MiB | not measurable |

That is the same failure mode `sq-col-rate30` hit at 7.8 GiB, reached from the
other end. A DuckDB accumulator makes a large batch affordable, which is exactly
what the single-table ceiling wants.

#### Four shards of one table

One connection was the wrong ceiling to quote: `Engine.Connection` is a per-query
mutex, so the single-shard figures above measure it as much as DuckDB. Four
shards, each with its own engine and its own accumulator, all feeding one table:

| arm | k=1 | k=4 | avg %CPU at k=4 | CPU-s per Mrow |
|---|---:|---:|---:|---:|
| Elixir: decode + coerce + Polars, x4 | 86,016 | 101,130 | 404 | 40.0 |
| **DuckDB: `read_json` + `COPY`, x4** | 317,152 | **380,342** | 530 | **13.9** |

**380,342 rows/s into one table.** That is ClickHouse's 368,347 and raw DuckDB's
361,123 from `scripts/duckdb` — a DuckDB accumulator with four shards reaches
parity with both, from inside the BEAM.

Both arms scale with shards; DuckDB scales better and starts higher. Elixir goes
32,420 → 86,016 (2.7x), DuckDB 90,023 → 317,152 (3.5x). At k=4 DuckDB is **3.8x**
the throughput for 1.3x the CPU, so 2.9x less CPU per row.

**Every arm's rows were read back off disk and matched.** The bench counts
`read_parquet` over each arm's own directory and prints `verified <n>` or a
mismatch, because a throughput number nobody checked against the bytes is a count
of function calls — an arm that wrote nothing would top the table.

#### What it costs, verified by running it

The validation contract does not survive the move as-is. `read_json` with an
explicit `columns` map, given one row whose `severity_number` is `"not-a-number"`:

* by default **the whole statement fails** — `Invalid Input Error: JSON transform
  error in file …, in line 2: Failed to cast value to numerical`. One bad row
  rejects the entire batch.
* with `ignore_errors=true` the row is **kept and the value becomes NULL**. The
  client gets a 200, the row is stored with a null it never sent, and nothing
  appears in `insertErrors`.

Neither is what `docs/api.md` promises: per-index `insertErrors` with partial
failure answered as a 200. Rebuilding that means reading the columns as JSON and
`TRY_CAST`ing each into its target type so per-row failures are attributable —
expressible in SQL, and real work.

A trap worth recording: `SELECT count(*)` over that same bad file returns 3 rows
and no error, because projection pushdown never reads the offending column. The
first version of this check passed for that reason and said nothing.

Two constraints come from the engine rather than the numbers:

* `Smolquery.Engine.Connection` wraps one ADBC connection in a GenServer, so it is
  a per-query mutex. A buffer on the query path's connection would serialize
  against user queries.
* **A bad batch costs only that batch.** `Connection.reply_or_stop/2` treats an
  error as fatal only when its message contains `database has been invalidated`,
  `FATAL Error` or `INTERNAL Error`; anything else is returned as
  `{:reply, {:error, …}}` and the connection lives. The `Invalid Input Error` above
  is an ordinary error — demonstrated in the same run, where the query after it
  succeeded on the same engine. A malformed row cannot take the buffer down.
* Whole-database invalidation is reserved for DuckDB's own internal faults, which
  data cannot trigger. It is still an argument for giving the buffer its own
  database instance — such a fault raised by a *user query* would otherwise take
  the buffer's unacked rows with it — but it is rare-fault isolation, not an
  everyday risk.

### The partition key decides whether pruning survives

`scripts/duckdb/partition` writes 100 segments of a million distinct rows over
1000 log-uniform tenants, three ways, then asks `parquet_metadata` which row
groups a tenant-filtered scan cannot skip. Every segment is sorted
`ORDER BY project_id, timestamp NULLS LAST`, ZSTD, `ROW_GROUP_SIZE 16384` — what
`Merge` and `seal_row_group_size` actually use.

Two probes, and the second one is the one that matters. Probe A is the largest
tenant, which is also *lexicographically first* — every shard that does not hold
it has a minimum above it and prunes on the boundary alone. Probe B sits in the
middle of the key space.

| layout | probe A groups read | probe B groups read |
|---|---:|---:|
| round-robin (today's shape) | 100 / 100 | 100 / 100 |
| hash(project_id) | 25 / 100 | **100 / 100** |
| range(project_id) | 25 / 100 | **25 / 100** |

**Hash partitioning buys no pruning.** It looked like it did on probe A and that
was an artifact of where that tenant sorts. A shard chosen by hash holds tenants
scattered across the whole key space, so its per-segment `project_id` min/max is
almost as wide as the table's, and a mid-space tenant matches every segment.

**These segments were written by DuckDB, so they have statistics.** Production
micro-segments do not (see above), so on the hot tier as it stands *no* partition
key prunes anything — there is nothing to prune on. This table therefore describes
sealed segments, and the choice of partition key only starts to matter once either
the statistics gap is closed or the question is about post-seal layout.

Range partitioning prunes 4 of every 5 row groups on both probes, because a
shard owns one contiguous slice of the key space and its min/max is tight.

So sharding a table's buffer must route on a **range** of the first clustering
column, not a hash of it. `Ring.owner/2` hashes its key, which is right for
spreading tables across nodes and wrong for choosing a shard inside a table —
those are two different decisions that would otherwise share one mechanism.

### Many small segments cost storage, not seal CPU

Same million rows, varying how many files carry them into one seal:

| input segments | seal ms | rows/s | sealed size | input size |
|---:|---:|---:|---:|---:|
| 25 | 1125 | 887,041 | 65 MiB | 73 MiB |
| 100 | 1062 | 939,703 | 65 MiB | 94 MiB |
| 200 | 1105 | 902,924 | 65 MiB | 118 MiB |
| 400 | 1300 | 767,481 | 65 MiB | 141 MiB |

Sealing 100 small files is *faster* than sealing 25 large ones — more files read
in parallel. Only at 400 does per-file overhead cost anything, and then just 18%.

The real price of many small segments is on disk before the seal: the same rows
occupy **73 MiB in 25 files and 141 MiB in 400**, because small files compress
worse and carry more metadata. Sealed output is 65 MiB regardless. So more shards
per table is cheap for the sealer and expensive for the hot tier's footprint.

### It is not the encode

`encode_concurrency` already exists as an A/B switch. It is not the lever.
Identical boot path, 8 VUs, one table, 60 s:

| `encode_concurrency` | rows/s | server avg %CPU |
|---:|---:|---:|
| 1 | 77,862 | 419 |
| 4 | 76,572 | 485 |

Zero throughput for 66% more CPU. `Committer`'s own moduledoc says why: above 1,
"the rows are copied on again to the encode's task". The option buys parallel
encoding and pays for it with an extra copy of every batch.

(A 20 s run at `4` reported 113,967 rows/s. That is a cold-table artifact and
does not survive 60 s. Short runs against a buffering server measure the buffer.)

### One table: group commit never groups

`flush_max_bytes` is `4_500_000` and counts the accumulated term, which the
config comment prices at ~1.5 KiB per columnar row. A single 3062-row request is
therefore ~4.6 MB — over the threshold on its own, so `handoff_when_full/1` fires
on arrival. Counted during a run: **1537 segments for 1514 accepted requests.**
One request, one flush, one Parquet file. The group commit the design is built
around never groups under this configuration.

Every request therefore pays a whole flush: `merge_chunks`, the copy to the
committer, the encode, the manifest fsync and one replication round. Paying that
less often per row is the lever. Single table, 8 VUs, 60 s:

| request rows | `flush_max_bytes` | rows/s | rows/segment | p50 | server avg %CPU |
|---:|---:|---:|---:|---:|---:|
| 3062 | 4.5 MB (default) | 76,874 | 3,014 | 309 ms | 408 |
| 3062 | 18 MB | 83,860 | 12,043 | **286 ms** | 398 |
| 3062 | 32 MB | 62,970 | 23,879 | 372 ms | 325 |
| 7000 | 4.5 MB | **94,306** | 6,856 | 570 ms | 495 |
| 7000 | 18 MB | 93,445 | 13,896 | 604 ms | 478 |

**~94,000 rows/s is the single-table ceiling found here**, and it costs latency:
p50 goes from 309 ms to 570 ms because each request carries 2.3x the work.
Raising `flush_max_bytes` to 18 MB is the better trade — +9% throughput, 4x fewer
segments, *lower* latency, and no extra CPU.

At 32 MB throughput falls to 62,970 while CPU drops to 325%, so the run is not
CPU-bound and something else limits it. That is unexplained, not diagnosed.

The 7000-row body is 7.51 MiB, just inside the wire cap. Probed live: 7,872,357
bytes returns 200 and 8,096,941 returns 413 with the documented envelope, so
`:max_body_bytes` is genuinely in force — `Parsers` now resolves `:length` per
call rather than letting `Plug.Parsers.init/1` bake it.

### Which process is the funnel

Sampled live over 5 s at 8 VUs, one table, through `process_info` on the
running node:

| process | mailbox avg | mailbox max | reductions/s | heap |
|---|---:|---:|---:|---:|
| `TableBuffer` | 1.5 | 8 | 1,317,430 | 50 MiB |
| `Committer` | 3.0 | 5 | 2,355,672 | 90 MiB |

There is exactly **one of each per table** — the probe confirms `buffers 1,
committers 1` — and both carry a standing queue under load. Two processes hold
the whole serialized write path for a table, and neither is idle.

The buffer's own accumulate is not the cost. `accumulate/7` conses the incoming
columns onto a list and bumps two counters; it is O(1). The serialized work is
in `handoff/1` — `merge_chunks/1` zipping 62 columns, and `Committer.commit/2`
copying the merged batch onto the committer's heap, a copy charged to the buffer
process. Replacing the accumulator with ETS would not remove either, and the
`GenServer.call` copy it *would* remove is charged to the calling request
process, where it already runs in parallel.

## Known asymmetries

These are real and are not controlled for. Read the ratios with them in view.

* **Wire size.** The smolquery columnar body is 3.28 MiB; the ClickHouse
  `JSONEachRow` body is 6.41 MiB for identical rows. smolquery moves half the
  bytes for the same work. Comparing them compares two endpoints, not two
  ingestion engines. The `sq-row-vus4` row-major run (6.42 MiB) is the
  byte-matched point: 60,289 rows/s against ClickHouse's 165,814 with fsync.
* **What a 200 promises.** smolquery: rows are fsynced into the buffer manifest
  and are immediately queryable — confirmed, a `SELECT count(*)` straight after
  each run returned the exact accepted total. ClickHouse without fsync: the part
  is committed and visible to `SELECT`, but only in the OS page cache. The
  `-fsync` runs close this gap and are the fair pair.
* **Timestamp format.** Each engine gets the format it parses fastest — ISO 8601
  with `Z` for smolquery, `2026-08-01 10:00:00.000` for ClickHouse. Handing
  ClickHouse the ISO form would force `date_time_input_format=best_effort`, its
  slowest parser, on two columns of every row.
* **Nullability.** ClickHouse marks only the four columns that actually carry
  nulls as `Nullable`; smolquery's schema declares every column nullable. This
  favours ClickHouse's storage layout.
* **Swap.** The machine reported 1.7 GiB of swap in use at the end of the series.
  It is boot-cumulative and cannot be attributed to a single run, but the 7.8 GiB
  peak in `sq-col-rate30` is the likely contributor.
* **macOS.** ClickHouse is built and tuned for Linux. A Linux number for either
  engine may differ.

## A trap worth recording

`fsync_after_insert` is a **MergeTree table setting, not a query setting**. Sent
as a query parameter it returns `404 UNKNOWN_SETTING`, and the run reports 24,691
"requests" in 60 s at 1 VU — a number that looks like a spectacular result until
you read the refusal count. It must be set with
`ALTER TABLE ... MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1`.

This is the reason `insert.js` keeps refused requests out of the latency
percentiles and prints the refusal count next to the throughput.

## Reproducing

```bash
mkdir -p /tmp/smolquery-bodies && k6 run -e ROWS=3062 -e PROJECTS=1000 -e OUT=/tmp/smolquery-bodies scripts/k6/generate.js
```

smolquery arm — start the server on its own data directory, create the table
from `schema.json`, set the clustering key, then load. ClickHouse arm —
`./scripts/clickhouse/up.sh --reset`, then
`.cache/clickhouse/25.8/clickhouse client --queries-file scripts/k6/clickhouse.sql`.
See `../README.md` for the exact commands, and run
`go run scripts/k6/watch.go` alongside each one.
