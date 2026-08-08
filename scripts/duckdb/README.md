# DuckDB write floor

A single Go process with DuckDB linked into it, writing the benchmark's own rows
to a `.duckdb` file and to Parquet. No server, no HTTP, no BEAM.

```bash
go run . -duration 30s
```

## Why

smolquery's storage is DuckDB and Parquet. Every throughput number the k6 arms
produce is bounded by what those two can absorb on this machine, and until that
bound is measured a slow arm cannot be blamed on the write path rather than on
what sits underneath it. This measures the bound with nothing in the way.

It reads `scripts/k6/schema.json` for the column list and the NDJSON bodies
`scripts/k6/generate.js` writes, so these are the rows the HTTP arms posted —
not a fixture that resembles them.

## What it measures

| write | what it isolates |
|---|---|
| `append` | the Appender API, a row at a time from Go. Every value crosses cgo boxed as an interface; at 62 columns that cost is most of it. This is **go-duckdb's** ceiling, not DuckDB's. |
| `json` | `INSERT ... SELECT * FROM read_json(...)`. DuckDB's own C++ reader parses the same NDJSON k6 posts. |
| `parquet-in` | `INSERT ... SELECT * FROM read_parquet(...)` over one pre-written 3062-row file. Nothing parses, nothing crosses cgo per row. The closest thing here to the storage engine alone. |
| `parquet-out` | `COPY ... TO`, zstd and uncompressed. Getting the table back out. |

`-sweep-to N` repeats `parquet-in` with 1, 2, 4 … N batches under one
transaction. That sweep is the point of the tool — see below.

Each ingest runs against a freshly created table. A table left full by the
previous write made the next one measurably slower, which would have made the
result depend on the order the writes happen to run in.

## The finding: it is the commit, not the rows

At one batch per transaction all three ingest paths land within a few percent of
each other — 29k, 36k and 37k rows/s — even though one of them crosses cgo per
value, one parses JSON in C++, and one does neither. Paths that different cannot
land that close unless the thing being measured is none of them.

It is the commit. Batches sharing one transaction (Apple M1 Pro, 20 s each):

| batches per transaction | rows/s | CPU-seconds per million rows |
|---:|---:|---:|
| 1 | 36,712 | 58.1 |
| 4 | 40,254 | 53.6 |
| 16 | 58,611 | 39.1 |
| 32 | 120,609 | 14.4 |
| 64 | 141,727 | 11.1 |
| 128 | 142,171 | 11.6 |
| 256 | 143,191 | 11.8 |

At one commit per 3062-row batch a single writer gets a quarter of what it gets
at 64, and pays five times the CPU per row.

## The second finding: DuckDB does take concurrent writers

A single writer is not DuckDB's ceiling, and assuming it was would have been the
easy mistake here. `-writers-to N` runs the same insert split across goroutines,
each with its own transaction:

| writers | batches per commit | rows/s | avg %CPU | CPU-s per million rows |
|---:|---:|---:|---:|---:|
| 1 | 1 | 38,783 | 216 | 55.7 |
| 2 | 1 | 42,772 | 211 | 49.4 |
| **4** | 1 | **361,123** | 427 | 11.8 |
| 8 | 1 | 337,332 | 570 | 16.9 |
| 4 | 1, `-checkpoint` | 294,059 | 505 | 17.2 |

Concurrent appends do not conflict — they touch different rows — so four writers
reach **361,000 rows/s**, which is what eight concurrent connections get out of
ClickHouse on this machine. The two engines are level on raw ingest.

The jump between 2 and 4 writers is real, not an artifact: every arm counts the
table afterwards and none reported a mismatch, and the advantage survives
`-checkpoint`, which forces the data down after every batch. Peak RSS does climb
from 0.7 GiB to 7.3 GiB across that step, so the win is partly bought with
memory.

Eight writers are worse than four while burning a third more CPU. Four is the
knee.

## Parquet is an order of magnitude cheaper than the database file

Export runs at **2.2M rows/s** with zstd and 2.5M uncompressed — roughly 15x
cheaper per row than committing the same rows into the `.duckdb` file. Whatever
a write path costs, encoding Parquet is not where it goes.

## Reading the output

`%CPU` is percent of one core, so 1000 is the whole ten-core machine. CPU comes
from `getrusage(RUSAGE_SELF)`, which covers the linked-in DuckDB threads — there
is no second process here for an outside sampler to point at, which is why
`scripts/k6/watch.go` is not used.

`RSS mark` is `ru_maxrss`, a **process high-water mark**. It never falls, so each
row is "the most this process had ever held by the end of that write", not that
write's own peak. Only the last row is a peak.

`-checkpoint` forces a `CHECKPOINT` after every batch. That is a durability
setting, not a speed one: it is what makes a batch survive a power cut rather
than merely a crash, and it is off by default because the k6 arms are not
compared under it either.

## Sizes, and why they flatter the format

| file | bytes/row |
|---|---:|
| `bench.duckdb` | 89.8 |
| Parquet, zstd | 5.7 |
| Parquet, uncompressed | 46.9 |

**Read these as a ratio, not as an absolute.** The run inserts the same 3062-row
batch over and over, so a table of 3.1M rows is a thousand copies of 3062
distinct ones, and zstd finds every copy. Real data with 3.1M distinct rows will
be several times larger per row. The zstd-to-uncompressed ratio and the
database-to-Parquet ratio still hold; the bytes/row figure does not.

## `partition/` — pruning and seal cost

```bash
go run ./partition -rows 1000000 -segments 100 -shards 4
```

A separate question from write throughput: if a table's buffer is sharded, each
shard writes its own segments, and the partition key decides which rows share a
file. That is a claim about Parquet row-group statistics, so it is measured
against them — `parquet_metadata` reports exactly which groups a predicate cannot
skip, with no profiler involved.

**Why this lives next to a DuckDB tool.** Both things it measures are DuckDB
inside smolquery. The seal *is* a DuckDB statement —
`Smolquery.StorageService.Merge` runs
`COPY (SELECT … FROM read_parquet([urls], union_by_name := true) ORDER BY …) TO staged`
— and the read path is DuckDB reading Parquet. The `ORDER BY … NULLS LAST`,
`ROW_GROUP_SIZE 16384` and ZSTD here are copied from `Merge` and from
`seal_row_group_size` in `config/config.exs`.

**What is not faithful:** micro-segments here are written by DuckDB `COPY`, where
production writes them with Polars via `Smolquery.Segments.Writer`. Both sort on
the clustering key and both write row-group statistics, so the ratios between
layouts carry over; the absolute times are DuckDB's.

Findings are in
`../k6/results/apple-m1-pro-16gb-macos-26.5.md`. The short version: hash
partitioning gives **no** pruning once you probe a tenant that is not at the edge
of the key space, range partitioning prunes 4 of 5 row groups, and many small
segments cost disk footprint rather than seal CPU.

## Rules for this directory

Go, because DuckDB has to be *in* the process for this to mean anything — a
number measured across a socket is measuring the socket. Standard library plus
`go-duckdb`; the DuckDB native library is vendored by that module, so `go build`
is the whole setup and nothing has to be installed on the machine.
