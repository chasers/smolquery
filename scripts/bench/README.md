# Reproducing the insert benchmark

Bash drivers for the runs recorded in `../k6/results/`. They exist because
`scripts/k6/` is JavaScript and Go only — see its README — and starting a server,
wiping a data directory and verifying rows afterwards is none of k6's business.

## What you need

| tool | why |
|---|---|
| k6 | the load generator (`scripts/k6/insert.js`) |
| Go | the CPU/RSS sampler (`scripts/k6/watch.go`), no modules |
| Elixir/OTP + this repo | the server under test |
| ClickHouse | only for the comparison arm — `scripts/clickhouse/install.sh` |

## Once: generate the bodies

```bash
mkdir -p /tmp/smolquery-bodies && k6 run -e ROWS=3062 -e PROJECTS=1000 -e OUT=/tmp/smolquery-bodies scripts/k6/generate.js
```

Same `SEED`, same bytes, so a run today compares with a run from last week.

## One measured run

```bash
VUS=16 ./scripts/bench/insert-run.sh polars baseline
```

It wipes the data directory, starts the server, creates the table with its
clustering key, runs a discarded 20 s warmup, measures 60 s while sampling the
server's and k6's CPU and RSS, and then **counts the rows back through
`POST /v1/queries`** and refuses to pass unless the count equals what k6 counted
as accepted. A number that has not been checked against the stored rows is a
count of round trips.

## The headline numbers

All end to end through k6 over HTTP against the real server, one table, 60 s,
every run verified. On an Apple M1 Pro (10 cores, 16 GiB):

```bash
# baseline: today's write path
VUS=8 ./scripts/bench/insert-run.sh polars polars-default          # 77,246 rows/s, 410% CPU
```

```bash
# the DuckDB flush writer, everything turned up
VUS=32 FLUSH_BYTES=32000000 ENCODE_CONCURRENCY=4 WRITE_POOL=4 \
  ./scripts/bench/insert-run.sh duckdb duckdb-tuned                # 383,157 rows/s, 464% CPU
```

Full tables, the sweeps behind those settings, and what each knob removed are in
`../k6/results/apple-m1-pro-16gb-macos-26.5.md`.

## The knobs

| variable | meaning |
|---|---|
| `FLUSH_WRITER` | `polars` (default) or `duckdb` — `Smolquery.BufferService`'s `:flush_writer` |
| `FLUSH_BYTES` | `:flush_max_bytes`. At the 4.5 MB default one request already exceeds it, so group commit never groups |
| `ENCODE_CONCURRENCY` | concurrent encodes per table. Helps DuckDB up to 4; **hurts** Polars |
| `WRITE_POOL` | `:write_pool_size`, DuckDB instances for the flush, picked per segment by `phash2`. Ignored under `polars` |
| `VUS` | k6 clients |
| `DURATION` | measured window, default `60s` |

## Two things that will mislead you

**The two writers do not take the same body.** The DuckDB path takes a body
nobody parsed, and an unparsed body is NDJSON: 6.41 MiB where the columnar JSON
is 3.28 MiB for identical rows. The DuckDB arm wins while moving twice the bytes.

**They do not promise the same thing.** The Polars path runs
`Smolquery.IngestService.Validator` over every value and answers with per-index
`insertErrors`, BigQuery-style. The DuckDB path runs no validation at all:
`insertErrors` is always empty, and a value the schema cannot take fails the
whole flush rather than one row. It is a full end-to-end measurement of a
**weaker contract**, not the same contract made faster.

## ClickHouse

```bash
./scripts/clickhouse/up.sh --reset
.cache/clickhouse/25.8/clickhouse client --queries-file scripts/k6/clickhouse.sql
```

Then post `eachrow.3062.ndjson` at it with `scripts/k6/insert.js` — the exact
invocation, and the `fsync_after_insert` setting that decides what its 200 means,
are in `../k6/README.md`.

Never run both databases at once. Each of these scripts stops the other first,
and the results are worthless if two servers share the machine.
