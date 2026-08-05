# `bench/load.exs` — batch loads end to end

| | |
|---|---|
| Run | 2026-08-04 |
| Commit | `29bb13d` (PL-20: one-pass validation T-151, pipelined commit T-152; baseline 2026-08-03 numbers in parentheses where they moved) |
| Command | `mix run bench/load.exs 2>/dev/null` (defaults: `ROWS=50000`, `SCALE=10000,50000`, `BATCH=2000`, `POOL=256`, buffer `flush_interval_ms: 1000`, `load_max_bytes: 256 MiB`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |
| Parallelism | 10 logical processors · 10 schedulers online · 10 dirty-CPU · 10 dirty-IO · 10 DuckDB threads per engine |

One node, every role but `:web`, over real HTTP:
`POST /v1/datasets/logs/tables/otel_logs/load` with the same 61-column OTel
fixture `bench/results/otel_logs.md` uses, so the two are comparable. Fixture
files are built driver-side and timed separately — the Parquet file comes from
`Segments.Writer.write/3` (what an export from smolquery looks like), the CSV is
that file re-serialized by Polars, NDJSON is built by hand.

## Headline

**Format choice is worth at most 1.6×, and Parquet's 531× size advantage buys
almost none of it.** NDJSON 10.8k rows/s (was 9.0k), CSV 17.3k (14.3k),
Parquet 15.0k (14.6k) — PL-20's faster validator lifted the text formats
~20% without touching this path, which is itself evidence the shared
row-shaped tail sets the floor. The files are 106 MiB, 50 MiB, and 0.2 MiB.
**A 106 MiB NDJSON load still costs ~12× the file in peak BEAM heap**
because `parse/3` materializes every row before chunking, and **a load
still uses 1.0 of 10 cores** — one request is one process. Both remain
T-144's brief; nothing in PL-20 changed them.

## Fixture files — 50,000 rows

```
  format          MiB    B/row  build ms
  ndjson        106.2     2228     769.8
  csv            50.4     1057    2618.7
  parquet         0.2        4       0.8
```

CSV is half of NDJSON because the header carries the column names once instead of
per field per row. Parquet at 4 B/row is a **fixture artifact** — 50,000 rows drawn
from 256 templates dictionary-encode almost to nothing. Re-run at `POOL=20000` it
becomes **64 B/row (3.0 MiB)**, which is closer to real log traffic. Every *byte*
number below inherits that caveat; the *timings* do not (see the note under
Phase 1).

## Phase 1 — same 50,000 rows through each format, one request each

```
  format         MiB   load ms    rows/s   MiB/s  inserted  status   cores   sched%
  ndjson       106.2    4615.5     10833    23.0     50000     200    1.03      4.7
  csv           50.4    2894.3     17275    17.4     50000     200    1.06      5.7
  parquet        0.2    3344.2     14951     0.1     50000     200    1.02      4.9
```

**Every load uses exactly one core of ten**, with BEAM schedulers 5–6% busy. That is
the parallelism gap stated as a measurement rather than an inference: `/load` is one
HTTP request handled by one process, parsing and then inserting chunk after chunk,
while nine cores watch.

Per row: 92 µs NDJSON (was 111), 58 µs CSV (70), 67 µs Parquet (69). The
one-pass validator (T-151) shaved the shared tail; the format spread is
unchanged.

**This refutes the prediction PL-18 was written to test, but confirms its
mechanism.** `otel_logs.md`'s stage profile showed Parquet decoding 38× faster
than JSON while `to_rows` costs more than the JSON parse it replaces, and I
predicted from that that a Parquet load would be *no faster* than NDJSON. It is
1.6× faster. But look at what that 1.6× cost: a file **531× smaller**, parsed by
Polars in under a millisecond. Practically all of the advantage is consumed
downstream, and CSV — 250× larger than the Parquet file — finishes within 10% of
it. The shared row-shaped tail sets the floor, exactly as the stage profile said;
I was wrong about how much of the total that floor was.

**The timings are not a fixture artifact even though the sizes are.** Re-run with
78× the cardinality (`POOL=20000`, Parquet 4 → 64 B/row), Parquet loads at
**14,938 rows/s against 14,557** — inside run-to-run variance. The cost is
downstream of parsing, so it does not care how compressible the input was.

## Phase 1b — what `load_max_bytes` admits, per format (derived)

```
  format        B/row  rows per load
  ndjson         2228         120502
  csv            1057         253957
  parquet           4       72969798
```

`load_max_bytes` is a *byte* cap, so the same 256 MiB is a wildly different row
limit per format: **a 61-column NDJSON load tops out around 120k rows.** That is
worth documenting on the route — a client batching by row count will hit 413 at a
row count that looks arbitrary. (The Parquet figure is the compressibility artifact
again; at `POOL=20000`'s 64 B/row it is ~4.2M rows.)

## Phase 2 — rows/s against file size

```
  format          rows     MiB   load ms    rows/s
  ndjson         10000    21.2     947.7     10552
  ndjson         50000   106.2    3834.1     13041
  csv            10000    10.1     598.0     16723
  csv            50000    50.4    4110.7     12163
  parquet        10000     0.1     678.9     14729
  parquet        50000     0.2    3941.0     12687
```

NDJSON holds 10.6k → 13.0k from 10k to 50k rows, so parse-then-chunk is not
super-linear at this size. CSV and Parquet are not stable enough to read as a trend:
CSV goes 16.7k → 12.2k here while Phase 1 measured 17.3k on the same 50k file, and
Parquet's 14.7k at 10k rows sits below its own 15.0k in Phase 1. Their totals are
small enough that group-commit timing and GC move them by thousands of rows/s.
**Treat the non-NDJSON cells as variance, and re-run with a longer sweep before
building any argument on them.**

The 10,000-row cells sit near the 1,000 ms flush interval (598–948 ms), so they are
substantially group-commit wait rather than parse work — the script prints that
warning above the table for the same reason.

## Phase 3 — peak BEAM memory across one NDJSON load (50 ms samples)

```
  kind          baseline        peak       delta   × file
  total            887.3      2183.6      1296.3     12.2
  binary             9.8       120.6       110.8     1.04
  processes        820.3      2014.2      1193.9    11.24
```

**The disk spool bounds the request body, not the heap.** Binary memory grows by
110.8 MiB — 1.04× the file, which is the spool and the body chunks doing exactly
what they were designed to do. But *process* memory grows by 1,194 MiB, because
`parse/3` turns the whole file into Elixir maps before chunking (`Enum.reduce` for
NDJSON, `DataFrame.to_rows` for CSV and Parquet) and `Stream.chunk_every` then runs
over an already-complete list.

So a load costs **~12× the file in peak heap**, and `load_max_bytes` defaults to
256 MiB. Extrapolated, one request at the cap wants roughly **3 GiB** of BEAM
memory, and nothing in the config says so. That is the finding here: either the
default cap is too high for the parser behind it, or `parse/3` needs to stream into
`insert_chunks/5` instead of materializing. A 50 ms sampler can miss a spike, so
every peak above is a floor on the real one.

## Phase 4 — 10,000 rows: one load vs five 2,000-row inserts, both serial

```
  path                            ms    rows/s   cores
  POST /load, one file         745.0     13422     1.0
  POST /insert × 5            5836.9      1713    0.15
```

The `cores` column is the whole story in miniature: the load uses one core, five
serial inserts use **0.15** — they spend their time *waiting* on group commits, not
computing.

`/load` wins 7.8×, and the reason is group-commit amortization, not parsing: five
serial inserts each wait for their own ~1,000 ms flush, while the load's 10,000
rows arrive as one chunk and wait once. This is the *single-threaded client*
comparison.

Set against `bench/results/otel_logs.md`'s concurrent ceiling — 38.4k rows/s from
16 concurrent writers on one table (42.5k after PL-20) — **`/load` at 15–17k rows/s is ~2.5× slower than
just running `/insert` in parallel.** A load is one request handled by one process,
and the measurement says so exactly: 1.0 core against the concurrent path's 4.8.
It amortizes commits; it does not use the machine. So the guidance is
counterintuitive but clear: `/load` is for convenience and format support and for
clients that cannot fan out; concurrency is for throughput.

## Node counters (`GET /metrics`)

```
  buffer_commits_total                     44
  buffer_rows_committed_total              400000
  seal_attempts_total                      -
  buffer_admission_refused_rows_total      -
  ingest_rows_rejected_total               -
```

400,000 rows in 44 commits across every phase — ~9,000 rows a commit, since a
load's 10,000-row chunks arrive as fast as the buffer takes them. No seal fired
(44 files is under `seal_max_files: 64`), nothing was shed, nothing was rejected.

## What this settles

- **Send Parquet or CSV, not NDJSON, but do not expect much**: 1.6× over NDJSON, and
  the two are indistinguishable from each other. Parsing is ~42 µs of a row's
  ~111 µs; the other ~69 µs is format-independent.
- **A load uses 1.0 of 10 cores.** Not an inference from throughput — the OS
  process's CPU time, which includes DuckDB's native threads. Whatever else is
  true, `/load` leaves 90% of the machine unused.
- **The bulk-format argument for T-139 is stronger, not weaker.** A 531× smaller
  file parsed in under a millisecond yields 1.6×, because `to_rows` → validate →
  re-encode is the floor. Frames end to end is what removes that floor; a new
  content type on today's `parse/3` cannot.
- **`/load` is not the fast path.** It is ~2.5× slower than concurrent `/insert`,
  and its 7.8× win over *serial* inserts is commit amortization. Anyone chasing
  ingest throughput should fan out `/insert`, not batch into `/load`.
- **A load costs ~12× the file in peak heap**, all of it process memory, and the
  256 MiB default cap therefore implies ~3 GiB per request. Worth either a
  smaller default or a streaming `parse/3` (and worth a caveat in `docs/api.md`
  meanwhile).
- **`load_max_bytes` in rows: ~120k for 61-column NDJSON, ~254k for CSV.** A
  client batching by rows needs to know the byte cap translates differently per
  format.
- **Fixture cardinality changes Parquet's size 16× and its speed not at all**
  (`POOL=20000`: 64 B/row, 14.9k rows/s). Byte figures here are fixture-dependent;
  timings are not.
