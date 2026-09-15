# `bench/rowbinary.exs` — transcoding ClickHouse RowBinary on the ingest node

| | |
|---|---|
| Run | 2026-09-15 |
| Commit | `2ce3eb3` (T-469: `Smolquery.RowBinary`, timestamps from a two-digit table), with `bench/rowbinary.exs` as committed beside this file |
| Command | `mix run bench/rowbinary.exs` (defaults: `ROWS=200000`, `REPS=5`, `WRITERS=1,2,4,8`) |
| Machine | ARM Neoverse-V1 · 8 vCPUs · 30 GiB · Ubuntu 24.04.4 container on an Amazon Linux 2023 6.12 kernel |
| Runtime | Elixir 1.20.2 / OTP 29 · 8 schedulers online · 8 dirty-CPU · 10 dirty-IO |

Two shapes. **logs** is a 6-column log line: `Int64`, `DateTime64(6)`,
`LowCardinality(String)`, `String`, `Nullable(String)`, `Float64`. **wide** adds
18 columns to it: eight `String`, six `Float64`, a `Decimal(18, 2)`, a `Date32`,
a `Bool` and a three-entry `Map(String, String)`. Each body is transcoded in one
process; the concurrent section runs one process per body. The driver shares
the machine with nothing else of note.

The prototype this decoder replaced measured 162,934 rows/s on the logs shape
on this machine. The first cut of `Smolquery.RowBinary` measured 70,440; the
two-digit table in `2ce3eb3` is what took it to the numbers below.

## Raw output

```
logs (6 cols): wire size, 200000 rows
─────────────────────────────────────
  RowBinary                    19.9 MiB    104 B/row
  NDJSON it becomes            33.3 MiB    174 B/row
  expansion                    1.67x
  rows per 8 MB body          45870 (SMOLQUERY_INSERT_MAX_NDJSON_BYTES, measured as NDJSON)

logs (6 cols): transcode one 200000-row body (5 reps, median)
─────────────────────────────────────────────────────────────
                                                  min ms    med ms     rows/s  µs/row  MiB/s in
  decode, RowBinary                                928.2    1094.6     182712    5.47      18.2
  decode, RowBinaryWithNamesAndTypes              1036.9    1122.7     178142    5.61      17.7
  decode typed + iodata_to_binary (the batch)     1041.2    1058.4     188971    5.29      18.8
  ref: NDJSON route's line count                    54.3      55.8    3586736    0.28     596.6
  ref: JSON.decode! every NDJSON line              448.2     502.5     398041    2.51      66.2

logs (6 cols): one request's transcode, typed header + binary (median)
──────────────────────────────────────────────────────────────────────
  rows          RowBinary       NDJSON        ms  µs/row
  1000            0.1 MiB      0.2 MiB       1.9     1.9
  10000           1.0 MiB      1.6 MiB      40.1    4.01
  50000           4.9 MiB      8.3 MiB     323.0    6.46

logs (6 cols): concurrent bodies, 200000 rows each (median)
───────────────────────────────────────────────────────────
  writers       wall ms      rows/s  scaling
  1              1378.7      145062     1.0x
  2              1448.1      276222     1.9x
  4              1540.5      519301    3.58x
  8              1792.7      892512    6.15x

wide (24 cols): wire size, 200000 rows
──────────────────────────────────────
  RowBinary                    56.1 MiB    294 B/row
  NDJSON it becomes           107.1 MiB    561 B/row
  expansion                    1.91x
  rows per 8 MB body          14252 (SMOLQUERY_INSERT_MAX_NDJSON_BYTES, measured as NDJSON)

wide (24 cols): transcode one 200000-row body (5 reps, median)
──────────────────────────────────────────────────────────────
                                                  min ms    med ms     rows/s  µs/row  MiB/s in
  decode, RowBinary                               3225.9    3428.5      58334   17.14      16.4
  decode, RowBinaryWithNamesAndTypes              3163.0    3442.4      58099   17.21      16.3
  decode typed + iodata_to_binary (the batch)     6383.0    6440.2      31055    32.2       8.7
  ref: NDJSON route's line count                    48.1      48.6    4119125    0.24    2204.9
  ref: JSON.decode! every NDJSON line             1970.0    2324.0      86057   11.62      46.1

wide (24 cols): one request's transcode, typed header + binary (median)
───────────────────────────────────────────────────────────────────────
  rows          RowBinary       NDJSON        ms  µs/row
  1000            0.3 MiB      0.5 MiB      19.1   19.15
  10000           2.8 MiB      5.3 MiB     305.4   30.54
  50000          14.0 MiB     26.7 MiB    1712.0   34.24

wide (24 cols): concurrent bodies, 200000 rows each (median)
────────────────────────────────────────────────────────────
  writers       wall ms      rows/s  scaling
  1              7783.5       25695     1.0x
  2              8184.4       48873     1.9x
  4              8431.8       94879    3.69x
  8              9640.9      165960    6.46x
```

## What this settles

- **A RowBinary row costs 0.7–0.9 µs per column to transcode.** That is
  5.3 µs a row on the logs shape and 17.1 µs on the wide one. It is 20–70× the
  NDJSON route's line count, and more than the `JSON.decode!/1` parse T-180
  took off this node: 2.1× that parse on logs, 1.5× on wide. PL-64 D1 buys
  compatibility with per-row CPU on the ingest node, not speed.
- **It scales with writers.** Eight concurrent bodies run 6.15× (logs) and
  6.46× (wide) one body's rate on 8 schedulers, so the cost is per-process CPU
  with no shared bottleneck. At the fleet's benchmarked 105,733 rows/s
  (`docs/benchmarks.md`), the transcode alone would take about 0.6 of a
  scheduler on the logs shape and about 3.4 on the wide one, counting the
  binary.
- **Per-row cost grows with body size, and flattening the output is not
  free.** Logs rise from 1.9 µs a row at 1,000 rows to 6.5 at 50,000; wide
  from 19 to 34. On the 200,000-row wide body, `IO.iodata_to_binary/1` doubles
  the decode, 17.2 µs to 32.2. The likely cause is one process heap
  accumulating hundreds of thousands of small binaries before a single
  flatten; that is inferred, not profiled. Building the output as bounded
  binary chunks is the next change to measure, before option 2 (transcoding at
  the flush) is weighed on these numbers.
- **The 8 MB insert limit is measured as NDJSON, and ClickHouse clients batch
  bigger.** An 8 MB NDJSON body holds 45,870 log rows or 14,252 wide rows. The
  RowBinary bodies are 1.67× and 1.91× smaller than what they become, so the
  limit belongs on the decoded size, which is what the buffer, the replicas and
  the in-flight valve hold. Logflare batches up to 60,000 rows per insert, so a
  ClickHouse-compatible route needs to split a decoded body into several
  forward-batches under one ack, or carry its own limit (T-470).
- **Not measured here: the ack.** End-to-end latency and throughput through the
  route, against NDJSON bodies of the same rows, is T-471 once T-470 lands.
