# Write path profile — socket to fsynced Parquet

`mix run bench/pathprof.exs`, macOS, one writer, `CLUSTERING=project_id,timestamp`,
`POOL=2000 PROJECTS=1000 REPS=15`. Two independent runs; both columns shown, so
the spread between them is the noise band.

## The unit

The flush trigger is whichever of `flush_max_rows: 100_000`,
`flush_max_bytes: 8_000_000` or `flush_interval_ms: 1_000` comes first. For this
workload it is always the byte bound, and it is not close: **3 062 rows**, 33×
below the row limit. `flush_max_rows` never fires on OTel-shaped rows.

One flush is 3 062 rows / 6.6 MiB of JSON on the wire.

## Where the milliseconds go

| group | run 1 | run 2 | share |
|---|---:|---:|---:|
| HTTP edge (socket, Bandit, Plug) | 21.1 ms | 24.6 ms | 12% |
| JSON decode | 35.9 ms | 35.1 ms | 18% |
| validate + coerce | 66.7 ms | 62.8 ms | 33% |
| process copies (ingest→buffer→committer) | 15.2 ms | 13.7 ms | 7% |
| frame build (maps → Arrow, sort) | 21.9 ms | 19.5 ms | 11% |
| Parquet encode + store fsync | 8.6 ms | 8.9 ms | 4% |
| hot manifest append + fsync | 4.8 ms | 6.4 ms | 3% |
| fresh-process heap growth + GC | 2.8 ms | 7.2 ms | 3% |
| unexplained residual | 17.7 ms | 14.7 ms | 8% |
| **end-to-end, one real POST** | **194.6 ms** | **193.0 ms** | |

15 700–15 900 rows/s for a single writer on a single table.

## What it says

Getting the rows *into* the VM and into map form costs 63% (HTTP edge, decode,
validate). Writing them costs 7% (Parquet encode, both fsyncs, manifest). The
Rust side of the path — Polars encoding a sorted 62-column frame and fsyncing it
— is the cheapest thing in the table.

fsync is not the problem here: both fsyncs together are under 8 ms per flush,
because group commit amortizes them over 3 062 rows.

## How the numbers are obtained

Groups are measured in isolation on the same rows rather than read off one
instrumented run, and reconciled against a real POST:

* **HTTP edge** is a real POST of the same body routed at a table that does not
  exist — the parsers run, the controller answers 404 before validating — minus
  the decode. It includes the driver's own `Req` encode and send, so the server
  pays less than shown.
* **frame build** is a copy of `Writer.build_frame/2`'s body, pinned by a guard
  that fails the run if it stops agreeing with `Writer.write/3` on row count,
  columns, timestamp bounds and sortedness.
* **Parquet encode** is `Writer.write/3` minus that frame build.
* **manifest** is one `write_batch` minus the copies and minus `Writer.write/3`.
* **fresh-process** is decode + validate + `write_batch` in a process born for
  them and dying after, minus the same three measured in the warm driver — the
  cost of growing and collecting a 6 MiB heap once per request.
* **residual** is what is left of the real POST. It is a genuine leftover, not
  an identity: at 8% it is the resolution limit of this decomposition.

## How big may one POST be — `bench/batchsweep.exs`

Sequential single writer, same fixture. `wire` is the JSON body, `term` is what
the buffer's bounds measure.

| rows | wire | term | latency | throughput |
|---:|---:|---:|---:|---:|
| 500 | 1.1 MiB | 1.3 MiB | 1045.7 ms | 478 rows/s |
| 1 000 | 2.2 MiB | 2.5 MiB | 1083.4 ms | 923 rows/s |
| 2 000 | 4.3 MiB | 5.0 MiB | 1156.3 ms | 1 730 rows/s |
| 3 000 | 6.5 MiB | 7.5 MiB | 1222.5 ms | 2 454 rows/s |
| **3 400** | **7.3 MiB** | **8.5 MiB** | **205.2 ms** | **16 570 rows/s** |
| 3 600 | 7.8 MiB | 9.0 MiB | HTTP 413 | refused |
| 3 800 | 8.2 MiB | 9.5 MiB | connection closed | refused |
| 4 000 | 8.6 MiB | 10.0 MiB | HTTP 413 | refused |
| 6 000 | 13.0 MiB | 15.0 MiB | HTTP 413 | refused |

Two bounds sit 500 rows apart and neither was chosen against the other:

* a request is refused past **7.63 MiB of JSON**, which is `Plug.Parsers`'
  default `:length` of 8 000 000 bytes. `SmolqueryApi.Parsers` never sets it.
* the accumulator is handed off at **7.63 MiB of term**, `flush_max_bytes`,
  also 8 000 000. Below it a lone writer waits out `flush_interval_ms`.

The constants are equal but measure different things, and term runs ~15% above
wire on these rows. That leaves a usable window of roughly **3 050–3 550 rows**:
below it a lone writer pays the 1 s timer, above it the request is refused. The
profile above was taken at 3 062 rows — just inside it. At 3 000 rows the same
POST takes 1 222 ms.

The 1 s floor is a lone-writer artifact: under concurrency the accumulator fills
from several requests and the timer rarely fires. The 413 is not — it is a hard
per-request cap regardless of load.

`max_buffered_bytes: 64_000_000`, the documented `:buffer_full` → 429 path, is
unreachable from a single request: nothing that large can get through a 7.63 MiB
body limit. Only accumulation across concurrent requests can reach it.

### Raising the ceiling makes it worse

Both caps raised together — `BODY_MIB=80 BUFFER_MIB=256`, since raising the wire
cap alone only moves the refusal into `max_buffered_bytes`:

| rows | wire | latency | throughput | RSS |
|---:|---:|---:|---:|---:|
| 3 400 | 7.3 MiB | 205.6 ms | **16 534 rows/s** | 610 MiB |
| 6 000 | 13.0 MiB | 402.9 ms | 14 891 rows/s | 861 MiB |
| 12 000 | 25.9 MiB | 878.0 ms | 13 668 rows/s | 1 303 MiB |
| 24 000 | 51.8 MiB | 1 759.5 ms | 13 640 rows/s | 2 043 MiB |
| 34 000 | 73.4 MiB | 2 619.4 ms | 12 980 rows/s | 2 914 MiB |

Ten times the body buys **21% less throughput**, 12.7× the latency and 4.8× the
RSS — and that is one writer. The gain a bigger batch was supposed to deliver is
amortizing the per-flush costs, which are 14 ms of 194: about 4 µs per row at
3 062 rows. The loss is that everything else is per row, and collecting a
multi-gigabyte heap is not free. The second effect is much larger than the first.

The throughput optimum sits where the flush trigger already is. What a bigger
limit is good for is not hitting a 413 by accident, not throughput.

### The 413 answers with the wrong body

`SmolqueryApi.Parsers` rescues `Plug.Parsers.ParseError` and
`UnsupportedMediaTypeError` but not `RequestTooLargeError`, so it is unrescued:

    status 413
    body   {"error":{"code":500,"message":"internal error","status":"INTERNAL"}}

The status line is right and the envelope — the thing the API contract tells
clients to read — says the server broke. One size (3 800 rows) had the
connection dropped instead of being answered at all.

## Converting fewer maps — `bench/columnar.exs`

A batch becomes maps twice and is then transposed back, all before Rust sees
anything. Steps 2 and 3 undo each other: the validator assembles a map out of
pairs it produced in column order, and the writer takes it apart again in that
same order, one full pass over the row list per column.

Deleting both, measured against a flat prototype whose output is verified
identical to today's — same columns, dtypes, values, order, and rejected
indices:

| | today | columnar | saved |
|---|---:|---:|---:|
| decoded rows → sorted DataFrame | 98.7 ms | 67.7 ms | 31.1 ms (31.5%) |
| two deep copies (ingest→buffer→committer) | 13.5 ms | 6.2 ms | 7.4 ms (54.5%) |
| **together** | | | **38.4 ms — 19.8% of end-to-end** |

The copies get cheaper for free: the same leaf values, but 62 containers instead
of 3 062.

### Built and measured

The change shipped across `Validator`, `IngestService.Client`,
`BufferService.Endpoint`, `TableBuffer`, `Committer` and `Writer`. Same 3 062
rows on both sides — `ROWS=3062` pins the count and moves `flush_max_bytes` down
to match, because column-major prices the same rows at roughly half the term
bytes and each side would otherwise pick a different batch.

| group | before | after | delta |
|---|---:|---:|---:|
| validate + coerce | 64.8 ms | 44.8 ms | −20.0 |
| frame build | 20.7 ms | 12.7 ms | −8.0 |
| process copies | 14.5 ms | 8.4 ms | −6.1 |
| **the three targeted** | **100.0 ms** | **65.9 ms** | **−34.1** |
| **end-to-end POST** | **193.8 ms** | **162.3 ms** | **−31.5 (−16%)** |
| **rows/s, one writer** | **15 800** | **18 900** | **+20%** |

Two runs each side; the pairs were 194.6/193.0 before and 165.4/159.2 after, so
the gap is well outside the spread. The predicted ceiling was −38.4 ms and the
three groups delivered −34.1.

Two caveats on the rest of the table. The fresh-process group rose (2.8/7.2 →
19.9/21.8 ms) while the residual fell (17.7/14.7 → 0.7/~0); together they are
20.5 → 21.8, unchanged, so mass moved between two derived quantities rather than
appearing. The HTTP-edge arm was also wrong in both columns and has since been fixed: it
routed at a missing table, and `SchemaCache` caches successes only, so every rep
paid a catalog query that was charged to the edge. The arm now posts a body of
the same size with the array under `"rowz"`, which `InsertController.rows/1`
rejects with a 400 before `SchemaCache` is reached. Two runs each side:

| | broken arm | fixed arm |
|---|---:|---:|
| HTTP edge | 24.0 / 23.1 ms | 19.7 / 20.5 ms |
| unexplained residual | 0.7 / ~0 ms | 8.5 / 7.7 ms |
| end-to-end | 165.4 / 159.2 ms | 166.4 / 159.2 ms |

End-to-end is untouched, which is the point: the defect moved mass between two
derived groups and never affected the total or the columnar delta.

### The first map is not worth attacking

`JSON.decode!` building a map per row looked like the same kind of waste. It is
not — the parse dominates and `maps:from_list` on 62 pairs is nearly free:

| decoder | | |
|---|---:|---:|
| `JSON` (OTP `:json`, in use) | 40.2 ms | 165 MiB/s |
| `Jason` | 59.0 ms | 112 MiB/s |
| OTP `:json`, `object_finish` keeping pairs instead of maps | 45.1 ms | 147 MiB/s |

Skipping the map construction makes decoding **12% slower**. The decoder already
in use is also the fastest of the two available. The only remaining lever on
this group is a NIF parser, which is a new dependency and is not measured here.

## `encode_concurrency` — refuted, keep the default of 1

`bench/encodeconc.exs`, 4 concurrent writers against one table, 20 s per arm,
`ack_budget_ms: :infinity` so the admission controller is not what is measured.
Threshold set before running: **2 must beat 1 by ≥30%**.

| concurrency | run 1 | run 2 |
|---:|---:|---:|
| 1 | **39 675 rows/s** | **39 067 rows/s** |
| 2 | 36 364 (−8.3%) | 35 415 (−9.3%) |
| 4 | 36 442 (−8.1%) | 36 429 (−6.8%) |

Both runs agree in sign and rough magnitude, so this is not the ±13% coin-flip:
overlapping a table's encodes is consistently a few percent *worse*. The encode
is a dirty-IO NIF and the manifest append is one held fd; neither parallelises
by being asked to, and every extra in-flight commit holds its own batch. The
knob stays at 1.

The run answers a second question in passing: 4 writers on one table reach
~39 000 rows/s against 18 900 for one writer. The single `TableBuffer` is a
serialization point, but at this batch size it is not the binding one — 2.1× of
the 4× is there for the taking without touching it.

## `DateTime.shift_zone!` on every timestamp — removed

`Schema.value_from_json(:timestamp, binary)` shifted to `Etc/UTC` a value that
`DateTime.from_iso8601/1` had already returned in `Etc/UTC`; the offset it hands
back describes the *input*, not a shift still owed. Verified across `+02:00`,
`-05:30`, `Z` and a fractional `+09:00` — all four already UTC, `shift_zone!`
a no-op on each — so the call cost one time-zone database lookup per timestamp
column per row and bought nothing.

## Not measured

One writer, one table. Nothing here says how the groups behave under
concurrency, where the single `TableBuffer` per table serializes.
