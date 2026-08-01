# `bench/buffer.exs` — what group commit costs, and where it bends

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `890b5a1` |
| Command | `mix run bench/buffer.exs` (defaults: `CALLS=20`, `BATCH=20`, `MAX_WRITERS=1024`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · 10 online schedulers |

## Headline

**One table does not plateau at 1024 concurrent writers — it reaches ~493K
rows/s and is still linear.** The thing that eventually bends is the Polars
encode, and it bends *structurally* rather than at a writer count: throughput is
`rows_per_flush / max(encode, flush_interval_ms)`, and group commit grows
`rows_per_flush` with load until one encode outgrows the interval. Measured off
to the side, that crossover puts one table's ceiling at **~1.7M rows/s on the
heavy schema and ~2.0–3.4M on the light one** — 10–20× the 148K rows/s
[PL-6](#what-this-settles) was written against.

Neither of the other two candidates bends first: toggling the segment fsync at
1024 writers moves throughput by less than the run-to-run noise, and mailbox
depth tracks in-flight calls (≤ writer count) instead of running away.

## Ack latency and throughput

Batch size × writers × table count, `flush_interval_ms: 25`.

```
  batch  writers  tables    batches/s      rows/s      MB/s      p50     p95     p99  (ms)
      1        1       1       18.7        18.7       0.0    37.9   268.6   268.6
     50        1       1       21.2      1060.5      0.18    38.7   103.0   103.0
    500        1       1       25.2     12599.4       2.1    38.1    63.8    63.8
      1        8       1      166.5       166.5      0.03    40.6   120.8   120.9
     50        8       1      134.8      6739.4      1.11    38.3   202.2   202.3
    500        8       1      124.6     62282.5     10.37    59.3   113.9   113.9
      1       32       1      691.7       691.7      0.12    44.0    64.9    65.1
     50       32       1      738.9     36945.7       6.1    40.1    80.5    80.9
    500       32       1      664.0    332000.1     55.27    46.4    68.6    69.2
      1        1       4       19.9        19.9       0.0    46.5    87.9    87.9
     50        1       4       22.8      1138.3      0.19    42.0    70.8    70.8
    500        1       4       20.6     10312.0      1.72    43.9   144.4   144.4
      1        8       4      191.9       191.9      0.03    39.7    62.3    78.7
     50        8       4      206.5     10326.6      1.71    37.0    62.5    64.6
    500        8       4      221.9    110942.8     18.47    35.8    41.5    43.2
      1       32       4      886.4       886.4      0.15    34.7    49.4    51.8
     50       32       4      912.7     45632.9      7.54    34.4    41.1    47.1
    500       32       4      893.0    446502.7     74.34    35.7    38.6    39.4
```

p50 stays in a 35–60 ms band across a 24,000× spread in throughput. Load moves
rows/s, not ack latency.

## Flush cadence

```
  flush_ms    batches/s      rows/s      MB/s      p50     p95     p99  (ms)
        10     1019.7     50985.5      8.42    15.8    17.2    17.4
        25      494.0     24702.1      4.08    31.9    38.7    38.8
       100      146.1      7305.4      1.21   109.6   116.7   117.1
       250       61.6      3081.6      0.51   259.8   265.3   265.5
      1000       15.7       787.2      0.13  1011.0  1059.4  1059.6
```

p50 ≈ interval + ~7 ms, linear over two orders of magnitude.

## The two fsyncs (D3)

```
  an open+write+fsync+close of a 4 KiB file: 0.3 ms min, 0.8 ms median

  store fsync    p50     p95     p99  (ms of one flush = one batch)
         true     2.5     5.6     7.7
        false     2.3     4.6    10.4
```

A whole group commit is ~2.5 ms; the segment fsync's marginal cost is ~0.2 ms.
Both fsyncs together are an order of magnitude under any interval worth running.

## The inline-flush ceiling (D6)

### The sweep

```
  schema  writers      rows/s      MB/s   flushes   rows/flush   mailbox     p50 ack     p99 ack  (ms)
  light         1         582       0.1       20         20.0        1        33.5        46.6
  light         4        2179      0.36       20         80.0        1        35.2        54.2
  light        16        8968      1.48       20        320.0        1        34.3        53.1
  light        64       32983      5.45       20       1280.0       11        36.1        78.4
  light       256      145944     24.13       20       5120.0      170        34.3        43.9
  light      1024      492904      81.5       20      20480.0      809        40.5        52.6
  heavy         1         483      0.12       20         20.0        1        31.0       104.6
  heavy         4        1586      0.41       20         80.0        1        36.5       145.4
  heavy        16       10145       2.6       20        320.0        1        30.7        42.7
  heavy        64       36865      9.46       20       1280.0       37        32.3        55.8
  heavy       256      143929     36.92       20       5120.0      168        34.9        47.2
  heavy      1024      427265    109.61       20      20480.0      532        46.9        60.6
```

`flushes` is 20 in **every** cell — one per `CALLS` round trip, regardless of how
many writers are in flight. That is the mechanism the whole table is explaining:
group commit converts added writers into a wider flush, not more flushes, so
rows/s rises with `rows/flush` (20 → 20,480) while the cadence holds. 1024
writers is not a ceiling; it is 1024 writers.

The heavy schema costs ~13% at the top (427K vs 493K rows/s) and 35% more bytes
per second, which is the encode doing more work per row — visible, but not a
wall.

Mailbox depth peaks below the writer count (809 of 1024) because each writer has
at most one outstanding `GenServer.call`. It never diverges from in-flight
demand, so the buffer is not falling behind its own inbox.

### The encode in isolation

`Writer.write` timed outside the buffer, against the `rows/flush` the sweep
produced:

```
  schema     rows   encode min   encode med   fits 25ms      ceiling rows/s
  light       100          2.3          2.9       true                4000
  light      1000          2.5          2.9       true               40000
  light     10000          4.3          5.0       true              400000
  light     50000         13.2         13.5       true             2000000
  light    100000         26.1         29.4      false             3402634
  heavy       100          2.9          3.4       true                4000
  heavy      1000          3.3          4.1       true               40000
  heavy     10000          7.7          8.5       true              400000
  heavy     50000         26.4         30.4      false             1646470
  heavy    100000         54.0         58.6      false             1707679
```

This is where the ceiling actually is. While an encode fits inside
`flush_interval_ms` the cadence sets the flush rate and rows/s is just
`rows_per_flush / interval`. The first row that misses the interval is the
crossover:

- **light** crosses between 50K and 100K rows/flush → ceiling **~2.0–3.4M rows/s**
- **heavy** crosses between 10K and 50K rows/flush → ceiling **~1.7M rows/s**

At 20-row batches that needs roughly 2,500–5,000 concurrent writers to reach; at
500-row batches, ~100–200. Big batches get there far sooner than many writers do.

The sweep's top cell agrees with the model: 20,480 rows/flush at 492,904 rows/s
is a 41.5 ms effective cycle, against 25 ms interval + ~7 ms encode. The
remaining ~9 ms is replying to 1024 blocked callers and re-scheduling them — a
real second-order cost at that fan-in, and the reason p50 drifts from 34 ms to
40 ms across the sweep.

### The fsyncs at the ceiling

Same store-fsync toggle as D3, re-run at 1024 writers:

```
  schema  store fsync      rows/s   flushes   mailbox     p50 ack     p99 ack  (ms)
  light   true             513821       20      697        38.9        45.9
  light   false            542440       20      698        36.5        43.7
  heavy   true             455206       20      568        44.4        51.9
  heavy   false            434294       20      509        45.1        78.6
```

±6%, and it goes the *wrong* way for the heavy schema — this is noise, not
signal. One fsync per flush amortized over 20,480 rows is nothing. The manifest
log's fsync is unconditional so it is not isolated here, but it is the same
single fsync per flush, amortized the same way.

## What this settles

- **PL-6 decision 1 — what the single-partition ceiling is: the encode.** Not the
  manifest fsync (amortized to nothing per flush; toggling the segment fsync at
  the top of the sweep moves less than noise) and not the mailbox (depth tracks
  in-flight calls, never runs away). One table's ceiling is
  `rows_per_flush / max(encode, flush_interval_ms)`, which lands at ~1.7M rows/s
  on the heavy schema and ~2.0–3.4M on the light one.
- **Partitioning is the right multiplier — and less urgent than PL-6 assumed.**
  P independent TableBuffers give P parallel encodes, which is exactly the
  quantity that binds, so PL-6's design multiplies the ceiling it should. But
  PL-6 was written against "~148K rows/s at 256 writers, no plateau found"; the
  actual wall is an order of magnitude higher. Partitioning buys headroom for
  heavy schemas and large batches, not relief from a 148K wall. Nothing here
  argues for shipping it before something needs >1M rows/s on one table.
- **Double-buffering (D6's original question) still has no evidence behind it.**
  It would hide the encode behind the next accumulation — worth ~7 ms of the
  41.5 ms cycle at 1024 writers, and worth more only past the crossover, where
  partitioning is the better lever anyway.
- **Ack latency is `flush_interval_ms` plus single-digit milliseconds, and load
  does not move it.** Confirmed across a 24,000× throughput spread. The one
  drift worth naming is fan-in: replying to ~1000 blocked callers adds ~6 ms to
  p50 versus a handful of writers.
