# `bench/buffer.exs` — what group commit costs, and where it bends

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `ec0b7ac` + working tree (byte bound, partition proxy, sealing disabled) |
| Command | `mix run bench/buffer.exs` (defaults: `CALLS=20`, `BATCH=20`, `MAX_WRITERS=1024`, `WRITERS_PER_BUFFER=128`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · 10 online schedulers |

Sealing is disabled in every cell (thresholds raised out of reach). Without that,
the high-volume sections cross `seal_max_bytes` mid-cell and signal seals into a
DuckLake catalog this script never creates; the failed seals then retry with
backoff inside the timed window. See [T-55](#a-note-on-what-this-run-fixed).

## Headline

**One table saturates at ~2.2M rows/s light and ~1.1M heavy — and eight buffers
take that to 6.8M and 3.3M.** The single-table number is set by the Polars encode
plus the write path around it, not by any of the three configured bounds:
sweeping `flush_max_bytes` from 8 MB to 512 MB moves rows-per-flush 7.5× and
throughput about 5%.

The result that matters most for PL-6 is not the throughput multiplier but the
latency one. Below saturation, p50 ack is `flush_interval_ms` plus a few
milliseconds no matter the load — group commit's whole promise. At saturation that
promise breaks: p50 ack degrades to 210 ms (light) and 458 ms (heavy), 8-19× the
25 ms interval. Partitioning recovers it — 211 ms → 67 ms at P=8.

## Ack latency and throughput

Batch size × writers × table count, `flush_interval_ms: 25`.

```
  batch  writers  tables    batches/s      rows/s      MB/s      p50     p95     p99  (ms)
      1        1       1       32.7        32.7      0.01    30.6    32.4    32.4
     50        1       1       31.9      1595.7      0.26    31.2    34.9    34.9
    500        1       1       30.8     15406.9      2.57    32.9    37.0    37.0
      1        8       1      255.1       255.1      0.04    31.5    34.0    34.2
     50        8       1      262.8     13138.2      2.17    30.1    35.4    35.9
    500        8       1      235.6    117803.1     19.61    33.4    43.7    43.8
      1       32       1     1048.9      1048.9      0.18    30.5    32.2    32.3
     50       32       1      985.1     49256.6      8.13    30.8    61.7    62.6
    500       32       1      927.0    463477.3     77.16    34.7    35.8    36.0
      1        1       4       33.4        33.4      0.01    30.1    31.3    31.3
     50        1       4       32.2      1612.2      0.27    30.8    32.7    32.7
    500        1       4       31.1     15533.0      2.59    31.7    38.4    38.4
      1        8       4      237.6       237.6      0.04    33.5    37.1    40.2
     50        8       4      237.6     11878.5      1.96    33.5    36.7    39.7
    500        8       4      232.8    116388.7     19.38    34.1    38.1    39.3
      1       32       4      899.6       899.6      0.15    32.5    82.4    87.7
     50       32       4      940.2     47008.5      7.76    34.0    37.9    40.0
    500       32       4      916.5    458228.2     76.29    33.4    42.2    50.6
```

p50 sits in a 30–35 ms band across a 14,000× throughput spread. **This is the
below-saturation regime** — see the byte-bound section for what happens past it.

## Flush cadence

```
  flush_ms    batches/s      rows/s      MB/s      p50     p95     p99  (ms)
        10     1015.8     50787.7      8.39    15.1    29.4    29.6
        25      525.9     26294.6      4.34    30.3    34.0    34.3
       100      150.2      7512.4      1.24   106.0   112.1   112.3
       250       61.8      3090.9      0.51   256.7   268.9   269.1
      1000       15.8       791.2      0.13  1011.6  1019.3  1019.6
```

p50 ≈ interval + ~5 ms, linear over two orders of magnitude.

## The two fsyncs (D3)

```
  an open+write+fsync+close of a 4 KiB file: 0.3 ms min, 0.5 ms median

  store fsync    p50     p95     p99  (ms of one flush = one batch)
         true     1.9     2.5     2.7
        false     1.6     2.1     2.2
```

A whole group commit is ~1.9 ms; the segment fsync's marginal cost is ~0.3 ms.

## The inline-flush ceiling (D6)

### The sweep

```
  schema  writers      rows/s      MB/s   flushes   rows/flush   mailbox     p50 ack     p99 ack  (ms)
  light         1         593       0.1       20           20        1        33.7        45.7
  light         4        2251      0.37       20           80        1        34.4        57.9
  light        16        8944      1.48       20          320       11        33.2        57.2
  light        64       36108      5.97       20         1280       37        34.3        51.3
  light       256      150589      24.9       20         5120      152        33.9        35.3
  light      1024      539471      89.2       20        20480      715        37.4        48.1
  heavy         1         610      0.16       20           20        1        32.6        40.1
  heavy         4        2324       0.6       20           80        1        34.7        36.0
  heavy        16        9044      2.32       20          320        1        35.4        43.2
  heavy        64       36546      9.38       20         1280       31        34.9        37.6
  heavy       256      143606     36.84       20         5120      139        35.0        47.7
  heavy      1024      476664    122.29       20        20480      752        42.5        48.8
```

`flushes` is 20 in **every** cell — one per `CALLS` round trip, regardless of
writers in flight. Group commit converts added writers into a wider flush, not
more flushes, so rows/s rises with rows/flush (20 → 20,480) while cadence holds.

**This sweep does not find the ceiling, and cannot.** At 20-row batches even 1024
writers only build a 20,480-row flush (3.4 MiB) — under every bound. The linear
scaling is real but it is measuring headroom, not a limit. Mailbox depth stays at
or below the writer count because each writer has one outstanding `GenServer.call`.

### The encode in isolation

`Writer.write` timed outside the buffer:

```
  schema     rows   encode min   encode med   fits 25ms      ceiling rows/s
  light       100          1.7          1.9       true                4000
  light      1000          2.1          2.1       true               40000
  light     10000          3.9          4.1       true              400000
  light     50000         12.4         13.4       true             2000000
  light    100000         22.5         22.5       true             4000000
  heavy       100          1.9          2.1       true                4000
  heavy      1000          2.8          2.8       true               40000
  heavy     10000          6.3          6.4       true              400000
  heavy     50000         23.9         26.0      false             1924335
  heavy    100000         43.5         43.6      false             2291318
```

Encode throughput above ~10K rows is roughly scale-invariant: **~4.4M rows/s light
(100K in 22.5 ms), ~2.3M heavy (43.6 ms)**. The light crossover against a 25 ms
interval sits around 100K rows/flush and is noisy — a previous run measured 29.4 ms
for the same cell, so treat it as a region, not a point.

**Read the last column as an upper bound, not a prediction.** It prices the encode
alone and overestimates measured saturation by roughly 2×; the write path around
the encode costs the other half.

### The fsyncs at the ceiling

Same store-fsync toggle as D3, re-run at 1024 writers:

```
  schema  store fsync      rows/s   flushes   mailbox     p50 ack     p99 ack  (ms)
  light   true             544083       20      672        37.3        39.1
  light   false            559394       20      720        36.3        38.6
  heavy   true             486456       20      903        41.7        46.9
  heavy   false            504145       20      570        40.0        45.5
```

±4%, the wrong way for both schemas — noise. One fsync per flush amortized over
20,480 rows is nothing. The manifest log's fsync is unconditional so it is not
isolated here, but it is the same single fsync per flush.

### The byte bound

1024 writers × 500-row batches offers 512,000 rows per cycle — enough to saturate.

```
  schema  flush_max_bytes      rows/s   flushes   rows/flush   MiB/flush     p50 ack
  light               8 MB     2056362      217        47189        7.5       239.0
  light              32 MB     2298303       56       182857       29.2       225.3
  light             512 MB     2170787       29       353103       56.3       222.8
  heavy               8 MB     1095334      345        29681        7.4       457.5
  heavy              32 MB     1218649       87       117701       29.3       400.1
  heavy             512 MB     1058182       39       262564       65.4       480.3
```

Two things fall out, and the second was a surprise:

1. **The 8 MB default is what caps rows/flush.** 47,189 light and 29,681 heavy are
   8 MB at the measured ~165 and ~256 bytes/row. Neither `flush_interval_ms` nor
   the encode gets to decide; the byte bound fires first, ~10× more often than the
   interval (217 flushes against the 20 the interval alone would give).
2. **Raising it 64× barely matters.** rows/flush moves 7.5× while throughput moves
   ~5% and is not even monotonic (32 MB beats 512 MB on both schemas). So the byte
   bound decides how rows are *packed* into flushes, not how many get through. One
   table's real ceiling is **~2.2M rows/s light, ~1.1M heavy**, set by the encode
   plus its write path.

**And p50 ack blows out to 210–480 ms**, 8–19× the interval. This is the boundary
condition the first table does not show: offered rows per cycle (512,000) far
exceed flushed rows per cycle (~47,000), so acks queue about ten cycles deep. Group
commit's "a client pays the cadence, not the load" holds right up until it doesn't.

### The partition proxy

P independent TableBuffers over one workload, addressed as P tables. Partitions
differ from tables in routing and identity, not in throughput mechanics, so this
reads the multiplier PL-6 would buy without building PL-6.

```
  mode    schema    P   writers      rows/s      vs P=1   flushes   rows/flush   mailbox     p50 ack
  split   light     1     1024     2113929           -       29       353103      704       210.6
  split   light     2     1024     3696704       1.75x       45       227556      591       123.5
  split   light     4     1024     4378941       2.07x       70       146286      313       111.9
  split   light     8     1024     6818348       3.23x      163        62822      155        67.2
  split   heavy     1     1024     1017859           -       39       262564     1017       497.5
  split   heavy     2     1024     1670392       1.64x       67       152836      788       290.9
  split   heavy     4     1024     2598246       2.55x      103        99417      381       182.1
  split   heavy     8     1024     3313477       3.26x      183        55956      199       133.7
  scale   light     1      128     1315315           -       20        64000       91        47.2
  scale   light     2      256     2459753       1.87x       40        64000      128        50.9
  scale   light     4      512     4382875       3.33x       80        64000      128        57.6
  scale   light     8     1024     6804954       5.17x      168        60952      133        71.5
  scale   heavy     1      128      946673           -       20        64000      106        65.5
  scale   heavy     2      256     1580189       1.67x       43        59535      131        77.4
  scale   heavy     4      512     2399310       2.53x       74        69189      190       103.3
  scale   heavy     8     1024     3047259       3.22x      196        52245      165       154.5
```

- **`split`** divides a fixed 1024-writer pool P ways — partitioning a real
  workload. 3.23× light and 3.26× heavy at P=8, and p50 ack falls 211 → 67 ms
  (light) and 498 → 134 ms (heavy) as each buffer gets a shallower queue.
- **`scale`** holds 128 writers per buffer, so `rows/flush` stays pinned at 64,000
  and total load grows with P — the headroom question. 5.17× light at P=8, 65%
  efficiency on 10 cores, with p50 ack barely moving (47 → 72 ms) because no buffer
  is ever more than 128 writers deep.
- **The two modes agree to 0.2% at P=8** (6,804,954 vs 6,818,348), which they must:
  both are 1024 writers across 8 buffers there. Different baselines, different
  paths, same endpoint — a free consistency check on the harness.
- The multiplier is nearly schema-independent (3.23× vs 3.26× on `split`), which is
  what you expect if what is being parallelized is the encode.
- `split` light P=4 (2.07×) sits below its own trend line between 1.75× and 3.23×.
  Treat that cell as noise rather than a knee until a repeat run says otherwise.

**On the prediction this section was built to test.** The claim was that below
saturation, dividing a fixed workload P ways should be throughput-*neutral* — P
encodes of 1/P the rows finish inside the same cycle, so nothing is gained. That
held: at 8 writers (a smoke run, far below saturation) `split` measured 0.95×,
0.89×, 0.83× at P=2/4/8 — neutral shading to slightly negative on per-flush fixed
costs. It correctly did **not** hold here, because at 1024 × 500 rows P=1 is
already deep past saturation. The prediction stands with its condition attached,
and the condition is the part PL-6 needs: partitioning buys nothing until a table
is at its ceiling, and buys a lot once it is.

## What this settles

- **PL-6 decision 1 — what the single-partition ceiling is.** The encode plus the
  write path around it: ~2.2M rows/s light, ~1.1M heavy. Not the manifest fsync
  (±4%, noise), not the mailbox (tracks in-flight calls), and not `flush_max_bytes`
  (64× the bound, ~5% the throughput). The byte bound is worth knowing about
  anyway, because at its 8 MB default it — not the interval — is what decides how
  many rows one encode swallows.
- **The writer sweep is not a ceiling measurement.** Its linear scaling to 1024
  writers is real, but 20-row batches cannot saturate anything. Any future "no
  plateau found" claim needs 500-row batches or an equivalently wide flush.
- **Partitioning is worth building, and the reason is latency as much as
  throughput.** 3.2× throughput on a fixed workload at P=8, and a 3.1× ack-latency
  recovery. Group commit's contract is that a client pays the flush cadence rather
  than the load on it; a single buffer stops honouring that at saturation (210 ms
  light, 458 ms heavy against a 25 ms interval) and P=8 largely restores it. That is
  a contract argument, not a headroom one.
- **This does not validate PL-6's identity work.** The proxy uses P tables, so it
  measures throughput mechanics only. Per-partition manifest logs, the legacy
  log-filename rule, claim/retire routing, and flush fan-out are all still
  unbuilt and unmeasured (PL-6 steps 2-3).
- **Inline flush stays; double-buffering still has no case.** It hides one encode
  behind the next accumulation, where partitioning multiplies the encode itself and
  fixes the ack degradation too.
- **Ack latency is `flush_interval_ms` plus a few ms — below saturation.** Held
  across a 14,000× throughput spread. The qualifier is new and load-bearing.

## A note on what this run fixed

An earlier attempt at this run was discarded. The two new high-volume sections
write ~10M rows per cell, which crosses `seal_max_bytes` mid-cell and signals seals
into a DuckLake catalog `bench/buffer.exs` never creates. Every one failed with
`Catalog Error: Table with name events does not exist!` and retried with backoff
*inside the timed window*. `start_buffer/2` now raises the three seal thresholds
out of reach: this script measures group commit, `bench/sealer.exs` measures
sealing. It is also why `flush_count/2` can trust the manifest — nothing retires
entries out from under it. The same error class in `sealer.exs` is tracked as T-55.
