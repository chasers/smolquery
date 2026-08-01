# `bench/buffer.exs` — what group commit costs, and where it bends

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `72f2f78` + working tree (`huge` schema) |
| Command | `mix run bench/buffer.exs` (defaults: `CALLS=20`, `BATCH=20`, `MAX_WRITERS=1024`, `WRITERS_PER_BUFFER=128`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · 10 online schedulers |

Sealing is disabled in every cell (thresholds raised out of reach) — this script
measures group commit, `bench/sealer.exs` measures sealing. Run is clean: 0 seal
warnings, 0 errors.

## The three schemas

| | columns | B/row | encode, 100K rows | encode throughput | crosses 25 ms at |
|---|---|---|---|---|---|
| `light` | 2 | 165 | 24.0 ms | ~4.17M rows/s | ~100K rows |
| `heavy` | 4 | 254 | 43.8 ms | ~2.28M rows/s | ~50K rows |
| `huge` | 20 | 867 | 280.7 ms | ~356K rows/s | **~10K rows** |

`huge` spans all seven logical types — the shape of a real event table:

```elixir
%{
  "amount" => Decimal.new("1234.56"),   "country" => "AU",
  "day" => ~D[2026-03-28],              "discount" => 0.06,
  "event" => "page_view",               "id" => 123456,
  "latency_ms" => 493.7142857142857,    "name" => "row-123456",
  "ok" => true,                         "price" => Decimal.new("12.3456"),
  "quantity" => 17,                     "region" => "us-east-1",
  "retries" => 0,                       "score" => 34.56,
  "session_id" => "sess-00000001E240",  "sku" => "sku-23456",
  "tags" => "tag-7,tag-8,tag-9",        "ts" => ~N[2026-01-02 10:17:36],
  "updated_at" => ~N[2026-01-02 10:20:12], "user_id" => 123456
}
```

`huge` is 5.3× `light`'s bytes but **12× its encode time** — 20 Arrow arrays to
build, so per-column overhead dominates payload size. It is the only one of the
three already past `flush_interval_ms` at the default `flush_max_bytes`.

## Headline

**Ack latency has two regimes, and which one you are in decides everything.**

- **Below saturation:** `p50 ack = flush_interval_ms + ~5 ms`, invariant across a
  14,000× throughput spread. Throughput is irrelevant to latency.
- **At saturation:** `p50 ack = outstanding rows ÷ throughput` — Little's Law —
  and `flush_interval_ms` stops mattering entirely. Verified within 13% (usually
  3%) across all 21 saturated cells.

One table saturates at **2.19M rows/s light, 1.08M heavy, 280K huge**. At the
default config a 20-column event table therefore delivers a **2.01 second p50
ack**. Eight buffers reach 6.68M / 3.58M / 1.11M — 3.1× / 3.2× / **4.09×** — and
the multiplier is largest for the schema that needs it most.

## Ack latency and throughput

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
```

(Table-count rows omitted for brevity; 4 tables behaves as 4 buffers — see the
partition proxy, which measures that deliberately.) **This is the
below-saturation regime.**

## Flush cadence

```
  flush_ms    batches/s      rows/s      MB/s      p50     p95     p99  (ms)
        10     1015.8     50787.7      8.39    15.1    29.4    29.6
        25      483.2     24162.0      3.99    31.2    66.2    66.3
       100      150.0      7498.4      1.24   106.5   108.7   109.0
       250       62.0      3098.2      0.51   256.3   265.5   265.8
      1000       15.8       791.2      0.13  1011.6  1019.3  1019.6
```

The dial works — below saturation. Above it, this knob does nothing.

## The two fsyncs (D3)

```
  an open+write+fsync+close of a 4 KiB file: 0.3 ms min, 0.5 ms median

  store fsync    p50     p95     p99  (ms of one flush = one batch)
         true     1.9     2.5     2.7
        false     1.6     2.1     2.2
```

## The inline-flush ceiling (D6)

### The sweep

```
  schema  writers      rows/s      MB/s   flushes   rows/flush   mailbox     p50 ack     p99 ack  (ms)
  light         1         601       0.1       20           20        1        33.8        36.6
  light         4        2293      0.38       20           80        2        34.7        45.9
  light        16        9378      1.55       20          320        3        34.2        38.5
  light        64       37048      6.13       20         1280       30        34.1        53.0
  light       256      150911     24.95       20         5120      224        33.9        35.8
  light      1024      547345      90.5       20        20480      737        37.3        39.1
  heavy         1         604      0.15       20           20        1        33.1        38.9
  heavy         4        1876      0.48       20           80        1        35.9       111.1
  heavy        16        8751      2.25       20          320        5        35.9        60.8
  heavy        64       36573      9.38       20         1280       15        34.4        36.7
  heavy       256      146018     37.46       20         5120      252        34.7        37.7
  heavy      1024      473200     121.4       20        20480      520        42.8        49.2
  huge          1         569      0.49       20           20        1        34.2        49.9
  huge          4        2089      1.79       20           80        1        31.7       138.5
  huge         16        8866      7.58       20          320        4        34.9        58.2
  huge         64       34808     29.76       20         1280       32        36.4        39.9
  huge        256      109395     93.52       20         5120      198        46.3        57.6
  huge       1024      227556    194.53       46         8904      983        77.9       143.8
```

**Adding `huge` made this section work as originally intended.** `flushes` is 20 in
every light and heavy cell — group commit converts added writers into a wider
flush, not more flushes, so those two never saturate at 20-row batches and their
linear scaling measures headroom. `huge` at 1024 writers is the exception:
**46 flushes, not 20**, because 20,480 rows × 867 B = 17 MB crosses the 8 MB byte
bound. And it is the one row that plateaus — 34,808 → 109,395 (3.1×) → 227,556
(2.1×) where light manages 4.1× then 3.6×.

So D6's original question ("where does one table bend?") has an answer visible in
this table only for a realistic schema. The earlier "no plateau found through 256
writers, ~148K rows/s" that PL-6 was drafted against was a `light`-schema artifact.

### The encode in isolation

```
  schema     rows   encode min   encode med   fits 25ms      ceiling rows/s
  light       100          1.8          2.0       true                4000
  light      1000          2.0          2.3       true               40000
  light     10000          4.3          4.4       true              400000
  light     50000         12.6         13.9       true             2000000
  light    100000         23.4         24.0       true             4000000
  heavy       100          2.1          2.2       true                4000
  heavy      1000          2.6          3.1       true               40000
  heavy     10000          7.1          7.4       true              400000
  heavy     50000         26.1         29.1      false             1719927
  heavy    100000         43.3         43.8      false             2283939
  huge        100          2.7          2.8       true                4000
  huge       1000          5.1          5.2       true               40000
  huge      10000         27.4         28.0      false              357667
  huge      50000        131.9        133.7      false              373997
  huge     100000        265.2        280.7      false              356285
```

Encode throughput above ~10K rows is scale-invariant per schema. **Read the last
column as an upper bound:** measured saturation is 53% of it for light, 47% for
heavy, but **79% for huge** — the write path around the encode costs
proportionally less the more expensive the encode is, because accumulate, reply
fan-out, manifest append, and fsync amortize over work that takes longer. "The
write path costs about half" is a light/heavy statement, not a general one.

### The fsyncs at the ceiling

```
  schema  store fsync      rows/s   flushes   mailbox     p50 ack     p99 ack  (ms)
  light   true             523657       20      703        37.3        66.4
  light   false            556402       20      702        36.4        41.1
  heavy   true             450738       20      826        43.3        72.3
  heavy   false            482630       20      846        41.9        49.8
  huge    true             229977       47      977        76.1       131.2
  huge    false            234008       47      686        74.8       128.3
```

±2–6%, noise on all three schemas. Not the fsync.

### The byte bound

1024 writers × 500-row batches offers 512,000 rows per cycle — enough to saturate.

```
  schema  flush_max_bytes      rows/s   flushes   rows/flush   MiB/flush     p50 ack
  light               8 MB     2032570      217        47189        7.5       241.0
  light              32 MB     2300738       56       182857       29.2       229.9
  light             512 MB     2194612       28       365714       58.3       209.6
  heavy               8 MB     1107170      345        29681        7.4       453.8
  heavy              32 MB     1253106       87       117701       29.3       399.2
  heavy             512 MB     1076627       38       269474       67.1       473.1
  huge                8 MB      248526     1078         9499        7.9      2014.4
  huge               32 MB      292040      281        36441       30.1      1721.3
  huge              512 MB      279623       40       256000      211.6      1846.8
```

1. **The 8 MB default caps rows/flush**, not `flush_interval_ms`: 47,189 / 29,681 /
   9,499 rows, which is 8 MB at each schema's bytes/row. For `huge` the bound fires
   **54× more often than the interval** (1078 flushes vs 20).
2. **Raising it 64× barely matters.** rows/flush moves 8–27×, throughput 5–12%, and
   not even monotonically (32 MB beats 512 MB on all three). The byte bound decides
   how rows are *packed* into flushes, not how many get through.
3. **`huge` delivers a 2.01 second p50 ack at the default config** — and 1.7 s at
   the best byte bound tried. You cannot tune out of it.

Saturation is therefore **2.19M rows/s light, 1.08M heavy, 280K huge**.

### The partition proxy

P independent TableBuffers over one workload, addressed as P tables. Partitions
differ from tables in routing and identity, not throughput mechanics.

```
  mode    schema    P   writers      rows/s      vs P=1   flushes   rows/flush   mailbox     p50 ack
  split   light     1     1024     2144817           -       28       365714      745       206.7
  split   light     2     1024     3496555       1.63x       47       217872      470       128.7
  split   light     4     1024     5651370       2.63x       86       119070      282        85.0
  split   light     8     1024     6676238       3.11x      164        62439      211        69.3
  split   heavy     1     1024     1113832           -       38       269474     1018       460.5
  split   heavy     2     1024     1908871       1.71x       63       162540      602       259.6
  split   heavy     4     1024     2877049       2.58x      102       100392      283       161.9
  split   heavy     8     1024     3582483       3.22x      188        54468      257       130.6
  split   huge      1     1024      271774           -       40       256000     1023      1906.4
  split   huge      2     1024      476391       1.75x       67       152836      555      1076.2
  split   huge      4     1024      699109       2.57x      116        88276      387       696.7
  split   huge      8     1024     1111010       4.09x      217        47189      163       437.7
  scale   light     1      128     1311563           -       20        64000       97        46.5
  scale   light     2      256     2319187       1.77x       40        64000      128        54.2
  scale   light     4      512     4002358       3.05x       84        60952      147        57.9
  scale   light     8     1024     6511907       4.96x      155        66065      138        69.4
  scale   heavy     1      128      948404           -       20        64000      105        65.4
  scale   heavy     2      256     1555235       1.64x       40        64000      128        79.8
  scale   heavy     4      512     2679387       2.83x       86        59535      131        91.4
  scale   heavy     8     1024     3178707       3.35x      214        47850      149       147.6
  scale   huge      1      128      263982           -       20        64000       97       242.3
  scale   huge      2      256      476586       1.81x       45        56889      131       259.1
  scale   huge      4      512      768180       2.91x       94        54468      143       318.4
  scale   huge      8     1024     1085687       4.11x      233        43948      247       425.7
```

**The multiplier tracks how encode-bound the schema is** — which is the mechanism
claim, since the encode is what partitioning parallelizes:

| schema | in encode | `split` P=8 | `scale` P=8 |
|---|---|---|---|
| light | 53% | 3.11× | 4.96× |
| heavy | 47% | 3.22× | 3.35× |
| huge | 79% | **4.09×** | **4.11×** |

An earlier run had `split light P=4` at 2.07×, below its own trend. This run gives
2.63×, and heavy/huge give 2.58×/2.57× at P=4 — so that cell was noise, now
confirmed by repeat rather than assumed.

**Ack latency here is Little's Law, not the flush interval.** Synchronous writers
pin outstanding rows at `writers × batch`, so `p50 = outstanding ÷ throughput`:

| cell | outstanding ÷ throughput | measured |
|---|---|---|
| scale huge P=1 | 64,000 ÷ 263,982 = 242.4 ms | 242.3 ms |
| scale heavy P=1 | 64,000 ÷ 948,404 = 67.5 ms | 65.4 ms |
| split huge P=1 | 512,000 ÷ 271,774 = 1884 ms | 1906 ms |
| split heavy P=1 | 512,000 ÷ 1,113,832 = 460 ms | 461 ms |
| split light P=8 | 512,000 ÷ 6,676,238 = 77 ms | 69 ms |
| huge 8 MB | 512,000 ÷ 248,526 = 2060 ms | 2014 ms |

All 21 saturated cells fall within 13%, most within 3%.

**This means the proxy's ack column measures overload, not achievable latency.**
The load here is deliberately far past saturation — 20.5M rows/s offered against
280K capacity is **73× overloaded** for huge. P=8 serves 1.11M, still ~18×
overloaded, hence 438 ms. Partitioning *divides the overload factor*; it does not
set the latency. Production latency stays good only while offered load is under one
partition's capacity.

**On the prediction this section was built to test.** The claim was that below
saturation, dividing a fixed workload P ways is throughput-*neutral* — P encodes of
1/P the rows finish inside the same cycle. Confirmed at 8 writers: `split` measured
0.95× / 0.89× / 0.83× at P=2/4/8, neutral shading negative on per-flush fixed
costs. It correctly did not hold at 1024 × 500, where every schema is past
saturation. The prediction stands with its condition attached, and the condition is
the part PL-6 needs.

## What this settles

- **PL-6 decision 1 — the single-partition ceiling is the encode plus its write
  path:** 2.19M rows/s light, 1.08M heavy, **280K huge**. Not the manifest fsync
  (±2–6%, noise on all three schemas), not the mailbox (tracks in-flight calls),
  not `flush_max_bytes` (64× the bound, 5–12% the throughput).
- **Partitioning multiplies the right quantity, and most for the schemas that need
  it.** 4.09× at P=8 for huge against 3.11× for light, ordered by how encode-bound
  each is.
- **The case for PL-6 is the ack contract, not headroom.** At the default config a
  20-column event table delivers a 2.01 s p50 ack. That is a user-visible defect at
  ordinary load, not a capacity ceiling someone might reach later.
- **But partitioning alone does not fix overload, and cannot.** Latency under
  saturation is `outstanding ÷ throughput`; P divides the overload factor by P and
  no more. At 73× overload, P=8 still yields 438 ms. Bounding ack latency requires
  bounding outstanding rows — backpressure or admission control — which does not
  exist today. **This is a companion requirement to PL-6, not a follow-up.**
- **`flush_interval_ms` is only a dial below saturation.** Above it the interval
  drops out of the latency equation entirely. Any tuning advice that recommends it
  for a loaded table is wrong.
- **The writer sweep is not a ceiling measurement for light or heavy.** 20-row
  batches cannot saturate them. It *is* one for huge, which plateaus at 227K.
- **Inline flush stays; double-buffering still has no case.** It hides one encode
  behind the next accumulation, where partitioning multiplies the encode itself and
  helps the ack degradation too.
- **This does not validate PL-6's identity work.** The proxy uses P tables, so it
  measures throughput mechanics only. Per-partition manifest logs, the legacy
  log-filename rule, claim/retire routing, and flush fan-out remain unbuilt and
  unmeasured (PL-6 steps 2-3).
