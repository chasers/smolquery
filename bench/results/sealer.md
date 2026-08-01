# `bench/sealer.exs` — what a seal costs, and how far behind it runs

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `890b5a1` |
| Command | `mix run bench/sealer.exs` (defaults: `INPUTS=32`, `ROWS=5000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 |

> **This run is not clean.** The seal-lag section logged
> `Catalog Error: Table with name events does not exist!` once per cell before
> succeeding on retry, so its lag and handoff numbers include a failed attempt and
> its backoff. See [Known defect](#known-defect-in-this-run). The merge, size, and
> implementation sections are unaffected.

## Headline

**DuckDB `COPY` beats Explorer concat by 31% and keeps rows out of the BEAM
entirely**, and merging gets cheaper per row as claims get bigger — 159K rows/s on
a 4×1000 claim, 6.5M rows/s on a 64×10000 one. Sealing also *shrinks* data:
64 inputs merge to 0.63× their input bytes, which is the compaction win arriving
before the compactor does.

## Merge implementation

32 inputs of 5,000 rows each, read over HTTP through httpfs:

```
  implementation      p50      min     peak heap (MiB)   rows/s
  DuckDB COPY            39.7     38.0              0.0   4033477.9
  Explorer concat        52.2     51.3              0.0   3067249.4
```

Peak heap is the merging process's own. Both read 0.0 MiB here because the
Explorer path also keeps frames off the BEAM heap — the number is there to catch a
regression that starts materializing rows, not to separate these two today.

## Merge throughput

```
  inputs     rows   total rows    merge ms      rows/s   sealed KiB
       4     1000         4000        25.1    159045.7         27.6
       4    10000        40000        30.6   1307360.4        184.2
      16     1000        16000        37.9    422419.9         96.2
      16    10000       160000        49.8   3212657.9        694.1
      64     1000        64000        62.9   1018232.7        312.6
      64    10000       640000        98.7   6481275.2       2746.6
```

There is a large fixed cost per merge (~25 ms floor at 4,000 rows) and a shallow
slope after it: 160× the rows costs 4× the time. Input *count* costs more than
input *size* — 64×1000 (64K rows) takes 62.9 ms while 4×10000 (40K rows) takes
30.6 ms, so each additional file to open is worth roughly 600 µs. Seal on bytes,
not on file count, wherever there is a choice.

## Seal lag

```
  seal_max_files    writes    lag ms    handoff ms   sealed files
               1         1        65          44.9             1
               8         8       185          45.8             1
              32        32       643          61.5             1
```

Lag excludes the threshold wait itself (this signals immediately); add
`seal_max_age_ms` to read the real loss window a deployment carries. **These three
numbers are upper bounds** — each includes one failed seal attempt, per the defect
below. Handoff (44.9–61.5 ms) is the more trustworthy column: it tracks the merge
cost above and is roughly flat in claim size.

## Sealed segment size

```
  inputs   input bytes (MiB)   sealed (MiB)   ratio   rows
       4                0.1            0.1    0.8    20000
      16                0.5            0.4   0.65    80000
      64                2.2            1.4   0.63   320000
```

A ratio under 1 is Parquet doing better on one large file than on many small ones.
This is the number the script exists for: sealing silently made data **2.85×
larger** until the codec was matched (`COPY` defaults to snappy, segments are
written with zstd), and no correctness test could catch it. A ratio above 1 here
means that regression is back.

## Known defect in this run

The seal-lag section logged, once per cell:

```
seal of {"analytics", "events"} failed: Catalog Error: Table with name events
does not exist! ... SELECT data_file FROM ducklake_list_files('lake', 'events',
schema => 'analytics'...
```

The seal retried and succeeded — hence `sealed files: 1` in every row — so the
section still produces numbers, but they carry a failed attempt plus its backoff.
The fixture appears to signal a seal before the DuckLake table it registers into
exists. Tracked as **T-55**; re-run and replace this file once fixed.

## What this settles

- **`COPY` is the merge implementation.** 31% faster, and the rows never enter the
  BEAM. Explorer concat stays only as the comparison that justifies the choice.
- **Claims should be bounded by bytes, not file count.** Per-file cost (~600 µs)
  dominates per-row cost at small sizes, and the merge has a ~25 ms floor no claim
  can amortize away below ~10K rows.
- **The codec must stay zstd on both sides.** The 0.63–0.8 ratio is the assertion;
  it was 2.85 when the codec drifted.
