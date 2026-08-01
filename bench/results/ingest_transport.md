# `bench/ingest_transport.exs` — ingest→buffer transport (PL-8 D4)

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `292cefe` |
| Command | `mix run bench/ingest_transport.exs` (defaults: `REPS=15`, `WRITERS=8`, `BATCHES=10`, `ROWS=5000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · real peer BEAM running the buffer role |

Shapes: narrow = the 4-column support schema (int64, timestamp, string,
numeric); wide = 20 columns (those four + 8 float64 + 8 string). Both
transports fully materialize the payload on the peer; every IPC timing
includes the ingest side's rows→DataFrame encode.

## Headline

**gen_rpc term transfer stays the v1 ingest→buffer transport; no Arrow IPC
transport gets built now.** End to end the two are a wash, because today's
row-based `TableBuffer` forces the IPC side to pay frame→rows — which erases
its transport win exactly where payloads are big. The measured route to more
concurrent throughput is T-29's channel partitioning (2.5–2.6×), not a
transport swap. Arrow IPC becomes the right call only when the buffer accepts
frames end to end (4–5× faster serially, 2–4× smaller on the wire) — that
pairs naturally with T-57's partitioned-write rework, so the option is
priced, not dead.

## Wire size — term_to_binary vs Arrow IPC

```
shape                 rows    term KiB     ipc KiB  ipc/term
narrow (4 cols)        100        26.3         6.9      0.26
narrow (4 cols)       1000       262.3        61.3      0.23
narrow (4 cols)      10000      2635.1       615.1      0.23
wide (20 cols)         100        59.1        28.2      0.48
wide (20 cols)        1000       585.1       285.9      0.49
wide (20 cols)       10000      5928.6      3468.0      0.58
```

Arrow IPC is 4× smaller on the narrow shape, ~2× on the wide one (string
columns compress the ratio).

## Round trip, serial — one writer, ms/batch (min/median of 15)

```
shape                 rows       gen_rpc     ipc→frame      ipc→rows
narrow (4 cols)        100       0.3/0.3       0.2/0.3       0.3/0.4
narrow (4 cols)       1000       2.3/2.4       0.6/0.7       1.2/1.4
narrow (4 cols)      10000     22.4/23.1       4.2/5.1     12.1/14.3
wide (20 cols)         100       0.5/0.7       0.6/0.9       1.8/2.1
wide (20 cols)        1000       4.9/5.8       3.9/5.0      9.0/12.0
wide (20 cols)       10000     54.7/59.2     32.5/35.6     77.8/82.5
```

As a byte mover, IPC wins: to a frame it is 4–5× faster than gen_rpc at 10k
rows. But `ipc→rows` — what a drop-in for the row-based `TableBuffer` must
pay — gives most of it back, and on the wide shape loses outright (77.8 vs
54.7 ms). The frame→rows conversion, not the wire, is the cost.

## Round trip, concurrent — 8 writers × 10 batches × 5000 rows, krows/s

```
shape                gen_rpc :bulk  gen_rpc/writer      ipc→rows
narrow (4 cols)              629.4          1639.5        1548.6
wide (20 cols)               344.6           850.5         303.5
```

The single `:bulk` channel is the bottleneck T-29 predicted: one connection
per destination serializes 8 writers, and giving each writer its own channel
is 2.5–2.6× — more than switching transports buys. HTTP's connection pool
gets the same parallelism for free on the narrow shape and loses it again to
frame→rows on the wide one.

## End to end — the real write, group commit included, ms/batch (min/median)

```
shape                 rows   gen_rpc write     ipc write
narrow (4 cols)       1000         7.1/8.0       6.4/9.0
wide (20 cols)        1000       15.5/17.5     17.8/20.6
```

Through `Client.write_batch/3` vs IPC into `Endpoint.write_batch/3`
(`flush_max_rows: 1`, so every write carries a full commit): within noise of
each other. The commit owns the write; the transport is not where a 1k-row
insert's time goes.

## What this settles

- **PL-8 D4 / PL-1's transport open question**: gen_rpc term transfer is the
  v1 ingest→buffer transport. No `Transport` implementation is added.
- **T-29 is the throughput lever**: channel partitioning bought 2.5–2.6×
  concurrent throughput in this harness; do that before reconsidering the
  transport.
- **The Arrow IPC trigger is architectural, not incremental**: a frame-based
  `TableBuffer` (natural alongside T-57's partitioned writes) is what would
  make IPC pay; revisit these numbers then.
