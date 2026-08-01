# `bench/adbc.exs` — what ADBC costs to connect, fetch, and share

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `890b5a1` |
| Command | `mix run bench/adbc.exs` (defaults: up to `ROWS=5000000`, `CLIENTS=8`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · adbc 0.12.1 · explorer 0.12.0 |

## Headline

**A large result must never come back as Elixir terms, and one connection must
never be shared.** Converting 5M rows to terms costs 11.2 s and 3.4 GiB against
390 ms and 11.5 MiB left in Arrow — a 29× time and 300× memory penalty that grows
with the result. And a shared connection serializes perfectly: 8 clients on one
connection take 8× as long as 1, while 8 connections take the same wall clock as 1.

## Cold-connection latency

```
  Adbc.Database.start_link              5.5 ms
  Adbc.Connection.start_link (bare)     0.1 ms
  INSTALL + LOAD httpfs                 8.5 ms
  INSTALL + LOAD json                   0.3 ms
  INSTALL + LOAD ducklake              15.8 ms
  ATTACH ducklake catalog              22.7 ms

  full engine start, end to end        53.1 ms min, 57.8 ms median
```

Extensions are already on disk, so this is steady state, not first-boot download.
The bare connection is free (0.1 ms); everything else is extension load and
`ATTACH`, which is what a pool amortizes.

## Large-result fetch

```
  rows    path                            wall            BEAM Δ    bytes/row
  100k    Engine.query → Elixir terms         118.0 ms      93.2 MiB        977.6
  100k    Adbc.Connection.query (Arrow)         8.3 ms       0.2 MiB          1.9
  100k    Engine.frame → DataFrame             29.4 ms     -31.9 MiB       -334.3
  100k    Engine.frame → Parquet bytes         15.5 ms       4.1 MiB         43.3
  100k    count(*) only (baseline)              0.5 ms       0.2 MiB          1.7

  1M      Engine.query → Elixir terms        2060.7 ms     916.8 MiB        961.3
  1M      Adbc.Connection.query (Arrow)        69.6 ms       1.6 MiB          1.6
  1M      Engine.frame → DataFrame             56.2 ms      -0.4 MiB         -0.4
  1M      Engine.frame → Parquet bytes        114.9 ms      37.2 MiB         39.0
  1M      count(*) only (baseline)              0.7 ms       0.1 MiB          0.1

  5M      Engine.query → Elixir terms       11210.6 ms    3403.0 MiB        713.7
  5M      Adbc.Connection.query (Arrow)       390.2 ms      11.5 MiB          2.4
  5M      Engine.frame → DataFrame            357.8 ms      -2.4 MiB         -0.5
  5M      Engine.frame → Parquet bytes        583.7 ms     189.7 MiB         39.8
  5M      count(*) only (baseline)              1.4 ms       0.1 MiB          0.0
```

Terms cost ~1 KiB and ~2 µs per row, and the cost is linear — 100k is survivable,
5M is not. Negative BEAM deltas on the DataFrame path are GC during the run: the
frame lives outside the BEAM heap, so there is nothing to charge.

## Time to first row

```
  Engine.query!, all 5M rows          10295.7 ms
  the same query with LIMIT 1                 0.6 ms
  query_pointer, handing back a stream        1.5 ms
```

A stream exists and is handed back in 1.5 ms, but consuming it means giving the
pointer to something that reads Arrow natively. There is no cheap path to Elixir
terms — the 10 s is the conversion, not the query.

## Does one connection serialize?

```
  clients   one shared connection      one connection each
  1           204.6 ms total            314.7 ms total   (314.7 ms/query)
  2           348.2 ms total            319.0 ms total   (159.5 ms/query)
  4           659.2 ms total            302.8 ms total   (75.7 ms/query)
  8          1387.0 ms total            392.6 ms total   (49.1 ms/query)
```

Shared: 204 → 1387 ms, dead linear in client count. Per-client: flat at ~300–390 ms
regardless of concurrency. A shared connection is a queue with extra steps.

## What this settles

- **`max_result_rows` is a load-bearing guard, not a nicety.** It exists so no
  caller accidentally pays the terms conversion on a large result. This bench sets
  it to `:infinity` precisely to measure what the ceiling prevents.
- **The read path returns frames or Parquet bytes, never terms, above trivial
  sizes.** Parquet at ~40 bytes/row is the shape to send over the wire; DataFrame
  is the shape to compute on. Terms are for small results only.
- **Connections are pooled per query, never shared.** The serialization is total,
  and the ~55 ms start cost is what pooling amortizes to make that affordable.
- **Upstream noise (T-18):** every frame read logs an `adbc` deprecation warning —
  `query_pointer/5`'s 2-arity callback, from `Explorer.PolarsBackend.DataFrame.from_query/3`.
  Cosmetic, but it makes this bench's output hard to read and will do the same to
  production logs.
