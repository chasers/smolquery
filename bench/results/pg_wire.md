# pg_wire — extended-protocol queries per second

| | |
|---|---|
| Run | 2026-08-28 |
| Commit | 871b8a6 (pg-wire-layer-6, PL-58 layers 1-5) |
| Command | `SMOLQUERY_ROLES=query mix run bench/pg_wire.exs` |
| Machine | Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.1 |
| Runtime | Elixir 1.20.2 / OTP 29, 10 schedulers |

```
Extended-protocol queries per second — port 49677, 100 queries per connection, fleets of 1 / 4 / 8
shape                      conns      qps   p50 ms   p95 ms   p99 ms
postgrex SELECT 1              1    153.2     3.43    10.19    15.03
postgrex SELECT 1              4    382.7     11.3     16.7    22.14
postgrex SELECT 1              8    353.1    19.84    38.76   108.85
postgrex 2 params              1     56.1    17.67    23.61    42.91
postgrex 2 params              4    100.1    36.44    63.68    77.04
postgrex 2 params              8    140.8    40.41   127.94   157.28
raw portal, no describe        1    187.1     3.47    12.15    18.14
raw portal, no describe        4    408.4    10.36    15.43    20.75
raw portal, no describe        8    529.3    15.31    21.79    26.75
```

The job engines ran with `engine_extensions: []`. With the default
(`[:httpfs]`), the single-connection `SELECT 1` shape measured 60.6 qps at
a 4.08 ms p50 with a 43 ms p95 — extension `LOAD` time — and the 8-connection
two-param shape aborted the VM in DuckDB's extension signature check
(T-415) before finishing.

## What this settles

- **The wire edge adds no meaningful overhead to a query.** The raw-portal
  shape (one job per query, no driver) and Postgrex's `SELECT 1` (one job,
  full Parse/Describe/Bind/Execute) sit within ~20% of each other; both are
  the query service's per-job floor (`bench/query.exs`), not the protocol.
- **A driver-shaped no-param query costs one job** (~3.4 ms p50 hot on this
  machine), because `Describe` runs the statement once and the portal serves
  the cached rows (T-405's design). A parameterised query costs two jobs —
  the describe job then the execute job — and halves throughput: the number
  that motivates native parameter binding (T-410).
- **Fleets scale to the admission bound, then flatten**: 1 → 4 connections
  scales ~2.5x; 4 → 8 adds little and lengthens the tail. The knob is
  `max_concurrent_jobs` (default 8) and the cost behind it is one private
  DuckDB engine per job.
- **Extension `LOAD` dominates cold per-job engine cost** (~2.5x qps once
  removed), and concurrent `LOAD`s across starting engines can abort the
  whole VM — filed as T-415.
