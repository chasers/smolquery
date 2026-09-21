# `bench/query.exs` — what a query job costs

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `2806180` |
| Command | `mix run bench/query.exs` (defaults: `ENTRIES=256`, `ROWS=1000`, `REPS=7`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 · DuckDB v1.5.1 |

## Headline

**A per-job engine costs ~50 ms, and 87% of that is extension loading and the
lake ATTACH — not the engine.** That is tens of milliseconds, not hundreds, so
PL-7 D8's warm-pool escape hatch stays unbuilt: invisible under an async-jobs
API, and the ~18 qps it caps a single *sync* caller at is a per-caller floor,
not a node ceiling — jobs run concurrently. Revisit when Milestone 6's HTTP
API gives the sync path real traffic.

## Job engine startup

```
  bare (database + connection)          5.8 ms
  + httpfs                             18.9 ms
  + httpfs + ducklake + ATTACH         50.2 ms
```

The engine itself is 5.8 ms. `LOAD httpfs` adds 13 ms, `LOAD ducklake` + the
ATTACH another 31 ms. If a pool is ever built, this says what it should hold:
bootstrapped connections, not bare ones.

## Sync query end to end, no tables

```
  Client.query("SELECT 1")             56.3 ms
  implied ceiling                      17.8 qps per synchronous caller
```

Submit → runner → engine start → plan → execute → frame. The 50 ms engine is
the whole story; everything else combined is ~6 ms.

## Hot tier: plan cost and scan latency vs micro-segment count

```
  entries    plan ms   scan ms       rows
  1              7.7      90.4       1000
  32             9.4     118.4      32000
  256           12.1     275.4     256000
```

Planning — manifest fetch over HTTP, membership filter, pruning, view SQL —
is 8-12 ms and nearly flat across a 256× entry spread. The scan grows ~0.7 ms
per micro-segment: the per-file HTTP footer read PL-7 D7 predicted. Linear
and modest, but it is the number that grows when sealing falls behind — the
hot tier's read cost is seal lag made visible to queries.

## Pruning: one batch's id range out of 256 micro-segments

```
  entries planned, no predicate         256
  entries planned, id range               1
  scan ms, no predicate               302.5
  scan ms, id range                   143.4
```

The pruner reduced 256 hot files to the 1 whose stats admit the range, and
the scan halved — the saving is per-file cost × files dropped, exactly the
footer reads the pruner exists to skip. The remaining 143 ms is the engine
(50 ms) plus the sealed side of the view and the one surviving file.

## Pruning a range written inside a CTE (T-532)

A partial run of the pruning section alone, on another machine, so not
comparable with the tables above: 2026-09-21, aarch64 Linux, 16 cores,
`ENTRIES=1024 REPS=5 mix run bench/query.exs`. "Before" is `main@ba126d0`.

The "range in a CTE" rows are the id range of one micro-segment written as
HyperDX's filters sidebar writes its window: a `WHERE` inside a CTE with a
`LIMIT`, aggregated outside it.

```
                                    before     after
  entries planned, no predicate       1024      1024
  entries planned, id range              1         1
  entries planned, range in a CTE     1024         1
  scan ms, no predicate              766.5     730.6
  scan ms, id range                  168.6     173.0
  scan ms, range in a CTE            797.6     159.1
```

Before, a predicate inside a CTE pruned nothing, so the statement cost what
a scan with no predicate costs, whatever its window. Here the files are read
over loopback; a query node reads them from the buffer nodes over the
network, a footer request each, so the gap in a deployment is wider.
