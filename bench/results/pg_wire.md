# pg_wire — the wire edge's parser, in isolation

| | |
|---|---|
| Run | 2026-08-28 |
| Commit | pg-wire-layer-6 (PL-58 layers 1-6) |
| Command | `SMOLQUERY_ROLES=query mix run bench/pg_wire.exs` |
| Machine | Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.1 |
| Runtime | Elixir 1.20.2 / OTP 29, 10 schedulers |

```
Wire-edge parser, no pipeline — 200000 reps per row, small query 37 B, big query 3583 B
operation                                        ops/s    us/op     MB/s
decode extended batch, small (5 msgs)         769835.0      1.3     91.6
decode extended batch, big (5 msgs)           773051.0     1.29   2833.2
decode simple Query message                  4413257.0     0.23    189.8
split multi-statement Query (3 stmts)         480899.0     2.08     43.8
split big single statement                     21299.0    46.95     76.3
leading_keyword                              6186970.0     0.16    228.9
params: oids (2 casts)                        188190.0     5.31
params: substitute 2 binary params            392024.0     2.55
```

An earlier revision of this script measured the whole pipeline through
real sockets (`git log bench/pg_wire.exs`): ~150 qps on one Postgrex
connection, ~530 qps at 8 raw connections, all of it the query service's
per-job engine floor.

## What this settles

- **The parser is nowhere near the bottleneck.** A driver-shaped query
  costs the parsing layer ~9 µs end to end (batch decode 1.3 + oids 5.3 +
  substitute 2.6); the job pipeline behind it costs ~3,400 µs. The wire
  edge could parse ~100k queries per second per scheduler before this
  layer mattered.
- **The binary decoders are effectively free**: one five-message extended
  batch decodes in 1.3 µs regardless of SQL size (2.8 GB/s on the big
  query — the SQL rides through `Parse` as one binary, uncopied).
- **The one soft spot is the lexer on large SQL**: `Statements.split` on
  a 3.6 KB statement costs 47 µs because `SmolqueryPg.Sql` accumulates
  reversed character lists per token. At 76 MB/s it still parses a
  thousand such statements per scheduler-millisecond-budget, so nothing
  needs to change until something feeds the edge very large SQL at rate;
  the fix, if ever needed, is sub-binary slicing instead of char
  accumulation.
- **`Params.oids` (5.3 µs) runs once per `Parse`**, not per `Execute`, so
  prepared-statement reuse already amortizes the priciest parser op.
