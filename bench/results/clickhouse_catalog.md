# ClickHouse edge catalog statements — `bench/clickhouse_catalog.exs`

| | |
|---|---|
| Run | 2026-09-21 |
| Commit | `9109584` for "before"; T-529 applied for "after" |
| Command | `mix run bench/clickhouse_catalog.exs` |
| Machine | aarch64, 8 cores, 31 GiB, Amazon Linux 2023 |
| Runtime | Elixir 1.20.2 / OTP 29 |

p50 ms against a local DuckLake (sqlite metadata). `idle` is the statement
after more than a second of nothing, `busy` the statement straight after
another, `page` the slowest of eleven sent at once after an idle, which is
what HyperDX sends for one page.

## before (T-529 not applied)

```
tables  statement               idle      busy      page
1       system.settings         48.5       3.3      84.2
1       DESCRIBE                45.5       1.5      59.5
8       system.settings        158.2       3.3     188.2
8       DESCRIBE               158.6       1.6     178.9
32      system.settings        535.2       3.3     580.1
32      DESCRIBE               540.4       1.7     542.1
128     system.settings       2296.4    2290.0   25935.5
128     DESCRIBE              2293.6    2397.8   26167.1
```

A rebuild is about 18 ms a table, and every statement after an idle second
paid one, `system.settings` included, which reads nothing the rebuild writes.
At 128 tables the rebuild outlasts the second it was good for, because the
second was counted from before it started: `busy` is a rebuild too, and a page
of eleven is eleven rebuilds in a row.

## after

```
tables  statement               idle      busy      page
1       system.settings          4.7       3.2      35.9
1       DESCRIBE                 4.7       1.6      20.4
8       system.settings          4.5       3.4      35.4
8       DESCRIBE                 5.0       1.6      21.0
32      system.settings          4.6       3.3      36.3
32      DESCRIBE                 5.1       1.6      22.1
128     system.settings          4.6       3.4      36.9
128     DESCRIBE                 5.4       1.6      21.7
```

Flat in depth. What is left of a rebuild's cost is paid when the schema
version moves, and once in `catalog_rebuild_ms`.
