# parser — DuckDB's SQL parser on the query path, 1.5.3 vs 2.0 preview

| | |
|---|---|
| Run | 2026-10-10 |
| Commit | 390f430 + `bench/parser.exs` (T-635) |
| Command | `REPS=500 SMOLQUERY_ROLES=query mix run bench/parser.exs`, once per driver pin |
| Drivers | `duckdb_driver_version` 1.5.3 (main's pin), then 2.0.0-alpha38195 (T-467's preview mirror) |
| Machine | AWS Graviton3 (Neoverse-V1), 8 cores, 30 GiB, Linux 6.12 (Ubuntu 24.04 userland) |
| Runtime | Elixir 1.20.2 / OTP 29, 8 schedulers |

Columns are medians in µs, each a full round trip through `Smolquery.Engine.query/2`
on a bare engine. **literal** is `SELECT length('<sql>')` (round trip plus the
outer statement's own parse); **serialize** is `SELECT json_serialize_sql('<sql>')`;
**parse** is serialize minus literal; **planner** is the planner's own parse statement
(three `json_serialize_sql` and one `json_deserialize_sql`).

```
DuckDB v1.5.3 — parse cost per statement, median of 500 (us)
────────────────────────────────────────────────────────────
statement                           bytes   ok  literal  serialize   parse  planner
select 1                                8    y      309        347      38      544
bq point lookup                        55    y      335        351      16      565
bq time-range aggregate               218    y      312        392      80      744
hyperdx rows                          233    y      315        376      61      692
hyperdx histogram                     418    y      313        420     107      907
hyperdx rows_term                     281    y      313        391      78      748
hyperdx count_field_filters           348    y      317        401      84      850
hyperdx key_values                    331    y      322        398      76      752
hyperdx rows_underscore_term          371    y      316        409      93      868
metricsql sum(rate)                  4484    y      378       1439    1061     6559
metricsql quantile(max_over_time)    1179    y      341        672     331     2327
metricsql avg(last_over_time)         906    y      335        586     251     1830
pgjdbc getColumns                    1075    y      333        541     208     1562
wide 3.6 KB                          3583    y      373        780     407     2908

sum                                                4612       7503    2891    21856

DuckDB v2.0.0-alpha38195 — parse cost per statement, median of 500 (us)
───────────────────────────────────────────────────────────────────────
statement                           bytes   ok  literal  serialize   parse  planner
select 1                                8    y      793       3099    2306    12676
bq point lookup                        55    y      796       3221    2425    13532
bq time-range aggregate               218    y      800       3710    2910    15699
hyperdx rows                          233    y      808       3558    2750    15065
hyperdx histogram                     418    y      814       4005    3191    17142
hyperdx rows_term                     281    y      832       3782    2950    16118
hyperdx count_field_filters           348    y      828       3924    3096    16485
hyperdx key_values                    331    y      809       3690    2881    15531
hyperdx rows_underscore_term          371    y      860       4088    3228    17725
metricsql sum(rate)                  4484  bad      920      16029   15109    77267
metricsql quantile(max_over_time)    1179  bad      875       6705    5830    30707
metricsql avg(last_over_time)         906  bad      840       5729    4889    25778
pgjdbc getColumns                    1075    y      872       5704    4832    25580
wide 3.6 KB                          3583    y      910       8789    7879    40561

sum                                               11757      76033   64276   339866
```

Both builds are release builds: `SELECT sum(range) FROM range(200000000)` took
358 ms on 1.5.3 and 346 ms on the preview. A bare `SELECT 1` round trip averaged
257 µs on 1.5.3 and 690 µs on the preview.

## What this settles

- **The 2.0 preview's parser is 15–22x slower on our statements.** Summed over
  the corpus, parse goes from 2.9 ms to 64.3 ms (22x) and the planner's parse
  statement from 21.9 ms to 339.9 ms (15.5x). The HN report of "10x slower"
  understates it for this build.
- **The planner pays it on every query.** A short BigQuery or HyperDX query's
  parse step goes from ~0.55–0.9 ms to ~12.7–17.7 ms. Against the ~56 ms sync
  floor in `bench/results/query.md`, that is +20–30% on the simplest query.
  MetricsQL `sum(rate)` SQL goes from 6.6 ms to 77 ms.
- **Every statement DuckDB runs pays a fixed cost too.** The literal column, a
  trivial statement, rises ~310 → ~800 µs. That cost lands on every engine query,
  including the user's statement itself after planning, and not only on
  `json_serialize_sql`.
- **The planner's statement already wastes time on 1.5.3.** It parses the
  text three times, so it costs 3–5x one `json_serialize_sql` (6.6 ms against 1.4 ms
  for the MetricsQL `rate` SQL). Parsing once and deserializing that AST is a
  saving on either version, and a large one on 2.0.
- **The preview breaks on large double literals.** `json_serialize_sql` writes any
  DOUBLE constant above float32's max (~3.4e38) as a bare `Infinity`, which is not
  JSON, and `json_deserialize_sql` turns it back into `1e1000` (+inf). 1.5.3
  writes `1.7e308`. Every MetricsQL pushdown statement carries
  `1.7976931348623157e308` (the stored-infinity sentinel, PL-70 D6), so on this
  build the planner cannot decode their AST (`bad` above). If decoding were made
  lenient, the canonical text would compare `value >= +inf` and stop matching
  the sentinel.

This is one alpha (alpha38195, September); the 2.0.0 release is due 2026-10-21.
Re-run against the release asset before the bump (T-395) and overwrite this file.
