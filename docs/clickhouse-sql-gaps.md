# ClickHouse SQL: what smolquery answers, and what it does not

smolquery's ClickHouse edge ([clickhouse.md](clickhouse.md)) runs **smolquery's own
SQL** — DuckDB's dialect over the two-tier planner. It does not translate ClickHouse
SQL. A client that sends ClickHouse-only syntax, a ClickHouse-only function, or reads
a `system.*` table gets an error, not a translation.

This page enumerates the gaps so nobody has to discover them one query at a time. It
covers **reads**. Ingest compatibility is a separate, already-closed analysis (T-472):
the wire format and column types, not SQL.

> **How this was checked.** Every row in sections 2, 3 and 5 was run through the
> query service on the pinned DuckDB (2026-09-19), one statement per construct. The
> ClickHouse side is from ClickHouse's documentation, not from a side-by-side run.
> The list of constructs is a comparison of the two dialects, not a corpus: the
> honest way to complete this page is to log the statements real clients send at
> the edge and close what actually appears (PL-65).

## The four kinds of gap

1. **Missing catalog** — `system.*`, `SHOW`, `DESCRIBE`. Clients hit these on connect, before any user query.
2. **Unparseable syntax** — ClickHouse-only clauses DuckDB's parser rejects outright.
3. **Missing functions** — many ClickHouse names do not resolve; some do, since DuckDB matches names without regard to case and carries a few ClickHouse-style aliases.
4. **Silent semantic differences** — the query runs and returns a *different answer*. The dangerous class.

## What works today

Plain analytical SQL is fine: `SELECT` with joins, `WHERE`, `GROUP BY`, `HAVING`,
`ORDER BY`, `LIMIT`/`OFFSET`, CTEs, window functions, subqueries, `UNION ALL`,
`CASE`, casts, `ILIKE`; `dataset.table` qualification; map access (`attrs['host']`);
`COLUMNS('regex')`; and the DuckDB function library. On the edge itself: a trailing
`FORMAT` clause, a `SETTINGS` clause, `{name:Type}` parameters, backquoted identifiers,
backslash escapes in literals, `version()`, `timezone()`, and the `max_execution_time`
setting.

These ClickHouse spellings already resolve, with the same meaning: `argMax`,
`argMin`, `countIf`, `median`, `quantile(x, 0.5)` (the two-argument form), `if`,
`length`, `ifNull`, `nullIf`, `now`.

## 1. Catalog and introspection

The largest practical gap: every driver and BI tool reads these before it runs
anything a user typed.

| ClickHouse | Status | Note |
|---|---|---|
| `system.tables`, `system.databases`, `system.columns` | Works (T-482) | Emulated from smolquery's catalog. `engine` is `MergeTree`; `sorting_key` and `primary_key` are the clustering key; `total_rows` is `NULL` |
| `system.settings`, `system.data_skipping_indices` | Works, empty (T-482) | HyperDX reads both before its first query and fails every query if `system.settings` fails |
| `system.table_engines`, `system.one` | Works (T-482) | |
| `system.numbers` | **Missing** | Unbounded; needs a table function, not a table |
| `system.parts`, `system.functions`, the rest | **Missing** | Code 60 `UNKNOWN_TABLE`, by their ClickHouse name |
| `SHOW TABLES [FROM db]`, `SHOW DATABASES` | Works (T-483) | |
| `SHOW CREATE TABLE` | **Missing** | `system.tables.create_table_query` holds a synthesized one |
| `DESCRIBE TABLE` / `DESC` | Works (T-483) | Seven columns, as ClickHouse answers; a table function argument is not taken |
| `EXISTS TABLE` | Works (T-483) | |

The Postgres edge solved the same problem for its clients with an emulated
`pg_catalog` plus macro shims (T-406, T-409, T-412). That is the model to copy.

## 2. Syntax

| ClickHouse | Status | Note |
|---|---|---|
| Trailing `SETTINGS` clause | Works (T-481) | Split off with `FORMAT`, in either order; `max_execution_time` is read, the rest ignored. One that ends a subquery is dropped |
| `{name:Type}` query parameters | Works (T-481) | Filled from `param_<name>` as literals. Scalar types and `Identifier`; an `Array`, `Map` or `Tuple` parameter is code 36 |
| `ARRAY JOIN` / `LEFT ARRAY JOIN` | **Parse error** | DuckDB's `UNNEST` is the equivalent shape |
| `LIMIT n BY expr` | **Parse error** | Rewritable as a windowed row-number filter |
| `PREWHERE` | **Parse error** | Could be accepted and folded into `WHERE` |
| `SAMPLE` | **Parse error** | |
| `WITH FILL`, `INTERPOLATE` | **Parse error** | Gap-filling time series |
| `GLOBAL IN` / `GLOBAL JOIN` | **Parse error** | Distributed-only in ClickHouse |
| `FINAL` | **Silently accepted** | `FROM t FINAL` parses with `FINAL` read as the table's *alias*, so the query runs and any `t.column` reference then fails. After an explicit alias it is a parse error. There are no merge semantics here to skip, so accepting and dropping it is the right fix |
| Backtick-quoted identifiers | Works (T-481) | Rewritten to double quotes on the edge |
| Backslash escapes in literals (`'it\'s'`) | Works (T-481) | Rewritten to the characters they stand for. `LIKE '%a\_b%'` keeps its backslash, but DuckDB's `LIKE` has no default escape character, so `\_` matches a backslash and any character, not a literal `_` — differs |
| Parametric aggregates: `quantile(0.5)(x)`, `topK(1)(x)` | **Parse error** | The two-argument `quantile(x, 0.5)` works |
| `any(x)` | **Parse error** | `ANY` is a keyword in DuckDB; `any_value(x)` is the equivalent |
| `position(haystack, needle)` | **Parse error** | DuckDB takes `position(needle IN haystack)` |
| Table functions: `numbers()`, `remote()`, `url()`, `s3()`, `file()` | **Refused** | The planner refuses table functions it does not know. `numbers()` is trivial; the remote ones are deliberately out of scope |

## 3. Functions that do not resolve

Each family below is a macro-shim candidate. The right-hand column is the DuckDB
function a shim would call.

| Family | ClickHouse names that fail today | DuckDB equivalent |
|---|---|---|
| Date/time | `toStartOfInterval`, `toStartOfDay/Hour/Minute`, `toDate`, `toDateTime`, `toYYYYMM`, `formatDateTime`, `now64`, `toUnixTimestamp64Nano` | `date_trunc`, `time_bucket`, `strftime`, `epoch_ns`, casts |
| Arrays | `has`, `hasAny`, `arrayExists`, `arrayMap`, `arrayFilter`, `arrayJoin`, `groupArray` | `list_contains`, `list_has_any`, `list_filter`, `list_transform`, `unnest`, `list()` |
| Aggregate combinators | `sumIf`, `avgIf` and the rest of `-If` (only `countIf` resolves), `-Array`, `-State`, `-Merge` | `sum(x) FILTER (WHERE …)`; `-State`/`-Merge` have no equivalent |
| Approximate aggregates | `uniq`, `uniqExact`, `uniqCombined`, `quantiles`, `quantileTDigest`, `topK` | `approx_count_distinct`, `count(DISTINCT …)`, `quantile_cont`, `approx_top_k` |
| Strings/regex | `match`, `splitByChar`, `extractAll`, `positionCaseInsensitive`, `multiSearchAny` | `regexp_matches`, `str_split`, `regexp_extract_all`, `position(… IN …)` |
| Conditional | `multiIf` | `CASE` |
| JSON | `JSONExtract*`, `simpleJSONExtract*`, `visitParamExtract*` | `json_extract` family, `VARIANT` |
| Hashes | `cityHash64`, `sipHash64`, `xxHash64` | `hash`; values will not match ClickHouse's |
| Dictionaries | `dictGet*` | **No equivalent**; out of scope |
| Type helpers | `toInt64`, `toString`, `toFloat64`, `toTypeName`, `assumeNotNull`, `intDiv` | casts, `typeof`, `//` |

## 4. Types with no home

| ClickHouse | Status |
|---|---|
| `Array(T)`, `Tuple(...)`, `Nested` | **Missing** — blocks Logflare's metrics and traces tables; `VARIANT` is the likely store |
| `Enum8` / `Enum16` | **Missing** — decode as the integer, or map to the label |
| `LowCardinality(T)` | Unwrapped on ingest; no read-side meaning |
| `FixedString(N)` | Read as `STRING` on ingest |
| Native `UUID` | Stored as `STRING` (T-473) |
| `IPv4` / `IPv6` | **Missing** |
| `Decimal256`, `Int128/256` | **Missing** — over `NUMERIC`'s range |
| `AggregateFunction(...)` | **Missing** — materialized-view state; out of scope |
| `DateTime('tz')` | Zone argument ignored; values are UTC |
| Dotted column names (`events.name`) | **Missing** — smolquery identifiers allow only letters, digits and `_` |

## 5. Semantic differences

These return an answer, just not always ClickHouse's.

| Behavior | ClickHouse | smolquery (DuckDB, observed) |
|---|---|---|
| `sum()` over zero rows | `0` | **`NULL`** — differs |
| `countIf` result type | `UInt64` | A `HUGEINT`, which answers as a `Decimal` in a result — differs |
| Column nullability | Columns are non-null unless `Nullable` | Every result column is `Nullable`, and `NULL` can appear where ClickHouse guarantees a value — differs |
| Timezones | `DateTime` carries a session timezone | UTC only — differs |
| Function-name case | Case-sensitive camelCase | Case-insensitive — a superset, harmless |
| `7 / 2` | `3.5` | `3.5` — same |
| `1 / 0` | `inf` | `inf` — same |
| `count()` over zero rows | `0` | `0` — same |
| `NULL` ordering | Last, for `ASC` and `DESC` | Last, for `ASC` and `DESC` — same |
| Float text form | ClickHouse's shortest round-trip | DuckDB's may differ in exponent form — not checked |

## 6. What this means per client

| Client | Works today | Blocked by |
|---|---|---|
| `curl`, hand-written SQL | Yes | Nothing, if the SQL is DuckDB-flavored |
| `ch` (Elixir) | Connects and queries | Nothing for plain SQL |
| Logflare reads | No | `toStartOfInterval`, `ARRAY JOIN`, `LIMIT BY`, `match`, `has`, `arrayExists`. Map access (`col['key']`), `{name:Type}` params and a trailing `SETTINGS` already work |
| `clickhouse-connect`, JS client | Connects, reads metadata | Untested against the real clients |
| Grafana ClickHouse plugin | No | `system.*` browsing, then the time-function family |
| BI tools (Metabase, Tableau) | No | Catalog introspection first, then functions |

## 7. How this gets closed

The Postgres edge is the precedent: eight layers, an emulated catalog, a 250-line
dialect rewrite (`SmolqueryPg.PgCatalog.Rewrite`) and a fixture corpus of what
clients actually send. The same shape applies here (PL-65):

1. **Log unrecognized statements at the edge** to build a real corpus instead of guessing.
2. **Cheap wins first:** strip a trailing `SETTINGS`, substitute `{name:Type}` parameters, read backticks and backslash escapes. Done (T-481).
3. **Emulate `system.*`** plus `SHOW` and `DESCRIBE`, the way `pg_catalog` is emulated. Done (T-482, T-483).
4. **A textual pre-pass** for constructs DuckDB refuses to parse (`ARRAY JOIN`, `LIMIT BY`, `PREWHERE`), and to drop `FINAL` rather than let it bind as an alias.
5. **Macro shims** for the function families, one family per layer, skipping the names that already resolve.
6. **Pin the semantic differences with tests**, since those are the ones that fail quietly.

**Non-goals:** full dialect parity, `AggregateFunction` states and materialized-view
semantics, distributed table functions (`remote`, `cluster`), dictionaries, and
ClickHouse DDL. Those are ClickHouse-engine features, not query-surface gaps.
