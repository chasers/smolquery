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
| `system.tables`, `system.databases`, `system.columns` | Works (T-482) | Emulated from smolquery's catalog. `engine` is `MergeTree`; `sorting_key` and `primary_key` are the clustering key; `total_rows` is the plan's row count for the tables a statement's `WHERE` selects, and `NULL` for a statement that does not read it (T-507) |
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
| `expr AS alias` inside `GROUP BY` / `ORDER BY` | Works (T-494) | The alias is dropped from the clause; HyperDX's histogram names its bucket in all three places |
| `CAST(x, 'Type')` | Works (T-494) | Rewritten to `CAST(x AS type)` for the scalar types |
| `SAMPLE` | **Parse error** | |
| `WITH FILL`, `INTERPOLATE` | **Parse error** | Gap-filling time series |
| `GLOBAL IN` / `GLOBAL JOIN` | **Parse error** | Distributed-only in ClickHouse |
| `FINAL` | **Silently accepted** | `FROM t FINAL` parses with `FINAL` read as the table's *alias*, so the query runs and any `t.column` reference then fails. After an explicit alias it is a parse error. There are no merge semantics here to skip, so accepting and dropping it is the right fix |
| Backtick-quoted identifiers | Works (T-481) | Rewritten to double quotes on the edge |
| Backslash escapes in literals (`'it\'s'`) | Works (T-481) | Rewritten to the characters they stand for. `LIKE '%a\_b%'` keeps its backslash, and the edge adds `ESCAPE '\'` so it means what it does in ClickHouse (T-496) |
| Parametric aggregates: `quantile(0.5)(x)`, `quantileIf`, `groupUniqArray(20)(x)`, `groupUniqArrayArray`, `groupArray` | Works (T-496) | Rewritten to `f(args, params)`; `quantile` is the engine's interpolating `quantile_cont`. `topK(1)(x)` and `histogram(n)(x)` are still parse errors |
| `quantiles(0.5, 0.95, 0.99)(x)`, `quantilesExact`, `quantilesIf` | Works (T-501) | The levels become one list, `quantile_cont(x, [...])`, and the answer is one `Array(Nullable(Float64))` in level order. Over no rows it is `[]`, where ClickHouse answers an array of `nan`. `quantilesTDigest`, `quantilesTiming` and the other sketch variants do not resolve |
| `any(x)`, `isNull(x)`, `isNotNull(x)` | Works (T-496) | Words of the parser's own; renamed to `any_value` and to macros |
| `WITH (expr) AS alias` | Works (T-496) | The alias is written as its expression wherever it is read; a common table expression beside it stays. `WITH expr AS alias` without parentheses is still a parse error |
| `JSONExtract(json, 'Type')` | Works (T-496) | A cast of the JSON to the type, for scalars, `Map(K, V)` and `Array(T)`. The forms with a path do not resolve |
| A bare `default.table` | Works (T-496) | `default` is a reserved word to the engine; the edge quotes it |
| `position(haystack, needle)` | **Parse error** | DuckDB takes `position(needle IN haystack)` |
| Table functions: `numbers()`, `remote()`, `url()`, `s3()`, `file()` | **Refused** | The planner refuses table functions it does not know. `numbers()` is trivial; the remote ones are deliberately out of scope |

## 3. Functions that do not resolve

Each family below is a macro-shim candidate. The right-hand column is the DuckDB
function a shim would call.

| Family | ClickHouse names that fail today | DuckDB equivalent |
|---|---|---|
| Date/time | `toYYYYMM`, `formatDateTime`, `dateDiff`, `toIntervalSecond`. **Resolve since T-485:** `toStartOfInterval`, the `toStartOf*` family, `toDate`, `toDateTime`, `toDateTime64`, `fromUnixTimestamp*`, `toUnixTimestamp*`, `now64`, `parseDateTime64BestEffort` | `date_trunc`, `time_bucket`, `strftime`, `epoch_ns`, casts |
| Arrays | `hasAny`, `arrayJoin`, `arraySplit`, `sumMap`, and a lambda over two arrays, `arrayFirstIndex((a, b) -> ..., x, y)`. **Resolve since T-485 and T-496:** `has`, `groupArray`, `groupUniqArray`, `groupUniqArrayArray`. **Resolve since T-501:** `arrayExists`, `arrayAll`, `arrayFirstIndex`, `arrayCount`, `arrayMap`, `arrayFilter` over one array (the edge swaps the lambda behind the array), `arrayZip`, `arrayZipUnaligned`, `arrayCumSum`, `sumForEach`, `arrayWithConstant`, `arraySum`, `arrayMin`, `arrayMax`, `arrayReverse`, `arrayConcat`, `arrayResize` (the two-argument form pads with `0`, as text in a string array), `arraySlice`, `emptyArray*`, a tuple's `.1` as `[1]` | `list_contains`, `list_has_any`, `list_filter`, `list_transform`, `unnest`, `list()`, `list_zip`, `list_reduce` |
| Array arithmetic | `a - b` element by element, which ClickHouse does for two arrays. **Resolves since T-501** between two names the statement binds to arrays (an alias of a `CAST(... AS Array(...))`, an array function, a list literal, or `any(...)`/`multiIf(...)` over one), as `clickhouse_arrayMinus(a, b)`. The names are collected statement-wide, so one bound as an array in one scope and as something else in another is left alone; so is a minus between a column and anything else, or after a dot | `list_transform(list_zip(a, b), p -> p[1] - p[2])` |
| Aggregate combinators | `-Array`, `-State`, `-Merge`, and `-If` beyond `countIf`, `sumIf`, `avgIf`, `minIf`, `maxIf`, `anyIf`, `anyOrNullIf`, which resolve (T-485, T-501) | `sum(x) FILTER (WHERE …)`; `-State`/`-Merge` have no equivalent |
| Conditionals and windows | `multiIf` past four branches, and a `SELECT` item that reads an alias defined later in the same list (HyperDX's exponential histogram does). **Resolve since T-501:** `multiIf` up to four branches, `ifNull`, `toNullable`, `assumeNotNull`, `tuple`, `anyLast`, `last_value` as an aggregate, `lagInFrame`, `leadInFrame`, `positionCaseInsensitive`, `* EXCEPT (...)`, `group` as a column name | `CASE`, `coalesce`, `row`, `last`, `lag`, `lead`, `instr`, `* EXCLUDE (...)`; an alias may be read only after its definition |
| Approximate aggregates | `uniqCombined`, `quantileTDigest`, `topK`. **Resolve since T-485, exactly:** `uniq`, `uniqExact`. **Resolve since T-501:** `quantiles`, `quantilesExact`, `quantilesIf` | `approx_count_distinct`, `count(DISTINCT …)`, `quantile_cont`, `approx_top_k` |
| Strings/regex | `splitByChar`, `extractAll`, `positionCaseInsensitive`, `multiSearchAny`. **Resolve since T-485:** `match`, `hasToken`, `startsWith`, `endsWith`, `notEmpty`, `empty` | `regexp_matches`, `str_split`, `regexp_extract_all`, `position(… IN …)` |
| Conditional | `multiIf` | `CASE` |
| JSON | `JSONExtract*`, `simpleJSONExtract*`, `visitParamExtract*` | `json_extract` family, `VARIANT` |
| Hashes | `sipHash64`, `xxHash64`. **Resolves since T-526:** `cityHash64`, as a macro over `hash` | `hash`; values will not match ClickHouse's |
| Dictionaries | `dictGet*` | **No equivalent**; out of scope |
| Type helpers | `toTypeName`, `assumeNotNull`, `intDiv`. **Resolve since T-485 and T-496:** `isNull`, `isNotNull`, `toString`, `toInt64`, `toUInt64`, `toFloat64` and their `-OrZero`/`-OrNull` forms, `toFloat64OrDefault` | casts, `typeof`, `//` |
| Maps and hints | **Resolve since T-485:** `mapContains`, `mapKeys`, `mapValues`, `indexHint` (always true) | `map_contains`, `map_keys`, `map_values` |

## 4. Types with no home

| ClickHouse | Status |
|---|---|
| `Array(T)` of a scalar | Works (T-489): `ARRAY<T>` in the API, `Array(Nullable(T))` in `DESCRIBE`, `Array(T)` or `Array(Nullable(T))` in a RowBinary insert. A computed list answers as `Array(Nullable(T))` (T-496) |
| `Array(Array(T))`, `Array(Map(...))`, `Tuple(...)`, `Nested` | **Missing** as a column type — the `Events.*` and `Links.*` columns of `otel_traces`, and `Exemplars.*` of the metrics tables, whose dotted names smolquery has no identifier for either |
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
| `toDateTime(x)` | A `DateTime`, in the session's zone; a number is seconds since the epoch | A `TIMESTAMP` truncated to the second, UTC; a number is refused — differs |
| `toStartOfInterval(t, INTERVAL 7 day)` | Buckets from 1970-01-01, a Thursday | The same, by an explicit origin — same |
| `uniq(x)` | Approximate | Exact, `count(DISTINCT x)` — a superset |
| `hasToken(s, t)` | Reads the token index; `t` must be one token | Splits `s` on non-alphanumerics and scans — same answer, no index |
| `rand()`, `rand32()`, `rand64()`, `randCanonical()` | `UInt32`, `UInt32`, `UInt64`, a `Float64` in [0, 1) | The same types, over DuckDB's `random()` (T-526). They are the only functions defined here that are not deterministic, so a statement naming one is never planned as a Top-N probe |
| `cityHash64(a[, b, c, d])` | CityHash64 of its arguments | DuckDB's `hash`, a `UInt64`: the same for the same input, **not** the number ClickHouse computes. Right for sampling and bucketing, `cityHash64(ts, rand()) % n = 0`; wrong for a value compared with one ClickHouse stored — differs |
| `sumIf(x, c)` result type | `Int64`/`UInt64` | A `HUGEINT`, which answers as a `Decimal` — differs |
| `notEmpty(x)` | `UInt8` | `UInt8`; `mapContains` answers a `Bool`, where ClickHouse answers `UInt8` — differs |
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
| HyperDX (ClickStack) | Search, the filters sidebar, charts, an aliased source, a row click, traces and the service map, and metrics (the catalog, gauge, sum and histogram charts, multi-series, ratios, formulas) answer, through HyperDX's own query code | Span events and links, sessions, exponential histograms (T-501); the UI itself has not been run. See [clickstack.md](clickstack.md) |
| Grafana ClickHouse plugin | No | `system.*` browsing, then the time-function family |
| BI tools (Metabase, Tableau) | No | Catalog introspection first, then functions |

## 7. How this gets closed

The Postgres edge is the precedent: eight layers, an emulated catalog, a 250-line
dialect rewrite (`SmolqueryPg.PgCatalog.Rewrite`) and a fixture corpus of what
clients actually send. The same shape applies here (PL-65):

1. **Log unrecognized statements at the edge** to build a real corpus instead of guessing. Done (T-480): `SmolqueryClickHouse.Unanswered`.
2. **Cheap wins first:** strip a trailing `SETTINGS`, substitute `{name:Type}` parameters, read backticks and backslash escapes. Done (T-481).
3. **Emulate `system.*`** plus `SHOW` and `DESCRIBE`, the way `pg_catalog` is emulated. Done (T-482, T-483).
4. **A textual pre-pass** for constructs DuckDB refuses to parse (`ARRAY JOIN`, `LIMIT BY`, `PREWHERE`), and to drop `FINAL` rather than let it bind as an alias.
5. **Macro shims** for the function families, one family per layer, skipping the names that already resolve. The date and time family and what HyperDX's Search page calls are done (T-485).
6. **Pin the semantic differences with tests**, since those are the ones that fail quietly.

**Non-goals:** full dialect parity, `AggregateFunction` states and materialized-view
semantics, distributed table functions (`remote`, `cluster`), dictionaries, and
ClickHouse DDL. Those are ClickHouse-engine features, not query-surface gaps.
