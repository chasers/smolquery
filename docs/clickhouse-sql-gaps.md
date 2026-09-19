# ClickHouse SQL: what smolquery answers, and what it does not

smolquery's ClickHouse edge ([clickhouse.md](clickhouse.md)) runs **smolquery's own
SQL** — DuckDB's dialect over the two-tier planner. It does not translate ClickHouse
SQL. A client that sends ClickHouse-only syntax, a ClickHouse-only function, or reads
a `system.*` table gets an error, not a translation.

This page enumerates the gaps so nobody has to discover them one query at a time. It
covers **reads**. Ingest compatibility is a separate, already-closed analysis (T-472):
the wire format and column types, not SQL.

> **Status.** Written from a comparison of the two dialects, not from a compatibility
> suite. Individual rows marked *verify* have not been run against both engines. The
> honest way to complete this page is corpus-driven: log the statements real clients
> send at the edge and close what actually appears.

## The four kinds of gap

1. **Missing catalog** — `system.*`, `SHOW`, `DESCRIBE`. Clients hit these on connect, before any user query.
2. **Unparseable syntax** — ClickHouse-only clauses DuckDB's parser rejects outright.
3. **Missing functions** — an equivalent may exist under another name, or not at all.
4. **Silent semantic differences** — the query runs and returns a *different answer*. The dangerous class.

## What works today

Plain analytical SQL is fine: `SELECT` with joins, `WHERE`, `GROUP BY`, `HAVING`,
`ORDER BY`, `LIMIT`/`OFFSET`, CTEs, window functions, subqueries, `UNION ALL`,
`CASE`, casts; `dataset.table` qualification; map access (`attrs['host']`), and the
DuckDB function library. On the edge itself: a trailing `FORMAT` clause,
`version()`, `timezone()`, and the `max_execution_time` setting.

## 1. Catalog and introspection

The largest practical gap: every driver and BI tool reads these before it runs
anything a user typed.

| ClickHouse | Status | Note |
|---|---|---|
| `system.tables`, `system.databases`, `system.columns` | **Missing** | What `clickhouse-connect`, the JS client and the Grafana plugin read on connect and on schema sync |
| `system.numbers`, `system.one` | **Missing** | Used for generated series and liveness probes |
| `system.parts`, `system.settings`, `system.functions` | **Missing** | Admin and UI surfaces |
| `SHOW TABLES`, `SHOW DATABASES`, `SHOW CREATE TABLE` | **Missing** | |
| `DESCRIBE TABLE` / `DESC` | **Missing** | The usual column-type probe |
| `EXISTS TABLE` | **Missing** | |

The Postgres edge solved the same problem for its clients with an emulated
`pg_catalog` plus macro shims (T-406, T-409, T-412). That is the model to copy.

## 2. Syntax DuckDB will not parse

| ClickHouse | Status | Note |
|---|---|---|
| Trailing `SETTINGS` clause | **Parse error** | The edge strips a trailing `FORMAT` but not `SETTINGS`. Cheap to fix; Logflare's transformer emits it |
| `{name:Type}` query parameters | **Not substituted** | Sent as `param_<name>` URL values. Any parameterized client query fails. Cheap to fix |
| `ARRAY JOIN` / `LEFT ARRAY JOIN` | **Missing** | DuckDB's `UNNEST` is the equivalent shape |
| `LIMIT n BY expr` | **Missing** | Rewritable as a windowed row-number filter |
| `PREWHERE` | **Missing** | Could be accepted and folded into `WHERE` |
| `FINAL` | **Missing** | No merge semantics here; could be accepted and ignored |
| `SAMPLE` | **Missing** | |
| `WITH FILL`, `INTERPOLATE` | **Missing** | Gap-filling time series |
| `GLOBAL IN` / `GLOBAL JOIN` | **Missing** | Distributed-only in ClickHouse |
| Table functions: `numbers()`, `remote()`, `url()`, `s3()`, `file()` | **Missing** | `numbers()` is trivial; the remote ones are deliberately out of scope |
| Backtick-quoted identifiers | *verify* | DuckDB accepts backticks in most positions |
| `COLUMNS('regex')` matcher | **Missing** | |

## 3. Functions

ClickHouse function names are camelCase **and case-sensitive**; DuckDB's are
snake_case and case-insensitive. So even where an equivalent exists, the
ClickHouse spelling does not resolve. Each row below is a macro-shim candidate.

| Family | Examples | DuckDB equivalent |
|---|---|---|
| Date/time | `toStartOfInterval`, `toStartOfDay/Hour/Minute`, `toDate`, `toDateTime`, `toYYYYMM`, `formatDateTime`, `now64`, `toUnixTimestamp64Nano` | `date_trunc`, `strftime`, `epoch_ns`, casts |
| Arrays | `has`, `hasAny`, `arrayExists`, `arrayMap`, `arrayFilter`, `arrayJoin`, `groupArray`, `length` | `list_contains`, `list_has_any`, lambdas, `unnest`, `list()` |
| Aggregate combinators | `countIf`, `sumIf`, `avgIf`, `-Array`, `-State`, `-Merge` | `count(*) FILTER (WHERE …)`; `-State`/`-Merge` have no equivalent |
| Approximate aggregates | `uniq`, `uniqExact`, `uniqCombined`, `quantile`, `quantiles`, `quantileTDigest`, `topK`, `median` | `approx_count_distinct`, `quantile_cont`, `approx_top_k` |
| Positional aggregates | `argMax`, `argMin`, `any`, `anyLast` | `arg_max`, `arg_min`, `first`, `last` |
| Strings/regex | `match`, `splitByChar`, `extractAll`, `position`, `positionCaseInsensitive`, `multiSearchAny` | `regexp_matches`, `str_split`, `regexp_extract_all`, `position` |
| Conditional | `multiIf`, `if` | `CASE`, `if` |
| JSON | `JSONExtract*`, `simpleJSONExtract*`, `visitParamExtract*` | `json_extract` family, `VARIANT` |
| Hashes | `cityHash64`, `sipHash64`, `xxHash64` | `hash`; values will not match ClickHouse's |
| Dictionaries | `dictGet*` | **No equivalent**; out of scope |
| Type helpers | `toInt64`, `toString`, `toFloat64`, `ifNull`, `nullIf`, `assumeNotNull`, `toTypeName` | casts, `coalesce`, `nullif` |

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

## 5. Silent semantic differences

These return an answer, just not ClickHouse's. Each needs a test before anyone
depends on it.

| Behavior | ClickHouse | smolquery (DuckDB) |
|---|---|---|
| `sum()` over zero rows | `0` | `NULL` *(verify)* |
| Integer division | `/` yields Float64; `intDiv` truncates | `/` behavior differs; no `intDiv` *(verify)* |
| Division by zero | `inf` / `nan` for floats | differs *(verify)* |
| Column nullability | Columns are non-null unless `Nullable` | Every smolquery column is nullable, so `NULL` appears where ClickHouse guarantees a value |
| Timezones | `DateTime` carries a session timezone | UTC only |
| `NULL` ordering | `NULLS LAST` by default for `ASC` | *(verify)* |
| String comparison | Byte-wise | Byte-wise, but collation settings differ *(verify)* |
| Function-name case | Case-sensitive camelCase | Case-insensitive snake_case |
| Float text form | ClickHouse's shortest round-trip | DuckDB's may differ in exponent form |

## 6. What this means per client

| Client | Works today | Blocked by |
|---|---|---|
| `curl`, hand-written SQL | Yes | Nothing, if the SQL is DuckDB-flavored |
| `ch` (Elixir) | Connects and queries | Nothing for plain SQL; `{name:Type}` params fail |
| Logflare reads | No | `toStartOfInterval`, `ARRAY JOIN`, `LIMIT BY`, `match`, map access, `{name:Type}` params, trailing `SETTINGS` |
| `clickhouse-connect`, JS client | Connects | `system.columns` and `DESCRIBE` on metadata calls |
| Grafana ClickHouse plugin | No | `system.*` browsing, then the time-function family |
| BI tools (Metabase, Tableau) | No | Catalog introspection first, then functions |

## 7. How this gets closed

The Postgres edge is the precedent: eight layers, an emulated catalog, a 250-line
dialect rewrite (`SmolqueryPg.PgCatalog.Rewrite`) and a fixture corpus of what
clients actually send. The same shape applies here:

1. **Log unrecognized statements at the edge** to build a real corpus instead of guessing.
2. **Cheap wins first:** strip a trailing `SETTINGS`, substitute `{name:Type}` parameters.
3. **Emulate `system.*`** plus `SHOW` and `DESCRIBE`, the way `pg_catalog` is emulated.
4. **A textual pre-pass** for constructs DuckDB refuses to parse (`ARRAY JOIN`, `LIMIT BY`, `PREWHERE`, `FINAL`).
5. **Macro shims** for the function families, one family per layer.
6. **Pin the semantic differences with tests**, since those are the ones that fail quietly.

**Non-goals:** full dialect parity, `AggregateFunction` states and materialized-view
semantics, distributed table functions (`remote`, `cluster`), dictionaries, and
ClickHouse DDL. Those are ClickHouse-engine features, not query-surface gaps.
