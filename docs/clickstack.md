# HyperDX (ClickStack) on smolquery

[ClickStack](https://clickhouse.com/docs/clickstack/getting-started/oss) is HyperDX, a
log and trace UI, on top of ClickHouse. HyperDX can read a smolquery table in
ClickHouse's place, through the `:clickhouse` edge ([clickhouse.md](clickhouse.md)).
This page is the recipe, and the list of what works.

> **How this was checked.** Two ways, neither of them the browser.
>
> 1. HyperDX's own code was run against a smolquery node (2026-09-20,
>    `hyperdxio/hyperdx @ c42dda8`): its SQL generator, its metadata reader and its
>    ClickHouse client wrapper, over the real `@clickhouse/client`. `scripts/hyperdx-probe/`
>    is that run, and says how to repeat it. It reads the source, lists fields and map
>    keys, runs eleven Lucene searches, the histogram and the filters sidebar's values:
>    17 of 17 answer. Five chart configurations through its chart builder answer too.
> 2. The statements it sent are a fixture
>    (`test/support/fixtures/clickstack/hyperdx_search.json`) run against real rows in
>    `test/smolquery_clickhouse/hyperdx_search_test.exs`, so they keep working.
>
> The HyperDX **UI and API server have not** been pointed at smolquery: no container
> runs on the dev box. That first run will find what the library did not show; T-480 is
> how those statements get recorded.

## Scope

- **In:** HyperDX reading logs. Its Search page: the results table, the histogram, a
  search term, and field filters.
- **Out, for now:** ClickStack's OpenTelemetry collector. It speaks ClickHouse's
  `Native` format and runs ClickHouse DDL at start, and smolquery answers neither
  (T-497, T-498, T-499). Rows reach smolquery through its own ingest instead.
- **Out, later:** traces, sessions and metrics (T-501). Their tables have `Array`
  columns with dotted names, which smolquery has no type or identifier for.

## 1. Run smolquery where HyperDX can reach it

The edge binds `127.0.0.1:8123`. HyperDX runs in a container, so bind the edge to an
address the container can reach. It does not terminate TLS; keep it on a private network.

```sh
SMOLQUERY_ROLES=api,ingest,buffer,storage,query,clickhouse \
SMOLQUERY_API_KEY=... \
SMOLQUERY_CLICKHOUSE_IP=0.0.0.0 \
  bin/smolquery start
```

The edge's password is the API key, or `SMOLQUERY_CLICKHOUSE_PASSWORD` when set. The
user name is accepted as given; HyperDX sends `default`.

## 2. Create the logs table

HyperDX's default log source reads these columns. Create them through the API
([api.md](api.md)); `TIMESTAMP_NS` is what answers as `DateTime64(9)`.

```sh
auth='authorization: Bearer '$SMOLQUERY_API_KEY
json='content-type: application/json'
api=http://127.0.0.1:4000

curl -H "$auth" -H "$json" -d '{"id": "default"}' $api/v1/datasets
curl -H "$auth" -H "$json" -d '{"id": "otel_logs", "schema": [
      {"name": "Timestamp", "type": "TIMESTAMP_NS", "nullable": false},
      {"name": "TraceId", "type": "STRING"},
      {"name": "SpanId", "type": "STRING"},
      {"name": "SeverityText", "type": "STRING"},
      {"name": "SeverityNumber", "type": "INT64"},
      {"name": "ServiceName", "type": "STRING"},
      {"name": "Body", "type": "STRING"},
      {"name": "ResourceAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "ScopeName", "type": "STRING"},
      {"name": "LogAttributes", "type": "MAP(STRING, STRING)"}
    ]}' $api/v1/datasets/default/tables
curl -X PATCH -H "$auth" -H "$json" -d '{"clustering": ["ServiceName", "Timestamp"]}' \
     $api/v1/datasets/default/tables/otel_logs
```

The clustering key is what `system.tables` answers as the table's `sorting_key`, which
HyperDX reads to order its results.

## 3. Write rows

Any smolquery insert works. NDJSON over the API:

```sh
printf '%s\n' \
  '{"Timestamp": "2026-09-19T10:11:29.123456789Z", "ServiceName": "api", "SeverityText": "error", "Body": "payment failed, id=1", "LogAttributes": {"http.status": "500"}}' \
  | curl -H "$auth" -H 'content-type: application/x-ndjson' --data-binary @- \
      $api/v1/datasets/default/tables/otel_logs/insert
```

A producer that already speaks ClickHouse can use the edge's RowBinary insert instead
([clickhouse.md](clickhouse.md#the-insert)).

## 4. Point HyperDX at it

The HyperDX image needs MongoDB for its own state. `DEFAULT_CONNECTIONS` and
`DEFAULT_SOURCES` are applied when the team has no connection and no source yet, which
is the first boot.

```yaml
services:
  mongo:
    image: mongo:5.0.14-focal
  hyperdx:
    image: docker.hyperdx.io/hyperdx/hyperdx
    ports: ["8080:8080"]
    environment:
      MONGO_URI: mongodb://mongo:27017/hyperdx
      FRONTEND_URL: http://localhost:8080
      DEFAULT_CONNECTIONS: >-
        [{"name":"smolquery","host":"http://smolquery:8123","username":"default","password":"<SMOLQUERY_API_KEY>"}]
      DEFAULT_SOURCES: >-
        [{"name":"Logs","kind":"log","connection":"smolquery",
          "from":{"databaseName":"default","tableName":"otel_logs"},
          "timestampValueExpression":"Timestamp",
          "displayedTimestampValueExpression":"Timestamp",
          "implicitColumnExpression":"Body",
          "bodyExpression":"Body",
          "serviceNameExpression":"ServiceName",
          "severityTextExpression":"SeverityText",
          "eventAttributesExpression":"LogAttributes",
          "resourceAttributesExpression":"ResourceAttributes",
          "traceIdExpression":"TraceId",
          "spanIdExpression":"SpanId",
          "defaultTableSelectExpression":"Timestamp,ServiceName,SeverityText,Body"}]
```

- **`host`** must be a name, not a private IP literal: HyperDX's connection test refuses
  `127.0.0.1` and takes `localhost` or a service name. Its test is
  `GET /?query=SELECT 1`, which the edge answers `1`.
- **`defaultTableSelectExpression`** may name its columns (`ServiceName as service`), as
  the source HyperDX auto-creates from its form does. HyperDX then sends ClickHouse's
  `WITH (expr) AS alias`, which the edge rewrites.
- **No `metadataMaterializedViews`, trace, session or metric source.** HyperDX's stock
  defaults name rollup tables and three more sources; leave them out.

## What works

| HyperDX | Status |
|---|---|
| Connection test | Works |
| Reading the source's columns and sorting key (`DESCRIBE`, `system.tables`) | Works |
| Search: results table, newest first, paged | Works |
| Onboarding checklist: whether a source has data (`sum(total_rows)` from `system.tables`) | Works: the table's row count, hot tier included |
| Search: histogram by severity | Works |
| Search: the rows a search will scan (`EXPLAIN ESTIMATE`, sent before each search) | Works: the rows and files the plan keeps. An upper bound: hot-tier pruning counts, sealed-tier pruning does not |
| Source form: checking an expression a user typed | Works: `EXPLAIN ESTIMATE` of a statement that does not bind answers its error |
| Search: a term (`error`), a phrase, a negation | Works; a term is a whole token, found whatever its case |
| Search: `field:value`, `field:"exact"`, `field:*`, a map key (`LogAttributes.http.status:500`), a number or a range | Works |
| Search: a term with `_` or `%` in it | Works: the edge gives `LIKE` the backslash escape ClickHouse assumes |
| Field list, map keys, and the filters sidebar's values per field | Works: `groupUniqArray(20)(x)` and its kin are rewritten, and answer as `Array` |
| Charts: `quantile(0.95)(x)` by a group, `avg`, `max`, a filtered `count` and `sum`, `count(DISTINCT x)` | Works, through HyperDX's chart builder |
| Row click (the side panel) | Works: HyperDX finds the row by the values it was shown. A `TIMESTAMP_NS` answers all nine digits and a map keeps its stored key order, so both match when sent back; `JSONExtract(s, 'Map(String, String)')`, `isNull` and the `MD5` of a long string are rewritten |
| A source whose select uses `x as y` | Works: `WITH (expr) AS alias` is rewritten |
| Alert sample rows | `CSV` output answers; alerts themselves have not been run |
| Traces, service map, sessions, metrics | **Not yet** (T-501) |
| ClickStack's collector writing to smolquery | **Out of scope** (T-497, T-498, T-499) |

## What differs from ClickHouse underneath

- A result column answers its plain type where it can never be `NULL`, as ClickHouse
  does, and `Nullable(...)` otherwise (T-510). The rule is ClickHouse's: it starts at the
  schema's `nullable: false` and carries through an expression — a cast, a listed function
  such as `toStartOfInterval`, `count()`, a `CASE` with an `ELSE`, another aggregate under
  a `GROUP BY` — and through subqueries and CTEs. A function that is not listed, an outer
  join or a `UNION` answers `Nullable`. HyperDX unwraps `Nullable` when it reads a source's
  column types, but **not** in a chart's `meta`: its histogram needs the time bucket to be
  `DateTime64`, so the source's timestamp column must be `nullable: false`. A
  `MATERIALIZED` timestamp can be: `ADD COLUMN ts TIMESTAMP NOT NULL MATERIALIZED
  epoch_ms(ts_int)`, or `"nullable": false` beside `"materialized"` (T-515). One that was
  added nullable has to be dropped and added again.
- A map column is `Map(String, String)`, not `Map(LowCardinality(String), String)`.
- A `VARIANT` column is `JSON` (T-521). Use one where attributes nest or hold more than
  strings, which `MAP(STRING, STRING)` has no room for: a Logflare drain's `metadata` is
  the case it was done for. HyperDX reads the type's prefix and searches a nested key as
  `toString(metadata.context.application)`, which answers. Listing a `JSON` column's keys
  for the filters sidebar (`JSONDynamicPathsWithTypes`) does not answer yet. This needs a
  HyperDX that knows the `JSON` type: the recipe is checked against `@hyperdx/app@2.39.1`.
  2.1.0, a year older, has only a stub for it, and words a term search in a function the
  edge does not have (`hasTokenCaseInsensitive`).
  HyperDX picks its map-key query by that prefix.
- There are no skip indexes, so HyperDX takes its plain `hasToken(lower(Body), ...)`
  path. `hasToken` here scans; there is no token index behind it.
- `system.settings` is empty, so HyperDX sends none of its optimization settings.
- Timestamps are UTC.
- An expression with no alias is named as the engine writes it, not as ClickHouse does:
  `quantile_cont(tofloat64ordefault(tostring(x)), 0.95)`, where ClickHouse answers
  `quantile(0.95)(toFloat64OrDefault(toString(x)))`. Only `count()` is renamed, since
  HyperDX looks that one up by name. Whether its charts read any other column by name
  is not known until the UI runs.

## Running it on one node with a SQLite catalog

HyperDX sends its metadata queries in parallel. On a single node whose DuckLake catalog
is SQLite, several job engines attaching the catalog at once can hit SQLite's
`database is locked`, and the node then stays locked until it restarts. This is
smolquery's existing SQLite issue, not HyperDX's: eight concurrent queries through the
plain API do the same. A Postgres catalog (`CATALOG_DATABASE_URL`) does not have it, and
is what a HyperDX deployment should use.

## When something fails

A statement the edge cannot answer for a reason that is the dialect's is logged, with
the client's `user-agent`, and counted in `smolquery_clickhouse_unanswered_total`:

```
clickhouse edge could not answer: code=62 name=SYNTAX_ERROR user_agent="hyperdx 2.1.0" statement="SELECT Body FROM ... ARRAY JOIN ... WHERE k = '?'" error="syntax error at or near \"ARRAY\""
```

String literals are replaced by default, since they are a user's search terms.
`SMOLQUERY_CLICKHOUSE_UNANSWERED_LOG=verbatim` keeps them, which is what to set for a
first run of HyperDX: those lines are the list of what to build next.

When something fails, the statement and the error are in HyperDX's UI: it prints the
rendered SQL beside the message. [clickhouse-sql-gaps.md](clickhouse-sql-gaps.md) says
what the edge does not take.
