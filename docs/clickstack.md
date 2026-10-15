# HyperDX (ClickStack) on smolquery

[ClickStack](https://clickhouse.com/docs/clickstack/getting-started/oss) is HyperDX, a
log and trace UI, on top of ClickHouse. HyperDX can read a smolquery table in
ClickHouse's place, through the `:clickhouse` edge ([clickhouse.md](clickhouse.md)).
This page is the recipe, and the list of what works.

> **How this was checked.** Two ways, neither of them the browser.
>
> 1. HyperDX's own code was run against a smolquery node (2026-09-20,
>    `hyperdxio/hyperdx @ c42dda8`; traces and metrics on 2026-10-14 and 2026-10-15,
>    `@hyperdx/app@2.41.0`): its SQL generator, its metadata reader and its
>    ClickHouse client wrapper, over the real `@clickhouse/client`. `scripts/hyperdx-probe/`
>    is that run, one script per screen family, and says how to repeat it. It reads the source, lists fields and map
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
- **In:** HyperDX reading traces (section 5): search in trace mode, the trace
  waterfall, the latency charts and the service map, over a table shaped like
  `otel_traces` without its `Events.*` and `Links.*` columns.
- **Out, for now:** ClickStack's OpenTelemetry collector. It speaks ClickHouse's
  `Native` format and runs ClickHouse DDL at start, and smolquery answers neither
  (T-497, T-498, T-499). Rows reach smolquery through its own ingest instead.
- **In:** HyperDX reading metrics (section 6): the metric catalog, the chart
  builder's gauge, sum and histogram charts, multi-series charts, ratios and
  formulas, over tables shaped like `otel_metrics_gauge`, `otel_metrics_sum` and
  `otel_metrics_histogram` without their `Exemplars.*` columns.
- **Out, later:** span events and links, exponential histograms, and sessions
  (T-501). The first two are `Array` columns with dotted names, which smolquery
  has no identifier for; the exponential histogram's SQL also names an alias
  before it defines it, which the engine cannot do.

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
- **No `metadataMaterializedViews`, session or metric source.** HyperDX's stock defaults
  name rollup tables and two more sources; leave them out. A trace source is section 5.

## 5. Traces

HyperDX's stock trace source reads these columns. The OpenTelemetry exporter's
`otel_traces` also has `Events.*` and `Links.*`, `Array` columns with dotted names,
which smolquery cannot create; HyperDX's stock source leaves its
`spanEventsValueExpression` unset, so nothing it sends reads them. `Duration` is in
nanoseconds, which the source's `durationPrecision: 9` says.

```sh
curl -H "$auth" -H "$json" -d '{"id": "otel_traces", "schema": [
      {"name": "Timestamp", "type": "TIMESTAMP_NS", "nullable": false},
      {"name": "TraceId", "type": "STRING"},
      {"name": "SpanId", "type": "STRING"},
      {"name": "ParentSpanId", "type": "STRING"},
      {"name": "TraceState", "type": "STRING"},
      {"name": "SpanName", "type": "STRING"},
      {"name": "SpanKind", "type": "STRING"},
      {"name": "ServiceName", "type": "STRING"},
      {"name": "ResourceAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "ScopeName", "type": "STRING"},
      {"name": "ScopeVersion", "type": "STRING"},
      {"name": "SpanAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "Duration", "type": "INT64"},
      {"name": "StatusCode", "type": "STRING"},
      {"name": "StatusMessage", "type": "STRING"}
    ]}' $api/v1/datasets/default/tables
curl -X PATCH -H "$auth" -H "$json" -d '{"clustering": ["ServiceName", "SpanName", "Timestamp"]}' \
     $api/v1/datasets/default/tables/otel_traces
```

Add the source beside the log source in `DEFAULT_SOURCES`. It is HyperDX's stock
`Traces` source without `metadataMaterializedViews`:

```json
{"name":"Traces","kind":"trace","connection":"smolquery",
 "from":{"databaseName":"default","tableName":"otel_traces"},
 "timestampValueExpression":"Timestamp",
 "displayedTimestampValueExpression":"Timestamp",
 "implicitColumnExpression":"SpanName",
 "serviceNameExpression":"ServiceName",
 "eventAttributesExpression":"SpanAttributes",
 "resourceAttributesExpression":"ResourceAttributes",
 "defaultTableSelectExpression":"Timestamp,ServiceName,StatusCode,round(Duration/1e6),SpanName",
 "traceIdExpression":"TraceId","spanIdExpression":"SpanId",
 "durationExpression":"Duration","durationPrecision":9,
 "parentSpanIdExpression":"ParentSpanId","spanNameExpression":"SpanName",
 "spanKindExpression":"SpanKind","statusCodeExpression":"StatusCode",
 "statusMessageExpression":"StatusMessage"}
```

Set `logSourceId` and `traceSourceId` on the two sources in HyperDX's source
settings afterwards (`DEFAULT_SOURCES` takes names there, as the stock file does), so
the waterfall shows a trace's log lines and a log's trace.

Checked with HyperDX 2.41.0's own query code against the edge: the columns and
sorting key, the field list and `SpanAttributes` keys, trace search by term
(`checkout`), phrase, `StatusCode:Error`, `SpanKind:Server`,
`SpanAttributes.http.method:POST` and a `Duration` range, the histogram
by `StatusCode`, p95 `Duration` by `ServiceName`, a filtered `avg`, the filters
sidebar's values, the waterfall's span list for one trace, and the service map's two-CTE join with `GROUPING SETS`, `GROUPING()`,
`countIf`, `cityHash64` and `quantiles(0.5, 0.95, 0.99)(duration)`.

## 6. Metrics

HyperDX's stock metric source names five tables, one per OpenTelemetry metric
type. Create the three its charts read. Every column of the exporter's tables
is here but `Exemplars.*`, an array of maps with dotted names, which HyperDX
never reads. `TimeUnix` is a `TIMESTAMP` that must not be null, so the chart's
time bucket answers as `DateTime64`, which HyperDX looks for in a chart's
`meta`. `BucketCounts` and `ExplicitBounds` are `ARRAY<T>` columns (T-489).

```sh
common='{"name": "ResourceAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "ResourceSchemaUrl", "type": "STRING"},
      {"name": "ScopeName", "type": "STRING"},
      {"name": "ScopeVersion", "type": "STRING"},
      {"name": "ScopeAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "ScopeDroppedAttrCount", "type": "INT64"},
      {"name": "ScopeSchemaUrl", "type": "STRING"},
      {"name": "ServiceName", "type": "STRING"},
      {"name": "MetricName", "type": "STRING"},
      {"name": "MetricDescription", "type": "STRING"},
      {"name": "MetricUnit", "type": "STRING"},
      {"name": "Attributes", "type": "MAP(STRING, STRING)"},
      {"name": "StartTimeUnix", "type": "TIMESTAMP"},
      {"name": "TimeUnix", "type": "TIMESTAMP", "nullable": false},
      {"name": "Flags", "type": "INT64"}'

create() {
  curl -H "$auth" -H "$json" -d "{\"id\": \"$1\", \"schema\": [$common, $2]}" $api/v1/datasets/default/tables
  curl -X PATCH -H "$auth" -H "$json" -d '{"clustering": ["ServiceName", "MetricName", "TimeUnix"]}' \
       $api/v1/datasets/default/tables/$1
}

create otel_metrics_gauge '{"name": "Value", "type": "FLOAT64"}'
create otel_metrics_sum '{"name": "Value", "type": "FLOAT64"},
      {"name": "AggregationTemporality", "type": "INT64"}, {"name": "IsMonotonic", "type": "BOOL"}'
create otel_metrics_histogram '{"name": "Count", "type": "INT64"}, {"name": "Sum", "type": "FLOAT64"},
      {"name": "BucketCounts", "type": "ARRAY<INT64>"}, {"name": "ExplicitBounds", "type": "ARRAY<FLOAT64>"},
      {"name": "Min", "type": "FLOAT64"}, {"name": "Max", "type": "FLOAT64"},
      {"name": "AggregationTemporality", "type": "INT64"}'
```

A row is the exporter's row as NDJSON: `TimeUnix` as an ISO 8601 string, the
attribute maps as objects, `BucketCounts` as a JSON array of integers and
`ExplicitBounds` as a JSON array of numbers. `AggregationTemporality` is `1`
for delta and `2` for cumulative, as in the protocol; HyperDX reads it to
turn a cumulative counter or histogram into per-bucket increases.

Add the source beside the others in `DEFAULT_SOURCES`. It is HyperDX's stock
`Metrics` source without the two tables that are not created:

```json
{"name":"Metrics","kind":"metric","connection":"smolquery",
 "from":{"databaseName":"default","tableName":""},
 "timestampValueExpression":"TimeUnix",
 "resourceAttributesExpression":"ResourceAttributes",
 "metricTables":{"gauge":"otel_metrics_gauge","sum":"otel_metrics_sum",
                 "histogram":"otel_metrics_histogram"}}
```

Checked with HyperDX 2.41.0's own query code against the edge, and kept as
the fixture `test/support/fixtures/clickstack/hyperdx_metrics.json` that
`test/smolquery_clickhouse/hyperdx_metrics_test.exs` runs over real rows: the
three tables' columns and sorting keys; the metric catalog per table
(`anyLast`); the metric name search (`positionCaseInsensitive`); a metric's
unit and description, its distinct attribute maps, and one attribute's values;
gauge charts (`avg` by `ServiceName`, `max` as a number, a filtered `avg`, a
group by `Attributes['cpu']`, a delta with `argMax`); sum charts (the rate of
a cumulative counter by service, `increase` with its top-20 groups, `increase`
as a number, the last cumulative value, a delta-temporality counter);
histogram charts (p95 by service, p50 as a number, the count per bucket, the
count by service); and multi-series charts (a gauge and a sum pivoted per
bucket, a ratio of two sums, a formula `A / B * 100`, a gauge beside a
histogram with its packed group). Every one answers with the `meta` types
HyperDX's chart code classifies: the time bucket as `DateTime64(6)`, values as
`Float64` or `Int128`.

Not answered: the exponential histogram's quantile. Its SQL names
`normalized_scale` before the item that defines it, which ClickHouse allows and
the engine does not, and it subtracts arrays whose names the rewrite cannot
tell are arrays (section "What differs"). Its count chart answers. The
`summary` type has no chart in HyperDX.

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
| Traces: search in trace mode, the waterfall, latency charts | Works, over the table in section 5 |
| Service map | Works: its `quantiles(0.5, 0.95, 0.99)(x)` is rewritten to one `quantile_cont` over a list and answers as `Array(Nullable(Float64))`, the three percentiles in order. Over no rows it answers `[]` where ClickHouse answers `[nan, nan, nan]`; the service map never groups an empty set |
| Metrics: the catalog, name search, a metric's unit, attributes and values | Works, over the tables in section 6 |
| Metrics: gauge, sum and histogram charts; multi-series, ratio and formula charts | Works: `last_value`, `lagInFrame`, `multiIf`, `sumForEach`, `arrayCumSum`, the lambda forms of `arrayExists` and `arrayFirstIndex`, a tuple's `.1`, `* EXCEPT`, `group` as a name and `counts - prev_counts` are rewritten or answered by macros. A count answers as `Int128`, since the engine's `sum` over an `Int64` is 128 bits wide |
| Metrics: exponential histograms | **Not yet** (T-501): a quantile over one names an alias before defining it |
| Span events and links, sessions | **Not yet** (T-501) |
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
  HyperDX picks its map-key query by that prefix.
- A `VARIANT` column is `JSON` (T-521). Use one where attributes nest or hold more than
  strings, which `MAP(STRING, STRING)` has no room for: a Logflare drain's `metadata` is
  the case it was done for. HyperDX reads the type's prefix and searches a nested key as
  `toString(metadata.context.application)`, which answers, and lists the column's keys for
  the filters sidebar with `JSONDynamicPathsWithTypes` and `groupUniqArrayMap`, which answer
  too: every leaf path, dotted, with the types seen at it. An array is one leaf, whatever it
  holds; a key that holds `null` everywhere is not listed; and a key with a dot in it,
  `{"g.h": 1}`, lists as `g.h` and reads as the nested path, which is how ClickHouse's
  `JSON` treats it too. This needs a
  HyperDX that knows the `JSON` type: the recipe is checked against `@hyperdx/app@2.39.1`.
  2.1.0, a year older, has only a stub for it, and words a term search in a function the
  edge does not have (`hasTokenCaseInsensitive`).
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
