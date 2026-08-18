# HTTP API v1

`SmolqueryApi` is the front door — a Phoenix endpoint served by Bandit (the
same stack as the web UI's `SmolqueryWeb`), started by the `:api` role, routing
only to service client modules and the catalog (the same boundary rule the
services hold each other to). Every `/v1` route requires the static
Bearer key (`SMOLQUERY_API_KEY`); a node with the `:api` role and no key
configured fails the boot rather than serve an open API. `/healthz` is the one
unauthenticated route.

```sh
curl http://127.0.0.1:4000/healthz

auth='authorization: Bearer '$SMOLQUERY_API_KEY
json='content-type: application/json'
curl -H "$auth" -H "$json" -d '{"id": "analytics"}' http://127.0.0.1:4000/v1/datasets
curl -H "$auth" -H "$json" -d '{"id": "events", "schema": [
      {"name": "id", "type": "INT64", "nullable": false},
      {"name": "ts", "type": "TIMESTAMP"},
      {"name": "amount", "type": "NUMERIC(38,2)"}
    ]}' http://127.0.0.1:4000/v1/datasets/analytics/tables
curl -H "$auth" http://127.0.0.1:4000/v1/datasets/analytics/tables/events
printf '%s\n' '{"id": 1, "ts": "2026-08-01T10:00:00Z", "amount": "12.50"}' '{"id": 2}' \
  | curl -H "$auth" -H 'content-type: application/x-ndjson' --data-binary @- \
    http://127.0.0.1:4000/v1/datasets/analytics/tables/events/insert
curl -H "$auth" -H 'content-type: application/x-ndjson' --data-binary @events.ndjson \
     http://127.0.0.1:4000/v1/datasets/analytics/tables/events/load
curl -H "$auth" -H "$json" -d '{"query": "SELECT count(*) AS n FROM analytics.events"}' \
     http://127.0.0.1:4000/v1/queries
```

## Routes

| route | |
|---|---|
| `GET /healthz` | liveness; the one unauthenticated route |
| `GET /metrics` | Prometheus text, gated by the *internal* secret (`x-smolquery-internal`), not the API key — metrics are for operators, not tenants. Every node also serves this route on its own metrics listener (`SMOLQUERY_METRICS_PORT`, default 4003), so nodes without the `:api` role are scrapable too |
| `GET /v1/datasets` | list datasets |
| `POST /v1/datasets` | create a dataset (idempotent) |
| `GET /v1/datasets/:ds/tables` | list a dataset's tables |
| `POST /v1/datasets/:ds/tables` | create a table — re-creating with the same schema is a 200, with a different one a 409, never a silent no-op |
| `GET /v1/datasets/:ds/tables/:t` | a table's schema, retention policy, clustering key, and partition count (`null` = deployment default) |
| `PATCH /v1/datasets/:ds/tables/:t` | set or clear retention and/or clustering. Retention: `{"retention": {"column": "ts", "ttlMs": 2592000000}}` ages rows out of `ts` after 30 days, segment-grained and conservative (a segment is dropped only once *every* row in it has aged out); `{"retention": null}` keeps rows forever again. Clustering: `{"clustering": ["project_id", "ts"]}` sorts future writes by those columns (ClickHouse `ORDER BY` analog); `{"clustering": []}` clears it. Columns must exist on the schema (unknown names are 422). Changing clustering does not rewrite existing segments. Partitions: `{"partitions": 3}` sets the table's own write-partition count (T-304), at most 64 — the effective count is never below the deployment's `SMOLQUERY_WRITE_PARTITIONS`, and it is raise-only (a lower value is 422, and the catalog write itself is monotonic, so concurrent raises converge on the maximum). Two caveats: an `insertId` retry that straddles the raise is at-least-once while writers' schema caches converge (up to one `schema_cache_ttl_ms`), and during a rolling deploy a query node still on a pre-T-304 release ignores catalog counts — raise a count only once the fleet runs this release. A body carrying several fields applies them atomically — an error response means none changed |
| `POST /v1/datasets/:ds/tables/:t/insert` | streaming insert — **`application/x-ndjson` only**, one JSON object per line, the same bytes ClickHouse takes as `JSONEachRow`; `insertId` is a query parameter. A JSON-array body is a 415: two content types were two ingest paths and the array one measured 3-4x slower with nothing announcing which you got. A 200 means the buffer service has every accepted row durable and queryable; rejected rows come back per-index in `insertErrors` (partial failure is a 200, BigQuery-style); a full or overloaded buffer is a 429 whose `retry-after` says how far behind the write path is; a node with too many ingest-body bytes already in flight also answers 429, with `retry-after: 1`, before it reads the body (`SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES`, T-245). An optional `insertId` makes the request idempotent: retrying after a timeout or dropped response with the same id (and the same rows) cannot double-count — without one, retries are at-least-once. One exception: a retry that straddles a partition-count raise (`PATCH {"partitions": N}`) can hash to a different partition than the original while writers' schema caches converge, and is at-least-once for that window |
| `POST /v1/datasets/:ds/tables/:t/load` | batch load — the body is the file (`application/x-ndjson`, `text/csv`, or `application/vnd.apache.parquet`), pushed through the same insert path in chunks; capped by `load_max_bytes` (413 past it), counted whole against the same in-flight admission limit as `/insert` for the request's full duration, synchronous, and not atomic — a mid-load failure reports what was already durable, and unlike `/insert` it takes no `insertId`, so a retry re-inserts. Two measured caveats ([benchmarks](benchmarks.md)): the body spools to disk but the parser materializes every row, so a load peaks at **~10× the file in memory**; and the cap is in *bytes*, which at 61 columns is ~120k rows of NDJSON but ~254k of CSV. It is also **not** the fast path — concurrent `/insert` is 2.4× quicker |
| `POST /v1/queries` | sync query — the finished job plus its first page of rows (`maxResults`, default 1000); a query that outlives `timeoutMs` is cancelled and answered 504. `"explain": "plan"` answers the engine's query plan instead of rows, `"analyze"` executes the query and answers the profiled plan; either way the text arrives as `explain` on the job and the response carries no rows. `"trace": true` returns the query's phase spans on the job for a waterfall view |
| `POST /v1/jobs` | the same query as an async job — returns it pending; takes the same `explain` and `trace` options, whose output lands on `GET /v1/jobs/:id` |
| `GET /v1/jobs/:id` | status and stats; once the result TTL expires, answered from durable job history |
| `GET /v1/jobs/:id/results` | page a finished job's rows with `max_results` + `page_token`; expired results are 410, unknown jobs 404 |
| `DELETE /v1/jobs/:id` | cancel — cancelling a finished job is still a 200 |

## Schema types

`INT64`, `FLOAT64`, `STRING`, `BOOL`, `TIMESTAMP`, `DATE`, and `NUMERIC(p,s)`.

## Values

Insert rows are JSON objects keyed by column name. Values coerce by the table's
schema — `INT64` accepts integers or digit strings (JS clients lose precision
past 2^53), `TIMESTAMP`/`DATE` take ISO 8601 strings (offsets convert to UTC),
`NUMERIC` prefers strings (floats round). The ingest edge validates against a
cached schema (`schema_cache_ttl_ms`, invalidated by CRUD on the same node),
forwards one request as one forward-batch, and never acks from memory — the
response returns when the rows are on the buffer node's disk and in its hot
manifest.

Query results page from the frame the runner holds until `result_ttl_ms`;
temporal and decimal values arrive as ISO 8601 / decimal strings, mirroring what
inserts accept. A result larger than `result_max_rows` (default 10,000,
matching the `maxResults` ceiling — see [configuration](configuration.md))
fails the query with `400 RESULT_TOO_LARGE` instead of materializing: add a
`LIMIT` or aggregate.

## Explain

`"explain": "plan"` plans the query for real — same views, same snapshot pin,
same lockdown — then asks DuckDB for the plan instead of executing it.
`"explain": "analyze"` executes under the profiler and reports per-operator
rows and timings. An explain job finishes `done` with the text on the job's
`explain` field, `rowCount` null, and nothing to page —
`GET /v1/jobs/:id/results` answers 409. Any other `explain` value is a 400:
silently running a query the caller asked to explain would be the worst
reading of a typo. Like `statistics`, history does not persist `explain`, so
it is gone once the result TTL expires — after that, `GET /v1/jobs/:id`
answers the job with `explain` null, and the results route answers the
generic 410 rather than the explain 409, because history cannot tell an
explain job from one whose rows expired.

## Tracing

`"trace": true` (default false) collects the query's phase spans and returns
them on the job as a waterfall-ready list — offsets and durations in
microseconds, rebased to the earliest span:

```json
{"trace": {"spans": [
  {"name": "engine_start",   "startUs": 0,     "durationUs": 41210, "meta": {}},
  {"name": "serialize",      "startUs": 41400, "durationUs": 803,   "meta": {}},
  {"name": "snapshot",       "startUs": 42250, "durationUs": 1100,  "meta": {}},
  {"name": "resolve",        "startUs": 43380, "durationUs": 2900,  "meta": {}},
  {"name": "manifests",      "startUs": 46300, "durationUs": 8100,  "meta": {}},
  {"name": "manifest_fetch", "startUs": 46310, "durationUs": 7900,  "meta": {"url": "http://buffer-0:4321"}},
  {"name": "build",          "startUs": 54500, "durationUs": 350,   "meta": {}},
  {"name": "statements",     "startUs": 54900, "durationUs": 4200,  "meta": {}},
  {"name": "execute",        "startUs": 59200, "durationUs": 812000,"meta": {}}
]}}
```

Every phase always emits an `[:smolquery, :query, :span]` telemetry event;
`trace` only decides whether this job collects them. A job that failed or was
cancelled still settles with the spans it got — the partial trace is exactly
what explains it. Like `statistics` and `explain`, history does not persist
the trace. Non-boolean `trace` values are a 400.

## Query statistics

A finished job carries a `statistics` object — on the `job` in a
`POST /v1/queries` response and on `GET /v1/jobs/:id` while the runner holds
the job (history does not persist it, so an expired job answers
`"statistics": null`):

```json
{
  "filesTotal": 12, "filesScanned": 4,
  "rowsScanned": 100000, "bytesScanned": 52428800, "mibScanned": 50.0,
  "hot":    {"filesTotal": 9, "filesScanned": 1, "rowsScanned": 1000, "bytesScanned": 4096},
  "sealed": {"filesTotal": 3, "filesScanned": 3, "rowsScanned": 99000, "bytesScanned": 52424704}
}
```

The numbers are plan-derived: which files the planner decided the query needs,
and those files' catalog/manifest sizes — not engine-measured I/O. The tiers
prune by different mechanisms, so they are reported separately. For the hot
tier, `filesTotal` counts micro-segments that passed the membership rule and
`filesScanned` what survived min-max pruning — the pruner's effect, counted.
For the sealed tier both fields count the segments listed at the pinned
snapshot; DuckDB may prune further at scan time, which the planner cannot see.

## Errors

Failures answer one JSON envelope everywhere:

```json
{"error": {"code": 401, "status": "UNAUTHENTICATED", "message": "missing or invalid API key"}}
```

A `503 UNAVAILABLE` from a query means a buffer node the reader expected could
not answer, so the result would have been silently short — see
[fan-out](architecture.md#clustered-fan-out).
