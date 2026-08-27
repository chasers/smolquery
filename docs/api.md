# HTTP API v1

`SmolqueryApi` is the entry point for the application programming interface (API). It is a Phoenix endpoint served by Bandit. The web user interface's `SmolqueryWeb` runs on the same stack. The `:api` role starts the endpoint.

The endpoint routes only to service client modules and the catalog. The services hold each other to the same boundary rule.

Every `/v1` route requires the static **Bearer key** (`SMOLQUERY_API_KEY`). A node with the `:api` role and no configured key fails the boot. It does not serve an open API. `/healthz` and `/v1/docs.json` are the unauthenticated routes.

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
| `GET /healthz` | Liveness check. Unauthenticated. |
| `GET /v1/docs.json` | This API described as JSON, for agents (`SmolqueryApi.Docs`). Unauthenticated. The web UI serves the same document on its own host, behind its basic auth. |
| `GET /metrics` | Prometheus text. The *internal* secret (`x-smolquery-internal`) gates this route, not the API key. Metrics are for operators, not for tenants. Every node also serves this route on its own metrics listener (`SMOLQUERY_METRICS_PORT`, default 4003). Thus a node without the `:api` role is also scrapable. |
| `GET /v1/datasets` | List the datasets. |
| `POST /v1/datasets` | Create a dataset. The route is idempotent. |
| `GET /v1/datasets/:ds/tables` | List a dataset's tables. |
| `POST /v1/datasets/:ds/tables` | Create a table. A re-create with the same schema answers 200. A re-create with a different schema answers 409. The route is never a silent no-op. |
| `GET /v1/datasets/:ds/tables/:t` | Answer a table's schema, retention policy, clustering key, and partition count. A `null` partition count means the deployment default. |
| `PATCH /v1/datasets/:ds/tables/:t` | Set or clear retention, clustering, or partitions. Retention: `{"retention": {"column": "ts", "ttlMs": 2592000000}}` ages rows out of `ts` after 30 days. Retention is segment-grained and conservative: the system drops a segment only after *every* row in it has aged out. `{"retention": null}` keeps rows forever again. Clustering: `{"clustering": ["project_id", "ts"]}` sorts future writes by those columns (the ClickHouse `ORDER BY` analog). `{"clustering": []}` clears it. The columns must exist on the schema; an unknown name is a 422. A clustering change does not rewrite existing segments. Partitions: `{"partitions": 3}` sets the table's own write-partition count (T-304), at most 64. The effective count is never below the deployment's `SMOLQUERY_WRITE_PARTITIONS`. The count is raise-only: a lower value is a 422. The catalog write itself is monotonic, so concurrent raises converge on the maximum. Two caveats: (1) an `insertId` retry that straddles the raise is at-least-once while the writers' schema caches converge (up to one `schema_cache_ttl_ms`); (2) during a rolling deploy, a query node on a pre-T-304 release ignores catalog counts — raise a count only after the full fleet runs this release. A body with several fields applies them atomically. An error response means none changed. |
| `DELETE /v1/datasets/:ds/tables/:t/segments` | Drops sealed segments from a table's current snapshot by path: `{"paths": ["analytics/events/01ABC....parquet"]}`. This is the operator route for a segment `Smolquery.StorageService.Compactor` quarantined as permanently corrupt (T-310) — the segment stays registered and keeps breaking every reader until an operator drops it. The route is idempotent: a path the table does not currently hold is not an error. The response separates `dropped` (paths the snapshot held) from `notFound` (paths it did not), so a typo'd path is visible instead of echoed as dropped. A catalog commit race answers 409; retry the request. Dropping does not delete the file; GC reclaims it once no snapshot references it. |
| `POST /v1/datasets/:ds/tables/:t/insert` | Streaming insert. The body is **`application/x-ndjson` only**: one JavaScript Object Notation (JSON) object per line, the same bytes ClickHouse takes as `JSONEachRow`. `insertId` is a query parameter. A JSON-array body is a 415. Two content types were two ingest paths. The array path measured 3-4x slower, with nothing to announce which path you got. A 200 means the buffer service has every accepted row durable and queryable. Rejected rows come back per-index in `insertErrors`; a partial failure is a 200, BigQuery-style. A full or overloaded buffer answers 429. Its `retry-after` header says how far behind the write path is. A node with too many ingest-body bytes already in flight also answers 429, with `retry-after: 1`, before it reads the body (`SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES`, T-245). A body over `SMOLQUERY_INSERT_MAX_NDJSON_BYTES` (`8000000`) is a 413 that points at `POST .../load`. An optional `insertId` makes the request idempotent: a retry after a timeout or a dropped response, with the same id and the same rows, cannot double-count. Without an `insertId`, retries are at-least-once. One exception: a retry that straddles a partition-count raise (`PATCH {"partitions": N}`) can hash to a different partition than the original while the writers' schema caches converge. That retry is at-least-once for the window. |
| `POST /v1/datasets/:ds/tables/:t/load` | Batch load. The body is the file: `application/x-ndjson`, `text/csv`, or `application/vnd.apache.parquet`. The route pushes the file through the same insert path in chunks. `SMOLQUERY_LOAD_MAX_BYTES` (`load_max_bytes`, `268435456`) caps the body; a larger body is a 413. The whole body counts against the same in-flight admission limit as `/insert`, for the request's full duration. The load is synchronous. It is not atomic: a mid-load failure reports what was already durable. Unlike `/insert`, the route takes no `insertId`, so a retry re-inserts. Two measured caveats ([benchmarks](benchmarks.md)): the body spools to disk, but the parser materializes every row, so a load peaks at **~10× the file in memory**; the cap is in *bytes*, so at 61 columns it is ~120k rows of newline-delimited JSON (NDJSON) but ~254k rows of comma-separated values (CSV). The load is also **not** the fast path: concurrent `/insert` is 2.4× quicker. |
| `GET /v1/connections` | List the registered federated Postgres connections. No route ever returns a password. |
| `POST /v1/connections` | Register a connection: `{"name": "warehouse", "host": "db.internal", "database": "app", "username": "reader", "password": "...", "port": 5432, "sslmode": "require"}`. `name` becomes the catalog a federated query qualifies with (`warehouse.public.users`), so it must be an identifier. `port` defaults to 5432 and `sslmode` to `require` — libpq's own default, `prefer`, silently accepts plaintext when the server declines TLS, so it is not the default here. A name that already exists is replaced, and the status says which happened: 201 for a new connection, 200 for a replacement. The password is sealed with `SMOLQUERY_CREDENTIAL_KEY` before it reaches the catalog; a node without that key answers 503. |
| `GET /v1/connections/:name` | One connection, without its password. |
| `PATCH /v1/connections/:name` | Change the fields the body names. An **absent** `password` leaves the stored one untouched, which is what lets you correct a host or a port without re-entering a credential you cannot read back. An empty-string password is a 400, not a clear: a connection with no password cannot open. |
| `DELETE /v1/connections/:name` | Remove a connection. Removing one that is already absent is a 200. |
| `POST /v1/connections/:name/test` | Attach the connection in a throwaway engine and read one row through it. 422 when the remote database does not answer. The reason names the connection and never quotes its connection string — a failed `ATTACH` otherwise echoes the password back. |
| `POST /v1/queries` | Sync query. The response is the finished job plus its first page of rows (`maxResults`, default 1000). The server cancels a query that outlives `timeoutMs`. It answers that query with a 504. `"explain": "plan"` answers the engine's query plan instead of rows. `"explain": "analyze"` executes the query, then answers the profiled plan. In both cases the text arrives as `explain` on the job. The response carries no rows. `"trace": true` returns the query's phase spans on the job, for a waterfall view. |
| `POST /v1/jobs` | The same query as an async job. The response returns the job pending. The route takes the same `explain` and `trace` options. Their output lands on `GET /v1/jobs/:id`. |
| `GET /v1/jobs/:id` | Status and stats. Once the result time to live (TTL) expires, the answer comes from durable job history. |
| `GET /v1/jobs/:id/results` | Page a finished job's rows with `max_results` + `page_token`. Expired results are a 410. Unknown jobs are a 404. |
| `DELETE /v1/jobs/:id` | Cancel the job. A cancel of a finished job is still a 200. |

## Schema types

`INT64`, `FLOAT64`, `STRING`, `BOOL`, `TIMESTAMP`, `DATE`, `NUMERIC(p,s)`, `MAP(STRING, STRING)`, and `VARIANT`.

The last two are semi-structured, and each has limits a caller must know. The limits are listed here, once. The sections on values and results below refer back here. There is no `JSON` type: `VARIANT` covers it, and `attrs::JSON` gives the text.

### `MAP(STRING, STRING)`

ClickHouse's `Map(String, String)`: an open set of string keys with string values, the shape OpenTelemetry attribute bags arrive in. Query it with DuckDB's map functions: `attrs['host']` (a string, or NULL for a missing key), `map_contains(attrs, 'host')`, `map_keys(attrs)`, `cardinality(attrs)`, and `to_json(attrs)`.

Limitations:

- **Values are strings.** A value that is not a string is stored as its JSON text: `1` becomes `"1"`, `true` becomes `"true"`, `["a","b"]` becomes `"[\"a\",\"b\"]"`.
- A value that is not a JSON object is rejected.
- A `NULL` map reads back as `{}`. The result frame cannot tell the two apart.
- No stats pruning. A filter on a key reads every row of the table.
- Only an NDJSON load can carry a map. A Parquet load into a table with a map column answers `400`. A CSV load works when the CSV has no map column.
- A map cannot be a clustering column: nothing prunes on it.
- In a result, a computed `LIST(STRUCT(key, value))` of strings arrives folded as an object, and a computed map with values that are not strings arrives as its entries. A stored map always arrives as an object.
- A value that is not a string is stored as its JSON text as this node encodes it. The passthrough path keeps the client's bytes. The two can differ on a float's exponent form and on the key order of a nested object.

### `VARIANT`

DuckDB's semi-structured type: any JSON value, with each value's own type kept. An integer stays an integer, an array stays an array, an object nests. Query it with DuckDB's variant functions:

- `attrs['host']::VARCHAR` — a key, cast to a scalar.
- `attrs['a']['b'][1]` — nested access. Array indexes start at 1.
- `variant_typeof(attrs)` — `OBJECT(host, n)`, `ARRAY(2)`, `VARCHAR`, `INT64`, `VARIANT_NULL`, and so on.
- `attrs::JSON` — the document as JSON text.

Limitations:

- **Stored as JSON text**, in both tiers. DuckLake cannot yet register a Parquet file with DuckDB's variant encoding, so each query parses the JSON of every row it scans. A key is not a column on disk.
- No stats pruning. A filter on a key reads every row of the table.
- **Casts are strict.** `attrs['n']::BIGINT` errors if any scanned row holds a string in `n`. Use `TRY_CAST(attrs['n'] AS BIGINT)` for a key with mixed types.
- A `VARIANT` result column crosses the engine boundary as JSON text and arrives decoded: an object, an array, or a scalar. A `NULL` arrives as `null`.
- The runner refuses a `VARIANT` nested in a struct or list in a result (`SELECT {'v': attrs}`) with a `400`, before the query runs. Select the variant on its own, or cast it with `::JSON`. An `EXPLAIN` of the same query still answers.
- A query that returns a `VARIANT` column does not use the distributed (scatter) path. The runner answers it from one engine.
- A `VARIANT` made without a table (`SELECT '1'::VARIANT`) is not cast at the boundary, and the query fails.
- Only an NDJSON load can carry a variant; a Parquet load answers `400`; a CSV load works when the CSV has no variant column.
- A variant cannot be a clustering column.

Choose `MAP(STRING, STRING)` for ClickHouse parity and string-only attributes. Choose `VARIANT` when values must keep their types or nest.

## Values

Insert rows are JSON objects keyed by column name. Values coerce by the table's schema:

- `INT64` accepts integers or digit strings. JavaScript clients lose precision past 2^53.
- `TIMESTAMP` and `DATE` take ISO 8601 strings. Offsets convert to Coordinated Universal Time (UTC).
- `NUMERIC` prefers strings; floats round.
- `MAP(STRING, STRING)` takes a JSON object. Every value is stored as a string — see its limits under [Schema types](#schema-types).
- `VARIANT` takes any JSON value, unchanged.

The ingest edge validates each row against a **cached schema** (`schema_cache_ttl_ms`). Create, read, update, and delete (CRUD) operations on the same node invalidate the cache. The edge forwards one request as one forward-batch. It never acknowledges from memory: the response returns when the rows are on the buffer node's disk and in its hot manifest.

Query results page from the frame the runner holds until `result_ttl_ms`. Temporal values arrive as ISO 8601 strings; decimal values arrive as decimal strings; a map arrives as a JSON object; a variant arrives as the JSON value it holds. This mirrors what inserts accept. The limits of a map or variant in a result are under [Schema types](#schema-types). A result larger than `result_max_rows` (default 10,000, the same as the `maxResults` ceiling — see [configuration](configuration.md)) fails the query with `400 RESULT_TOO_LARGE` instead of materializing: add a `LIMIT` or aggregate.

## Explain

`"explain": "plan"` plans the query for real: the same views, the same snapshot pin, the same lockdown. It then asks DuckDB for the plan instead of executing the query. `"explain": "analyze"` executes the query under the profiler. It reports per-operator rows and timings.

An explain job finishes `done`. The text is on the job's **`explain`** field. `rowCount` is null. There is nothing to page: `GET /v1/jobs/:id/results` answers 409.

Any other `explain` value is a 400. To run a query silently, when the caller asked to explain it, would be the worst result of a typo.

History does not persist `explain`, the same as `statistics`. The text is gone once the result TTL expires. After that, `GET /v1/jobs/:id` answers the job with `explain` null. The results route then answers the generic 410, not the explain 409: history cannot tell an explain job from a job whose rows expired.

## Tracing

`"trace": true` (default false) collects the query's **phase spans**. The job returns them as a waterfall-ready list. Offsets and durations are in microseconds, rebased to the earliest span:

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

A query the Top-N bound applies to (T-400) — one SELECT over one table with `ORDER BY col LIMIT n` — also emits a `top_n` span between `manifests` and `build`. Its meta says what the probe found: `{"bounded": true, "rounds": 1, "candidates": 2}` — whether a bound was applied, how many probe rounds ran, and how many hot entries the last round read.

A query over a table with a `VARIANT` column also emits a `variants` span inside `execute`: the `DESCRIBE` that finds which result columns need the cast to JSON. It appears only then.

Every phase always emits an `[:smolquery, :query, :span]` telemetry event. The `trace` option only decides whether this job collects them. A job that failed or was cancelled still settles with the spans it got: the partial trace is exactly what explains the failure. History does not persist the trace, the same as `statistics` and `explain`. A non-boolean `trace` value is a 400.

## Distributed execution (PL-49)

`"distributed": true | false` overrides the deployment's `SMOLQUERY_DISTRIBUTED_QUERY` default (on) for this job only. `POST /v1/queries` and `POST /v1/jobs` both take it. A distributed answer carries a **`scatter`** object on the job — `{"shards": 3, "partialBytes": 41210}` — and `"scatter": null` means the ordinary single-engine scan answered, including every fallback. A query that does not decompose, or any distributed failure, falls back silently and answers the same rows. History does not persist `scatter`. A non-boolean `distributed` value is a 400.

## Query statistics

A finished job carries a **`statistics`** object. It appears on the `job` in a `POST /v1/queries` response. It also appears on `GET /v1/jobs/:id` while the runner holds the job. History does not persist it, so an expired job answers `"statistics": null`:

```json
{
  "filesTotal": 12, "filesScanned": 4,
  "rowsScanned": 100000, "bytesScanned": 52428800, "mibScanned": 50.0,
  "hot":    {"filesTotal": 9, "filesScanned": 1, "rowsScanned": 1000, "bytesScanned": 4096},
  "sealed": {"filesTotal": 3, "filesScanned": 3, "rowsScanned": 99000, "bytesScanned": 52424704}
}
```

The numbers are plan-derived. They report the files the planner decided the query needs, plus those files' catalog and manifest sizes. They are not engine-measured input/output (I/O). The tiers prune by different mechanisms, so the report keeps them separate.

For the hot tier, `filesTotal` counts the micro-segments that passed the membership rule. `filesScanned` counts what survived min-max pruning: the pruner's effect, counted. For the sealed tier, both fields count the segments listed at the pinned snapshot. DuckDB may prune further at scan time; the planner cannot see that.

## Errors

Failures answer one JSON envelope everywhere:

```json
{"error": {"code": 401, "status": "UNAUTHENTICATED", "message": "missing or invalid API key"}}
```

A `503 UNAVAILABLE` from a query means a buffer node the reader expected could not answer. The result would have been silently short. See [fan-out](architecture.md#clustered-fan-out).

A `503 UNAVAILABLE` from a `/v1/connections` route means this node holds no `SMOLQUERY_CREDENTIAL_KEY`, so it can neither seal a new password nor open a stored one. Set the key and restart the node. A `422 FAILED_PRECONDITION` on the same routes means the stored password does not open with the key this node holds — the key changed, and the connection must be registered again.
