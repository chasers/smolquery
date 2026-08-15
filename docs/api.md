# HTTP API v1

`SmolqueryApi` is the front door — a Phoenix endpoint served by Bandit (the
same stack as the web UI's `SmolqueryWeb`), started by the `:api` role, routing
only to service client modules and the catalog (the same boundary rule the
services hold each other to). Set `SMOLQUERY_AUTH_MODE=static` while OIDC
runtime support is unavailable. Every `/v1` route then requires the static
Bearer key (`SMOLQUERY_API_KEY`); a node with the `:api` role and no mode or key
configured fails the boot rather than serve an open API. Successful static
requests carry a normalized service principal and context. `/healthz` is the
one unauthenticated route.

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
| `GET /metrics` | Prometheus text, gated by the *internal* secret (`x-smolquery-internal`), not the API key — metrics are for operators, not tenants |
| `GET /v1/datasets` | list datasets |
| `POST /v1/datasets` | create a dataset (idempotent) |
| `GET /v1/datasets/:ds/tables` | list a dataset's tables |
| `POST /v1/datasets/:ds/tables` | create a table — re-creating with the same schema is a 200, with a different one a 409, never a silent no-op |
| `GET /v1/datasets/:ds/tables/:t` | a table's schema, retention policy, and clustering key |
| `PATCH /v1/datasets/:ds/tables/:t` | set or clear retention and/or clustering. Retention: `{"retention": {"column": "ts", "ttlMs": 2592000000}}` ages rows out of `ts` after 30 days, segment-grained and conservative (a segment is dropped only once *every* row in it has aged out); `{"retention": null}` keeps rows forever again. Clustering: `{"clustering": ["project_id", "ts"]}` sorts future writes by those columns (ClickHouse `ORDER BY` analog); `{"clustering": []}` clears it. Columns must exist on the schema (unknown names are 422). Changing clustering does not rewrite existing segments. A body carrying both fields applies them atomically — an error response means neither changed |
| `POST /v1/datasets/:ds/tables/:t/insert` | streaming insert — **`application/x-ndjson` only**, one JSON object per line, the same bytes ClickHouse takes as `JSONEachRow`; `insertId` is a query parameter. A JSON-array body is a 415: two content types were two ingest paths and the array one measured 3-4x slower with nothing announcing which you got. A 200 means the buffer service has every accepted row durable and queryable; rejected rows come back per-index in `insertErrors` (partial failure is a 200, BigQuery-style); a full or overloaded buffer is a 429 whose `retry-after` says how far behind the write path is. An optional `insertId` makes the request idempotent: retrying after a timeout or dropped response with the same id (and the same rows) cannot double-count — without one, retries are at-least-once |
| `POST /v1/datasets/:ds/tables/:t/load` | batch load — the body is the file (`application/x-ndjson`, `text/csv`, or `application/vnd.apache.parquet`), pushed through the same insert path in chunks; capped by `load_max_bytes` (413 past it), synchronous, and not atomic — a mid-load failure reports what was already durable, and unlike `/insert` it takes no `insertId`, so a retry re-inserts. Two measured caveats ([benchmarks](benchmarks.md)): the body spools to disk but the parser materializes every row, so a load peaks at **~10× the file in memory**; and the cap is in *bytes*, which at 61 columns is ~120k rows of NDJSON but ~254k of CSV. It is also **not** the fast path — concurrent `/insert` is 2.4× quicker |
| `POST /v1/queries` | sync query — the finished job plus its first page of rows (`maxResults`, default 1000); a query that outlives `timeoutMs` is cancelled and answered 504 |
| `POST /v1/jobs` | the same query as an async job — returns it pending |
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
inserts accept.

## Errors

Failures answer one JSON envelope everywhere:

```json
{"error": {"code": 401, "status": "UNAUTHENTICATED", "message": "missing or invalid API key"}}
```

A `503 UNAVAILABLE` from a query means a buffer node the reader expected could
not answer, so the result would have been silently short — see
[fan-out](architecture.md#clustered-fan-out).
