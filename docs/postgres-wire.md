# Postgres wire protocol

smolquery speaks the Postgres wire protocol on its own listener, so a
Postgres client runs `SELECT` queries against it without the HTTP API. The
`:pg` role starts the edge (`SmolqueryPg`). Plan: PL-58.

```sh
psql "host=127.0.0.1 port=5432 user=smolquery password=$SMOLQUERY_API_KEY"

smolquery=> SELECT count(*) AS n FROM analytics.events;
```

## What works today (layer 1)

- **Startup and cleartext password auth.** The password is the API key
  (`SMOLQUERY_API_KEY`), or `SMOLQUERY_PG_PASSWORD` when set. The user and
  database names are accepted as given. The edge declines `SSLRequest` and
  `GSSENCRequest` with `N`; the client then continues in plaintext.
- **The simple query protocol.** `psql` and any client that sends `Query`
  messages. A message may carry several statements; they run in order and
  the first error stops the rest.
- **`SELECT` in text format.** The query runs through the same
  `Smolquery.QueryService` job an HTTP query does: the same planner, the
  same two-tier view, the same result cap (`result_max_rows`, 10,000 rows;
  a larger result answers `54000`). The same table references apply:
  `dataset.table`, or a registered connection's `catalog.schema.table`.
- **Session statements.** `SET`, `RESET`, and `SHOW` keep values in the
  session. `SET statement_timeout = <ms>` bounds each query. `BEGIN`,
  `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, and `ABORT` track the
  status the prompt shows. A failed statement inside a block aborts the
  block (`25P02`) until it ends, as Postgres does.

Reads only. DDL, DML, and `COPY` answer `0A000 feature_not_supported`.

## Not yet

Each is a layer of PL-58, in order:

- The extended query protocol (`Parse`/`Bind`/`Execute`) and binary
  results. Drivers that use it — Postgrex, psycopg, JDBC, pgx — connect but
  cannot run a query yet: the edge answers `0A000` and resynchronises on
  `Sync`.
- `pg_catalog` and `information_schema`. `psql`'s `\d` commands do not
  answer yet.
- Cursors, `EXPLAIN`, and `postgres_fdw`.
- SCRAM-SHA-256 and TLS.

## Types

| smolquery | Postgres | text form |
|---|---|---|
| `INT64` | `bigint` | `42` |
| `FLOAT64` | `double precision` | `1.5`, `NaN`, `Infinity` |
| `STRING` | `text` | as is |
| `BOOL` | `boolean` | `t` / `f` |
| `TIMESTAMP` | `timestamp` | `2026-08-01 10:00:00` |
| `DATE` | `date` | `2026-08-01` |
| `NUMERIC(p,s)` | `numeric(p,s)` | `12.50` |
| `MAP(STRING, STRING)` | `jsonb` | `{"host":"a"}` |
| `VARIANT` | `jsonb` | the JSON value |

A computed list or struct also arrives as `jsonb`. A smaller integer
arrives as `bigint`.

## Transactions pin nothing

A transaction block is status only. Each statement reads its own catalog
snapshot, exactly as an HTTP query does. Two `SELECT`s inside one `BEGIN`
can see different snapshots if a seal lands between them.

## Security

The password crosses the wire in cleartext until layer 5 adds SCRAM and
TLS. The listener binds loopback by default (`SMOLQUERY_PG_IP`). Expose it
beyond the node only behind a TLS terminator, or on a private network. The
`ErrorResponse` for a wrong password names the user, never the password.

Every SQL string is untrusted, the same as on the HTTP API: the planner's
gate and the job engine's lockdown apply unchanged.

## Errors

Every failure is an `ErrorResponse` with a SQLSTATE a client can act on:

| code | when |
|---|---|
| `28P01` | wrong password |
| `42601` | the query does not parse, or is not one `SELECT` |
| `42P01` | an unknown `dataset.table` |
| `54000` | the result exceeds `result_max_rows` |
| `57014` | the statement timeout cancelled the query |
| `57P03` | the query service, or a buffer node it needs, is not available |
| `25P02` | a statement inside an aborted transaction block |
| `0A000` | a statement or a protocol message this layer does not serve |
| `XX000` | anything else; the message is the reason's inspected form |

A DuckDB error keeps its message and takes the code its class implies:
`Parser Error` is `42601`, `Catalog Error` is `42P01`, `Binder Error` is
`42000`, `Conversion Error` is `22000`.

## Configuration

| variable | effect (default) |
|---|---|
| `SMOLQUERY_PG_PASSWORD` | The password every client must present (the API key) |
| `SMOLQUERY_PG_IP` / `SMOLQUERY_PG_PORT` | The bind address and port (`127.0.0.1` / `5432`) |

The role is `pg` in `SMOLQUERY_ROLES`. A node with the role and no
password refuses to boot. The edge sends queries to the node's own
`Smolquery.QueryService`, so the node needs the `query` role too.
