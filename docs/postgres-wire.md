# Postgres wire protocol

smolquery speaks the Postgres wire protocol on its own listener, so a
Postgres client runs `SELECT` queries against it without the HTTP API. The
`:pg` role starts the edge (`SmolqueryPg`). Plan: PL-58.

```sh
psql "host=127.0.0.1 port=5432 user=smolquery password=$SMOLQUERY_API_KEY"

smolquery=> SELECT count(*) AS n FROM analytics.events;
```

## What works today (layers 1 to 5)

- **Startup and SCRAM-SHA-256 auth.** The password is the API key
  (`SMOLQUERY_API_KEY`), or `SMOLQUERY_PG_PASSWORD` when set — and with
  SCRAM (the default) it never crosses the wire: the client proves it,
  and the server proves it back. `SMOLQUERY_PG_AUTH=cleartext` restores
  the plain password message for a legacy client. The user and database
  names are accepted as given.
- **TLS.** With `SMOLQUERY_PG_TLS_CERT` and `SMOLQUERY_PG_TLS_KEY` set,
  `SSLRequest` answers `S` and the connection upgrades before the
  startup packet. Without them the edge declines with `N` and the session
  stays plaintext — bind loopback, or terminate TLS in front, before
  exposing the listener. Channel binding is not offered; `psql` connects
  with its default `channel_binding=prefer`.
- **The simple query protocol.** `psql` and any client that sends `Query`
  messages. A message may carry several statements; they run in order and
  the first error stops the rest.
- **`SELECT` in text format.** The query runs through the same
  `Smolquery.QueryService` job an HTTP query does: the same planner, the
  same two-tier view, the same result cap (`result_max_rows`, 10,000 rows;
  a larger result answers `54000`). The same table references apply:
  `dataset.table`, or a registered connection's `catalog.schema.table`.
- **Session statements.** `SET`, `RESET`, and `SHOW` keep values in the
  session. `SET statement_timeout = <ms>` bounds each query (Postgres
  units — `30s`, `5min`, `1h` — are accepted and stored as ms). `BEGIN`,
  `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, and `ABORT` track the
  status the prompt shows. A failed statement inside a block aborts the
  block (`25P02`) until it ends, as Postgres does.
- **The extended query protocol.** `Parse`, `Bind`, `Describe`, `Execute`,
  `Close`, `Flush`, `Sync`; named prepared statements and portals; an
  `Execute` row limit answers `PortalSuspended` and a later `Execute`
  continues the portal. Results in binary format on request, every type.
- **Parameters.** A `Bind` value is decoded by its declared OID and
  format (`SmolqueryPg.Params`) and bound as an engine parameter through
  the query service (T-410): the SQL keeps its `$n`, DuckDB prepares it,
  and no client value is ever SQL text. The pruner and the Top-N bound
  read the bound values where the `WHERE` names a `$n`, so a
  driver-shaped time-range query keeps its hot-tier optimisations. The
  edge infers no types: an undeclared parameter is `text`, and a cast the
  client writes (`$1::bigint`) is read as its declaration. A `NULL` binds
  typed by its declared OID, so `1 + $1` stays arithmetic. A `json` value
  binds as text, and a `JSON` expression is described as `text` too: the
  wire has no JSON type, as the [API](api.md) has none (`VARIANT` covers
  it). Parameters bind in a `SELECT`, an `EXPLAIN`, and a
  `DECLARE ... CURSOR FOR`; any other statement class with parameters
  answers `0A000`.
- **`Describe` before `Bind`.** A statement with parameters is described
  through the query service's `describe` mode — the planner plans it for
  real, and DuckDB's `DESCRIBE` names the columns without running the
  query. A statement without parameters runs once, and the portal that
  binds it serves the same rows: one job per driver-shaped query.
- **Cancellation.** `BackendKeyData` is real: a `CancelRequest` quoting it
  cancels the session's running job (`57014`).
- **`pg_catalog`.** A statement whose tables all live in `pg_catalog` or
  `information_schema` runs in an emulation engine (`SmolqueryPg.PgCatalog`)
  instead of the query service: a static `pg_type`/`pg_range`/`pg_collation`
  snapshot from a real Postgres — so the OIDs drivers key on are the real
  ones — plus `pg_namespace`, `pg_class`, and `pg_attribute` generated from
  the smolquery catalog, the neighbouring catalogs as empty tables, and the
  functions the client corpus calls as macros. Postgrex connects and its
  type bootstrap answers; `psql`'s `\dn`, `\dt`, `\d table` work;
  `SELECT version()` says PostgreSQL. `information_schema` has the breadth
  a BI tool's schema sync reads (T-412): `tables`, `schemata`, `columns`
  with `udt_name`, precision, scale, and the identity and generation
  columns, `table_privileges`, and — empty, since smolquery has none of
  them — `views`, `table_constraints`, `key_column_usage`,
  `referential_constraints`, `constraint_column_usage`, `sequences`,
  `routines`, `parameters`; `pg_tables`, `pg_views`, `pg_indexes`,
  `pg_stat_user_tables`, `pg_settings`, and the `has_*_privilege`,
  `pg_get_viewdef`, `txid_current`, `to_regclass`, and
  `information_schema._pg_expandarray` functions answer what pgjdbc
  (Metabase, DBeaver) and psqlODBC (Tableau) send on connect and on sync.
  A dialect rewrite bridges the rest:
  Postgres's `~` operators become `regexp_matches`, `pg_catalog.`
  qualifications drop, a name literal cast to `regclass` or `regtype`
  becomes the catalog `oid` lookup it means, other `reg*` casts become
  `BIGINT`, and
  `current_setting('x')` inlines the session's value.

- **`postgres_fdw`.** A Postgres database attaches smolquery as a foreign
  server. `IMPORT FOREIGN SCHEMA <dataset>` builds the foreign tables with
  the right types; scans, joins, and aggregates run through cursors
  (`DECLARE`/`FETCH`/`MOVE`/`CLOSE`), inside the fdw's `REPEATABLE READ`
  block with its savepoints; `EXPLAIN` answers one `Foreign Scan` cost
  line with the plan's real row estimate, for `use_remote_estimate`.
  `DEALLOCATE ALL` and `DISCARD ALL` reset a pooled connection.

  ```sql
  CREATE SERVER smol FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'smolquery-host', port '5432', dbname 'smolquery');
  CREATE USER MAPPING FOR CURRENT_USER SERVER smol
    OPTIONS (user 'smolquery', password '<the API key>');
  IMPORT FOREIGN SCHEMA analytics FROM SERVER smol INTO analytics;
  SELECT count(*) FROM analytics.events;
  ```

  One caveat: a scan is bounded by `result_max_rows` (10,000) — the whole
  result materializes at `DECLARE`, and a larger one fails with `54000`;
  push the aggregation down, or raise `SMOLQUERY_MAX_RESULT_ROWS`. The
  fdw's `REPEATABLE READ` block is honored: every cursor in it reads the
  block's pinned snapshot.

Reads only. DDL, DML, and `COPY` answer `0A000 feature_not_supported`.

## Not yet

- Result streaming past `result_max_rows` (T-411): a `FETCH` loop could
  page an unbounded result; today the cap applies at `DECLARE`.

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

## Transactions pin their read

A `REPEATABLE READ` or `SERIALIZABLE` block gives repeatable reads: the
block's first query captures the catalog snapshot it ran at and a
hot-tier time bound, each table's first touch captures the exact set of
micro-segment ids that query read, and every later statement in the
block — the several cursors of a `postgres_fdw` join included, and a
nested-loop rescan of one — reads the same data. The pin forms lazily at
the first query, as Postgres does, and clears when the block ends. The
data is append-only, so this closes the per-statement anomaly a block
could see: rows inserted mid-block appear only after `COMMIT`, per
table from its first touch. A
plain `BEGIN` is `READ COMMITTED`, as in Postgres, and reads fresh per
statement; `SHOW transaction_isolation` reports the block's level.

The block surface is honest about what it does not do. Explicit
`READ WRITE` (in `BEGIN` or `SET TRANSACTION`) answers `25006` — the
server is read-only. Two-phase commit (`COMMIT PREPARED`) answers
`0A000`. `SET LOCAL` reverts at block end; savepoint names are tracked,
so `RELEASE` or `ROLLBACK TO` an unknown one answers `3B001`, and a
savepoint outside a block answers `25P01`. `SET TRANSACTION ISOLATION
LEVEL` works until the block's first query, then answers `25001`.

Two bounds protect the pin. `idle_in_transaction_session_timeout`
(default 5 minutes, `SMOLQUERY_PG_IDLE_TXN_TIMEOUT_MS`) terminates a
connection that sits idle inside a block, with Postgres's own
`FATAL 25P03`. A session may `SET` it lower, never higher than the
server's bound, and cannot disable it — the timeout guards the server's
pinned snapshots, so a raise past the cap answers a warning and stores
the cap. And a block older than `SMOLQUERY_SNAPSHOT_KEEP_MS`
(24 hours) can lose its snapshot to expiry — that surfaces as a query
error, never as wrong rows.

A block also has a lifetime: the query service's
`SMOLQUERY_HOT_PIN_MAX_AGE_MS` (5 minutes). A micro-segment the block
pinned is reaped from the hot tier `retire_grace_ms` (10 minutes) after
a sealer retires it, and a read past that point could come back short
with no error — so the lifetime must sit below the grace, and a block
older than it answers `72000 snapshot_too_old` on its next statement.
The idle timeout re-arms per statement, so it bounds idle gaps, not
block length; the lifetime is what bounds the block. A pinned segment
gone from the hot tier inside the lifetime answers the same `72000` —
never a silent re-read of the rows from the sealed tier, which would not
be the same read. `EXPLAIN` inside a block pins and reads the pin like a
query, since `postgres_fdw` estimates before it scans. Only a table's
first touch keys on micro-segment ULID timestamps, where cross-node
clock skew is the precision; from then on the block reads that table by
id.

## Security

SCRAM keeps the password off the wire; TLS keeps the rows off it too. The
listener binds loopback by default (`SMOLQUERY_PG_IP`); expose it beyond
the node only with `SMOLQUERY_PG_TLS_CERT`/`_KEY` set, or behind a TLS
terminator. The `ErrorResponse` for a wrong password names the user,
never the password.

The edge holds a few more lines. The SCRAM verifier derives once at boot,
so an unauthenticated reconnect storm cannot buy PBKDF2 CPU with garbage
proofs. The password normalizes to NFKC before the derivation — the core
of SASLprep, which libpq applies client-side; the prohibited-character
tables are not enforced. Bytes pipelined behind an `SSLRequest` are
refused rather than carried across the upgrade, and a connection that has
not authenticated within 30 seconds (`auth_timeout_ms`) is closed, TLS
handshake included. Certificate and key files are read and PEM-checked at
boot, so a typo'd path fails the node instead of every client.

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
| `SMOLQUERY_PG_AUTH` | `scram-sha-256` (default) or `cleartext` |
| `SMOLQUERY_PG_IDLE_TXN_TIMEOUT_MS` | Terminate a connection idle inside a transaction block (`300000`; `0` disables) |
| `SMOLQUERY_PG_TLS_CERT` / `SMOLQUERY_PG_TLS_KEY` | PEM certificate and key; set both to accept `SSLRequest` |
| `SMOLQUERY_PG_IP` / `SMOLQUERY_PG_PORT` | The bind address and port (`127.0.0.1` / `5432`) |

In `dev`, the port is `15432`: a developer machine often runs its own
Postgres on `5432`, and a bound port fails the whole node's boot.

The role is `pg` in `SMOLQUERY_ROLES`. A node with the role and no
password refuses to boot. The edge sends queries to the node's own
`Smolquery.QueryService`, so the node needs the `query` role too.
