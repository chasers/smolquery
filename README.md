# smolquery

An open source BigQuery alternative, powered by DuckDB and Elixir.

Datasets, tables, async query jobs, and streaming inserts over an HTTP API —
in one self-hostable BEAM release.

> **Status: pre-alpha.** The read engine, segment writer, and catalog work;
> no ingest, sealing, or HTTP API yet. Plans and milestones live in the project
> tracker — see [`CONTRIBUTING.md`](CONTRIBUTING.md). Everything below is
> subject to change.

## Architecture (draft)

One Elixir app, four services, enabled per node by role config:

```
inserts → IngestService ──→ BufferService ──seal──→ StorageService
          (stateless)       (hot tier: durable,     (large Parquet → S3,
                             queryable Parquet       DuckLake catalog commit)
                             micro-segments)
queries → QueryService ──────────────────────────────────────────┘
          (DuckDB via ADBC: catalog ∪ hot tiers, one query plan)
```

- Writes never touch DuckDB: Elixir batches into immutable Parquet segments —
  small in (~1s group commit = durable + queryable ack), large out (few big
  files on object storage).
- DuckDB is a disposable, stateless read engine; storage of record is
  Parquet + a DuckLake catalog (SQLite dev → Postgres cluster).
- Only BufferService is stateful (seconds-to-minutes of unsealed data);
  everything else scales elastically.

## What works today

`Smolquery.Engine` — the DuckDB read engine, a supervised
`Adbc.Database` → `Adbc.Connection` subtree with extensions and session settings
applied before the connection is reachable:

```elixir
iex> Smolquery.Engine.query!(Smolquery.Engine, "SELECT $1::int + 1 AS n", [41])
%Smolquery.Engine.Result{columns: ["n"], rows: [[42]], num_rows: 1}
```

It reads Parquet segments from local disk and over HTTP (`httpfs`), and unions
both in a single plan — the shape a real query takes across the sealed and hot
tiers:

```sql
SELECT * FROM read_parquet('/segments/sealed.parquet')
UNION ALL
SELECT * FROM read_parquet('http://buffer-node:4000/hot.parquet')
```

Results come back as `Smolquery.Engine.Result` — ordered column names plus row
lists of plain Elixir terms — so callers never see Arrow or ADBC types.

### Segments and the catalog

The storage of record: write-once Parquet segments plus a DuckLake catalog.
`Smolquery.Segments.Writer` encodes rows through Explorer (Polars), naming each
segment with a ULID and renaming it into place so a reader never sees a partial
file. `Smolquery.Catalog` registers those files without copying them:

```elixir
schema = Smolquery.Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])

{:ok, segment} = Smolquery.Segments.Writer.write(rows, schema, dir: "priv/data/segments")

{:ok, _lake} =
  Smolquery.Catalog.DuckLake.start_link(
    name: MyLake,
    metadata: "sqlite:priv/data/catalog.sqlite",
    data_path: "priv/data/ducklake"
  )

catalog = Smolquery.Catalog.DuckLake.new(engine: MyLake)

:ok = Smolquery.Catalog.create_dataset(catalog, "analytics")
:ok = Smolquery.Catalog.create_table(catalog, {"analytics", "events"}, schema)
{:ok, snapshot} = Smolquery.Catalog.register_segments(catalog, {"analytics", "events"}, [segment])

Smolquery.Engine.query!(MyLake, ~s|SELECT count(*) FROM lake."analytics"."events"|)
```

Properties worth knowing:

- **Registration is idempotent.** `register_segments/3` diffs against the paths
  the catalog already holds, because DuckLake itself would happily register a
  path twice and double-count its rows. A sealer that crashed mid-handoff can
  retry safely.
- **Snapshots are the read contract.** Every mutation reports the snapshot it
  committed at, and `segments/3` lists a table's files as of a snapshot — the
  basis of exactly-once sealing.
- **Pruning is free, but only if the parameter types match.** DuckLake reads each
  Parquet footer at registration and keeps min-max stats, so a filtered query
  touches only the segments it must. A segment also carries its own stats for the
  hot tier. Pruning is lost silently — right rows, every file read — when a
  predicate compares mismatched types, so `Smolquery.Engine.Params` binds
  timestamps to the type the columns declare instead of letting ADBC infer one.
- **Compaction is not free.** `ducklake_merge_adjacent_files` crashes DuckDB on
  externally-registered files, so smolquery never calls it; compaction is built
  from `register_segments/3` + `drop_segments/3` instead.

Types map through one table in `Smolquery.Schema` — logical type ↔ Explorer
dtype ↔ DuckDB type (`:int64`/`{:s, 64}`/`BIGINT`, `{:numeric, p, s}` ↔
`DECIMAL(p,s)`, and so on).

## Roles

One release, four services; a node starts only the subtrees its roles name.
`SMOLQUERY_ROLES` is a comma-separated list, or `all`:

```sh
SMOLQUERY_ROLES=all                # default when unset — single-node dev
SMOLQUERY_ROLES=query              # a query-only node
SMOLQUERY_ROLES=ingest,buffer
```

Unknown role names fail the boot rather than silently starting nothing. Roles
whose services aren't implemented yet are accepted and contribute no subtree.
See `Smolquery.Roles`.

## Configuration

```elixir
config :smolquery, Smolquery.Engine,
  memory_limit: "2GB",
  threads: System.schedulers_online(),
  extensions: [:httpfs, :json]

config :smolquery, :data_dir, "priv/data"

config :smolquery, Smolquery.Catalog.DuckLake,
  metadata: "sqlite:priv/data/catalog.sqlite",
  data_path: "priv/data/ducklake"
```

Runtime environment variables:

| variable | effect |
|---|---|
| `SMOLQUERY_ROLES` | which service subtrees start (`all` or a comma-separated list) |
| `SMOLQUERY_MEMORY_LIMIT` | DuckDB memory limit per engine |
| `SMOLQUERY_DATA_DIR` | data directory; the catalog and DuckLake data path derive from it |
| `SMOLQUERY_CATALOG` | catalog metadata database, e.g. `postgres:dbname=smolquery host=…` |

Engine options can also be passed per instance to
`Smolquery.Engine.start_link/1`, which overrides the application config.

## Development

Requires Elixir ~> 1.20 (CI pins OTP 29.0.2 / Elixir 1.20.2).

```sh
mix deps.get
mix test         # fast suite; add --include integration for everything
mix precommit    # format + full local quality gate before committing
```

Integration-tagged tests are excluded by default: they download DuckDB
extensions and serve Parquet over a real HTTP server.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for quality gates and the project
tracker, and [`AGENTS.md`](AGENTS.md) for codebase tooling.
