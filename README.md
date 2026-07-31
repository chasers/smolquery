# smolquery

An open source BigQuery alternative, powered by DuckDB and Elixir.

Datasets, tables, async query jobs, and streaming inserts over an HTTP API —
in one self-hostable BEAM release.

> **Status: pre-alpha.** Foundation and role config only — no engine, ingest,
> storage, or HTTP API yet. Plans and milestones live in the project tracker —
> see [`CONTRIBUTING.md`](CONTRIBUTING.md). Everything below is subject to
> change.

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

## Roles

One release, four services; a node starts only the subtrees its roles name.
`SMOLQUERY_ROLES` is a comma-separated list, or `all`:

```sh
SMOLQUERY_ROLES=all                # default when unset — single-node dev
SMOLQUERY_ROLES=query              # a query-only node
SMOLQUERY_ROLES=ingest,buffer
```

Unknown role names fail the boot rather than silently starting nothing. Roles
whose services aren't implemented yet are accepted and contribute no subtree —
which is all four of them today. See `Smolquery.Roles`.

## Development

Requires Elixir ~> 1.20 (CI pins OTP 29.0.2 / Elixir 1.20.2).

```sh
mix deps.get
mix test         # fast suite; add --include integration for everything
mix precommit    # format + full local quality gate before committing
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for quality gates and the project
tracker, and [`AGENTS.md`](AGENTS.md) for codebase tooling.
