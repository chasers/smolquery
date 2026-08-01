# smolquery

An open source BigQuery alternative, powered by DuckDB and Elixir.

Datasets, tables, async query jobs, and streaming inserts over an HTTP API —
in one self-hostable BEAM release.

> **Status: pre-alpha.** The read engine, segment writer, catalog, and the
> buffer service's hot tier work; no sealing, ingest HTTP, or query API yet.
> Plans and milestones live in the project tracker — see
> [`CONTRIBUTING.md`](CONTRIBUTING.md). Everything below is subject to change.

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

There are two result contracts, chosen by how big the answer might be.
`query/3` returns a `Smolquery.Engine.Result` — ordered column names plus row lists
of plain Elixir terms, so callers never see Arrow or ADBC types. That conversion
costs about a kilobyte and 2 µs per row, which is free for the queries the system
asks itself and ruinous for a user query matching millions of rows, so `query/3`
refuses a result over `:max_result_rows` (default 100 000) instead of spending the
heap.

`frame/3` is the read path for results nobody sized in advance. DuckDB's Arrow
stream goes straight to Polars in Rust, so no row becomes an Elixir term, and the
frame serializes to Parquet or Arrow IPC from Rust as well:

```elixir
{:ok, frame} = Smolquery.Engine.frame(MyEngine, "SELECT * FROM lake.analytics.events")
{:ok, parquet} = Explorer.DataFrame.dump_parquet(frame)
```

Five million rows take 307 ms and no measurable heap that way, against 11.5 s and
4.8 GiB through `query/3` — see `bench/adbc.exs`.

### Segments and the catalog

The storage of record: write-once Parquet segments plus a DuckLake catalog.
`Smolquery.Segments.Writer` encodes rows through Explorer (Polars), naming each
segment with a ULID; a `Smolquery.Segments.Store` commits the bytes and owns what
"durable" means. `Smolquery.Catalog` registers those files without copying them:

```elixir
schema = Smolquery.Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
store = Smolquery.Segments.Store.Local.new(dir: "priv/data/segments")

{:ok, segment} = Smolquery.Segments.Writer.write(rows, schema, store: store)

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

- **The store owns durability, and it is swappable.** `Store.put/3` returns only
  once the segment is durable *by that store's definition*, so everything above it
  — including the buffer service's "acked means durable" promise — is written
  against one contract and never names a storage medium. `Store.Local` fsyncs the
  file and renames it into place, both under one root, so a reader sees either
  nothing or a complete segment. It fsyncs contents, not the directory entry
  (Erlang cannot fsync a directory without a NIF): an acked segment survives a
  process, BEAM, or node crash, and a hard power cut can lose the rename — a
  strictly smaller window than this store's single-copy exposure to losing the
  disk. `Store.shared?/1` says whether a segment's location means anything from
  another node, which is what decides whether it needs serving over HTTP.
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

### The hot tier

`Smolquery.BufferService` owns the promise the rest of the system leans on: rows
are durable when, and only when, the owning buffer node has persisted them.
Writes go through one client module and come back acked:

```elixir
batch = %{schema: schema, rows: [%{"id" => 1, "ts" => ~N[2026-07-31 12:00:00]}]}

{:ok, ack} =
  Smolquery.BufferService.Client.write_batch(
    Smolquery.BufferService,
    {"analytics", "events"},
    batch
  )
#=> {:ok, %{segment_id: "01K...", row_count: 1}}

{:ok, entries} =
  Smolquery.BufferService.Client.hot_manifest(Smolquery.BufferService, {"analytics", "events"})
```

What that ack means:

- **One event makes rows durable *and* queryable.** Batches group-commit into a
  micro-segment; the ack is withheld until the segment is in the store and its
  entry is fsynced into the table's manifest log. There is no second mechanism
  for read-your-writes — the manifest a query plans against is the thing that
  made the rows durable.
- **The manifest log is the authority, not the directory.** On restart a table's
  buffer replays its log and reconciles against the store: a segment with no log
  record was never acked and is deleted (adopting it would double-count a
  client's retry), and a record whose segment is gone is dropped.
- **Backpressure is immediate.** A batch that would exceed `max_buffered_rows` or
  `max_buffered_bytes` gets `{:error, :buffer_full}` rather than being queued, for
  the ingest edge to turn into a 429.
- **One table, one node.** `Smolquery.BufferService.Ring` maps a table to its
  owning buffer node by consistent hashing; a table this node does not own is
  refused with `{:error, {:not_owner, node}}`. Milestone 3 runs a single-node
  ring.
- **The loss window is honest.** With the default local store, a buffer node's
  disk holds a single copy of its unsealed tail: acked rows survive a process,
  BEAM, or node crash, and losing the disk loses that tail. Sealing (Milestone 4)
  is what bounds it.

Calls reach the owning node through a transport seam rather than a hardcoded hop.
`Client` resolves ownership and dispatches; `Transport.Local` is a direct call for
a table this node owns, and `Transport.GenRpc` carries the rest over `:gen_rpc`:

```elixir
config :gen_rpc,
  tcp_server_port: 5369,
  tcp_client_port: 5369,
  rpc_module_control: :whitelist,
  rpc_module_list: [Smolquery.BufferService.Endpoint]
```

- **Not Erlang distribution.** Forward-batches are large and constant; on the
  cluster's single distribution connection they would sit in front of heartbeats
  and monitors, and a `busy_dist_port` stall looks to OTP like nodes going down.
  gen_rpc carries this traffic on its own sockets.
- **Bulk and control are separate channels.** gen_rpc keys one connection per
  destination, so routing everything at a node through one key just moves the
  head-of-line blocking onto that socket. Writes travel on `:bulk`, manifest reads
  and retirements on `:control`.
- **The allowlist is not optional.** gen_rpc's default `rpc_module_control` is
  `:disabled`, which lets any peer holding the cookie run arbitrary MFAs on a
  second port. Naming `Endpoint` — the single module every transport dispatches
  to — closes that. Per-node TLS is the other half, and belongs to the cluster
  milestone.
- **Ownership routes, it does not refuse.** A call for a table this node does not
  own is forwarded, and `Routing` answers ownership from configuration even on a
  node running no buffer at all — which is what lets a query node ask a buffer
  node for a hot manifest.

Segment *bytes* deliberately do not travel this way: DuckDB opens them itself via
`httpfs`, reading only the column chunks a query needs. Pulling them through RPC
would put every segment on the BEAM heap and lose that pushdown.

`Smolquery.BufferService.HotServer` is what DuckDB opens them from — a Bandit
listener, one per instance, serving two unauthenticated routes:

```
GET /v1/datasets/:dataset/tables/:table/manifest                  # JSON entries
GET /v1/datasets/:dataset/tables/:table/segments/:id.parquet      # segment bytes
```

- **A manifest entry's `url` is built from the request that fetched it**, not
  from static configuration — the same response is correct whether this bound
  its configured port or an OS-assigned one, and whether it sits behind a proxy
  or not. An entry backed by a *shared* store (one where
  `Smolquery.Segments.Store.shared?/1` is `true`, e.g. a future S3 hot tier)
  reports that store's own location instead, and never round-trips through this
  route at all.
- **A segment id is validated and resolved through the manifest**, never by
  joining request input into a path — `Smolquery.Segments.Id.valid?/1` rejects
  anything that isn't a well-formed ULID before it gets near the filesystem.
- **Both routes answer `HEAD` as well as `GET`**, because `httpfs` sends a `HEAD`
  first to learn a segment's size before issuing the ranged reads Parquet's
  footer-first format needs.
- **Auth is Milestone 6's job**, over `CREATE SECRET` headers DuckDB attaches to
  `httpfs` requests.

The other half of the hot tier is handing data off. A table that crosses
`seal_max_bytes`, `seal_max_files`, or `seal_max_age_ms` signals a configured
consumer, and a sealer stamps the segments it committed:

```elixir
config :smolquery, Smolquery.BufferService, seal_consumer: {MyApp.Sealer, []}

:ok = Smolquery.BufferService.Client.retire(Smolquery.BufferService, table, ids, snapshot)
```

- **What gets signalled is a frozen claim, not the tail as it stands.** Crossing a
  threshold first writes a `claim` record to the manifest log — the micro-segment
  ids, plus the key of the sealed segment they will become, derived from those ids
  — and only then signals. Rows written afterwards wait for the next claim. A
  sealer therefore merges the same inputs into the same output no matter how many
  times it is told or which side crashed, which is what makes retrying safe
  instead of duplicating rows.
- **The claim is how a query planner dedups, exactly.** Each manifest entry
  carries its claim's `claim_keys`, so at catalog snapshot `S` the rule is:
  include a micro-segment unless its claim's keys are all in the catalog's segment
  list at `S`. Since the commit that adds those keys is atomic, a micro-segment
  stops counting at the instant its rows start counting — there is no window where
  both tiers hold them, which snapshot-stamp comparison could not achieve.
- **Retiring one member of a claim retires all of them**, because the sealed
  segment holds every input's rows. Stamping only some would leave the rest
  claimed but unsealed, and the next re-signal would rebuild that claim's segment
  from a subset — overwriting a committed segment with fewer rows.
- **The signal is level-triggered, not an event.** It repeats every
  `seal_retry_ms` until the claim is retired, so a sealer that dies mid-handoff
  costs a retry interval rather than leaving that table's tail parked forever.
  Consumers should expect repeats, and can rely on them being identical.
- **Retirement is a stamp, not a delete.** `retire/4` records the catalog snapshot
  the sealer committed at and leaves the segments readable, because a query
  planned at an older snapshot is still entitled to them. Deletion happens
  `retire_grace_ms` later, which must exceed the longest query a planner can hold
  open. Retiring an already-sealed id, or one the sweep has already deleted, is
  `:ok` — every direction a crashed sealer retries from.
- **Boot adopts what is already on disk.** A buffer is what runs the seal check,
  so a node restarting with an unsealed tail for a table nobody writes to again
  would strand it. `Smolquery.BufferService.Adopter` starts a buffer for every
  owned table with a manifest log, and does it before the subtree reports started
  — a query arriving mid-adoption would otherwise read an empty manifest and
  quietly return results missing that table's unsealed rows.

### The sealed tier

`Smolquery.StorageService` owns what happens after the hot tier: merge a table's
micro-segments into large sealed segments, commit them to the catalog, retire the
inputs. Started by the `:storage` role, it is where seal signals now go —

```elixir
config :smolquery, Smolquery.BufferService,
  seal_consumer: {Smolquery.StorageService.Client, []}
```

— and the buffer service still names no storage module of its own, which is why
that wiring is configuration. So far the scheduling half is built:

- **One seal in flight per table, a bounded pool per node.**
  `Smolquery.StorageService.Sealer` coalesces a signal for a table it is already
  sealing and sheds one arriving at `max_concurrent_seals`. Both are safe because
  signalling is level-triggered — a dropped signal costs a `seal_retry_ms` delay,
  never a lost seal, so the sealer needs no queue of its own.
- **An attempt runs as a monitored task, not a linked one.** A merge that crashes
  frees its table and leaves the sealer and its siblings alone; the next re-signal
  retries it.
- **Signalling a node that runs no storage service is reported, not raised.**
  Raising would take down the `TableBuffer` that signalled, and it signals from
  the write path.
- **Sealed segments get their own store handle** (`dir: "priv/data/sealed"`),
  separate from the buffer's. The two tiers have opposite write profiles — one put
  per flush against one per seal — and that difference is what makes an object
  store plausible here long before it is for the hot tier.

What a seal attempt *does* is `Smolquery.StorageService.Handoff`, and it is not
implemented yet: the default reports `{:error, :not_implemented}` and logs, so a
storage node accepts and schedules signals but seals nothing, visibly. It already
receives the frozen claim, though — the ids to merge and the key to write them to —
so the merge, catalog commit, and retirement are what remain.

## Roles

One release, four services; a node starts only the subtrees its roles name.
`SMOLQUERY_ROLES` is a comma-separated list, or `all`:

```sh
SMOLQUERY_ROLES=all                # default when unset — single-node dev
SMOLQUERY_ROLES=query              # a query-only node
SMOLQUERY_ROLES=ingest,buffer
```

Unknown role names fail the boot rather than silently starting nothing. `:query`,
`:buffer`, and `:storage` start their subtrees today; `:ingest` is accepted and
contributes nothing until its milestone lands. See `Smolquery.Roles`.

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

config :smolquery, Smolquery.BufferService,
  dir: "priv/data/buffer",
  flush_interval_ms: 1_000,
  flush_max_rows: 100_000,
  flush_max_bytes: 8_000_000,
  max_buffered_rows: 500_000,
  max_buffered_bytes: 64_000_000,
  write_timeout_ms: 15_000,
  seal_max_bytes: 67_108_864,
  seal_max_files: 64,
  seal_max_age_ms: 60_000,
  seal_retry_ms: 30_000,
  retire_grace_ms: 600_000,
  maintenance_interval_ms: 5_000,
  seal_consumer: {Smolquery.StorageService.Client, []},
  hot_server_ip: {127, 0, 0, 1},
  hot_server_port: 4001

config :smolquery, Smolquery.StorageService,
  dir: "priv/data/sealed",
  buffer_base_url: "http://127.0.0.1:4001",
  target_segment_bytes: 268_435_456,
  max_concurrent_seals: 2,
  gc_interval_ms: 300_000,
  gc_grace_ms: 3_600_000,
  handoff: {Smolquery.StorageService.Handoff.Log, []}
```

`:dir` is the buffer's root: micro-segments go to a `Store.Local` beneath
`segments/`, manifest logs to `manifests/`. They are separate because they answer
to different rules — segments can move to another store, while the log stays on
the node that gave the ack. Point the segments elsewhere with
`store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}`.

`flush_interval_ms` is the ack-latency dial: a batch waits out the remainder of
the current group commit, so lowering it trades throughput for latency.

The storage service's `:dir` is where sealed segments land, and `:store`
overrides it the same way. `buffer_base_url` is where the sealer reaches
`HotServer` to pull manifests and segment bytes — honest for a single node, and
replaced by ownership-ring lookup when the cluster arrives.

Runtime environment variables:

| variable | effect |
|---|---|
| `SMOLQUERY_ROLES` | which service subtrees start (`all` or a comma-separated list) |
| `SMOLQUERY_MEMORY_LIMIT` | DuckDB memory limit per engine |
| `SMOLQUERY_DATA_DIR` | data directory; the catalog and DuckLake data path derive from it |
| `SMOLQUERY_CATALOG` | catalog metadata database, e.g. `postgres:dbname=smolquery host=…` |
| `SMOLQUERY_MAX_RESULT_ROWS` | ceiling on rows `Engine.query/3` converts to Elixir terms (`infinity` to disable) |
| `SMOLQUERY_BUFFER_DIR` | buffer service root for micro-segments and manifest logs |
| `SMOLQUERY_FLUSH_INTERVAL_MS` | group-commit cadence, and so the ack-latency bound |
| `SMOLQUERY_HOT_SERVER_PORT` | port `HotServer` binds to serve micro-segments over `httpfs` |
| `SMOLQUERY_SEALED_DIR` | storage service root for sealed segments |
| `SMOLQUERY_BUFFER_BASE_URL` | `HotServer` base URL the sealer pulls micro-segments from |

Engine options can also be passed per instance to
`Smolquery.Engine.start_link/1`, which overrides the application config.

## Development

Toolchain versions are pinned in `.tool-versions`, matching CI — OTP 29.0.2 /
Elixir 1.20.2.

```sh
mise install     # or asdf install
mix deps.get
mix test         # fast suite; add --include integration for everything
mix precommit    # format + full local quality gate before committing
```

Integration-tagged tests are excluded by default: they download DuckDB
extensions and serve Parquet over a real HTTP server. CI runs them in a
dedicated job (`mix test --only integration`).

### Benchmarks

`bench/` holds the measurements architectural decisions were made on, so they can
be re-run rather than re-argued — after a DuckDB or DuckLake upgrade, or before
revisiting the decision they settled.

```sh
mix run bench/planner.exs                         # scan DuckLake, or plan around it?
mix run bench/adbc.exs                            # what ADBC costs to connect, fetch, and share
mix run bench/buffer.exs                          # what group commit costs, and where it bends

SEGMENTS=1500 ROWS=2000 mix run bench/planner.exs # bigger catalog, smaller segments
ROWS=10000000 CLIENTS=16 mix run bench/adbc.exs   # push the fetch and concurrency sizes
CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs # more samples, more concurrency
```

`bench/buffer.exs` reports batches/s, rows/s, MB/s, and p50/p95/p99 ack latency
across batch size × writers × tables, sweeps `flush_interval_ms`, prices the two
fsyncs behind an ack (D3), and probes the one-table inline-flush ceiling (D6).
Its other knobs: `MAX_BATCH`, `MAX_TABLES`, `WRITERS`, `BATCH`.

Each script's `@moduledoc` records what it measures and what it concluded. Two
results worth knowing before writing a read path:

- **A large result must not come back as Elixir terms.** `Smolquery.Engine.Result`
  converts Arrow row by row, which costs roughly a kilobyte and 2 µs per row — 5M
  rows take 11.5 s and 4.8 GiB, against 307 ms and 8.7 MiB left in Arrow. It is
  the right shape for catalog and control-plane queries, and a trap for user
  results, which is what `Engine.frame/3` and the `:max_result_rows` ceiling are
  for.
- **One connection serializes.** `Engine.Connection` is a per-query mutex, so
  query throughput is flat in client count. Eight connections serve eight
  concurrent clients about 2.9× faster than one does.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for quality gates and the project
tracker, and [`AGENTS.md`](AGENTS.md) for codebase tooling.
