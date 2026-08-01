# smolquery

An open source BigQuery alternative, powered by DuckDB and Elixir.

Datasets, tables, async query jobs, and streaming inserts over an HTTP API —
in one self-hostable BEAM release.

> **Status: pre-alpha.** The read engine, segment writer, catalog, the buffer
> service's hot tier, the seal handoff to the sealed tier, and query jobs
> planned across both tiers work. The HTTP API is landing (Milestone 6):
> today it serves `/healthz` behind Bearer-key auth; inserts, CRUD, and query
> endpoints arrive layer by layer (queries run through the Elixir client
> meanwhile).
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
{:ok, _pid} = Smolquery.Engine.start_link(name: MyEngine)

Smolquery.Engine.query!(MyEngine, "SELECT $1::int + 1 AS n", [41])
#=> %Smolquery.Engine.Result{columns: ["n"], rows: [[42]], num_rows: 1}
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
- **A claim that can never seal retries forever, and is counted while it does.**
  Bounding the retries would be worse — the buffer holds the only durable record of
  what wants sealing, so giving up strands a table's tail with nothing left to
  notice. `Sealer.failures/1` reports consecutive failed attempts per table, cleared
  by the first success, and the log escalates from a warning to an error once a
  table stops looking transient. Distinguishing "retrying" from "stuck" is the part
  that was missing; stopping is not.
- **Signalling a node that runs no storage service is reported, not raised.**
  Raising would take down the `TableBuffer` that signalled, and it signals from
  the write path.
- **Sealed segments get their own store handle** (`dir: "priv/data/sealed"`),
  separate from the buffer's. The two tiers have opposite write profiles — one put
  per flush against one per seal — and that difference is what makes an object
  store plausible here long before it is for the hot tier.

The merge itself is built. `Smolquery.StorageService.Merge` turns a claim into one
sealed segment inside DuckDB, writing straight to the store's staging path:

```sql
COPY (SELECT <the catalog's columns> FROM read_parquet([urls], union_by_name := true))
TO staged
```

- **No segment's bytes become an Elixir term**, which matters most for the largest
  objects the system writes. Same reason the segment writer hands Polars a path.
- **The inputs come over HTTP**, from `HotServer`. The bytes have to travel that
  way regardless — `httpfs` speaks HTTP and nothing else — so the manifest comes
  the same way, and a remote buffer node needs nothing new in a cluster.
- **`union_by_name` is what makes additive schema evolution work**: micro-segments
  written before and after a column was added merge into one segment carrying the
  union.
- **But the union of the inputs is not the schema the catalog declares**, and
  registration compares against the catalog. A claim whose inputs *all* predate an
  added column unions to the older, narrower schema, and `add_data_files` rejects
  the file — a rejection no retry can clear, because the claim's input set is
  frozen. So the merge projects onto the catalog's columns instead: each declared
  column in the catalog's order, cast to the catalog's type, and one the inputs do
  not carry as a typed `NULL`. The sealed file matches the table by construction.
- **A column the inputs carry and the catalog does not is an error**
  (`{:error, {:undeclared_columns, names}}`), refused before any byte moves.
  Projecting it away would silently drop a column of acked rows, which is worse
  than a stuck claim; widening the table is the catalog's call, not the sealer's.
- **The output key comes from the claim, not from the merge**, so a retry is an
  idempotent overwrite of identical rows rather than a second segment.
- **An input the manifest no longer lists is skipped, not fatal** — its rows are
  gone either way, and refusing would strand the table's whole tail on one lost
  file. A claim with nothing left is an error rather than an empty segment.

`Smolquery.StorageService.Handoff.Seal` composes that into the whole handoff, and
this is the one cross-service dance in smolquery:

```
merge → put → register → retire
```

An attempt starts by asking the catalog whether the claim's sealed segment is
already registered. If it is, some earlier attempt got that far before dying, and
this one skips to retirement. So a crash costs a `seal_retry_ms` delay and nothing
more, at every point:

- **before the commit** — nothing is registered, so the next attempt merges again,
  overwriting its own half-written output at the same key.
- **after the commit, before retirement** — the rows are in the sealed tier and the
  micro-segments are still unretired. This is exactly the window the
  catalog-membership rule is built for: a query at any snapshot counts them once.
  The next attempt finds the keys registered and retires.
- **after retirement** — nothing left to do; a repeated retire is `:ok`.

Retirement goes through `BufferService.Client`, not HTTP: it is a control-plane
call with no bulk data, `HotServer` is read-only, and the client already owns
ownership routing and idempotence. Same bulk/control split the buffer draws
internally.

A table the catalog does not hold is an error rather than something the sealer
creates — the ingest edge validated against the catalog before forwarding, so a
table with micro-segments is a table the catalog already knows.

`Smolquery.StorageService.GC` collects the one kind of garbage this can leave: a
segment put but never registered, because an attempt died in between. Nothing will
ever name it — the next attempt writes the same key and registers that one.

- **The test is membership in every snapshot, not the current one.** A path dropped
  from the current snapshot is still the only copy of rows an older snapshot can
  read, so `Catalog.known_segments/1` spans all of history. Reclaiming expired
  snapshots' files is retention's job, not GC's.
- **Two sightings, not a timestamp.** An object is deleted only after being seen
  unreferenced for `gc_grace_ms` continuously. A sealed segment's key encodes when
  its *inputs* were written, so a claim that waited an hour for a sealer produces an
  "old" key the moment it lands — reading the key's age would sweep live work. So
  `gc_grace_ms` must exceed the longest merge, the way `retire_grace_ms` must exceed
  the longest query.

### Queries

`Smolquery.QueryService` is the read path: async query jobs planned against the
catalog ∪ the hot tier, started by the `:query` role. The surface is
`Smolquery.QueryService.Client`:

```elixir
{:ok, job, frame} = Smolquery.QueryService.Client.query(Smolquery.QueryService,
  "SELECT count(*) FROM analytics.events")

{:ok, job} = Smolquery.QueryService.Client.submit(name, sql)   # async
{:ok, job, frame} = Smolquery.QueryService.Client.fetch(name, job.id)
:ok = Smolquery.QueryService.Client.cancel(name, job.id)
```

Each job runs in its own process with its own private DuckDB engine — two jobs'
views never collide, cancellation kills the engine and the query dies with it,
and `job_memory_limit` binds one job, not the node. The engine costs ~50 ms to
start (`bench/results/query.md`), which an async job never notices.

The planner never rewrites SQL. For each table the query references (found with
DuckDB's own parser, which doubles as the read-only gate — only a single SELECT
serializes), it creates `<dataset>.<table>` as a view in the job engine's own
catalog, shadowing the attached lake:

```sql
CREATE VIEW "analytics"."events" AS SELECT "id", "ts" FROM (
  SELECT * FROM "lake"."analytics"."events" AT (VERSION => 42)   -- sealed, pinned
  UNION ALL BY NAME
  SELECT * FROM read_parquet(['http://…/01A.parquet'], union_by_name := true)
)
```

Properties worth knowing:

- **One snapshot per job, and rows count exactly once while sealing runs
  underneath.** The sealed side is pinned `AT (VERSION => S)`; a hot
  micro-segment is included iff it carries no claim or its claim's sealed keys
  are not all in the catalog at `S`. The commit that makes rows appear in the
  sealed tier is the same event that excludes their micro-segments — no gap, at
  any crash point, which the reader-side crash matrix walks through the public
  surface.
- **Hot rows have no snapshot.** An acked write is visible to the next query —
  read-your-writes, not an inconsistency.
- **Both tiers project onto the catalog's schema.** A micro-segment written
  before a column was added reads back with NULLs there, the way a sealed
  segment does.
- **Manifest-level pruning drops hot files before DuckDB pays an HTTP footer
  read for each** (~0.7 ms/file): top-level WHERE conjuncts against flush-time
  min-max stats, conservative in every uncertain case. The sealed tier prunes
  itself — DuckLake keeps stats at registration.
- **An unreachable buffer owner fails the query.** Sealed-only rows behind a
  green status would be a wrong answer.
- **v1 trusts its SQL.** A SELECT can `read_csv('/etc/passwd')`; scoping
  DuckDB's `allowed_directories` lands with auth in Milestone 6, the same
  posture as `HotServer`'s unauthenticated routes.

## HTTP API

`Smolquery.Api` is the front door — one Bandit listener, started by the `:api`
role, routing only to service client modules and the catalog (the same
boundary rule the services hold each other to). Every `/v1` route requires the
static Bearer key; a node with the `:api` role and no key configured fails the
boot rather than serve an open API. `/healthz` is the one unauthenticated
route.

```sh
curl http://127.0.0.1:4000/healthz
curl -H "authorization: Bearer $SMOLQUERY_API_KEY" http://127.0.0.1:4000/v1/...
```

Failures answer one JSON envelope everywhere:

```json
{"error": {"code": 401, "status": "UNAUTHENTICATED", "message": "missing or invalid API key"}}
```

The v1 surface (datasets/tables CRUD, streaming inserts, query jobs, batch
loads) lands across Milestone 6's layers; the routes exist in
`Smolquery.Api.Router` as they arrive.

## Roles

One release, four services plus the HTTP front door; a node starts only the
subtrees its roles name. `SMOLQUERY_ROLES` is a comma-separated list, or `all`:

```sh
SMOLQUERY_ROLES=all                # default when unset — single-node dev
SMOLQUERY_ROLES=query              # a query-only node
SMOLQUERY_ROLES=api,ingest,buffer
```

Unknown role names fail the boot rather than silently starting nothing. `:api`,
`:query`, `:buffer`, and `:storage` start their subtrees today; `:ingest` is
accepted and contributes nothing until its milestone lands. See
`Smolquery.Roles`.

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
  buffer_timeout_ms: 30_000,
  engine_extensions: [:httpfs],
  compression: :zstd,
  target_segment_bytes: 268_435_456,
  max_concurrent_seals: 2,
  gc_interval_ms: 300_000,
  gc_grace_ms: 3_600_000,
  handoff: {Smolquery.StorageService.Handoff.Seal, []}

config :smolquery, Smolquery.QueryService,
  buffer_base_url: "http://127.0.0.1:4001",
  buffer_timeout_ms: 30_000,
  engine_extensions: [:httpfs],
  max_concurrent_jobs: 8,
  default_timeout_ms: 60_000,
  job_memory_limit: "1GB",
  result_ttl_ms: 300_000
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
`engine_extensions` loads `httpfs` into the sealer's own engine, which the merge
cannot work without.

The query service runs each query as a job with a private DuckDB engine
(`Smolquery.QueryService.Client.query/3` sync, `submit/3` + `fetch/2` async).
Given `catalog:` options it starts its own DuckLake engine to plan through;
given a `%Smolquery.Catalog{}` it starts none — but then `job_bootstrap:` must
carry the `ATTACH` job engines need, since they attach the lake themselves.
`buffer_base_url` is where the planner reaches `HotServer` for hot manifests —
the same single-node honesty as the storage service's. `max_concurrent_jobs`
refuses rather than queues; `default_timeout_ms` bounds every job's runtime;
`job_memory_limit` is each job engine's DuckDB `memory_limit`; `result_ttl_ms`
is how long a finished job holds its result frame for an async caller.

Runtime environment variables:

| variable | effect |
|---|---|
| `SMOLQUERY_ROLES` | which service subtrees start (`all` or a comma-separated list) |
| `SMOLQUERY_API_KEY` | the Bearer key every `/v1` route requires; a node with the `:api` role and no key refuses to boot |
| `SMOLQUERY_API_PORT` | port the HTTP API binds (default 4000) |
| `SMOLQUERY_API_IP` | address the HTTP API binds (default `127.0.0.1` — exposing it is deliberate) |
| `SMOLQUERY_MEMORY_LIMIT` | DuckDB memory limit per engine |
| `SMOLQUERY_DATA_DIR` | data directory; the catalog and DuckLake data path derive from it |
| `SMOLQUERY_CATALOG` | catalog metadata database, e.g. `postgres:dbname=smolquery host=…` |
| `SMOLQUERY_MAX_RESULT_ROWS` | ceiling on rows `Engine.query/3` converts to Elixir terms (`infinity` to disable) |
| `SMOLQUERY_BUFFER_DIR` | buffer service root for micro-segments and manifest logs |
| `SMOLQUERY_FLUSH_INTERVAL_MS` | group-commit cadence, and so the ack-latency bound |
| `SMOLQUERY_HOT_SERVER_PORT` | port `HotServer` binds to serve micro-segments over `httpfs` |
| `SMOLQUERY_SEALED_DIR` | storage service root for sealed segments |
| `SMOLQUERY_BUFFER_BASE_URL` | `HotServer` base URL the sealer and the query planner pull the hot tier from |

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
mix run bench/sealer.exs                          # what a seal costs, and how far behind it runs
mix run bench/query.exs                           # what a query job costs, and the hot tier's read path

SEGMENTS=1500 ROWS=2000 mix run bench/planner.exs # bigger catalog, smaller segments
ROWS=10000000 CLIENTS=16 mix run bench/adbc.exs   # push the fetch and concurrency sizes
CALLS=50 MAX_WRITERS=128 mix run bench/buffer.exs # more samples, more concurrency
INPUTS=64 ROWS=20000 mix run bench/sealer.exs     # bigger claims, bigger merges
```

`bench/results/` holds the last recorded run of each script — the tables verbatim,
the machine they came from, and the decisions they settled. Re-run a script after a
DuckDB, DuckLake, Explorer, or OTP upgrade and overwrite its file there, so the
next comparison has something to diff against.

`bench/buffer.exs` reports batches/s, rows/s, MB/s, and p50/p95/p99 ack latency
across batch size × writers × tables, sweeps `flush_interval_ms`, prices the two
fsyncs behind an ack (D3), and locates the one-table inline-flush ceiling (D6) in
five parts — a writer sweep to 1024 against a light and a heavy schema, the Polars
encode timed in isolation, the fsync toggle re-run at the top of the sweep, a
`flush_max_bytes` sweep, and a partition proxy running P independent buffers over
one workload. Its other knobs: `MAX_BATCH`, `MAX_TABLES`, `WRITERS`, `BATCH`,
`WRITERS_PER_BUFFER`. Sealing is disabled throughout — this script measures group
commit, `bench/sealer.exs` measures sealing.

Every D6 section runs three schemas, because the encode is what it investigates and
cost-per-row is the variable that moves everything else: `light` (2 columns,
165 B/row), `heavy` (4 columns with a `Decimal`, 254 B/row), and `huge` (20 columns
spanning all seven logical types, 867 B/row — the shape of a real event table).
`huge` is 5.3× `light`'s bytes but 12× its encode time, since per-column overhead
dominates payload size.

One table saturates at **2.19M rows/s light, 1.08M heavy, 280K huge**, set by the
encode plus the write path around it. None of the configured bounds sets it:
`flush_max_bytes` from 8 MB to 512 MB moves rows-per-flush 8–27× and throughput
5–12%, non-monotonically — it decides how rows are *packed* into flushes, not how
many get through.

The number to design against is ack latency, and it has two regimes. Below
saturation, `p50 = flush_interval_ms + ~5 ms` regardless of load — group commit's
whole promise. At saturation, `p50 = outstanding rows ÷ throughput` (Little's Law)
and **`flush_interval_ms` drops out entirely**. So at the default config a
20-column table returns a **2.01 second p50 ack**. Eight independent buffers reach
6.68M rows/s — 3.11× light, 4.09× huge, ordered by how encode-bound each schema is
— which is the case for partitioned writes (PL-6). Partitioning divides the overload
factor but does not define the cliff; bounding ack latency needs backpressure (T-56).

`bench/sealer.exs` compares the two merge implementations, measures merge
throughput against input count and rows, times the whole handoff, and reports the
sealed-to-hot size ratio. That last number is why it exists: DuckDB's `COPY`
defaults to snappy while segments are written with zstd, so sealing silently made
data 2.85× larger until the codec was matched. No correctness test could catch
that.

Each script's `@moduledoc` records what it measures and what it concluded; the
numbers behind those conclusions are in `bench/results/`. Two results worth knowing
before writing a read path:

- **A large result must not come back as Elixir terms.** `Smolquery.Engine.Result`
  converts Arrow row by row, which costs roughly a kilobyte and 2 µs per row — 5M
  rows take 11.2 s and 3.4 GiB, against 390 ms and 11.5 MiB left in Arrow. It is
  the right shape for catalog and control-plane queries, and a trap for user
  results, which is what `Engine.frame/3` and the `:max_result_rows` ceiling are
  for.
- **One connection serializes.** `Engine.Connection` is a per-query mutex, so
  query throughput is flat in client count. Eight connections serve eight
  concurrent clients about 3.5× faster than one does.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for quality gates and the project
tracker, and [`AGENTS.md`](AGENTS.md) for codebase tooling.
