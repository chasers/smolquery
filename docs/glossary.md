# Glossary

The terms smolquery uses, grouped by the service that owns them. This page
defines the words. The [architecture doc](architecture.md) explains how the
parts work together.

## System-wide

- **table ref** — the pair `{dataset, table}` that names a table inside the
  system.
- **segment** — one write-once Parquet file in the sealed tier.
- **micro-segment** — one small Parquet file a buffer flush writes. It lives
  in the hot tier until a seal merges it into a segment.
- **catalog** — the DuckLake metadata database behind `Smolquery.Catalog`.
  It records datasets, tables, schemas, registered segments, and the
  per-table options: retention, clustering, partitions.
- **snapshot** — the catalog version a mutation commits at. A query pins one
  snapshot, so it reads a consistent sealed tier. Snapshots also bound time
  travel.
- **schema** — a table's columns and types (`Smolquery.Schema`). It also
  carries the clustering key and the partition count, so one cached read
  reaches every write point.
- **clustering key** — the column names writes sort by. smolquery's analog
  of ClickHouse's `ORDER BY`. The sorted Parquet's row-group stats are the
  sparse index.
- **retention policy** — a column plus a TTL. Rows age out segment-grained:
  a segment drops only once every row in it has aged out.
- **partition count** — how many buffer identities one table's writes
  spread over. Partition `i` of `logs.events` is the ordinary ref
  `events__pi`; partition 0 is the bare ref. The effective count is the
  maximum of the table's own catalog count and `SMOLQUERY_WRITE_PARTITIONS`.
  It is raise-only, at most 64.
- **ring** — the consistent-hash mapping from a routing key to a node
  (`Smolquery.BufferService.Ring`). The buffer and storage services each run
  a ring over their own node set. Partition `i` rotates to the `i mod N`-th
  distinct node clockwise from its parent, so one table's partitions cover
  `min(P, N)` nodes exactly.
- **role** — which service subtrees one node starts (`SMOLQUERY_ROLES`).
- **engine** — one supervised DuckDB instance (`Smolquery.Engine`). Engines
  are disposable; the storage of record is Parquet plus the catalog.
- **store** — the segment storage backend (`Smolquery.Segments.Store`):
  local disk or S3. The store owns what "durable" means.

## Ingest service

- **ingest edge** — the API-side service that turns one HTTP insert into one
  forward batch for the owning buffer node.
- **forward batch** — the unit the ingest edge sends to a buffer node: the
  schema plus the rows or the NDJSON bytes of one request.
- **NDJSON passthrough** — the edge forwards an NDJSON body as the bytes the
  client sent. DuckDB parses the rows once, at flush. Always on: the writer
  that needed parsed rows is gone (PL-57).
- **schema cache** — a per-node ETS cache of table schemas, bounded by
  `schema_cache_ttl_ms`. CRUD invalidates it on the acting node; the TTL
  bounds staleness everywhere else.
- **insertId** — the client's idempotency key. A retry hashes to the
  partition that holds the first attempt, so it dedups there.
- **write ref** — the partition ref one batch writes to. Sticky by
  `insertId`; round-robin without one.

## Buffer service

- **TableBuffer** — the process that accumulates one ref's rows. It is the
  write path's serialization point, and what makes group commit possible.
- **group commit** — one flush makes many requests' rows durable at once.
  The ack returns only then, so "acked" means "on the owner's disk".
- **hot tier** — the seconds-to-minutes of unsealed data one ref holds:
  micro-segments plus the hot manifest.
- **hot manifest** — the buffer's authoritative log of a ref's
  micro-segments and their state: pending, claimed, then sealed and retired.
  What was acked is exactly what the manifest holds.
- **HotServer** — the HTTP listener that serves micro-segments and manifests
  to storage and query nodes over `httpfs`.
- **claim** — a frozen set of micro-segments one seal works on. One live
  claim per table ref; a retried seal reuses the claim, so it produces the
  same sealed segment.
- **seal signal** — the buffer telling storage that a ref's claim is ready.
  Level-triggered: it repeats every `seal_retry_ms` until the claim retires,
  so a dropped signal costs one interval and nothing else.
- **replication / follower** — micro-segments copy to the next ring nodes,
  so an acked row survives the owner's loss. A partition's replica set
  begins at that partition's own owner.
- **drain** — moving a ref's unsealed tail off a node during a ring change
  or shutdown.
- **ring epoch** — the fence around a ring change, so two nodes never both
  accept one ref's writes.

## Storage service

- **seal** — the merge of one claim's micro-segments into one sealed
  segment, registered in the catalog. The unit of hot-to-sealed movement.
- **sealer** — the per-node process that answers seal signals: one seal in
  flight per ref, `max_concurrent_seals` slots per node, and a per-table
  cooldown after failures.
- **handoff** — the seal's catalog side. It maps a partition ref back to the
  parent, because the sealed tier and the catalog know only real tables.
  Registration is idempotent, so a crashed handoff retries safely.
- **storage ring** — the second ring, over storage nodes. The sealer and
  compactor act only on keys they own. Ownership is advisory; the catalog's
  re-diffed commit retries absorb the overlap a ring change opens.
- **compaction** — merging many small sealed segments into fewer large ones
  through `replace_segments`, in one transaction, so no snapshot
  double-counts rows.
- **bucket** — compaction's ownership shard: `{table, bucket}`, where the
  bucket is a segment ULID's timestamp over `compact_bucket_ms`. Buckets
  spread one table's compaction over the storage nodes.
- **retention sweep** — drops the segments whose newest retained value
  passed the TTL horizon.
- **GC** — deletes the files no snapshot references any more. Snapshot
  expiry is what lets GC finally say no.

## Query service

- **planner** — builds one plan per query over the catalog and the hot tier,
  pinned at one snapshot.
- **resolve** — the planner's per-ref catalog read: schema, registered
  segments, and stats.
- **manifest fetch** — one hot-manifest GET per partition per manifest URL.
  The pages gather under the parent ref, so the rest of the plan sees one
  hot tier per table.
- **job** — one query's lifecycle. Results page from the runner's frame
  until the result TTL, then the job answers from durable history.
- **runner** — executes a plan on a job-private engine, so one query's scan
  never queues behind another's.
- **pruning** — skipping segments and row groups by min-max stats. It is
  lost silently when a predicate's parameter type mismatches the column.

## API and web

- **admission** — the API refuses ingest bodies past the in-flight byte
  limit before it reads them, with a 429.
- **API key** — the tenant bearer key every `/v1` route requires.
- **internal secret** — what internal HTTP proves itself with. It gates the
  operator surfaces, such as `/metrics`.
- **lifecycle page** — the web UI's table page: each hot partition's
  pending → claimed → sealed stages, plus the sealed tier.
