# smolquery

<p align="center">
  <img src="docs/assets/banner.svg" alt="smolquery — an open source BigQuery alternative, powered by DuckDB and Elixir" width="100%">
</p>

> An open source BigQuery alternative — datasets, tables, streaming inserts, and
> async query jobs over an HTTP API, in one self-hostable BEAM release.

[![CI](https://github.com/chasers/smolquery/actions/workflows/ci.yml/badge.svg)](https://github.com/chasers/smolquery/actions/workflows/ci.yml)
[![Cluster](https://github.com/chasers/smolquery/actions/workflows/cluster.yml/badge.svg)](https://github.com/chasers/smolquery/actions/workflows/cluster.yml)
[![Elixir](https://img.shields.io/badge/elixir-~%3E%201.20-4B275F.svg)](https://elixir-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![Status](https://img.shields.io/badge/status-pre--alpha-orange.svg)

Create a dataset, declare a table, stream rows in, and query them back — the
whole lifecycle is a handful of `curl`s against one container. Rows are durable
*and* queryable the moment the insert returns; a background tier merges them
into large Parquet files on object storage without a query ever seeing the seam.
Written in Elixir/OTP, with DuckDB as a disposable read engine over Parquet and
a DuckLake catalog.

> **Status: pre-alpha.** Single-node is complete — the read engine, hot tier,
> sealing, query jobs, HTTP API, storage maintenance, and a Docker release.
> The cluster layers are in: Postgres-backed membership and catalog, an S3
> sealed tier, live ownership rings with drain, query fan-out, seal-work
> distribution, and a kind-cluster test suite that runs on pull requests and
> pushes to `main`. Plans and milestones live in the project tracker — see
> [`CONTRIBUTING.md`](CONTRIBUTING.md). Everything below is subject to change.

## Quick start

```sh
docker build -t smolquery .

docker run -d --name smolquery \
  -p 4000:4000 -p 4002:4002 \
  -v smolquery-data:/data \
  -e SMOLQUERY_API_KEY=change-me \
  -e SMOLQUERY_WEB_IP=0.0.0.0 \
  -e SMOLQUERY_WEB_USERNAME=smolquery \
  -e SMOLQUERY_WEB_PASSWORD=change-me-too \
  -e SMOLQUERY_SECRET_KEY_BASE="$(openssl rand -base64 48)" \
  smolquery
```

```sh
auth='authorization: Bearer change-me'
json='content-type: application/json'

# create a dataset and a table
curl -H "$auth" -H "$json" -d '{"id": "analytics"}' \
     http://127.0.0.1:4000/v1/datasets
curl -H "$auth" -H "$json" -d '{"id": "events", "schema": [
       {"name": "id", "type": "INT64", "nullable": false},
       {"name": "ts", "type": "TIMESTAMP"},
       {"name": "amount", "type": "NUMERIC(38,2)"}
     ]}' http://127.0.0.1:4000/v1/datasets/analytics/tables

# stream rows in — NDJSON, one object per line. A 200 means they are durable
# and queryable. (This is the only accepted insert body; see docs/api.md.)
echo '{"id": 1, "ts": "2026-08-01T10:00:00Z", "amount": "12.50"}' \
  | curl -H "$auth" -H 'content-type: application/x-ndjson' --data-binary @- \
    http://127.0.0.1:4000/v1/datasets/analytics/tables/events/insert

# query them back
curl -H "$auth" -H "$json" \
     -d '{"query": "SELECT count(*) AS n FROM analytics.events"}' \
     http://127.0.0.1:4000/v1/queries
```

The full surface is in [`docs/api.md`](docs/api.md); a LiveView UI for the same
thing is on [`localhost:4002`](http://localhost:4002), behind the basic-auth
credential you just set.

Releases are created automatically for a merged stable `mix.exs` version bump
only after the successful main-push Kind workflow and the exact-SHA CI run. The
release publishes a multi-architecture image to GHCR and attaches both a
`ghcr.io/chasers/smolquery@sha256:...` reference and an image-pinned base
manifest. `release-manifest.yaml` is not a standalone production deployment:
integrate it with, and provide, the `smolquery-env` Secret, Postgres catalog and
discovery, and the sealed-store dependencies before deploying. The Secret must
also hold `SMOLQUERY_WEB_USERNAME`, `SMOLQUERY_WEB_PASSWORD`, and
`SMOLQUERY_SECRET_KEY_BASE` for any pod whose roles include `web`; a pod
without them refuses to boot.

## Features

- **A BigQuery-shaped API.** Datasets, tables, streaming inserts, batch loads
  (NDJSON / CSV / Parquet), sync queries and async jobs with paged results. One
  Bearer token, plain JSON, no SDK required.
- **Durable and queryable are the same event.** An insert acks only once its
  rows are fsynced into a Parquet micro-segment *and* into the table's manifest
  log — the same manifest a query plans against. Read-your-writes with no second
  mechanism behind it.
- **Two tiers, one query plan.** DuckDB unions a pinned catalog snapshot with
  the hot tier's unsealed micro-segments, and a row counts exactly once while
  sealing and compaction run underneath it.
- **Exactly-once inserts.** A retried batch carrying the same `insertId` lands
  once, through a lost ack, a transport timeout, or a crash before reply.
- **Parquet is the storage of record.** Write-once segments plus a DuckLake
  catalog (SQLite in dev, Postgres in a cluster). DuckDB is disposable; the data
  outlives any engine.
- **Storage maintains itself.** Sealing merges micro-segments into large ones,
  compaction re-merges the undersized residue, per-table retention policies age
  data out segment-by-segment, snapshot expiry and GC reclaim the files.
- **Elastic by role.** One release, six roles (`api`, `ingest`, `buffer`,
  `storage`, `query`, `web`). A node starts only the subtrees it is given, and
  only buffer nodes hold state.
- **A cluster is one Postgres.** Point every node at the same database and they
  discover each other, share a catalog, fan queries across buffer owners, and
  seal from a second ring — nothing else to stand up.
- **Honest failures.** A reader that cannot reach a buffer node holding unsealed
  rows returns `503`, never a green status over a short answer.

## How it works

The short version — see [`docs/architecture.md`](docs/architecture.md) for the
full walk-through.

```
inserts → IngestService ──→ BufferService ──seal──→ StorageService
          (stateless)       (hot tier: durable,     (large Parquet → object store,
                             queryable Parquet       DuckLake catalog commit)
                             micro-segments)
queries → QueryService ──────────────────────────────────────────┘
          (DuckDB via ADBC: catalog ∪ hot tiers, one query plan)
```

- **The write path is columnar.** The default DuckDB writer parses the forwarded
  NDJSON body at flush time and writes immutable Parquet segments — small in (a
  ~1 s group commit is what "durable" means), large out (a few big files on
  object storage).
- **DuckDB is a disposable, stateless read engine.** Each query job gets its own
  engine, its own memory limit, and its own views; cancelling a job kills the
  engine under it.
- **Only the buffer service is stateful**, holding seconds-to-minutes of
  unsealed data. Everything else scales elastically.
- **Sealing is exactly-once by construction.** Crossing a threshold freezes a
  *claim* — a fixed input set and the key its merged output will take — so every
  retry produces the same file, and a micro-segment stops counting at the
  instant its rows start counting in the catalog.
- **Bulk traffic gets its own sockets.** Forward-batches and seal signals travel
  over `gen_rpc` on split bulk/control channels, never Erlang distribution;
  segment *bytes* travel over HTTP so DuckDB's `httpfs` keeps its projection and
  range-read pushdown.
- **A query fans out to every expected buffer node**, not just a table's current
  owner — a ring change moves ownership instantly while the previous owner still
  holds the unsealed tail.
- **Everything durable is one directory** (or one bucket plus one Postgres).
  Back that up and you have backed smolquery up.

## Running locally

Requires OTP 29.0.2 / Elixir 1.20.2 (pinned in `.tool-versions`, matching CI).

```sh
mise install      # or asdf install
mix deps.get
mix assets.setup  # once — the web UI asset toolchain (tailwind + esbuild)
iex -S mix
```

The API is on [`localhost:4000`](http://localhost:4000) (dev Bearer key:
`smolquery-dev`) and the LiveView UI on
[`localhost:4002`](http://localhost:4002) (dev credential: `smolquery` /
`smolquery`) — browse datasets and tables, create
both, edit a table's retention policy, preview rows, run SQL through the query
service's job lifecycle (submit, live state, paged results, cancel), and watch
the fleet on `/cluster` — every node's alive/ring-epoch/drain state, live, with
three ways to disturb one on purpose: **kill** (ungraceful pod force-delete —
watches `RingEpoch`'s fencing actually earn its keep), **restart** (a plain
pod delete — also what un-sticks a drained node), and **drain** (a genuinely
graceful ring exit over distributed Erlang, no pod involved). Kill/restart
work against a local `kind` cluster (below) via `kubectl`, or against a real
deployment via the in-cluster ServiceAccount `deploy/base/rbac.yaml` grants —
same `Smolquery.Cluster.Pods` module either way. The UI calls service client
modules directly, never loopback HTTP. Every route requires the basic-auth
credential (`SMOLQUERY_WEB_USERNAME` / `SMOLQUERY_WEB_PASSWORD`). The bind
defaults to `127.0.0.1`; set `SMOLQUERY_WEB_IP=0.0.0.0` to expose it.

## Deploying

The deployable is a `mix release` in a Docker image — one container, one volume,
env-configured, as in the [quick start](#quick-start). Everything durable lives
under `SMOLQUERY_DATA_DIR` (`/data` in the container): the hot tier's
micro-segments and manifest logs under `buffer/`, sealed segments under
`sealed/`, the DuckLake catalog SQLite and its `ducklake/` data path, and
DuckDB's extension cache.

`SIGTERM` drains before it stops: buffers flush their accumulators on shutdown
(a rolling restart loses nothing), in-flight seals finish or are retried by the
next boot's re-signal, and everything acked is already on disk. `docker stop` is
a clean shutdown.

Every environment variable is documented in
[`docs/configuration.md`](docs/configuration.md). The two that change the shape
of a deployment:

- `SMOLQUERY_ROLES` — which service subtrees this node starts.
- `CATALOG_DATABASE_URL` — a Postgres every node can reach. Setting it tiers the
  catalog onto Postgres and turns on node discovery; that is the whole cluster
  setup.

### Production Kubernetes (Helm)

The production chart is at [`charts/smolquery`](charts/smolquery). It consumes
an existing runtime Secret and does not bundle PostgreSQL, S3-compatible
storage, or certificate issuance:

```sh
kubectl create namespace smolquery --dry-run=client -o yaml | kubectl apply -f -
kubectl -n smolquery create secret generic smolquery-env \
  --from-literal=SMOLQUERY_API_KEY=change-me \
  --from-literal=SMOLQUERY_INTERNAL_SECRET=change-me-too \
  --from-literal=CATALOG_DATABASE_URL=postgres://user:password@postgres/smolquery \
  --from-literal=SMOLQUERY_S3_BUCKET=smolquery \
  --from-literal=SMOLQUERY_WEB_USERNAME=smolquery \
  --from-literal=SMOLQUERY_WEB_PASSWORD=change-me-web \
  --from-literal=SMOLQUERY_SECRET_KEY_BASE=smolquery-secret-key-base-01234567890123456789012345678901234567890123456789 \
  --from-literal=RELEASE_COOKIE=smolquery-release-cookie
helm upgrade --install smolquery ./charts/smolquery \
  --namespace smolquery --wait --timeout 10m
helm test smolquery --namespace smolquery
```

The default split chart runs API, buffer, and storage StatefulSets; set
`--set topology=symmetric` for the all-role server topology. Only split buffer
or symmetric server pods receive durable PVCs; API and storage use `emptyDir`.
The chart defaults to the released v0.13.0 image digest. TLS uses a pre-created
Secret with `ca.pem` and per-pod certificate/key files; enabling chart TLS sets
`GEN_RPC_TLS=true` and `DIST_TLS=true`. External Secret rotation needs a rollout
or reloader. Full values, upgrade, PVC, Secret, and TLS guidance is
in [`docs/deployment.md`](docs/deployment.md) and [`charts/smolquery/README.md`](charts/smolquery/README.md).

### Local cluster (kind)

`deploy/` holds kustomize manifests: `base/` is the smolquery fleet itself,
`overlays/kind/` adds what local dev needs around it (Postgres, MinIO,
NodePorts, dev TLS certs). The Kind overlays intentionally use the mutable
`smolquery:dev` image for local iteration; production uses the image-pinned base
manifest attached to a release, integrated with its required Secret, Postgres
catalog/discovery, and sealed-store dependencies. One command boots the whole stack — six nodes with
split roles, clustered over plaintext inter-node transport by default, sealing
to object storage through a Postgres catalog. The overlay mounts development TLS
certificates and can opt into both TLS transports:

```sh
./scripts/kind-up.sh    # kind cluster + certs + image build/load + apply + wait
mix test --only cluster # ingest, fan-out, seal, drain, kill
# Set GEN_RPC_TLS=true and DIST_TLS=true in the overlay to exercise TLS.
```

| workload | replicas | roles | state |
| --- | --- | --- | --- |
| `smolquery-api` | 1 | `api,ingest,query,web` | none (`emptyDir`) |
| `smolquery-buffer` | 3 | `buffer` | PVC — the acked-but-unsealed tail lives here |
| `smolquery-storage` | 2 | `storage` | none — sealed segments in MinIO, catalog in Postgres |
| `postgres` / `minio` | 1 each | — | kind-overlay only |

`SMOLQUERY_KIND_OVERLAY=kind-symmetric ./scripts/kind-up.sh` deploys the same
release as three identical all-role servers instead (`smolquery-server`, 3
replicas, `SMOLQUERY_ROLES=all`, PVC each) — the ClickHouse-replica shape,
where any stage's demand can use any node's CPU and there is no per-tier
capacity split to get wrong.

All StatefulSets, because every cluster member needs a stable pod name: peers
derive each other's URLs from node names, and the mounted per-node certificates
are used when `GEN_RPC_TLS` and `DIST_TLS` are enabled. The API lands on
`http://localhost:8080` (Bearer
`kind-only-api-key`), the web UI on `http://localhost:8082` (`smolquery` /
`kind-only-web-password`).

Draining a buffer node is the one operation with no HTTP surface — it
force-seals everything the node owns and waits for the seal to land before the
node stops being an owner:

```sh
kubectl -n smolquery exec smolquery-buffer-0 -c smolquery -- /app/bin/smolquery rpc \
  ':ok = Smolquery.BufferService.Drain.drain(Smolquery.BufferService, timeout_ms: 120_000)'
```

All three of drain, restart, and an *ungraceful* kill are a click away
instead: the web UI's `/cluster` page lists the fleet with buttons per node
when it detects this `kind-smolquery` context (kill/restart) or an alive
buffer node to RPC into (drain), so you can watch ring ownership, `RingEpoch`,
and `ExpectedNodes` recover live.

Iterate with `./scripts/kind-up.sh` again (rebuilds the image, reloads it,
restarts the fleet); tear down with `kind delete cluster --name smolquery`.
`kubectl` in this repo is scoped to that cluster via [direnv](https://direnv.net):
`.envrc` exports `KUBECONFIG=$PWD/.kube/config`, a gitignored single-context
kubeconfig (run `direnv allow` once), so a bare `kubectl` can never hit an
ambient context from another cluster.

## Tests

```sh
mix test                        # fast suite
mix test --include integration  # + DuckDB extensions, real HTTP, :peer nodes, Postgres, MinIO
mix test --only cluster         # just the kind-cluster suite (see above)
```

Integration-tagged tests are excluded by default: they download DuckDB
extensions, serve Parquet over a real HTTP server, boot `:peer` nodes (`epmd`
must be running: `epmd -daemon`), and expect a Postgres on `localhost:5432`
(`postgres`/`postgres`, override with `TEST_POSTGRES_*`) and a MinIO on
`localhost:9000` (`smolquery`/`smolquery-secret`, override with `TEST_S3_*`).

The DuckLake catalog suite creates and uses a `smolquery_test` database rather
than sharing `postgres`. DuckLake's metadata tables carry no primary keys, and
a database under a `FOR ALL TABLES` publication — a developer Postgres also
used for logical replication or CDC — rejects the `UPDATE`s those tables need,
failing every catalog commit after a table's first. Publications are
per-database, so a database of its own settles it; override with
`TEST_POSTGRES_DATABASE` only toward a database under no such publication:

```sh
docker run -d --rm -p 9000:9000 \
  -e MINIO_ROOT_USER=smolquery -e MINIO_ROOT_PASSWORD=smolquery-secret \
  minio/minio server /data
```

`:cluster`-tagged tests go further: they need the whole fleet on real distinct
hosts, so they run against the kind cluster rather than starting anything
themselves. They exist because that is where the cluster milestone's real bugs
were — four on first boot, plus a query that returned `200 OK` with a killed
owner's acked rows silently missing, which survived a fully green suite. `:peer`
cannot reproduce them: it cannot give two nodes distinct hosts sharing one port,
which is the assumption node-name-derived URLs rest on. Each test restores the
fleet in `on_exit` and takes its own dataset; a full run is one to two minutes,
most of it waiting out `seal_max_age_ms` and pod restarts.

## Quality gate

```sh
mix precommit  # mutating: compile -Werror, unused deps, format, credo --strict, ex_dna, test
mix ci         # the non-mutating superset CI runs
```

`mix ci` is the merge gate: `hex.audit` (dependency CVEs) → compile
(warnings-as-errors) → `deps.unlock --check-unused` → `format --check-formatted`
→ `credo --strict` (with the [ExSlop](https://github.com/elixir-vibe/ex_slop)
plugin's AI-slop checks) → `deps.audit` →
[ExDNA](https://github.com/elixir-vibe/ex_dna) duplication ratchet →
`reach.check --arch --smells --strict`
([Reach](https://github.com/elixir-vibe/reach)'s service-boundary policy in
`.reach.exs`: services communicate only through client modules). `mix dialyzer`,
the integration suite, and the kind-cluster suite run as their own CI jobs.

## Repo layout

```
lib/smolquery/engine*          # DuckDB via ADBC — the disposable read engine
lib/smolquery/schema*          # logical types ↔ Explorer dtypes ↔ DuckDB types
lib/smolquery/segments*        # Parquet writer + swappable segment stores (Local, S3)
lib/smolquery/catalog*         # DuckLake: datasets, tables, snapshots, registration
lib/smolquery/ingest_service*  # schema lookup, batching; flush/salvage validation
lib/smolquery/buffer_service*  # the hot tier: group commit, manifest log, HotServer, ring
lib/smolquery/storage_service* # seal, compact, retention, GC
lib/smolquery/query_service*   # query jobs and the two-tier planner
lib/smolquery/cluster*         # node discovery and :pg membership
lib/smolquery_api/             # the HTTP front door, Phoenix (see docs/api.md)
lib/smolquery_web/             # Phoenix LiveView UI
deploy/                        # kustomize manifests (base + kind overlay)
bench/                         # performance harnesses + results/ (see docs/benchmarks.md)
skills/                        # Claude Code skills (./skills/install.sh; see skills/README.md)
```

## Documentation

- [**Architecture**](docs/architecture.md) — the full walk-through: the read
  engine, segments and the catalog, the hot tier, sealing and the sealed tier,
  storage maintenance, query planning, roles, clustering, and the security
  posture.
- [**Configuration**](docs/configuration.md) — every environment variable and
  application-config key.
- [**Deployment**](docs/deployment.md) — how a release is published, the
  release artifacts, and the upgrade procedures, catalog format migrations
  included.
- [**HTTP API**](docs/api.md) — the `/v1` surface, schema types, and the error
  envelope.
- [**Glossary**](docs/glossary.md) — the terms each service uses, defined in
  one place.
- [**Benchmarks**](docs/benchmarks.md) — the measurements the design decisions
  were made on.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — dev setup, the PR workflow, and the
  project tracker.
- [`AGENTS.md`](AGENTS.md) — codebase tooling for coding agents.

## License

[Apache License 2.0](LICENSE).
