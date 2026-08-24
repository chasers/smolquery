defmodule Smolquery.StorageService.Runtime do
  @moduledoc """
  A running storage service's resolved configuration.

  The same shape as `Smolquery.BufferService.Runtime`, and for the same reason:
  configuration becomes handles once at boot and lands in `:persistent_term`, so
  `Client` can answer a seal signal without rebuilding a store on the buffer's
  write path.

  Naming derives from one instance name, so a test can run an isolated storage
  service beside the application's own.

  ## Configuration

      config :smolquery, Smolquery.StorageService,
        dir: "priv/data/sealed",
        buffer_base_url: "http://127.0.0.1:4001",
        buffer_timeout_ms: 30_000,
        engine_extensions: [:httpfs],
        compression: :zstd,
        seal_row_group_size: 1_048_576,
        target_segment_bytes: 268_435_456,
        max_concurrent_seals: 2,
        gc_interval_ms: 300_000,
        gc_grace_ms: 3_600_000,
        compact_interval_ms: 300_000,
        compact_below_bytes: 33_554_432,
        compact_min_inputs: 2,
        compact_max_bytes: 134_217_728,
        merge_inputs_per_call: 12,
        merge_copy_timeout_ms: 300_000,
        merge_staging_timeout_ms: 120_000,
        merge_describe_timeout_ms: 120_000,
        retention_interval_ms: 3_600_000,
        snapshot_keep_ms: 86_400_000,
        handoff: {Smolquery.StorageService.Handoff.Seal, []},
        ring: [:"storage1@host"]

  `:dir` is where sealed segments land, through a `Store.Local` beneath it. Pass
  `:store` to override the store outright — it is a wholly separate handle from
  the buffer's, because the two tiers have opposite write profiles: the buffer
  puts a micro-segment per flush, the sealer puts a large one per seal. That
  difference is what makes an object store plausible for this tier long before
  the hot one.

      store: {Smolquery.Segments.Store.Local, dir: "/mnt/bulk/sealed"}

  `buffer_base_url` is where the sealer reaches `BufferService.HotServer` to pull
  a table's manifest and micro-segment bytes — honest for a single-node
  deployment. With clustering on, a seal signal carries its `:origin` node and
  `Smolquery.StorageService.HotTier` derives that node's URL from its name
  instead, using `buffer_hot_port` — the port every buffer node's `HotServer`
  binds (`Smolquery.BufferService`'s own `hot_server_port`) — the same
  derivation the query planner uses (Milestone 8 L5/L6).

  `max_concurrent_seals` bounds seals in flight on this node. Signalling is
  level-triggered, so a signal shed at the bound costs a `seal_retry_ms` delay
  rather than a lost seal.

  `seal_backoff_base_ms` and `seal_backoff_max_ms` pace a table whose seal
  attempts keep failing (T-299). A claim failing identically every
  re-signal burns an attempt slot and the merge engine's shared memory for
  the full failed attempt, every time — the shape that stalled sealing
  cluster-wide on the eu-central-1 sandbox. After `n` consecutive failures
  the table's signals are shed for
  `min(seal_backoff_base_ms * 2^(n-1), seal_backoff_max_ms)`; a success
  clears the cooldown. Shedding is as safe here as at the concurrency
  bound: the buffer re-signals every `seal_retry_ms`, so the cost is delay
  on a table already failing, never a lost seal. The base defaults to
  `seal_retry_ms`'s default, so the first failure adds nothing. A base of
  `0` disables the cooldown.

  `ring` is the static node list `Smolquery.StorageService.Routing` falls back
  to — the storage-side ownership ring's node set once clustering is off, or
  before any node has joined `Smolquery.Cluster.PgGroup`'s `:pg` group for
  this service. It defaults to `[node()]`, so a single-node deployment owns every
  table's seal work, unchanged from before Milestone 8 L6. Distinct from
  `Smolquery.BufferService.Runtime`'s `ring`: the two rings are independent —
  which node buffers a table and which node seals it need not be the same.

  `retention_interval_ms` paces `Smolquery.StorageService.Retention`'s sweep;
  which tables expire and how fast is per-table policy in the catalog, not
  node configuration. `snapshot_keep_ms` is the time-travel promise: each
  sweep expires catalog snapshots older than this, which is what lets GC
  physically reclaim dropped and compacted-away files — so it must exceed the
  longest query a reader may still have pinned, *and* the buffer's
  `retire_grace_ms`: expiry erases the registration history the planner's
  seal-membership rule reads, so hot entries must be reaped before the
  snapshots that sealed them expire. The defaults (24 h against 10 min) hold
  that order with room to spare.

  The `compact_*` knobs shape `Smolquery.StorageService.Compactor`'s sweep:
  every `compact_interval_ms` it groups sealed segments under
  `compact_below_bytes` into merges of at least `compact_min_inputs` inputs
  and at most `compact_max_bytes` output, one group per table per sweep. The
  defaults (32 MiB floor, 128 MiB ceiling) sit under `target_segment_bytes`
  on purpose — compaction exists to clean up eager and age-cap seals, not to
  re-merge segments the sealer already sized well.

  `compact_max_rows` bounds the same group by decompressed rows (T-260),
  because bytes alone do not: on ~100x-compressible data a 47 MiB group held
  ~25M rows, and merge cost — the staging inserts and the clustered `ORDER
  BY` on the final `COPY` — scales with rows, so the byte-bounded group blew
  the merge's five-minute `COPY` budget and re-planned identically every
  sweep. Left `nil`, the cap starts at 4Mi rows, resolved by
  `with_compact_max_rows/1` once when the compactor starts. The start
  assumes no bytes-per-row pin rate on purpose: the sandbox did measure a
  4Mi-row group pinning ~1 GiB during a merge — the whole of a 1 GiB budget
  — but those were very large rows, and a cap derived from their pin rate
  would starve typical workloads. The compactor fits the cap to the
  workload instead: a merge OOM halves a table's cap and sustained evidence
  earns it back (`Smolquery.StorageService.Compactor.adjusted_row_caps/3`,
  T-262), so a workload that pins more per row converges on its own cap
  after one bad merge, and every other workload keeps the full cap. Normal
  ~3x-compressible data hits the byte cap first, so the row cap only bites
  the pathological case it exists for. Sizing already reads each footer's
  `num_rows`, so the cap costs no new I/O.

  `compact_bucket_ms` shards a table's compaction across the storage ring
  (T-269). Ownership used to be per table, so one node compacted a hot table
  alone — and OOMed alone — while its peers idled. The compactor now owns
  `{table_ref, bucket}`, where a segment's bucket is its ULID timestamp
  divided by this value. **Set it identically on every storage node**:
  disjoint ownership is a property of every node computing the same bucket
  ids, and divergent values during a rolling change give two nodes
  overlapping owned sets until the rollout completes. Buckets are otherwise
  disjoint, so two nodes cannot plan the same group, and a group crosses a
  bucket boundary only when a bucket cannot meet `compact_min_inputs` alone
  — its candidates roll forward into the node's next owned bucket, so quiet
  tables still compact. The default of one hour trades parallelism against
  time-locality: a smaller bucket spreads a backlog across more nodes but
  widens straggler carries; a larger one concentrates work. A backlog
  inside one bucket still compacts on one node — a small backlog by
  definition.

  Compaction runs on its own engine, `compact_engine/1` (T-259).
  `compact_engine_memory_limit` sizes it the way `engine_memory_limit` sizes
  the merge engine, one rung down: explicit knob, else a quarter of the
  cgroup limit, else `Smolquery.Engine`'s application config; see
  `compact_engine_memory_limit/2`.

  `merge_inputs_per_call` bounds how many `read_parquet` inputs any one of the
  merge's engine calls carries (T-246, T-247). Per-input cost is what outruns
  the engine's 30 s call timeout: the eu-central-1 sandbox measured ≥ ~830 ms
  per input over `httpfs`, so a 36-input read already exceeded it. The default
  of 12 comes from that measurement, made when the merge engine serialized
  up to three callers on one connection; each seal slot now serializes only
  its own calls (T-299), which leaves the cap comfortable rather than
  tight. An input list over the cap does not shrink the merge.
  `Smolquery.StorageService.Merge` reads it in capped chunks into a session
  temp table and writes one output, so a claim of any size seals and no
  engine call is unbounded.

  The same cap chunks every other footer read on this engine: the compactor's
  sizing query and retention's footer-stats query. A compaction group itself
  has no input-count cap (T-248). A group cap made a small-segment backlog
  re-ingest its own output sweep after sweep, and the chunked merge is what
  removed the need for one. Sizing stops once the undersized bytes found
  reach `compact_max_bytes`, because the group cannot grow past it.

  `merge_copy_timeout_ms`, `merge_staging_timeout_ms` and
  `merge_describe_timeout_ms` are the merge's three call budgets — the final
  `COPY`, one staging chunk, and the schema `DESCRIBE` that precedes a
  projection. `Smolquery.StorageService.Merge` states why each one is longer
  than the engine's 30 s default (T-261, T-288). They were module attributes
  until T-335, where a 488-segment claim on a 63-column table exhausted engine
  memory, then spill, and then this budget — `%Smolquery.Engine.CallExited{}`
  with 625 MB of a 32 GiB spill in use and 89 GB of disk free. That last
  failure is not a resource limit at all, and no configuration could move it.

  A merge that times out re-stages its whole claim on the next attempt, so a
  budget below what the claim needs never completes and never shrinks. Raise
  these to seal a backlog that a bounded claim still cannot finish; the
  cheaper lever is a smaller claim (`claim_valve_factor` on
  `Smolquery.BufferService.Runtime`).

  `handoff` names what one seal attempt does; see
  `Smolquery.StorageService.Handoff`.

  `buffer_name` is the buffer service instance `retire/4` is called on. Retirement
  goes through `Smolquery.BufferService.Client` rather than HTTP, unlike the
  manifest pull — it is a control-plane call, and `HotServer` is read-only. That
  split mirrors the buffer's own: bulk on one channel, control on another.

  `catalog` is where sealed segments are committed. Given options (or nothing), the
  service starts its own `Smolquery.Catalog.DuckLake` engine and commits through
  it; given a `%Smolquery.Catalog{}` outright, it commits through that and starts
  nothing:

      catalog: [metadata: "postgres:dbname=smolquery", data_path: "/mnt/bulk/lake"]

  `compression` is the Parquet codec sealed segments are written with — one of
  `:zstd`, `:snappy`, `:gzip`, or `:uncompressed`, validated here at boot because
  a bad value discovered per seal attempt would crash every re-signalled seal
  forever. It defaults to `:zstd` to match `Smolquery.Segments.Writer`, and that
  match matters: DuckDB's own `COPY` default is snappy, which made sealed segments
  2.85 times larger than the micro-segments they replaced (`bench/sealer.exs`).

  `seal_row_group_size` is the `ROW_GROUP_SIZE` passed to DuckDB's `COPY` when
  the sealer and compactor write sealed Parquet. It defaults to `1_048_576`
  (T-280): a sealed-tier scan over `httpfs` pays roughly one range request per
  row group, so large groups cut a query's request count ~8x against DuckDB's
  122_880-row default. The trade is coarser row-group pruning on clustered
  tables and more `COPY` buffering per group — the merge engine's memory limit
  is the bound that has to hold. Unlike compaction's row cap, the seal path
  has no OOM adaptation, and a seal claim's inputs are frozen: a `COPY` that
  deterministically OOMs at this size retries identically forever and wedges
  the table's seal path. On a memory-tight node whose seal OOMs recur after
  a raise, lower `SMOLQUERY_SEAL_ROW_GROUP_SIZE` or raise the merge engine's
  memory limit. It is validated here at boot for the same reason as
  `compression`: a bad value discovered per attempt would crash every
  re-signalled seal forever.

  `engine_extensions` are loaded into this service's own engine. `httpfs` is not
  optional in a real deployment — the merge reads micro-segments over HTTP, and an
  object-store tier would need it too — so it is the default rather than something
  a deployment has to remember. Tests that never merge set it to `[]` and skip the
  extension download.

  `engine_memory_limit` (default `nil`) is the merge engine's own DuckDB
  memory limit — a size string like `"2GiB"`, validated at boot as present
  and stringly, with DuckDB judging the grammar on the engine's first `SET`.
  Left `nil`, the limit derives from the container's cgroup memory limit,
  and without one of those the engine inherits `Smolquery.Engine`'s
  application config; see `engine_memory_limit/2` for the order and the
  reasoning (T-250).
  """

  alias Smolquery.BufferService.Ring
  alias Smolquery.Catalog
  alias Smolquery.Catalog.Dataset
  alias Smolquery.Engine
  alias Smolquery.EngineSecrets
  alias Smolquery.Segments.Store

  @enforce_keys [:name, :store, :catalog, :ring]
  defstruct [
    :name,
    :store,
    :catalog,
    :catalog_opts,
    :ring,
    buffer_name: Smolquery.BufferService,
    buffer_base_url: "http://127.0.0.1:4001",
    buffer_hot_port: 4001,
    buffer_timeout_ms: 30_000,
    engine_extensions: [:httpfs],
    engine_memory_limit: nil,
    compression: :zstd,
    seal_row_group_size: 1_048_576,
    target_segment_bytes: 268_435_456,
    max_concurrent_seals: 2,
    seal_backoff_base_ms: 30_000,
    seal_backoff_max_ms: 600_000,
    gc_interval_ms: 300_000,
    gc_grace_ms: 3_600_000,
    compact_interval_ms: 300_000,
    compact_below_bytes: 33_554_432,
    compact_min_inputs: 2,
    compact_max_bytes: 134_217_728,
    compact_max_rows: nil,
    compact_bucket_ms: 3_600_000,
    compact_engine_memory_limit: nil,
    merge_engine: nil,
    merge_inputs_per_call: 12,
    merge_copy_timeout_ms: 300_000,
    merge_staging_timeout_ms: 120_000,
    merge_describe_timeout_ms: 120_000,
    retention_interval_ms: 3_600_000,
    snapshot_keep_ms: 86_400_000,
    handoff: {Smolquery.StorageService.Handoff.Seal, []}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          store: Store.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          ring: Ring.t(),
          buffer_name: atom(),
          buffer_base_url: String.t(),
          buffer_hot_port: :inet.port_number(),
          buffer_timeout_ms: timeout(),
          engine_extensions: [atom() | String.t()],
          engine_memory_limit: String.t() | nil,
          compression: atom(),
          seal_row_group_size: pos_integer(),
          target_segment_bytes: pos_integer(),
          max_concurrent_seals: pos_integer(),
          seal_backoff_base_ms: non_neg_integer(),
          seal_backoff_max_ms: non_neg_integer(),
          gc_interval_ms: pos_integer(),
          gc_grace_ms: pos_integer(),
          compact_interval_ms: pos_integer(),
          compact_below_bytes: pos_integer(),
          compact_min_inputs: pos_integer(),
          compact_max_bytes: pos_integer(),
          compact_max_rows: pos_integer() | nil,
          compact_bucket_ms: pos_integer(),
          compact_engine_memory_limit: String.t() | nil,
          merge_engine: Smolquery.Engine.handle() | nil,
          merge_inputs_per_call: pos_integer(),
          merge_copy_timeout_ms: pos_integer(),
          merge_staging_timeout_ms: pos_integer(),
          merge_describe_timeout_ms: pos_integer(),
          retention_interval_ms: pos_integer(),
          snapshot_keep_ms: pos_integer(),
          handoff: {module(), term()}
        }

  @limits [
    :buffer_name,
    :buffer_base_url,
    :buffer_hot_port,
    :buffer_timeout_ms,
    :engine_extensions,
    :engine_memory_limit,
    :compression,
    :seal_row_group_size,
    :target_segment_bytes,
    :max_concurrent_seals,
    :seal_backoff_base_ms,
    :seal_backoff_max_ms,
    :gc_interval_ms,
    :gc_grace_ms,
    :compact_interval_ms,
    :compact_below_bytes,
    :compact_min_inputs,
    :compact_max_bytes,
    :compact_max_rows,
    :compact_bucket_ms,
    :compact_engine_memory_limit,
    :merge_inputs_per_call,
    :merge_copy_timeout_ms,
    :merge_staging_timeout_ms,
    :merge_describe_timeout_ms,
    :retention_interval_ms,
    :snapshot_keep_ms,
    :handoff
  ]

  @codecs [:zstd, :snappy, :gzip, :uncompressed]

  @default_dir "priv/data/sealed"

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.StorageService` supplies the defaults; `opts`
  overrides them, so a test passes what it needs and inherits the rest.

  Raises on a `compression` outside #{inspect(@codecs)}: seals are level-triggered
  retries, so a codec discovered bad per attempt would crash forever rather than
  once, here, at boot.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.StorageService, []), opts)
    name = Keyword.get(config, :name, Smolquery.StorageService)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    %__MODULE__{
      name: name,
      store: build_store(config),
      catalog: catalog,
      catalog_opts: catalog_opts,
      ring: Ring.new!(Keyword.get(config, :ring, [node()]))
    }
    |> struct!(Keyword.take(config, @limits))
    |> validate_engine_memory_limit()
    |> validate_compact_engine_memory_limit()
    |> validate_compression()
    |> validate_seal_row_group_size()
    |> validate_compact_bucket_ms()
    |> validate_compact_inputs()
    |> validate_compact_max_rows()
    |> validate_merge_inputs_per_call()
    |> validate_merge_timeouts()
  end

  use Smolquery.Runtime

  @doc """
  The DuckDB memory limit the merge engine starts with.

  Resolution order (T-250):

  1. An explicit `engine_memory_limit` — the operator's word, taken whole.
  2. Half the container's cgroup memory limit, when one exists. The merge
     engine is the only engine on a storage node that moves data volume, so
     it gets the largest share; the other half covers the BEAM, the catalog
     engine, and allocations DuckDB's limit does not account for.
  3. `nil` — no cgroup limit either (bare metal, dev, pods without a memory
     limit), so the engine inherits `Smolquery.Engine`'s application config
     unchanged.

  The derivation is what turns a bigger pod into a bigger merge without the
  deploy restating the number. The global `SMOLQUERY_MEMORY_LIMIT` is one
  size for every engine on every role, and sizing the storage merge from it
  is what left a 4 Gi pod OOMing at 954 MiB, on the same group, every sweep.
  """
  @spec engine_memory_limit(t(), {:ok, pos_integer()} | :none) :: String.t() | nil
  def engine_memory_limit(runtime, cgroup \\ Smolquery.CgroupMemory.limit_bytes())

  def engine_memory_limit(%__MODULE__{engine_memory_limit: limit}, cgroup),
    do: derived_memory_limit(limit, cgroup, 2)

  @doc """
  The DuckDB memory limit the compaction engine starts with.

  The same resolution as `engine_memory_limit/2`, one rung down: an explicit
  `compact_engine_memory_limit` wins, else a **quarter** of the container's
  cgroup memory limit, else `nil` and the engine inherits `Smolquery.Engine`'s
  application config. The merge engine keeps T-250's half — seals are the
  volume this node exists to move — so the transient worst case of a seal and
  a compaction copying at once budgets three quarters, and both engines spill
  past their limits rather than balloon (T-259).
  """
  @spec compact_engine_memory_limit(t(), {:ok, pos_integer()} | :none) :: String.t() | nil
  def compact_engine_memory_limit(runtime, cgroup \\ Smolquery.CgroupMemory.limit_bytes())

  def compact_engine_memory_limit(%__MODULE__{compact_engine_memory_limit: limit}, cgroup),
    do: derived_memory_limit(limit, cgroup, 4)

  defp derived_memory_limit(limit, _cgroup, _divisor) when is_binary(limit), do: limit

  defp derived_memory_limit(nil, {:ok, bytes}, divisor),
    do: "#{max(div(bytes, divisor * 1024 * 1024), 1)}MiB"

  defp derived_memory_limit(nil, :none, _divisor), do: nil

  @compact_max_rows_default 4_194_304

  @doc """
  The runtime with `compact_max_rows` resolved to an integer.

  An explicit cap survives untouched. A `nil` cap starts at
  #{@compact_max_rows_default} rows. The start deliberately assumes no
  bytes-per-row pin rate: the one figure the sandbox measured came from a
  bench with very large rows, and deriving every deployment's cap from it
  would starve typical workloads. The compactor owns fitting the cap to the
  workload instead — a merge OOM halves a table's cap and sustained evidence
  earns it back (`Smolquery.StorageService.Compactor.adjusted_row_caps/3`,
  T-262).
  """
  @spec with_compact_max_rows(t()) :: t()
  def with_compact_max_rows(%__MODULE__{compact_max_rows: rows} = runtime)
      when is_integer(rows),
      do: runtime

  def with_compact_max_rows(%__MODULE__{} = runtime),
    do: %{runtime | compact_max_rows: @compact_max_rows_default}

  @doc """
  The store a dataset's sealed segments live in (PL-51 L4): the dataset's own
  S3 store when its record names one, otherwise `:store`. The dataset's store
  stages through the same scratch directory as the default store, since a
  seal is staged locally before its upload either way.

  A catalog that stores no dataset records, and a dataset with no record,
  both answer the default store — the pre-PL-51 behavior.
  """
  @spec store_for(t(), String.t()) :: {:ok, Store.t()} | {:error, term()}
  def store_for(%__MODULE__{} = runtime, dataset) do
    case Catalog.dataset(runtime.catalog, dataset) do
      {:ok, %Dataset{storage: nil}} -> {:ok, runtime.store}
      {:ok, %Dataset{} = owned} -> own_store(runtime, owned)
      {:error, :datasets_unsupported} -> {:ok, runtime.store}
      {:error, {:unknown_dataset, _name}} -> {:ok, runtime.store}
      {:error, reason} -> {:error, reason}
    end
  end

  defp own_store(runtime, dataset) do
    with {:ok, options} <- Dataset.store_options(dataset) do
      {:ok, Store.S3.new(options ++ [staging_dir: staging_dir(runtime.store)])}
    end
  end

  defp staging_dir(%Store{config: %{staging_dir: dir}}) when is_binary(dir), do: dir
  defp staging_dir(%Store{config: %{dir: dir}}) when is_binary(dir), do: Path.join(dir, "staging")
  defp staging_dir(_store), do: Path.join(System.tmp_dir!(), "smolquery-dataset-staging")

  @doc """
  Gives the merge engine what it needs to read `store` back: the sealed-tier
  secret and the extension its credential mode needs. The default store's
  secret is a bootstrap statement; a dataset's store is met here, before the
  merge that reads it, with statements that are no-ops the second time.
  """
  @spec prepare_store(t(), Store.t()) :: :ok | {:error, term()}
  def prepare_store(%__MODULE__{store: store}, store), do: :ok
  def prepare_store(%__MODULE__{}, %Store{impl: Store.Local}), do: :ok

  def prepare_store(%__MODULE__{} = runtime, %Store{} = store) do
    extensions = EngineSecrets.sealed_tier_extensions(store, []) -- runtime.engine_extensions
    loads = Enum.flat_map(extensions, &["INSTALL #{&1}", "LOAD #{&1}"])

    Enum.reduce_while(loads ++ EngineSecrets.sealed_tier(store), :ok, fn sql, :ok ->
      case Engine.query(merge_engine(runtime), sql) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  The engine handle a merge's calls run on.

  `nil` resolves to `engine/1`, the seal merge engine, on its first
  connection. The compactor overrides the field on the runtime it hands
  `Smolquery.StorageService.Merge.compact/5`, so its merges run on
  `compact_engine/1` and a timed-out compaction statement cannot starve a
  seal (T-259). `Smolquery.StorageService.Sealer` overrides it per attempt
  with `{engine, slot}`, one merge-engine connection per seal slot, so one
  table's slow merge is not another table's wait (T-299).
  """
  @spec merge_engine(t()) :: Smolquery.Engine.handle()
  def merge_engine(%__MODULE__{merge_engine: nil, name: name}), do: engine(name)
  def merge_engine(%__MODULE__{merge_engine: engine}) when is_atom(engine), do: engine

  def merge_engine(%__MODULE__{merge_engine: {engine, slot} = handle})
      when is_atom(engine) and is_integer(slot),
      do: handle

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The process accepting seal signals.
  """
  @spec sealer(atom()) :: atom()
  def sealer(name), do: Module.concat(name, "Sealer")

  @doc """
  The engine instance seal merges run in.
  """
  @spec engine(atom()) :: atom()
  def engine(name), do: Module.concat(name, "Engine")

  @doc """
  The engine instance compaction sizes and merges through.

  Separate from the merge engine because an `Adbc.Connection` serializes its
  queries and a timed-out statement keeps running: one abandoned compaction
  merge starved every seal and every sizing call behind it (T-259). On its
  own engine, the compactor can also recycle the connection after a timeout
  without aborting a healthy in-flight seal.
  """
  @spec compact_engine(atom()) :: atom()
  def compact_engine(name), do: Module.concat(name, "CompactEngine")

  @doc """
  The engine instance catalog commits go through.

  Separate from the merge engine because an `Adbc.Connection` serializes its
  queries: a commit waiting behind a multi-gigabyte `COPY` would make the catalog
  as slow as the largest merge in flight.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")

  @doc """
  The task supervisor seal attempts run under.
  """
  @spec seals(atom()) :: atom()
  def seals(name), do: Module.concat(name, "Seals")

  @doc """
  The process sweeping uncommitted sealed segments.
  """
  @spec gc(atom()) :: atom()
  def gc(name), do: Module.concat(name, "GC")

  @doc """
  The process re-merging undersized sealed segments.
  """
  @spec compactor(atom()) :: atom()
  def compactor(name), do: Module.concat(name, "Compactor")

  @doc """
  The process dropping segments past their table's TTL.
  """
  @spec retention(atom()) :: atom()
  def retention(name), do: Module.concat(name, "Retention")

  defp validate_engine_memory_limit(%__MODULE__{engine_memory_limit: limit} = runtime)
       when is_binary(limit) or is_nil(limit),
       do: runtime

  defp validate_engine_memory_limit(%__MODULE__{engine_memory_limit: limit}) do
    raise ArgumentError,
          "unsupported engine_memory_limit: #{inspect(limit)} " <>
            "(expected a DuckDB size string like \"2GiB\", or nil)"
  end

  defp validate_compression(%__MODULE__{compression: compression} = runtime)
       when compression in @codecs,
       do: runtime

  defp validate_compression(%__MODULE__{compression: compression}) do
    raise ArgumentError,
          "unsupported sealed-segment compression: #{inspect(compression)} " <>
            "(expected one of #{inspect(@codecs)})"
  end

  defp validate_seal_row_group_size(%__MODULE__{seal_row_group_size: size} = runtime)
       when is_integer(size) and size > 0,
       do: runtime

  defp validate_seal_row_group_size(%__MODULE__{seal_row_group_size: size}) do
    raise ArgumentError,
          "unsupported seal_row_group_size: #{inspect(size)} (expected a positive integer)"
  end

  defp validate_compact_bucket_ms(%__MODULE__{compact_bucket_ms: ms} = runtime)
       when is_integer(ms) and ms > 0,
       do: runtime

  defp validate_compact_bucket_ms(%__MODULE__{compact_bucket_ms: ms}) do
    raise ArgumentError,
          "unsupported compact_bucket_ms: #{inspect(ms)} (expected a positive integer)"
  end

  defp validate_compact_inputs(%__MODULE__{compact_min_inputs: min} = runtime)
       when is_integer(min) and min > 0,
       do: runtime

  defp validate_compact_inputs(%__MODULE__{} = runtime) do
    raise ArgumentError,
          "unsupported compact_min_inputs: #{inspect(runtime.compact_min_inputs)} " <>
            "(expected a positive integer)"
  end

  defp validate_compact_max_rows(%__MODULE__{compact_max_rows: rows} = runtime)
       when (is_integer(rows) and rows > 0) or is_nil(rows),
       do: runtime

  defp validate_compact_max_rows(%__MODULE__{compact_max_rows: rows}) do
    raise ArgumentError,
          "unsupported compact_max_rows: #{inspect(rows)} " <>
            "(expected a positive integer, or nil for the default)"
  end

  defp validate_compact_engine_memory_limit(
         %__MODULE__{compact_engine_memory_limit: limit} = runtime
       )
       when is_binary(limit) or is_nil(limit),
       do: runtime

  defp validate_compact_engine_memory_limit(%__MODULE__{compact_engine_memory_limit: limit}) do
    raise ArgumentError,
          "unsupported compact_engine_memory_limit: #{inspect(limit)} " <>
            "(expected a DuckDB size string like \"1GiB\", or nil)"
  end

  defp validate_merge_inputs_per_call(%__MODULE__{merge_inputs_per_call: per_call} = runtime)
       when is_integer(per_call) and per_call > 0,
       do: runtime

  defp validate_merge_inputs_per_call(%__MODULE__{} = runtime) do
    raise ArgumentError,
          "unsupported merge_inputs_per_call: #{inspect(runtime.merge_inputs_per_call)} " <>
            "(expected a positive integer)"
  end

  defp validate_merge_timeouts(%__MODULE__{} = runtime) do
    Enum.each(
      [:merge_copy_timeout_ms, :merge_staging_timeout_ms, :merge_describe_timeout_ms],
      &validate_merge_timeout(runtime, &1)
    )

    runtime
  end

  defp validate_merge_timeout(runtime, key) do
    case Map.fetch!(runtime, key) do
      ms when is_integer(ms) and ms > 0 ->
        :ok

      ms ->
        raise ArgumentError,
              "unsupported #{key}: #{inspect(ms)} (expected a positive integer of milliseconds)"
    end
  end

  defp build_store(config) do
    case Keyword.get(config, :store) do
      nil -> Store.Local.new(dir: Keyword.get(config, :dir, @default_dir))
      {impl, opts} -> impl.new(opts)
      %Store{} = store -> store
    end
  end
end
