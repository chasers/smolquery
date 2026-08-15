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
        seal_row_group_size: 16_384,
        target_segment_bytes: 268_435_456,
        max_concurrent_seals: 2,
        gc_interval_ms: 300_000,
        gc_grace_ms: 3_600_000,
        compact_interval_ms: 300_000,
        compact_below_bytes: 33_554_432,
        compact_min_inputs: 2,
        compact_max_inputs: 12,
        compact_max_bytes: 134_217_728,
        merge_inputs_per_call: 12,
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

  `merge_inputs_per_call` bounds how many `read_parquet` inputs any one of the
  merge's engine calls carries (T-246, T-247). Per-input cost is what outruns
  the engine's 30 s call timeout: the eu-central-1 sandbox measured ≥ ~830 ms
  per input over `httpfs`, so a 36-input read already exceeded it. The default
  of 12 comes from that measurement — the merge engine serializes up to three
  calls (two seals plus a compaction), so each gets ~10 s, and
  10 s / 830 ms ≈ 12. An input list over the cap does not shrink the merge:
  `Smolquery.StorageService.Merge` reads it in capped chunks into a session
  temp table and writes one output, so a claim of any size seals and no
  engine call is unbounded.

  `compact_max_inputs` caps how many segments one compaction *group* holds
  (T-244) — a planning bound, distinct from the call bound above. The byte
  ceiling alone does not bound the file count — 128 MiB of small sealed
  segments is over a hundred inputs — and a bounded group keeps a sweep's
  merge short so seal attempts on the shared engine are not starved. It must
  be at least `compact_min_inputs`, validated at boot; a run larger than the
  cap compacts across sweeps, one group at a time. The same cap chunks the
  sweep's footer-sizing query, oldest first, and sizing stops once a full
  group of undersized candidates is found — a mostly-small backlog needs one
  sizing call rather than one per chunk. Retention's footer-stats query is
  chunked by the same cap.

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
  the sealer and compactor write sealed Parquet. It defaults to `16_384` and is
  validated here at boot for the same reason as `compression`: a bad value
  discovered per attempt would crash every re-signalled seal forever.

  `engine_extensions` are loaded into this service's own engine. `httpfs` is not
  optional in a real deployment — the merge reads micro-segments over HTTP, and an
  object-store tier would need it too — so it is the default rather than something
  a deployment has to remember. Tests that never merge set it to `[]` and skip the
  extension download.
  """

  alias Smolquery.BufferService.Ring
  alias Smolquery.Catalog
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
    compression: :zstd,
    seal_row_group_size: 16_384,
    target_segment_bytes: 268_435_456,
    max_concurrent_seals: 2,
    gc_interval_ms: 300_000,
    gc_grace_ms: 3_600_000,
    compact_interval_ms: 300_000,
    compact_below_bytes: 33_554_432,
    compact_min_inputs: 2,
    compact_max_inputs: 12,
    compact_max_bytes: 134_217_728,
    merge_inputs_per_call: 12,
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
          compression: atom(),
          seal_row_group_size: pos_integer(),
          target_segment_bytes: pos_integer(),
          max_concurrent_seals: pos_integer(),
          gc_interval_ms: pos_integer(),
          gc_grace_ms: pos_integer(),
          compact_interval_ms: pos_integer(),
          compact_below_bytes: pos_integer(),
          compact_min_inputs: pos_integer(),
          compact_max_inputs: pos_integer(),
          compact_max_bytes: pos_integer(),
          merge_inputs_per_call: pos_integer(),
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
    :compression,
    :seal_row_group_size,
    :target_segment_bytes,
    :max_concurrent_seals,
    :gc_interval_ms,
    :gc_grace_ms,
    :compact_interval_ms,
    :compact_below_bytes,
    :compact_min_inputs,
    :compact_max_inputs,
    :compact_max_bytes,
    :merge_inputs_per_call,
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
    |> validate_compression()
    |> validate_seal_row_group_size()
    |> validate_compact_inputs()
    |> validate_merge_inputs_per_call()
  end

  use Smolquery.Runtime

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

  defp validate_compact_inputs(%__MODULE__{compact_min_inputs: min} = runtime)
       when not (is_integer(min) and min > 0) do
    raise ArgumentError,
          "unsupported compact_min_inputs: #{inspect(runtime.compact_min_inputs)} " <>
            "(expected a positive integer)"
  end

  defp validate_compact_inputs(%__MODULE__{compact_max_inputs: max} = runtime)
       when is_integer(max) and max >= runtime.compact_min_inputs,
       do: runtime

  defp validate_compact_inputs(%__MODULE__{} = runtime) do
    raise ArgumentError,
          "unsupported compact_max_inputs: #{inspect(runtime.compact_max_inputs)} " <>
            "(expected an integer >= compact_min_inputs, " <>
            "which is #{inspect(runtime.compact_min_inputs)})"
  end

  defp validate_merge_inputs_per_call(%__MODULE__{merge_inputs_per_call: per_call} = runtime)
       when is_integer(per_call) and per_call > 0,
       do: runtime

  defp validate_merge_inputs_per_call(%__MODULE__{} = runtime) do
    raise ArgumentError,
          "unsupported merge_inputs_per_call: #{inspect(runtime.merge_inputs_per_call)} " <>
            "(expected a positive integer)"
  end

  defp build_store(config) do
    case Keyword.get(config, :store) do
      nil -> Store.Local.new(dir: Keyword.get(config, :dir, @default_dir))
      {impl, opts} -> impl.new(opts)
      %Store{} = store -> store
    end
  end
end
