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
        target_segment_bytes: 268_435_456,
        max_concurrent_seals: 2,
        gc_interval_ms: 300_000,
        gc_grace_ms: 3_600_000,
        handoff: {Smolquery.StorageService.Handoff.Seal, []}

  `:dir` is where sealed segments land, through a `Store.Local` beneath it. Pass
  `:store` to override the store outright — it is a wholly separate handle from
  the buffer's, because the two tiers have opposite write profiles: the buffer
  puts a micro-segment per flush, the sealer puts a large one per seal. That
  difference is what makes an object store plausible for this tier long before
  the hot one.

      store: {Smolquery.Segments.Store.Local, dir: "/mnt/bulk/sealed"}

  `buffer_base_url` is where the sealer reaches `BufferService.HotServer` to pull
  a table's manifest and micro-segment bytes. Configuration is honest for a
  single-node deployment; a cluster resolves it from the ownership ring instead,
  which arrives with Milestone 8.

  `max_concurrent_seals` bounds seals in flight on this node. Signalling is
  level-triggered, so a signal shed at the bound costs a `seal_retry_ms` delay
  rather than a lost seal.

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

  `engine_extensions` are loaded into this service's own engine. `httpfs` is not
  optional in a real deployment — the merge reads micro-segments over HTTP, and an
  object-store tier would need it too — so it is the default rather than something
  a deployment has to remember. Tests that never merge set it to `[]` and skip the
  extension download.
  """

  alias Smolquery.Catalog
  alias Smolquery.Segments.Store

  @enforce_keys [:name, :store, :catalog]
  defstruct [
    :name,
    :store,
    :catalog,
    :catalog_opts,
    buffer_name: Smolquery.BufferService,
    buffer_base_url: "http://127.0.0.1:4001",
    buffer_timeout_ms: 30_000,
    engine_extensions: [:httpfs],
    compression: :zstd,
    target_segment_bytes: 268_435_456,
    max_concurrent_seals: 2,
    gc_interval_ms: 300_000,
    gc_grace_ms: 3_600_000,
    handoff: {Smolquery.StorageService.Handoff.Seal, []}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          store: Store.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          buffer_name: atom(),
          buffer_base_url: String.t(),
          buffer_timeout_ms: timeout(),
          engine_extensions: [atom() | String.t()],
          compression: atom(),
          target_segment_bytes: pos_integer(),
          max_concurrent_seals: pos_integer(),
          gc_interval_ms: pos_integer(),
          gc_grace_ms: pos_integer(),
          handoff: {module(), term()}
        }

  @limits [
    :buffer_name,
    :buffer_base_url,
    :buffer_timeout_ms,
    :engine_extensions,
    :compression,
    :target_segment_bytes,
    :max_concurrent_seals,
    :gc_interval_ms,
    :gc_grace_ms,
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
      catalog_opts: catalog_opts
    }
    |> struct!(Keyword.take(config, @limits))
    |> validate_compression()
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

  defp validate_compression(%__MODULE__{compression: compression} = runtime)
       when compression in @codecs,
       do: runtime

  defp validate_compression(%__MODULE__{compression: compression}) do
    raise ArgumentError,
          "unsupported sealed-segment compression: #{inspect(compression)} " <>
            "(expected one of #{inspect(@codecs)})"
  end

  defp build_store(config) do
    case Keyword.get(config, :store) do
      nil -> Store.Local.new(dir: Keyword.get(config, :dir, @default_dir))
      {impl, opts} -> impl.new(opts)
      %Store{} = store -> store
    end
  end
end
