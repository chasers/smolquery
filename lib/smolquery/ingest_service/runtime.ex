defmodule Smolquery.IngestService.Runtime do
  @moduledoc """
  A running ingest service's resolved configuration.

  The same shape as the other services' runtimes: configuration becomes
  handles once at boot and lands in `:persistent_term`, so the insert hot
  path reads them for free.

  ## Configuration

      config :smolquery, Smolquery.IngestService,
        schema_cache_ttl_ms: 60_000

  `catalog` is where insert validation resolves table schemas — the same seam
  the API and the query service use. Given options (or nothing), the service
  starts its own `Smolquery.Catalog.DuckLake` engine; given a
  `%Smolquery.Catalog{}` outright, it reads through that and starts nothing.

  `buffer_name` is the buffer service instance forward-batches go to —
  `BufferService.Client` resolves ownership from there, so a single-node
  deployment and a cluster look the same from here.

  `schema_cache_ttl_ms` bounds how stale a cached table schema may be. CRUD
  through the API invalidates the cache on the node it runs on; the TTL is
  what bounds staleness everywhere else.

  `write_partitions` spreads each table's writes over that many buffer
  identities (`Smolquery.Partitions`) — across the ring, so across nodes. The
  query service's `write_partitions` must match; see the caveats there and in
  `Smolquery.Partitions`.

  `ndjson_passthrough` forwards an `application/x-ndjson` body to the owning
  buffer as the bytes the client sent, so this node spends no CPU per row. It
  defaults to true, matching `Smolquery.BufferService`'s `:duckdb` writer.

  It only engages if the *caller* sends the ndjson content type. A caller
  posting a JSON array takes the parse-and-cast path no matter what is
  configured here, which is a 3-4x difference the server cannot see: the
  comparison rig measured 26,733 rows/s at a 901 ms ack against 105,733 at
  189 ms on one fleet, and spent most of a day looking for it in the flush
  path. `Smolquery.BufferService.Runtime` carries the other half of that story.
  """

  alias Smolquery.Catalog

  @enforce_keys [:name, :catalog]
  defstruct [
    :name,
    :catalog,
    :catalog_opts,
    buffer_name: Smolquery.BufferService,
    schema_cache_ttl_ms: 60_000,
    write_partitions: 1,
    ndjson_passthrough: true
  ]

  @type t :: %__MODULE__{
          name: atom(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          buffer_name: atom(),
          schema_cache_ttl_ms: pos_integer(),
          write_partitions: pos_integer(),
          ndjson_passthrough: boolean()
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.IngestService` supplies the defaults;
  `opts` overrides them.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.IngestService, []), opts)
    name = Keyword.get(config, :name, Smolquery.IngestService)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    %__MODULE__{
      name: name,
      catalog: catalog,
      catalog_opts: catalog_opts
    }
    |> struct!(
      Keyword.take(config, [
        :buffer_name,
        :schema_cache_ttl_ms,
        :write_partitions,
        :ndjson_passthrough
      ])
    )
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The schema cache's table (and owning process) name for an instance.
  """
  @spec cache(atom()) :: atom()
  def cache(name), do: Module.concat(name, "SchemaCache")

  @doc """
  The engine instance schema lookups resolve the catalog through.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")
end
