defmodule Smolquery.IngestService.SchemaCache do
  @moduledoc """
  Table schemas, cached so the insert hot path does not pay a catalog query
  per request.

  A public named ETS table owned by a GenServer that does nothing else —
  reads and misses run entirely in the caller, so the cache adds no process
  hop. Entries expire after the runtime's `schema_cache_ttl_ms`; the API's
  CRUD routes invalidate eagerly on the node they run on, and the TTL bounds
  staleness for everything else (another node's CRUD, out-of-band catalog
  changes).

  A miss reads through `Smolquery.Catalog.table_schema/2` and caches only
  success — an unknown table stays a per-request catalog answer rather than a
  cached rejection that would outlive the table's creation.
  """

  use GenServer

  alias Smolquery.Catalog
  alias Smolquery.IngestService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Segments.Store

  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.cache(runtime.name))
  end

  @doc """
  The table's schema, from cache or through the catalog.
  """
  @spec fetch(Runtime.t(), Store.table_ref()) :: {:ok, Schema.t()} | {:error, term()}
  def fetch(%Runtime{} = runtime, table_ref) do
    table = Runtime.cache(runtime.name)

    case :ets.lookup(table, table_ref) do
      [{^table_ref, schema, expires_at}] ->
        if now() < expires_at, do: {:ok, schema}, else: read_through(runtime, table, table_ref)

      [] ->
        read_through(runtime, table, table_ref)
    end
  end

  @doc """
  Drops a table's cached schema, so the next insert reads the catalog.
  """
  @spec invalidate(Runtime.t(), Store.table_ref()) :: :ok
  def invalidate(%Runtime{} = runtime, table_ref) do
    :ets.delete(Runtime.cache(runtime.name), table_ref)

    :ok
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    table =
      :ets.new(Runtime.cache(runtime.name), [
        :named_table,
        :public,
        :set,
        read_concurrency: true
      ])

    {:ok, table}
  end

  defp read_through(runtime, table, table_ref) do
    with {:ok, schema} <- Catalog.table_schema(runtime.catalog, table_ref) do
      :ets.insert(table, {table_ref, schema, now() + runtime.schema_cache_ttl_ms})

      {:ok, schema}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
