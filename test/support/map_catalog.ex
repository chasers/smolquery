defmodule Smolquery.Test.MapCatalog do
  @moduledoc """
  A mutable in-memory `Smolquery.Catalog` for exercising the CRUD surface.

  Mirrors the DuckLake implementation's observable semantics without a DuckDB:
  creates are `IF NOT EXISTS` (re-creating keeps the original schema and still
  answers `:ok`), listings sort, and a missing table is
  `{:error, {:unknown_table, ref}}`. Segment callbacks are unsupported — this
  catalog exists for the dataset/table surface.
  """

  @behaviour Smolquery.Catalog

  alias Smolquery.Catalog
  alias Smolquery.Schema

  @spec new() :: Catalog.t()
  def new do
    {:ok, agent} =
      Agent.start_link(fn -> %{datasets: MapSet.new(), tables: %{}, retention: %{}} end)

    %Catalog{impl: __MODULE__, config: agent}
  end

  @impl Catalog
  def create_dataset(agent, dataset) do
    Agent.update(agent, &%{&1 | datasets: MapSet.put(&1.datasets, dataset)})
  end

  @impl Catalog
  def list_datasets(agent) do
    {:ok, agent |> Agent.get(& &1.datasets) |> Enum.sort()}
  end

  @impl Catalog
  def create_table(agent, table_ref, %Schema{} = schema) do
    Agent.update(agent, &%{&1 | tables: Map.put_new(&1.tables, table_ref, schema)})
  end

  @impl Catalog
  def list_tables(agent, dataset) do
    tables =
      agent
      |> Agent.get(& &1.tables)
      |> Map.keys()
      |> Enum.filter(fn {ds, _table} -> ds == dataset end)
      |> Enum.map(fn {_ds, table} -> table end)
      |> Enum.sort()

    {:ok, tables}
  end

  @impl Catalog
  def table_schema(agent, table_ref) do
    case agent |> Agent.get(& &1.tables) |> Map.fetch(table_ref) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, {:unknown_table, table_ref}}
    end
  end

  @impl Catalog
  def register_segments(_agent, _table_ref, _segments), do: {:error, :not_supported}

  @impl Catalog
  def segments(_agent, _table_ref, _snapshot), do: {:error, :not_supported}

  @impl Catalog
  def registered_through(_agent, _table_ref, _snapshot), do: {:error, :not_supported}

  @impl Catalog
  def drop_segments(_agent, _table_ref, _paths), do: {:error, :not_supported}

  @impl Catalog
  def replace_segments(_agent, _table_ref, _segments, _paths), do: {:error, :not_supported}

  @impl Catalog
  def put_retention(agent, table_ref, nil) do
    Agent.update(agent, &%{&1 | retention: Map.delete(&1.retention, table_ref)})
  end

  def put_retention(agent, table_ref, %{column: _column, ttl_ms: _ttl_ms} = policy) do
    Agent.update(agent, &%{&1 | retention: Map.put(&1.retention, table_ref, policy)})
  end

  @impl Catalog
  def retention(agent, table_ref) do
    {:ok, agent |> Agent.get(& &1.retention) |> Map.get(table_ref)}
  end

  @impl Catalog
  def expire_snapshots(_agent, _older_than_ms), do: {:ok, 0}

  @impl Catalog
  def current_snapshot(_agent), do: {:error, :not_supported}

  @impl Catalog
  def known_segments(_agent), do: {:error, :not_supported}
end
