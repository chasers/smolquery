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
      Agent.start_link(fn ->
        %{datasets: MapSet.new(), tables: %{}, retention: %{}, clustering: %{}, partitions: %{}}
      end)

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
    state = Agent.get(agent, & &1)

    case Map.fetch(state.tables, table_ref) do
      {:ok, schema} ->
        clustering = Map.get(state.clustering, table_ref, [])
        partitions = Map.get(state.partitions, table_ref)
        {:ok, %{schema | clustering: clustering, partitions: partitions}}

      :error ->
        {:error, {:unknown_table, table_ref}}
    end
  end

  @impl Catalog
  def register_segments(_agent, _table_ref, _segments), do: {:error, :not_supported}

  @impl Catalog
  def segments(_agent, _table_ref, _snapshot), do: {:error, :not_supported}

  @impl Catalog
  def registered_through(_agent, _table_ref, _snapshot), do: {:error, :not_supported}

  @impl Catalog
  def segment_stats(_agent, _table_ref, _snapshot), do: {:error, :not_supported}

  @impl Catalog
  def segment_files(_agent, _table_ref, _snapshot), do: {:error, :not_supported}

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
  def put_clustering(agent, table_ref, []) do
    Agent.update(agent, &%{&1 | clustering: Map.delete(&1.clustering, table_ref)})
  end

  def put_clustering(agent, table_ref, columns) when is_list(columns) do
    Agent.update(agent, &%{&1 | clustering: Map.put(&1.clustering, table_ref, columns)})
  end

  @impl Catalog
  def clustering(agent, table_ref) do
    {:ok, agent |> Agent.get(& &1.clustering) |> Map.get(table_ref, [])}
  end

  @impl Catalog
  def put_partitions(agent, table_ref, count) when is_integer(count) and count > 0 do
    Agent.update(agent, &%{&1 | partitions: Map.put(&1.partitions, table_ref, count)})
  end

  @impl Catalog
  def partitions(agent, table_ref) do
    {:ok, agent |> Agent.get(& &1.partitions) |> Map.get(table_ref)}
  end

  @impl Catalog
  def put_table_options(agent, table_ref, options) when is_map(options) do
    Agent.update(agent, fn state ->
      state
      |> apply_option(options, :retention, table_ref)
      |> apply_option(options, :clustering, table_ref)
      |> apply_option(options, :partitions, table_ref)
    end)
  end

  defp apply_option(state, options, key, table_ref) do
    case Map.fetch(options, key) do
      {:ok, value} when value in [nil, []] -> %{state | key => Map.delete(state[key], table_ref)}
      {:ok, value} -> %{state | key => Map.put(state[key], table_ref, value)}
      :error -> state
    end
  end

  @impl Catalog
  def expire_snapshots(_agent, _older_than_ms), do: {:ok, 0}

  @impl Catalog
  def current_snapshot(_agent), do: {:error, :not_supported}

  @impl Catalog
  def known_segments(_agent), do: {:error, :not_supported}
end
