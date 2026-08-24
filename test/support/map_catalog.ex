defmodule Smolquery.Test.MapCatalog do
  @moduledoc """
  A mutable in-memory `Smolquery.Catalog` for exercising the CRUD surface.

  Mirrors the DuckLake implementation's observable semantics without a DuckDB:
  creates are `IF NOT EXISTS` (re-creating keeps the original schema and still
  answers `:ok`), listings sort, and a missing table is
  `{:error, {:unknown_table, ref}}`. Segment callbacks are unsupported — this
  catalog exists for the dataset/table surface.

  Federated connections are supported here (T-323), because the API's
  connection routes are the CRUD surface this double exists to exercise. It
  mirrors DuckLake's semantics: a put replaces by name and keeps the original
  `created_at`, listings sort by name, a missing name is
  `{:error, {:unknown_connection, name}}`, and deleting an absent name is `:ok`.
  """

  @behaviour Smolquery.Catalog

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Catalog.Dataset
  alias Smolquery.Schema

  @spec new() :: Catalog.t()
  def new do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{
          datasets: %{},
          tables: %{},
          retention: %{},
          clustering: %{},
          partitions: %{},
          connections: %{}
        }
      end)

    %Catalog{impl: __MODULE__, config: agent}
  end

  @impl Catalog
  def put_connection(agent, %Connection{} = connection) do
    now = System.system_time(:millisecond)

    Agent.update(agent, fn state ->
      created_at =
        case Map.fetch(state.connections, connection.name) do
          {:ok, existing} -> existing.created_at
          :error -> now
        end

      stored = %{connection | created_at: created_at, updated_at: now}

      %{state | connections: Map.put(state.connections, connection.name, stored)}
    end)
  end

  @impl Catalog
  def connection(agent, name) do
    case Agent.get(agent, &Map.fetch(&1.connections, name)) do
      {:ok, connection} -> {:ok, connection}
      :error -> {:error, {:unknown_connection, name}}
    end
  end

  @impl Catalog
  def list_connections(agent) do
    {:ok, agent |> Agent.get(& &1.connections) |> Map.values() |> Enum.sort_by(& &1.name)}
  end

  @impl Catalog
  def delete_connection(agent, name) do
    Agent.update(agent, &%{&1 | connections: Map.delete(&1.connections, name)})
  end

  @impl Catalog
  def create_dataset(agent, %Dataset{} = dataset) do
    Agent.get_and_update(agent, &put_new_dataset(&1, dataset))
  end

  defp put_new_dataset(state, dataset) do
    now = System.system_time(:millisecond)

    case Map.get(state.datasets, dataset.name) do
      nil ->
        stored = %{dataset | created_at: now, updated_at: now}
        {:ok, %{state | datasets: Map.put(state.datasets, dataset.name, stored)}}

      existing ->
        if Dataset.same_settings?(existing, dataset),
          do: {:ok, state},
          else: {{:error, {:dataset_exists, dataset.name}}, state}
    end
  end

  @impl Catalog
  def list_datasets(agent) do
    {:ok, agent |> Agent.get(& &1.datasets) |> Map.keys() |> Enum.sort()}
  end

  @impl Catalog
  def dataset(agent, name) do
    case Agent.get(agent, &Map.get(&1.datasets, name)) do
      nil -> {:error, {:unknown_dataset, name}}
      dataset -> {:ok, dataset}
    end
  end

  @impl Catalog
  def datasets(agent) do
    {:ok, agent |> Agent.get(& &1.datasets) |> Map.values() |> Enum.sort_by(& &1.name)}
  end

  @impl Catalog
  def update_dataset(agent, %Dataset{} = dataset) do
    stored = %{dataset | updated_at: System.system_time(:millisecond)}
    Agent.update(agent, &%{&1 | datasets: Map.put(&1.datasets, dataset.name, stored)})
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
    Agent.update(agent, &raise_partitions(&1, table_ref, count))
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
      |> apply_partitions(options, table_ref)
    end)
  end

  defp apply_partitions(state, options, table_ref) do
    case Map.fetch(options, :partitions) do
      {:ok, count} -> raise_partitions(state, table_ref, count)
      :error -> state
    end
  end

  defp raise_partitions(state, table_ref, count) do
    %{state | partitions: Map.update(state.partitions, table_ref, count, &max(&1, count))}
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
  def current_snapshot(agent, _dataset), do: current_snapshot(agent)

  @impl Catalog
  def known_segments(_agent), do: {:error, :not_supported}
end
