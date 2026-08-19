defmodule Smolquery.Test.FixedCatalog do
  @moduledoc """
  A `Smolquery.Catalog` answering from a fixed map, without a DuckDB.

  `StubCatalog` proves the dispatch seam; this one lets a test choose what the
  planner reads — the pinned snapshot, each table's schema, and each
  `{table, snapshot}`'s segment list — so membership-rule cases are staged as
  data rather than driven through a real seal.
  """

  @behaviour Smolquery.Catalog

  alias Smolquery.Catalog

  @typedoc """
  The answers: `:snapshot`, `:schemas` (`table_ref => Schema.t`), and
  `:segments` (`{table_ref, snapshot} => [path]`). `:registered` overrides
  `registered_through/3`; `:stats` overrides `segment_stats/3`, which
  otherwise counts the `:segments` entry with zero rows and bytes.
  """
  @type answers :: %{
          :snapshot => Catalog.snapshot(),
          :schemas => %{Catalog.table_ref() => Smolquery.Schema.t()},
          :segments => %{{Catalog.table_ref(), Catalog.snapshot()} => [String.t()]},
          optional(:registered) => %{{Catalog.table_ref(), Catalog.snapshot()} => [String.t()]},
          optional(:stats) => %{
            {Catalog.table_ref(), Catalog.snapshot()} => Catalog.segment_stats()
          }
        }

  @spec new(answers()) :: Catalog.t()
  def new(answers), do: %Catalog{impl: __MODULE__, config: answers}

  @impl Catalog
  def current_snapshot(%{snapshot: snapshot}), do: {:ok, snapshot}

  @impl Catalog
  def table_schema(%{schemas: schemas}, table) do
    case Map.fetch(schemas, table) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, {:unknown_table, table}}
    end
  end

  @impl Catalog
  def segments(%{segments: segments}, table, snapshot),
    do: {:ok, Map.get(segments, {table, snapshot}, [])}

  @impl Catalog
  def segment_files(answers, table, snapshot) do
    with {:ok, paths} <- segments(answers, table, snapshot) do
      {:ok, Enum.map(paths, &%{path: &1, rows: 0, bytes: 0})}
    end
  end

  @impl Catalog
  def registered_through(answers, table, snapshot) do
    case Map.fetch(answers, :registered) do
      {:ok, registered} -> {:ok, Map.get(registered, {table, snapshot}, [])}
      :error -> segments(answers, table, snapshot)
    end
  end

  @impl Catalog
  def segment_stats(answers, table, snapshot) do
    case Map.fetch(answers, :stats) do
      {:ok, stats} ->
        case Map.get(stats, {table, snapshot}, %{files: 0, rows: 0, bytes: 0}) do
          {:error, _reason} = error -> error
          sizes -> {:ok, sizes}
        end

      :error ->
        with {:ok, paths} <- segments(answers, table, snapshot) do
          {:ok, %{files: length(paths), rows: 0, bytes: 0}}
        end
    end
  end

  @impl Catalog
  def connection(%{connections: connections}, name) do
    case Map.fetch(connections, name) do
      {:ok, connection} -> {:ok, connection}
      :error -> {:error, {:unknown_connection, name}}
    end
  end

  def connection(_answers, name), do: {:error, {:unknown_connection, name}}

  @impl Catalog
  def list_connections(%{connections: connections}), do: {:ok, Map.values(connections)}
  def list_connections(_answers), do: {:ok, []}

  @impl Catalog
  def create_dataset(_answers, _dataset), do: {:error, :fixed}

  @impl Catalog
  def list_datasets(_answers), do: {:error, :fixed}

  @impl Catalog
  def create_table(_answers, _table, _schema), do: {:error, :fixed}

  @impl Catalog
  def list_tables(_answers, _dataset), do: {:error, :fixed}

  @impl Catalog
  def register_segments(_answers, _table, _segments), do: {:error, :fixed}

  @impl Catalog
  def drop_segments(_answers, _table, _paths), do: {:error, :fixed}

  @impl Catalog
  def replace_segments(_answers, _table, _segments, _paths), do: {:error, :fixed}

  @impl Catalog
  def put_retention(_answers, _table, _policy), do: {:error, :fixed}

  @impl Catalog
  def retention(_answers, _table), do: {:error, :fixed}

  @impl Catalog
  def put_clustering(_answers, _table, _columns), do: {:error, :fixed}

  @impl Catalog
  def clustering(_answers, _table), do: {:error, :fixed}

  @impl Catalog
  def put_partitions(_answers, _table, _count), do: {:error, :fixed}

  @impl Catalog
  def partitions(_answers, _table), do: {:error, :fixed}

  @impl Catalog
  def expire_snapshots(_answers, _older_than_ms), do: {:error, :fixed}

  @impl Catalog
  def known_segments(_answers), do: {:error, :fixed}
end
