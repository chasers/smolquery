defmodule Smolquery.Test.StubCatalog do
  @moduledoc """
  A `Smolquery.Catalog` implementation that records the calls it receives.

  Proves the seam holds without a DuckDB: every dispatch function must reach
  the implementation module with the configuration it was built with, which is
  what lets a caller be tested against a catalog that never touches disk.
  """

  @behaviour Smolquery.Catalog

  alias Smolquery.Catalog
  alias Smolquery.Schema

  @schema Schema.new!([{"id", :int64}])
  @snapshot 7

  @doc """
  A catalog handle that sends each call to `owner` as `{:called, function, args}`.
  """
  @spec new(pid()) :: Catalog.t()
  def new(owner), do: %Catalog{impl: __MODULE__, config: owner}

  @doc """
  The schema `table_schema/2` answers with.
  """
  @spec schema() :: Schema.t()
  def schema, do: @schema

  @doc """
  The snapshot every mutating callback reports.
  """
  @spec snapshot() :: non_neg_integer()
  def snapshot, do: @snapshot

  @impl Catalog
  def create_dataset(owner, dataset), do: record(owner, :create_dataset, [dataset], :ok)

  @impl Catalog
  def list_datasets(owner), do: record(owner, :list_datasets, [], {:ok, ["analytics"]})

  @impl Catalog
  def create_table(owner, table, schema),
    do: record(owner, :create_table, [table, schema], :ok)

  @impl Catalog
  def list_tables(owner, dataset), do: record(owner, :list_tables, [dataset], {:ok, ["events"]})

  @impl Catalog
  def table_schema(owner, table), do: record(owner, :table_schema, [table], {:ok, @schema})

  @impl Catalog
  def register_segments(owner, table, segments),
    do: record(owner, :register_segments, [table, segments], {:ok, @snapshot})

  @impl Catalog
  def segments(owner, table, snapshot),
    do: record(owner, :segments, [table, snapshot], {:ok, ["/stub.parquet"]})

  @impl Catalog
  def drop_segments(owner, table, paths),
    do: record(owner, :drop_segments, [table, paths], {:ok, @snapshot})

  @impl Catalog
  def replace_segments(owner, table, segments, paths),
    do: record(owner, :replace_segments, [table, segments, paths], {:ok, @snapshot})

  @impl Catalog
  def current_snapshot(owner), do: record(owner, :current_snapshot, [], {:ok, @snapshot})

  @impl Catalog
  def known_segments(owner), do: record(owner, :known_segments, [], {:ok, ["/stub.parquet"]})

  defp record(owner, function, args, reply) do
    send(owner, {:called, function, args})

    reply
  end
end
