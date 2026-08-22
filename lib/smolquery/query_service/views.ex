defmodule Smolquery.QueryService.Views do
  @moduledoc """
  The one place a table's view SQL is rendered.

  `Smolquery.QueryService.Planner` shadows the lake with a view per
  referenced table; `Smolquery.QueryService.Scatter` defines the same view
  name over one shard's files on each worker. Both must project exactly the
  catalog's columns in catalog order — the projection is what keeps a column
  neither tier carries from leaking in — so both render through here, and a
  change to the shape cannot land in one site and not the other (PL-49
  review).
  """

  alias Smolquery.Identifier
  alias Smolquery.Schema

  @doc """
  A `read_parquet` over `sources` (paths or URLs), union-by-name.
  """
  @spec read_parquet([String.t()]) :: String.t()
  def read_parquet(sources) do
    "read_parquet([" <>
      Enum.map_join(sources, ", ", &Identifier.sql_string/1) <> "], union_by_name := true)"
  end

  @doc """
  A full `SELECT * FROM read_parquet(...)` over `sources`.
  """
  @spec parquet_select([String.t()]) :: String.t()
  def parquet_select(sources), do: "SELECT * FROM " <> read_parquet(sources)

  @doc """
  The statements defining `dataset.table` as `schema`'s columns projected
  over `from_sql`.
  """
  @spec table_view(Smolquery.Catalog.table_ref(), Schema.t(), String.t()) :: [String.t()]
  def table_view({dataset, table}, schema, from_sql) do
    ds = Identifier.quote_name!(dataset)
    t = Identifier.quote_name!(table)
    columns = Enum.map_join(schema.fields, ", ", &Identifier.quote_name!(&1.name))

    [
      "CREATE SCHEMA IF NOT EXISTS #{ds}",
      "CREATE VIEW #{ds}.#{t} AS SELECT #{columns} FROM (#{from_sql})"
    ]
  end
end
