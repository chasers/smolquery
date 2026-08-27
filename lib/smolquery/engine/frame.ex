defmodule Smolquery.Engine.Frame do
  @moduledoc """
  Rows of a result frame as plain Elixir terms.

  `Explorer.DataFrame.to_rows/1` is almost that, with one exception this
  module exists for: a `MAP(VARCHAR, VARCHAR)` column crosses Arrow into
  Explorer as a list of `%{"key" => k, "value" => v}` structs, because
  Explorer has no map dtype. Every reader of a frame — the API's result
  pages, the web data table — wants the map back, so the conversion lives
  here once, keyed on the column's dtype rather than on the shape of any one
  value.

  A `NULL` map is the one thing that does not survive the crossing: Polars
  reads it as an empty list, so it comes out as `%{}`.
  """

  alias Explorer.DataFrame

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}

  @doc """
  `frame` as maps keyed by column name, with each map column's entries
  folded back into a map.
  """
  @spec to_rows(DataFrame.t()) :: [%{optional(String.t()) => term()}]
  def to_rows(%DataFrame{} = frame) do
    rows = DataFrame.to_rows(frame)

    case map_columns(frame) do
      [] -> rows
      columns -> Enum.map(rows, &fold_maps(&1, columns))
    end
  end

  @doc """
  The names of `frame`'s columns that carry a map.
  """
  @spec map_columns(DataFrame.t()) :: [String.t()]
  def map_columns(%DataFrame{} = frame) do
    frame
    |> DataFrame.dtypes()
    |> Enum.filter(fn {_name, dtype} -> dtype == @map_dtype end)
    |> Enum.map(fn {name, _dtype} -> name end)
  end

  defp fold_maps(row, columns) do
    Enum.reduce(columns, row, fn column, row ->
      Map.update!(row, column, &entries_to_map/1)
    end)
  end

  defp entries_to_map(nil), do: nil
  defp entries_to_map(entries), do: Map.new(entries, &{&1["key"], &1["value"]})
end
