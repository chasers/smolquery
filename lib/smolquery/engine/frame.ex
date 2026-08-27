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

  A `VARIANT` column crosses as JSON text instead
  (`Smolquery.QueryService.VariantResults`), and the job names which columns
  those are; `to_rows/2` decodes them back into JSON values.
  """

  alias Explorer.DataFrame

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}

  @doc """
  `frame` as maps keyed by column name, with each map column's entries
  folded back into a map and each `:json_columns` column's text decoded.
  """
  @spec to_rows(DataFrame.t(), keyword()) :: [%{optional(String.t()) => term()}]
  def to_rows(%DataFrame{} = frame, opts \\ []) do
    rows = DataFrame.to_rows(frame)
    maps = map_columns(frame)
    json = Keyword.get(opts, :json_columns, [])

    case {maps, json} do
      {[], []} -> rows
      _decode -> Enum.map(rows, &(&1 |> fold_maps(maps) |> decode_json(json)))
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

  defp decode_json(row, columns) do
    Enum.reduce(columns, row, fn column, row ->
      Map.update!(row, column, &decode_text/1)
    end)
  end

  defp decode_text(nil), do: nil
  defp decode_text(text), do: JSON.decode!(text)

  defp entries_to_map(nil), do: nil
  defp entries_to_map(entries), do: Map.new(entries, &{&1["key"], &1["value"]})
end
