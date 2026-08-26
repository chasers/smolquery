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
  those are; `to_rows/2` decodes them back into JSON values. Text that is not
  JSON stays text: DuckDB renders a non-finite double as `Infinity` or `NaN`,
  which no JSON decoder takes, and a string is the honest value for it.
  """

  alias Explorer.DataFrame

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}

  @doc """
  `frame` as maps keyed by column name, with each map column's entries
  folded back into a map and each `:json_columns` column's text decoded.
  """
  @spec to_rows(DataFrame.t(), keyword()) :: [%{optional(String.t()) => term()}]
  def to_rows(%DataFrame{} = frame, opts \\ []) do
    maps = map_columns(frame)
    json = Keyword.get(opts, :json_columns, [])

    frame
    |> DataFrame.to_rows()
    |> Enum.map(fn row ->
      row
      |> update_columns(maps, &entries_to_map/1)
      |> update_columns(json, &decode_text/1)
    end)
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

  defp update_columns(row, columns, fun),
    do: Enum.reduce(columns, row, &Map.replace_lazy(&2, &1, fun))

  defp decode_text(nil), do: nil

  defp decode_text(text) do
    case JSON.decode(text) do
      {:ok, value} -> value
      {:error, _not_json} -> text
    end
  end

  defp entries_to_map(nil), do: nil
  defp entries_to_map(entries), do: Map.new(entries, &{&1["key"], &1["value"]})
end
