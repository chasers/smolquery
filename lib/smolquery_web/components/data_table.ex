defmodule SmolqueryWeb.DataTable do
  @moduledoc """
  Renders query results — a dynamic column set over rows of scalar cells.

  `Phoenix.Component`'s table wants its columns declared as slots at compile
  time; result frames only know theirs at runtime, so this renders straight
  from a column list plus row lists, with one formatting rule per value shape.
  """

  use Phoenix.Component

  alias Explorer.DataFrame
  alias Smolquery.Engine.Frame

  attr :id, :string, required: true
  attr :columns, :list, required: true
  attr :rows, :list, required: true

  def data_table(assigns) do
    ~H"""
    <div class="overflow-x-auto" id={@id}>
      <table class="table table-zebra table-sm">
        <thead>
          <tr>
            <th :for={column <- @columns} class="font-mono">{column}</th>
          </tr>
        </thead>
        <tbody>
          <tr :for={row <- @rows}>
            <td :for={value <- row} class="font-mono whitespace-nowrap">{cell(value)}</td>
          </tr>
        </tbody>
      </table>
    </div>
    """
  end

  @doc """
  One page of a result frame as `{columns, rows}` ready for `data_table/1`.
  """
  @spec frame_page(DataFrame.t(), non_neg_integer(), pos_integer()) :: {[String.t()], [[term()]]}
  def frame_page(%DataFrame{} = frame, offset, limit) do
    columns = DataFrame.names(frame)

    rows =
      frame
      |> DataFrame.slice(offset, limit)
      |> Frame.to_rows()
      |> Enum.map(fn row -> Enum.map(columns, &Map.fetch!(row, &1)) end)

    {columns, rows}
  end

  defp cell(nil), do: ""
  defp cell(value) when is_binary(value), do: value
  defp cell(value) when is_number(value) or is_boolean(value), do: to_string(value)
  defp cell(%Decimal{} = value), do: Decimal.to_string(value)
  defp cell(%Date{} = value), do: Date.to_iso8601(value)
  defp cell(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp cell(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp cell(value) when is_map(value), do: JSON.encode!(value)
  defp cell(value), do: inspect(value)
end
