defmodule Smolquery.Engine.Result do
  @moduledoc """
  The neutral result shape every read path returns.

  `Adbc.Result` is column-oriented, batched, and carries Arrow metadata. This
  struct is the seam between the engine and everything above it: ordered column
  names plus row tuples of plain Elixir terms. Swapping the engine (or adding a
  second one) means writing a new `from_adbc/1`-alike, not touching callers.
  """

  defstruct columns: [], rows: [], num_rows: 0

  @type t :: %__MODULE__{
          columns: [String.t()],
          rows: [[term()]],
          num_rows: non_neg_integer()
        }

  @doc """
  Builds a result from an `Adbc.Result`, flattening its record batches.
  """
  @spec from_adbc(Adbc.Result.t()) :: t()
  def from_adbc(%Adbc.Result{data: nil, num_rows: num_rows}) do
    %__MODULE__{columns: [], rows: [], num_rows: num_rows || 0}
  end

  def from_adbc(%Adbc.Result{data: batches}) do
    columns =
      case batches do
        [first | _] -> Enum.map(first, & &1.field.name)
        [] -> []
      end

    rows = Enum.flat_map(batches, &batch_to_rows/1)

    %__MODULE__{columns: columns, rows: rows, num_rows: length(rows)}
  end

  @doc """
  Rows as maps keyed by column name.
  """
  @spec to_maps(t()) :: [%{optional(String.t()) => term()}]
  def to_maps(%__MODULE__{columns: columns, rows: rows}) do
    Enum.map(rows, fn row -> columns |> Enum.zip(row) |> Map.new() end)
  end

  @doc """
  The single value of a one-row, one-column result.
  """
  @spec one!(t()) :: term()
  def one!(%__MODULE__{rows: [[value]]}), do: value

  def one!(%__MODULE__{columns: columns, rows: rows}) do
    raise ArgumentError,
          "expected exactly one row and one column, got #{length(rows)} row(s) " <>
            "and #{length(columns)} column(s)"
  end

  defp batch_to_rows([]), do: []

  defp batch_to_rows(batch) do
    batch
    |> Enum.map(&Adbc.Column.to_list/1)
    |> Enum.zip_with(& &1)
  end
end
