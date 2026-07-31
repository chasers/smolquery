defmodule Smolquery.Engine.Result do
  @moduledoc """
  The result shape for bounded reads.

  `Adbc.Result` is column-oriented, batched, and carries Arrow metadata. This
  struct is the seam between the engine and everything above it: ordered column
  names plus row tuples of plain Elixir terms. Swapping the engine (or adding a
  second one) means writing a new `from_adbc/1`-alike, not touching callers.

  ## This shape is for bounded results only

  Converting Arrow to Elixir terms costs about a kilobyte and two microseconds
  per row, nearly all of it transposing columns into rows: five million rows take
  11.5 s and 4.8 GiB, against 307 ms and almost no heap for the same batch left in
  Arrow (`bench/adbc.exs`). Every query the system asks itself — catalog lookups,
  snapshot ids, counts — returns tens of rows and belongs here. A user query that
  might match millions does not; that is `Smolquery.Engine.frame/3`.

  `Smolquery.Engine` enforces the distinction rather than documenting it, refusing
  a conversion over `:max_result_rows` with `Smolquery.Engine.ResultTooLarge`.
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
    rows = Enum.flat_map(batches, &batch_to_rows/1)

    %__MODULE__{columns: column_names(batches), rows: rows, num_rows: length(rows)}
  end

  @doc """
  Builds a result, refusing one longer than `max_rows`.

  A batch is converted, counted, and kept only while the running total is within
  the limit, so no more than `max_rows` rows are ever built as Elixir terms. There
  is no cheaper check available: an `Adbc.Result` holds raw Arrow buffers whose row
  count cannot be read without interpreting each type's memory layout, and the
  driver leaves `num_rows` nil for a select. Stopping early is the check.

  This bounds the conversion, not the query — ADBC has already fetched the whole
  Arrow result by the time this runs.

  `:infinity` converts whatever came back.
  """
  @spec from_adbc(Adbc.Result.t(), pos_integer() | :infinity) ::
          {:ok, t()} | {:error, :too_many_rows}
  def from_adbc(%Adbc.Result{} = result, :infinity), do: {:ok, from_adbc(result)}

  def from_adbc(%Adbc.Result{data: nil, num_rows: num_rows}, _max_rows) do
    {:ok, %__MODULE__{columns: [], rows: [], num_rows: num_rows || 0}}
  end

  def from_adbc(%Adbc.Result{data: batches}, max_rows) do
    batches
    |> Enum.reduce_while({0, []}, fn batch, {count, acc} ->
      rows = batch_to_rows(batch)
      total = count + length(rows)

      if total > max_rows do
        {:halt, :too_many_rows}
      else
        {:cont, {total, [rows | acc]}}
      end
    end)
    |> case do
      :too_many_rows ->
        {:error, :too_many_rows}

      {count, acc} ->
        rows = acc |> Enum.reverse() |> Enum.concat()

        {:ok, %__MODULE__{columns: column_names(batches), rows: rows, num_rows: count}}
    end
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

  defp column_names([first | _rest]), do: Enum.map(first, & &1.field.name)
  defp column_names([]), do: []

  defp batch_to_rows([]), do: []

  defp batch_to_rows(batch) do
    batch
    |> Enum.map(&Adbc.Column.to_list/1)
    |> Enum.zip_with(& &1)
  end
end
