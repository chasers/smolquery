defmodule Smolquery.Test.SegmentFixture do
  @moduledoc """
  The fixture writer: builds a Parquet segment from rows through Explorer.

  Tests and benches use it to put segments in a store without spooling NDJSON
  and running a DuckDB engine. Nothing in a deployment writes this way — a
  flush is one DuckDB `COPY` (`Smolquery.Segments.Writer`, PL-57) — so this
  lives outside `lib/`. Benches load it with `Code.require_file/2` from
  `bench/support.exs`.

  It refuses a schema Explorer cannot write (a map or a variant), sorts the
  frame on the clustering key the way the flush sorts in DuckDB, and carries
  the same manifest stats shape (`min`, `max`, `null_count`) so a fixture
  segment prunes like a real one.
  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store

  @orderable [:int64, :float64, :timestamp, :date]

  @doc """
  Writes `rows` as a segment in `:store`, returning the `Segment` describing it.

  Rows are maps keyed by column name; a column missing from a row is written as
  null. Options are `:store` (required), `:prefix`, `:id`, and `:compression`,
  with the meanings `Smolquery.Segments.Writer.write/3` gives them.
  """
  @spec write([map()], Schema.t(), keyword()) :: {:ok, Segment.t()} | {:error, term()}
  def write(rows, %Schema{} = schema, opts) when is_list(rows) do
    store = Keyword.fetch!(opts, :store)
    prefix = Keyword.get(opts, :prefix, "")
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    compression = Keyword.get(opts, :compression, :zstd)

    with {:ok, key} <- Store.key(prefix, id),
         {:ok, frame} <- build_frame(rows, schema),
         {:ok, put} <-
           Store.put(store, key, &DataFrame.to_parquet(frame, &1, compression: compression)) do
      {:ok,
       %Segment{
         id: id,
         key: key,
         path: put.location,
         row_count: DataFrame.n_rows(frame),
         byte_size: put.byte_size,
         stats: stats(frame, schema)
       }}
    end
  end

  defp build_frame([], _schema), do: {:error, :no_rows}

  defp build_frame(rows, schema) do
    with :none <- Schema.explorer_unwritable(schema),
         {:ok, frame} <- frame_from_rows(rows, schema) do
      {:ok, sort_frame(frame, schema)}
    else
      {:ok, %Field{type: type}} -> {:error, {:unsupported_type, type}}
      {:error, _reason} = error -> error
    end
  end

  defp frame_from_rows(rows, schema) do
    with {:ok, dtypes} <- Schema.explorer_dtypes(schema) do
      columns =
        Enum.map(dtypes, fn {name, dtype} ->
          {name, Series.from_list(Enum.map(rows, &Map.get(&1, name)), dtype: dtype)}
        end)

      {:ok, DataFrame.new(columns)}
    end
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, {:invalid_rows, Exception.message(error)}}
  end

  defp sort_frame(frame, schema) do
    case Schema.clustering_columns(schema) do
      [] ->
        frame

      columns ->
        DataFrame.sort_with(frame, fn lf -> Enum.map(columns, &lf[&1]) end,
          stable: true,
          nils: :last
        )
    end
  end

  defp stats(frame, %Schema{fields: fields} = schema) do
    sorted = leading_clustering_column(schema)
    row_count = DataFrame.n_rows(frame)

    Map.new(fields, fn %Field{} = field ->
      series = frame[field.name]
      null_count = Series.nil_count(series)

      {field.name,
       %{
         min: bound(series, field, sorted, :min, row_count, null_count),
         max: bound(series, field, sorted, :max, row_count, null_count),
         null_count: null_count
       }}
    end)
  end

  defp bound(series, %Field{name: name, type: :string}, name, which, row_count, null_count) do
    case row_count - null_count do
      0 -> nil
      present -> Series.at(series, if(which == :min, do: 0, else: present - 1))
    end
  end

  defp bound(series, %Field{type: {:numeric, _precision, _scale}}, _sorted, which, _rows, _nulls),
    do: extreme(series, which)

  defp bound(series, %Field{type: type}, _sorted, which, _rows, _nulls) when type in @orderable,
    do: extreme(series, which)

  defp bound(_series, _field, _sorted, _which, _rows, _nulls), do: nil

  defp extreme(series, :min), do: Series.min(series)
  defp extreme(series, :max), do: Series.max(series)

  defp leading_clustering_column(%Schema{} = schema) do
    case Schema.clustering_columns(schema) do
      [column | _rest] -> column
      [] -> nil
    end
  end
end
