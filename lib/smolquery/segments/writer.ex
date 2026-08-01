defmodule Smolquery.Segments.Writer do
  @moduledoc """
  Writes rows into an immutable Parquet segment.

  This is the whole write path's terminal step, and the reason DuckDB never
  writes: rows become an `Explorer.DataFrame` and Polars encodes the Parquet
  file in Rust. Both tiers use it — buffer nodes write micro-segments, the
  sealer writes large sealed segments — so the durability property has to hold
  once, here.

  Where the bytes land is `Smolquery.Segments.Store`'s business, and durability is
  its contract: this module encodes into the staging path the store provides and
  the store commits it. That split is what lets the hot tier move between local
  disk and an object store without the write path knowing.

  Stats come from the in-memory DataFrame rather than a read-back of the file:
  the numbers are the same, and the hot manifest needs them at flush time
  (Milestone 3) without a DuckDB round trip. The sealed tier needs no help
  here — DuckLake reads the Parquet footer itself when a segment is registered.

  ## Usage

      schema = Smolquery.Schema.new!([{"id", :int64}, {"ts", :timestamp}])
      rows = [%{"id" => 1, "ts" => ~N[2026-07-31 12:00:00]}]
      store = Smolquery.Segments.Store.Local.new(dir: "/data/segments")

      {:ok, segment} = Smolquery.Segments.Writer.write(rows, schema, store: store)

  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store

  @type row :: %{optional(String.t()) => term()}

  @type option ::
          {:store, Store.t()}
          | {:prefix, String.t()}
          | {:id, String.t()}
          | {:compression, atom() | {atom(), integer() | nil}}

  @orderable [:int64, :float64, :timestamp, :date]

  @doc """
  Writes `rows` as a segment in `:store`, returning the `Segment` describing it.

  Rows are maps keyed by column name; a column missing from a row is written as
  null. An `Explorer.DataFrame` may be passed instead, in which case its
  columns must already match `schema`.

  ## Options

    * `:store` (required) — the `Smolquery.Segments.Store` the segment is put in
    * `:prefix` — key prefix the segment is written under, typically a table's
      (see `Smolquery.Segments.Store.prefix/1`). Defaults to the store root.
    * `:id` — segment id, and so the last component of its key. Defaults to a
      fresh ULID.
    * `:compression` — Parquet codec, defaulting to `:zstd`

  """
  @spec write([row()] | DataFrame.t(), Schema.t(), [option()]) ::
          {:ok, Segment.t()} | {:error, term()}
  def write(rows, %Schema{} = schema, opts) do
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

  defp build_frame(%DataFrame{} = frame, _schema), do: {:ok, frame}

  defp build_frame([], _schema), do: {:error, :no_rows}

  defp build_frame(rows, schema) when is_list(rows) do
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

  defp stats(frame, %Schema{fields: fields}) do
    Map.new(fields, fn %Field{} = field ->
      series = frame[field.name]

      {field.name,
       %{
         min: bound(series, field.type, &Series.min/1),
         max: bound(series, field.type, &Series.max/1),
         null_count: Series.nil_count(series)
       }}
    end)
  end

  defp bound(series, {:numeric, _precision, _scale}, fun), do: fun.(series)
  defp bound(series, type, fun) when type in @orderable, do: fun.(series)
  defp bound(_series, _type, _fun), do: nil
end
