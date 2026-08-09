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

  ## Sorting on the clustering key

  Rows are sorted by the schema's clustering key before the frame is encoded —
  stably, in declared order, nulls last — so the row-group stats above are tight
  enough for a reader to prune on. An empty key sorts nothing, and both
  row-list and DataFrame inputs take the same path: the frame is built first
  and Polars sorts it. Sorting the frame rather than the row list is a
  correctness requirement, not a convenience — Elixir's `Enum.sort` orders
  `NaiveDateTime`, `Date` and `Decimal` values by Erlang term order, which
  compares struct fields alphabetically (`:day` before `:month` before
  `:year`), so January 31 would sort after February 1. Polars compares the
  column's logical values.

  The columns come from `Smolquery.Schema.clustering_columns/1` rather than the
  `:clustering` field, so a key naming a column this schema no longer has sorts
  by the rest instead of failing the write. That function documents why the two
  can differ.

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
           Store.put(store, key, &encode_parquet(frame, &1, compression)) do
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

  @doc """
  Merges a group commit's accumulated chunks — row lists and/or DataFrames,
  oldest first — into the single `[row()] | DataFrame.t()` that `write/3`
  takes.

  All-list chunks stay a row list, exactly the shape the accumulator used to
  concatenate itself. Once any chunk is a frame, every list chunk becomes one
  (schema-ordered columns, so the frames agree) and the frames concatenate —
  rows never materialize as terms on the commit path that was fed frames.
  """
  @spec merge_chunks([[row()] | DataFrame.t()], Schema.t()) ::
          {:ok, [row()] | DataFrame.t()} | {:error, term()}
  def merge_chunks([chunk], _schema), do: {:ok, chunk}

  def merge_chunks(chunks, schema) when is_list(chunks) do
    if Enum.all?(chunks, &is_list/1) do
      {:ok, Enum.concat(chunks)}
    else
      with {:ok, frames} <- chunk_frames(chunks, schema) do
        {:ok, DataFrame.concat_rows(frames)}
      end
    end
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, {:invalid_rows, Exception.message(error)}}
  end

  defp chunk_frames(chunks, schema) do
    Enum.reduce_while(chunks, {:ok, []}, fn
      %DataFrame{} = frame, {:ok, frames} ->
        {:cont, {:ok, [frame | frames]}}

      rows, {:ok, frames} ->
        case frame_from_rows(rows, schema) do
          {:ok, frame} -> {:cont, {:ok, [frame | frames]}}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
    |> case do
      {:ok, frames} -> {:ok, Enum.reverse(frames)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Builds the unsorted frame for `rows` — the schema's columns, in order.
  """
  @spec frame_from_rows([row()], Schema.t()) :: {:ok, DataFrame.t()} | {:error, term()}
  def frame_from_rows(rows, %Schema{} = schema) when is_list(rows) do
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

  defp encode_parquet(frame, device, compression) do
    DataFrame.to_parquet(frame, device, compression: compression)
  end

  defp build_frame(%DataFrame{} = frame, schema), do: {:ok, sort_frame(frame, schema)}

  defp build_frame([], _schema), do: {:error, :no_rows}

  defp build_frame(rows, schema) when is_list(rows) do
    with {:ok, frame} <- frame_from_rows(rows, schema) do
      {:ok, sort_frame(frame, schema)}
    end
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
