defmodule Smolquery.Segments.Writer do
  @moduledoc """
  Writes rows into an immutable Parquet segment.

  This is the whole write path's terminal step, and the reason DuckDB never
  writes: rows become an `Explorer.DataFrame` and Polars encodes the Parquet
  file in Rust. Both tiers use it — buffer nodes write micro-segments, the
  sealer writes large sealed segments — so the durability property has to hold
  once, here.

  A segment is never observable half-written. The file lands in a `.tmp`
  subdirectory of the target directory and is renamed into place, so a reader
  listing the directory, or a crash mid-encode, sees either nothing or a
  complete segment. The rename is within one filesystem, which makes it atomic.

  Stats come from the in-memory DataFrame rather than a read-back of the file:
  the numbers are the same, and the hot manifest needs them at flush time
  (Milestone 3) without a DuckDB round trip. The sealed tier needs no help
  here — DuckLake reads the Parquet footer itself when a segment is registered.

  ## Usage

      schema = Smolquery.Schema.new!([{"id", :int64}, {"ts", :timestamp}])
      rows = [%{"id" => 1, "ts" => ~N[2026-07-31 12:00:00]}]

      {:ok, segment} = Smolquery.Segments.Writer.write(rows, schema, dir: "/data/segments")

  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Segment

  @type row :: %{optional(String.t()) => term()}

  @type option ::
          {:dir, String.t()}
          | {:id, String.t()}
          | {:compression, atom() | {atom(), integer() | nil}}

  @orderable [:int64, :float64, :timestamp, :date]

  @doc """
  Writes `rows` as a segment in `:dir`, returning the `Segment` describing it.

  Rows are maps keyed by column name; a column missing from a row is written as
  null. An `Explorer.DataFrame` may be passed instead, in which case its
  columns must already match `schema`.

  ## Options

    * `:dir` (required) — directory the segment is written into
    * `:id` — segment id, and so its filename. Defaults to a fresh ULID.
    * `:compression` — Parquet codec, defaulting to `:zstd`

  """
  @spec write([row()] | DataFrame.t(), Schema.t(), [option()]) ::
          {:ok, Segment.t()} | {:error, term()}
  def write(rows, %Schema{} = schema, opts) do
    dir = Keyword.fetch!(opts, :dir)
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    compression = Keyword.get(opts, :compression, :zstd)

    with {:ok, id} <- validate_id(id),
         {:ok, frame} <- build_frame(rows, schema),
         {:ok, path} <- encode(frame, dir, id, compression) do
      {:ok,
       %Segment{
         id: id,
         path: path,
         row_count: DataFrame.n_rows(frame),
         byte_size: File.stat!(path).size,
         stats: stats(frame, schema)
       }}
    end
  end

  @doc """
  The path a segment with `id` occupies in `dir`.
  """
  @spec path(String.t(), String.t()) :: String.t()
  def path(dir, id), do: Path.join(dir, id <> ".parquet")

  defp validate_id(id) do
    if Id.valid?(id), do: {:ok, id}, else: {:error, {:invalid_segment_id, id}}
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

  defp encode(frame, dir, id, compression) do
    scratch = Path.join(dir, ".tmp")
    :ok = File.mkdir_p!(scratch)
    target = path(dir, id)
    staged = Path.join(scratch, id <> ".parquet")

    with :ok <- DataFrame.to_parquet(frame, staged, compression: compression),
         :ok <- File.rename(staged, target) do
      {:ok, target}
    else
      {:error, reason} -> {:error, {:write_failed, reason}}
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
