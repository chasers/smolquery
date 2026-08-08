defmodule Smolquery.Segments.Writer do
  @moduledoc """
  Writes rows into an immutable Parquet segment.

  This is the whole write path's terminal step, and the reason DuckDB never
  writes: a batch becomes an `Explorer.DataFrame` and Polars encodes the Parquet
  file in Rust. Both tiers use it — buffer nodes write micro-segments, the
  sealer writes large sealed segments — so the durability property has to hold
  once, here.

  ## Columns, not rows

  A batch arrives already transposed (`t:columns/0`), because a `DataFrame` is
  column-major and every row-shaped term between the socket and here has to be
  taken apart again to build one. Accepting rows means walking the batch once
  per column — `Map.get/2` on every row, 62 times for an OTel-shaped table — to
  undo a grouping nothing asked for. `Smolquery.IngestService.Validator` has to
  visit every value anyway, so it emits columns and that pass disappears.

  Rows are still accepted, for the sealer and for callers with a handful of
  them, and `bench/columnar.exs` is what measured the difference.

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
  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store

  @type row :: %{optional(String.t()) => term()}

  @typedoc """
  A batch already in column order: one list of values per field of the schema,
  in `t:Smolquery.Schema.t/0`'s `:fields` order, every one the same length.

  This is what the write path carries. `Smolquery.IngestService.Validator`
  produces it directly, so nothing between the socket and Polars ever holds a
  row-shaped term — see this module's "Columns, not rows" section.
  """
  @type columns :: {:columns, [[term()]]}

  @typedoc """
  A batch that is still bytes on disk: NDJSON files the API spooled without
  parsing. DuckDB reads them itself, sorts on the clustering key and writes the
  Parquet, so no row in the batch ever becomes an Elixir term.

  Needs `:engine` and a store whose staging path is a local file — DuckDB's
  `COPY` writes to a filesystem path, not to an object store.
  """
  @type ndjson :: {:ndjson, [Path.t()]}

  @type option ::
          {:store, Store.t()}
          | {:prefix, String.t()}
          | {:id, String.t()}
          | {:compression, atom() | {atom(), integer() | nil}}
          | {:engine, atom()}

  @orderable [:int64, :float64, :timestamp, :date]

  @doc """
  Writes `rows` as a segment in `:store`, returning the `Segment` describing it.

  Takes any of three shapes. `{:columns, columns}` is what the write path uses
  and is described by `t:columns/0`. Rows as maps keyed by column name are
  accepted too — a column missing from a row is written as null — as is an
  `Explorer.DataFrame`, whose columns must already match `schema`.

  ## Options

    * `:store` (required) — the `Smolquery.Segments.Store` the segment is put in
    * `:prefix` — key prefix the segment is written under, typically a table's
      (see `Smolquery.Segments.Store.prefix/1`). Defaults to the store root.
    * `:id` — segment id, and so the last component of its key. Defaults to a
      fresh ULID.
    * `:compression` — Parquet codec, defaulting to `:zstd`

  """
  @spec write(columns() | [row()] | DataFrame.t(), Schema.t(), [option()]) ::
          {:ok, Segment.t()} | {:error, term()}
  def write({:ndjson, paths}, %Schema{} = schema, opts) when is_list(paths) do
    store = Keyword.fetch!(opts, :store)
    engine = Keyword.fetch!(opts, :engine)
    prefix = Keyword.get(opts, :prefix, "")
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    compression = Keyword.get(opts, :compression, :zstd)

    with :ok <- some_paths(paths),
         {:ok, key} <- Store.key(prefix, id),
         {:ok, put} <-
           Store.put(store, key, &copy_ndjson(engine, paths, &1, schema, compression)),
         {:ok, row_count} <- footer_rows(engine, put.location),
         {:ok, stats} <- read_stats(engine, put.location, schema) do
      {:ok,
       %Segment{
         id: id,
         key: key,
         path: put.location,
         row_count: row_count,
         byte_size: put.byte_size,
         stats: stats
       }}
    end
  end

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

  defp encode_parquet(frame, device, compression) do
    DataFrame.to_parquet(frame, device, compression: compression)
  end

  defp some_paths([]), do: {:error, :no_rows}
  defp some_paths(_paths), do: :ok

  # One statement for the whole flush: DuckDB reads every spooled body, sorts the
  # union on the clustering key and writes one Parquet file. `read_json` with an
  # explicit `columns` map pins the types, so nothing is inferred per request and
  # the file matches the schema by construction.
  #
  # Paths are parameters rather than interpolated: a spool path is generated here,
  # but a quoted literal in a COPY is exactly the place a future caller-supplied
  # name would become an injection.
  defp copy_ndjson(engine, paths, staged, schema, compression) do
    sql = """
    COPY (
      SELECT * FROM read_json([#{placeholders(length(paths))}],
        format = 'newline_delimited',
        columns = {#{columns_spec(schema)}})#{order_clause(schema)}
    )
    TO $#{length(paths) + 1} (FORMAT PARQUET, COMPRESSION #{codec(compression)})
    """

    case Engine.query(engine, sql, paths ++ [staged]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, {:ndjson_copy_failed, Exception.message(error)}}
    end
  end

  # The footer, not the data: `parquet_file_metadata` reads the tail of the file,
  # so the authoritative row count costs no pass over the rows. Trusting a count
  # the caller sent instead would let a miscounted body scale the ack.
  defp footer_rows(engine, path) do
    case Engine.query(engine, "SELECT num_rows FROM parquet_file_metadata($1)", [path]) do
      {:ok, %{rows: [[rows] | _rest]}} -> {:ok, rows}
      {:ok, _other} -> {:error, {:ndjson_copy_failed, "no parquet footer at #{path}"}}
      {:error, error} -> {:error, {:ndjson_copy_failed, Exception.message(error)}}
    end
  end

  # The manifest's stats, computed by DuckDB over the file just written. One pass
  # over a local file still in the page cache.
  #
  # Unlike the Polars path this includes bounds for string columns:
  # `Explorer.Series.min/1` raises for `:string`, so `@orderable` cannot carry
  # them, and without them `Smolquery.QueryService.Pruner` cannot prune on a
  # tenant id — the first column of every clustering key this schema is used with.
  defp read_stats(engine, path, %Schema{fields: fields}) do
    selects =
      Enum.map_join(fields, ", ", fn %Field{} = field ->
        name = Identifier.quote_name!(field.name)

        if bounded?(field.type) do
          "min(#{name}), max(#{name}), count(*) - count(#{name})"
        else
          "NULL, NULL, count(*) - count(#{name})"
        end
      end)

    case Engine.query(engine, "SELECT #{selects} FROM read_parquet($1)", [path]) do
      {:ok, %{rows: [values]}} -> {:ok, zip_stats(fields, values)}
      {:ok, _other} -> {:error, {:ndjson_copy_failed, "no stats row for #{path}"}}
      {:error, error} -> {:error, {:ndjson_copy_failed, Exception.message(error)}}
    end
  end

  defp zip_stats(fields, values) do
    fields
    |> Enum.zip(Enum.chunk_every(values, 3))
    |> Map.new(fn {%Field{} = field, [min, max, nulls]} ->
      {field.name, %{min: min, max: max, null_count: nulls}}
    end)
  end

  defp bounded?({:numeric, _precision, _scale}), do: true
  defp bounded?(:string), do: true
  defp bounded?(type), do: type in @orderable

  defp order_clause(%Schema{} = schema) do
    case Schema.clustering_columns(schema) do
      [] ->
        ""

      columns ->
        "\n      ORDER BY " <>
          Enum.map_join(columns, ", ", &"#{Identifier.quote_name!(&1)} ASC NULLS LAST")
    end
  end

  defp columns_spec(%Schema{fields: fields}) do
    Enum.map_join(fields, ", ", fn %Field{} = field ->
      "'#{field.name}': '#{sql_type(field.type)}'"
    end)
  end

  defp sql_type(:timestamp), do: "TIMESTAMP"
  defp sql_type(:date), do: "DATE"
  defp sql_type(:int64), do: "BIGINT"
  defp sql_type(:float64), do: "DOUBLE"
  defp sql_type(:bool), do: "BOOLEAN"
  defp sql_type({:numeric, precision, scale}), do: "DECIMAL(#{precision},#{scale})"
  defp sql_type(_other), do: "VARCHAR"

  defp placeholders(count), do: Enum.map_join(1..count, ", ", &"$#{&1}")

  defp codec(:zstd), do: "ZSTD"
  defp codec(:snappy), do: "SNAPPY"
  defp codec(:gzip), do: "GZIP"
  defp codec(:uncompressed), do: "UNCOMPRESSED"
  defp codec({algorithm, _level}), do: codec(algorithm)

  defp build_frame(%DataFrame{} = frame, schema), do: {:ok, sort_frame(frame, schema)}

  defp build_frame({:columns, []}, _schema), do: {:error, :no_rows}

  defp build_frame({:columns, [[] | _rest]}, _schema), do: {:error, :no_rows}

  # The columns arrive in the schema's field order, which is the order
  # `Schema.explorer_dtypes/1` returns, so each one is already paired with its
  # dtype and no lookup by name is needed.
  defp build_frame({:columns, columns}, schema) do
    with {:ok, dtypes} <- Schema.explorer_dtypes(schema),
         :ok <- same_width(columns, dtypes) do
      {:ok, sort_frame(frame(dtypes, columns), schema)}
    end
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, {:invalid_rows, Exception.message(error)}}
  end

  defp build_frame([], _schema), do: {:error, :no_rows}

  defp build_frame(rows, schema) when is_list(rows) do
    with {:ok, dtypes} <- Schema.explorer_dtypes(schema) do
      columns =
        Enum.map(dtypes, fn {name, dtype} ->
          {name, Series.from_list(Enum.map(rows, &Map.get(&1, name)), dtype: dtype)}
        end)

      {:ok, sort_frame(DataFrame.new(columns), schema)}
    end
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, {:invalid_rows, Exception.message(error)}}
  end

  defp frame(dtypes, columns) do
    dtypes
    |> Enum.zip(columns)
    |> Enum.map(fn {{name, dtype}, values} -> {name, Series.from_list(values, dtype: dtype)} end)
    |> DataFrame.new()
  end

  # A column list shorter or longer than the schema would silently drop or
  # misname columns, since the two are paired by position.
  defp same_width(columns, dtypes) do
    given = length(columns)
    wanted = length(dtypes)

    if given == wanted, do: :ok, else: {:error, {:column_count_mismatch, given, wanted}}
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
