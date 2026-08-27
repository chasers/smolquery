defmodule Smolquery.Segments.Writer do
  @moduledoc """
  Writes an immutable Parquet segment.

  Two writers live here, with one contract — a `Segment` describing a file the
  store has committed:

    * `write({:ndjson, paths}, schema, opts)` is the buffer's flush. DuckDB
      reads the spooled NDJSON bodies, sorts on the clustering key, writes the
      Parquet, and the row count and stats are read off the staged file. No
      row becomes an Elixir term. Every type the catalog declares is written
      here, `MAP(STRING, STRING)` and `VARIANT` included (PL-57).
    * `write(rows, schema, opts)` is the fixture writer: tests and benches
      build a segment from rows through Explorer. It refuses a schema Explorer
      cannot write. Nothing in a deployment calls it.

  Where the bytes land is `Smolquery.Segments.Store`'s business, and durability is
  its contract: this module encodes into the staging path the store provides and
  the store commits it. That split is what lets the hot tier move between local
  disk and an object store without the write path knowing.

  ## Sorting on the clustering key

  Rows are sorted by the schema's clustering key before the file is written —
  in declared order, nulls last — so the row-group stats are tight enough for a
  reader to prune on. The flush sorts in DuckDB (`ORDER BY` in the `COPY`); the
  fixture writer sorts the frame in Polars. Neither sorts Elixir terms, and that
  is a correctness requirement: Erlang term order on `NaiveDateTime`, `Date` and
  `Decimal` compares struct fields alphabetically (`:day` before `:month` before
  `:year`), so January 31 would sort after February 1.

  The columns come from `Smolquery.Schema.clustering_columns/1` rather than the
  `:clustering` field, so a key naming a column this schema no longer has sorts
  by the rest instead of failing the write. That function documents why the two
  can differ.

  ## Usage

      schema = Smolquery.Schema.new!([{"id", :int64}, {"ts", :timestamp}])
      store = Smolquery.Segments.Store.Local.new(dir: "/data/segments")

      {:ok, segment} =
        Smolquery.Segments.Writer.write({:ndjson, [spooled_path]}, schema,
          store: store, engine: MyEngine)

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
  A batch that is still bytes on disk: NDJSON files the API spooled without
  parsing. DuckDB reads them itself, sorts on the clustering key and writes the
  Parquet, so no row in the batch ever becomes an Elixir term.

  Needs `:engine` and a store whose staging path is a local file — DuckDB's
  `COPY` writes to a filesystem path, not to an object store. The row count
  and the stats are read off that staged file before the store moves it, so
  the store's own location never has to be readable by DuckDB (a `memory://`
  test store is one; an `s3://` one is not read back either). Carries no
  per-row validation, so a value the schema cannot take fails the whole batch
  rather than one row: see `PL-22` for what that costs and whether it pays.
  """
  @type ndjson :: {:ndjson, [Path.t()]}

  @type option ::
          {:store, Store.t()}
          | {:engine, atom()}
          | {:prefix, String.t()}
          | {:id, String.t()}
          | {:compression, atom() | {atom(), integer() | nil}}

  @orderable [:int64, :float64, :timestamp, :date]

  @doc """
  Writes `rows` as a segment in `:store`, returning the `Segment` describing it.

  Rows are maps keyed by column name; a column missing from a row is written as
  null. An `Explorer.DataFrame` may be passed instead, in which case its
  columns must already match `schema`.

  This is the fixture writer: tests and tools build a segment from rows
  through Explorer. The buffer never calls it — a flush encodes through DuckDB
  (`{:ndjson, paths}`, PL-57) — and it refuses a schema Explorer cannot write,
  a map or a variant.

  ## Options

    * `:store` (required) — the `Smolquery.Segments.Store` the segment is put in
    * `:prefix` — key prefix the segment is written under, typically a table's
      (see `Smolquery.Segments.Store.prefix/1`). Defaults to the store root.
    * `:id` — segment id, and so the last component of its key. Defaults to a
      fresh ULID.
    * `:compression` — Parquet codec, defaulting to `:zstd`

  """
  @spec write(ndjson() | [row()] | DataFrame.t(), Schema.t(), [option()]) ::
          {:ok, Segment.t()} | {:error, term()}
  def write({:ndjson, paths}, %Schema{} = schema, opts) when is_list(paths) do
    store = Keyword.fetch!(opts, :store)
    engine = Keyword.fetch!(opts, :engine)
    prefix = Keyword.get(opts, :prefix, "")
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    compression = Keyword.get(opts, :compression, :zstd)

    facts = {self(), make_ref()}

    with :ok <- some_paths(paths),
         {:ok, key} <- Store.key(prefix, id),
         {:ok, put} <-
           Store.put(store, key, &encode_ndjson(engine, paths, &1, schema, compression, facts)),
         {:ok, row_count, stats} <- staged_facts(facts) do
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

  defp build_frame(%DataFrame{} = frame, schema) do
    case Schema.explorer_unwritable(schema) do
      :none -> {:ok, sort_frame(frame, schema)}
      {:ok, %Field{type: type}} -> {:error, {:unsupported_type, type}}
    end
  end

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

  @doc """
  Whether DuckDB can read `path` as this schema, without writing anything.

  `count(*)` over `read_json` still parses and casts every value, so this
  answers the same question a `COPY` would at a fraction of the cost — no
  Parquet, no sort, no bytes on disk. It exists so a failed group commit can
  find *which* spooled body it choked on, instead of failing every caller that
  happened to share the commit.
  """
  @spec readable_ndjson?(atom(), Path.t(), Schema.t()) :: boolean()
  def readable_ndjson?(engine, path, %Schema{fields: fields} = schema) do
    # `count(*)` is not enough: it needs no column values, so DuckDB is free to
    # skip the casts and answer a row count for a body it could not actually
    # read. Counting every column forces each one to be evaluated, which is the
    # work a `COPY` would do, without writing a Parquet file to find out.
    counts = Enum.map_join(fields, ", ", &"count(#{Identifier.quote_name!(&1.name)})")

    sql = """
    SELECT #{counts} FROM read_json([$1],
      format = 'newline_delimited',
      columns = {#{columns_spec(schema)}})
    """

    match?({:ok, _result}, Engine.query(engine, sql, [path]))
  end

  defp some_paths([]), do: {:error, :no_rows}
  defp some_paths(_paths), do: :ok

  defp encode_ndjson(engine, paths, staged, schema, compression, {parent, ref}) do
    with :ok <- copy_ndjson(engine, paths, staged, schema, compression),
         {:ok, row_count} <- footer_rows(engine, staged),
         {:ok, stats} <- ndjson_stats(engine, staged, schema) do
      send(parent, {ref, row_count, stats})

      :ok
    end
  end

  defp staged_facts({_parent, ref}) do
    receive do
      {^ref, row_count, stats} -> {:ok, row_count, stats}
    after
      0 -> {:error, :no_staged_facts}
    end
  end

  # One statement for the whole flush: DuckDB reads every spooled body, sorts the
  # union on the clustering key and writes one Parquet file. `read_json` with an
  # explicit `columns` map pins the types, so nothing is inferred per request and
  # the file matches the schema by construction.
  #
  # Paths are parameters rather than interpolated: a spool path is generated by
  # the API, but a quoted literal in a COPY is exactly the place a future
  # caller-supplied name would become an injection.
  defp copy_ndjson(engine, paths, staged, schema, compression) do
    count = length(paths)

    sql = """
    COPY (
      SELECT * FROM read_json([#{placeholders(count)}],
        format = 'newline_delimited',
        columns = {#{columns_spec(schema)}})#{order_clause(schema)}
    )
    TO $#{count + 1} (FORMAT PARQUET, COMPRESSION #{codec(compression)})
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

  # The manifest's stats, computed by DuckDB over the file just written — one
  # pass over a local file still in the page cache. Unlike the frame path this
  # bounds every string column rather than only the sorted one, since DuckDB
  # compares text natively where `Explorer.Series.min/1` refuses it (T-179).
  defp ndjson_stats(engine, path, %Schema{fields: fields}) do
    selects =
      Enum.map_join(fields, ", ", fn %Field{} = field ->
        name = Identifier.quote_name!(field.name)

        if ndjson_bounded?(field.type) do
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
      {field.name, column_stats(min, max, nulls)}
    end)
  end

  # One shape for a column's manifest stats, whichever writer produced them —
  # `Smolquery.BufferService.HotManifest.Entry` reads them by these names.
  defp column_stats(min, max, null_count),
    do: %{min: min, max: max, null_count: null_count}

  defp ndjson_bounded?({:numeric, _precision, _scale}), do: true
  defp ndjson_bounded?(:string), do: true
  defp ndjson_bounded?(type), do: type in @orderable

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
      {:ok, type} = Schema.duckdb_type(field.type)

      "'#{field.name}': '#{type}'"
    end)
  end

  defp placeholders(count), do: Enum.map_join(1..count, ", ", &"$#{&1}")

  defp codec(:zstd), do: "ZSTD"
  defp codec(:snappy), do: "SNAPPY"
  defp codec(:gzip), do: "GZIP"
  defp codec(:lz4raw), do: "LZ4_RAW"
  defp codec(:lz4), do: "LZ4"
  defp codec(:uncompressed), do: "UNCOMPRESSED"
  defp codec({algorithm, _level}), do: codec(algorithm)

  defp stats(frame, %Schema{fields: fields} = schema) do
    sorted = leading_clustering_column(schema)
    row_count = DataFrame.n_rows(frame)

    Map.new(fields, fn %Field{} = field ->
      series = frame[field.name]
      null_count = Series.nil_count(series)

      {field.name,
       column_stats(
         bound(series, field, sorted, :min, row_count, null_count),
         bound(series, field, sorted, :max, row_count, null_count),
         null_count
       )}
    end)
  end

  # `Series.min/1` refuses `:string`, so a text column would carry no bounds and
  # `Smolquery.QueryService.Pruner` could not prune on a tenant id — the first
  # column of most clustering keys, and the one an equality predicate names most.
  #
  # The leading clustering column needs no comparison pass to answer: `sort_frame/2`
  # has already ordered the whole frame by it, ascending with nulls last, so its
  # bounds are the first and last non-null positions. Later clustering columns are
  # ordered only within a run of the first, and unclustered columns not at all, so
  # neither gets bounds this way — a wrong bound prunes away real rows, which is
  # worse than no bound at all.
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
