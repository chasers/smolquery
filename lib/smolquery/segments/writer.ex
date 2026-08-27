defmodule Smolquery.Segments.Writer do
  @moduledoc """
  Writes an immutable Parquet segment: one DuckDB `COPY` over the spooled
  NDJSON bodies of a group commit.

  DuckDB reads the bodies, sorts on the clustering key, and writes the Parquet;
  the row count and the stats are read off the staged file before the store
  moves it. No row becomes an Elixir term, and every type the catalog declares
  is written here, `MAP(STRING, STRING)` and `VARIANT` included — this is the
  one writer since PL-57. Tests and benches that need a segment from rows use
  `Smolquery.Test.SegmentFixture`, which lives outside `lib/`.

  Where the bytes land is `Smolquery.Segments.Store`'s business, and durability is
  its contract: this module encodes into the staging path the store provides and
  the store commits it. That split is what lets the hot tier move between local
  disk and an object store without the write path knowing.

  ## Sorting on the clustering key

  The `COPY` orders rows by the schema's clustering key — in declared order,
  nulls last — so the row-group stats are tight enough for a reader to prune
  on. The sort is DuckDB's, never Erlang term order, and that is a correctness
  requirement: term order on `NaiveDateTime`, `Date` and `Decimal` compares
  struct fields alphabetically (`:day` before `:month` before `:year`), so
  January 31 would sort after February 1.

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

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
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
  and the stats are read off that staged file inside the encoder and come
  back as the put's `meta`, so the store's own location never has to be
  readable by DuckDB (a `memory://` test store is one; an `s3://` one is not
  read back either). Carries no
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
  Writes the spooled NDJSON `paths` as one segment in `:store`, returning the
  `Segment` describing it.

  ## Options

    * `:store` (required) — the `Smolquery.Segments.Store` the segment is put in
    * `:engine` (required) — the DuckDB engine that runs the `COPY`
    * `:prefix` — key prefix the segment is written under, typically a table's
      (see `Smolquery.Segments.Store.prefix/1`). Defaults to the store root.
    * `:id` — segment id, and so the last component of its key. Defaults to a
      fresh ULID.
    * `:compression` — Parquet codec, defaulting to `:zstd`

  """
  @spec write(ndjson(), Schema.t(), [option()]) :: {:ok, Segment.t()} | {:error, term()}
  def write({:ndjson, paths}, %Schema{} = schema, opts) when is_list(paths) do
    store = Keyword.fetch!(opts, :store)
    engine = Keyword.fetch!(opts, :engine)
    prefix = Keyword.get(opts, :prefix, "")
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    compression = Keyword.get(opts, :compression, :zstd)

    with :ok <- some_paths(paths),
         {:ok, key} <- Store.key(prefix, id),
         {:ok, %{meta: %{row_count: row_count, stats: stats}} = put} <-
           Store.put(store, key, &encode_ndjson(engine, paths, &1, schema, compression)) do
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

  @doc """
  Whether DuckDB can read `path` as this schema, without writing anything.

  `count(*)` over `read_json` still parses and casts every value, so this
  answers the same question a `COPY` would at a fraction of the cost — no
  Parquet, no sort, no bytes on disk. It exists so a failed group commit can
  find *which* spooled body it choked on, instead of failing every caller that
  happened to share the commit.
  """
  @spec readable_ndjson?(atom(), Path.t(), Schema.t()) :: boolean()
  def readable_ndjson?(engine, path, %Schema{} = schema),
    do: ndjson_problem(engine, path, schema) == :ok

  @doc """
  What DuckDB refuses about `path` read as this schema — `{:refused, message}`
  with its own words — or `:ok`. `{:error, reason}` is the engine failing, not
  the bytes: a dead pool member, a call that exited. `readable_ndjson?/3` is
  this without the message; the salvage uses the message to tell a caller why
  a row was refused, and stops on an engine failure rather than blame the rows.
  """
  @spec ndjson_problem(atom(), Path.t(), Schema.t()) ::
          :ok | {:refused, String.t()} | {:error, {:engine_failed, String.t()}}
  def ndjson_problem(engine, path, %Schema{fields: fields} = schema) do
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

    case Engine.query(engine, sql, [path]) do
      {:ok, _result} -> :ok
      {:error, error} -> problem(classify(error, :refused))
    end
  end

  defp some_paths([]), do: {:error, :no_rows}
  defp some_paths(_paths), do: :ok

  defp classify(%Adbc.Error{} = error, refusal) do
    if Connection.fatal?(error),
      do: {:engine_failed, Exception.message(error)},
      else: {refusal, Exception.message(error)}
  end

  defp classify(error, _refusal), do: {:engine_failed, Exception.message(error)}

  defp problem({:refused, _message} = refused), do: refused
  defp problem({:engine_failed, _message} = failed), do: {:error, failed}

  defp encode_ndjson(engine, paths, staged, schema, compression) do
    with :ok <- copy_ndjson(engine, paths, staged, schema, compression),
         {:ok, row_count} <- footer_rows(engine, staged),
         {:ok, stats} <- ndjson_stats(engine, staged, schema) do
      {:ok, %{row_count: row_count, stats: stats}}
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
      {:error, error} -> {:error, classify(error, :ndjson_copy_failed)}
    end
  end

  # The footer, not the data: `parquet_file_metadata` reads the tail of the file,
  # so the authoritative row count costs no pass over the rows. Trusting a count
  # the caller sent instead would let a miscounted body scale the ack.
  defp footer_rows(engine, path) do
    case Engine.query(engine, "SELECT num_rows FROM parquet_file_metadata($1)", [path]) do
      {:ok, %{rows: [[rows] | _rest]}} -> {:ok, rows}
      {:ok, _other} -> {:error, {:segment_facts_failed, "no parquet footer at #{path}"}}
      {:error, error} -> {:error, {:segment_facts_failed, Exception.message(error)}}
    end
  end

  # The manifest's stats, computed by DuckDB over the file just written — one
  # pass over a local file still in the page cache. Unlike Explorer's fixture writer this
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
      {:ok, _other} -> {:error, {:segment_facts_failed, "no stats row for #{path}"}}
      {:error, error} -> {:error, {:segment_facts_failed, Exception.message(error)}}
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
end
