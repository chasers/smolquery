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

  ## Materialized columns

  A column computed from the row (`Smolquery.Schema.Materialized`, PL-61 L4)
  is not read from the body: the `COPY`'s select list
  (`Smolquery.Schema.computed_select/2`) evaluates its expression over the
  body's regular columns, wrapped in `TRY`, so the file carries the value
  and a row whose values the expression cannot take stores `NULL` rather
  than failing the batch. The stats are read off
  the written file, so a materialized column is bounded like any other.
  `readable_ndjson?/3` reads the regular columns only, as the `COPY` does.

  ## Nanosecond bounds

  A `TIMESTAMP_NS` column's bounds are read as `epoch_ns` integers and rounded
  outward to microseconds, the minimum down and the maximum up (T-475). The
  manifest holds a `NaiveDateTime`, which has no nanoseconds, and ADBC's own
  conversion truncates toward zero, which rounds a pre-1970 minimum up past
  the data. A bound a microsecond wide of the data never prunes a segment that
  holds a match.

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

  @orderable [:int64, :float64, :timestamp, :timestamp_ns, :date]
  @epoch ~N[1970-01-01 00:00:00.000000]

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
         field_ids: Schema.field_ids(schema),
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

  A row for which a `nullable: false` materialized column's expression gives
  nothing is refused too (T-516), whatever `opts` say: the buffer's `COPY`
  raises on it (`Smolquery.Schema.computed_expression/2`, `:inserted`), and
  the salvage can only hand a row back to its caller if this check names the
  body, and then the row, that caused it. The expression reads the body as
  the write does, a variant as `VARIANT` (`Smolquery.Schema.queried/2`).
  """
  @spec ndjson_problem(atom(), Path.t(), Schema.t(), keyword()) ::
          :ok | {:refused, String.t()} | {:error, {:engine_failed, String.t()}}
  def ndjson_problem(engine, path, %Schema{} = schema, opts \\ []) do
    # `count(*)` is not enough: it needs no column values, so DuckDB is free to
    # skip the casts and answer a row count for a body it could not actually
    # read. Counting every column forces each one to be evaluated, which is the
    # work a `COPY` would do, without writing a Parquet file to find out.
    counts =
      schema
      |> Schema.regular_fields()
      |> Enum.map_join(", ", &"count(#{Identifier.quote_name!(&1.name)})")

    body = """
    read_json([$1],
      format = 'newline_delimited',
      columns = {#{columns_spec(schema)}})
    """

    sql =
      "SELECT #{missing_required(schema, opts)}, #{uncomputed(schema)}, #{counts} " <>
        "FROM #{Schema.queried(schema, String.trim_trailing(body))}"

    case Engine.query(engine, sql, [path]) do
      {:ok, %{rows: [[missing | _counts] | _rest]}} when is_integer(missing) and missing > 0 ->
        {:refused, "#{missing} row(s) hold NULL in a column that must not be null"}

      {:ok, %{rows: [[_missing, uncomputed | _counts] | _rest]}}
      when is_binary(uncomputed) ->
        {:refused, uncomputed}

      {:ok, _result} ->
        :ok

      {:error, error} ->
        problem(classify(error, :refused))
    end
  end

  # A `COPY` does not refuse a NULL in a column the schema says must not hold
  # one, so a body can be readable and still break the schema. `required: true`
  # counts those rows too, for a caller that promised all or nothing (T-474).
  defp uncomputed(schema) do
    case Schema.required_materialized(schema) do
      [] ->
        "NULL"

      fields ->
        whens =
          Enum.map_join(fields, " ", fn %Field{} = field ->
            "WHEN #{Schema.tried_expression(field)} IS NULL " <>
              "THEN #{Identifier.sql_string(Schema.no_value_message(field))}"
          end)

        "min(CASE #{whens} END)"
    end
  end

  defp missing_required(schema, opts) do
    required =
      for %Field{nullable: false} = field <- Schema.regular_fields(schema),
          Keyword.get(opts, :required, false),
          do: "#{Identifier.quote_name!(field.name)} IS NULL"

    case required do
      [] -> "0"
      checks -> "count(*) FILTER (WHERE #{Enum.join(checks, " OR ")})"
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

    spooled = """
    read_json([#{placeholders(count)}],
        format = 'newline_delimited',
        columns = {#{columns_spec(schema)}})
    """

    sql = """
    COPY (
      #{Schema.computed_select(schema, String.trim_trailing(spooled), :inserted)}#{order_clause(schema)}
    )
    TO $#{count + 1} (FORMAT PARQUET, COMPRESSION #{codec(compression)}#{field_ids_option(schema)})
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
        stats_select(field.type, Identifier.quote_name!(field.name))
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
      {field.name,
       column_stats(bound(field.type, min, :floor), bound(field.type, max, :ceil), nulls)}
    end)
  end

  # One shape for a column's manifest stats, whichever writer produced them —
  # `Smolquery.BufferService.HotManifest.Entry` reads them by these names.
  defp column_stats(min, max, null_count),
    do: %{min: min, max: max, null_count: null_count}

  defp stats_select(:timestamp_ns, name),
    do: "epoch_ns(min(#{name})), epoch_ns(max(#{name})), count(*) - count(#{name})"

  defp stats_select(type, name) do
    if ndjson_bounded?(type),
      do: "min(#{name}), max(#{name}), count(*) - count(#{name})",
      else: "NULL, NULL, count(*) - count(#{name})"
  end

  defp bound(:timestamp_ns, ns, direction) when is_integer(ns),
    do: NaiveDateTime.add(@epoch, micros(ns, direction), :microsecond)

  defp bound(_type, value, _direction), do: value

  defp micros(ns, :floor), do: Integer.floor_div(ns, 1_000)
  defp micros(ns, :ceil), do: -Integer.floor_div(-ns, 1_000)

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

  defp columns_spec(%Schema{} = schema) do
    schema
    |> Schema.regular_fields()
    |> Enum.map_join(", ", fn %Field{} = field ->
      {:ok, type} = Schema.duckdb_type(field.type)

      "'#{field.name}': '#{type}'"
    end)
  end

  defp field_ids_option(%Schema{} = schema) do
    case Schema.parquet_field_ids(schema) do
      nil -> ""
      literal -> ", FIELD_IDS #{literal}"
    end
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
