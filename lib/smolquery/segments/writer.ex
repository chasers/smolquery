defmodule Smolquery.Segments.Writer do
  @moduledoc """
  Writes an immutable Parquet segment: one DuckDB `COPY` over the spooled
  NDJSON bodies of a group commit.

  DuckDB reads the bodies, sorts on the clustering key, and writes the Parquet;
  the row count and the stats come back from that same `COPY` (see "Stats
  from the write" below), and one read of the file's footer checks the file
  before the store moves it. No row becomes an
  Elixir term, and every type the catalog declares
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
  than failing the batch. The `COPY` reports stats for every column it
  wrote, computed ones included, so a materialized column is bounded like any
  other.
  `readable_ndjson?/3` reads the regular columns only, as the `COPY` does.

  ## Stats from the write

  The `COPY` runs with `RETURN_STATS`, so DuckDB answers with the file's row
  count and, per column, the min, max and null count its Parquet writer
  gathered while encoding: the data pages are never read back to learn them.
  The stats come keyed by the quoted column name (`"ts"`), every value as
  text, and each is turned back into the type the manifest holds. A bound
  that text does not give, or does not give soundly, is `nil` (unknown),
  never a guess, since a bound narrower than the data would prune a segment
  that holds a match:

    * `INT64` and `DECIMAL` parse exactly. `DATE` and `TIMESTAMP` parse
      exactly through `Smolquery.StatText`, years past 9999 included;
      `infinity` and `(BC)` values have no `Date`/`NaiveDateTime` form there
      and give `nil`.
    * `DOUBLE` prints the shortest text that reads back as the same float,
      and `inf`/`-inf` become `:infinity`/`:neg_infinity`. Parquet stats
      leave NaN out of min and max and say `has_nan` instead; DuckDB orders
      NaN above every number, so a column holding one gets `:nan` as its
      max, and as its min too when it holds nothing else.
    * `VARCHAR` bounds are taken as DuckDB gives them, cut or not. DuckDB
      cuts a string stat to 256 bytes, and either way the bound stays sound
      for a byte-wise comparison: a cut min is a prefix of the true min, so
      it sorts below every value; a cut max is a prefix with its last
      raisable ASCII byte incremented, so it sorts above every value, also
      when DuckDB shortened it further to stop before a multi-byte
      character. A column DuckDB could not cut that way gets no bounds at
      all, `nil` on both sides. A bound that is not valid UTF-8 is `nil`
      too, as the manifest log is JSON. Every string column is bounded, not
      only the sorted one (T-179).
    * A column the `COPY` reports no top-level stats for — a `MAP` is only
      described by its key and value leaves, whose null counts do not tell a
      null map from an empty one — is counted with one scan of those columns
      alone, as every column was before.

  ## The footer is the readability gate

  Before the stats are trusted, the footer of the staged file is read once
  (`parquet_file_metadata`) and its row count must equal the one the `COPY`
  reported, or the write fails. That read is a gate, never the source of a
  bound: a micro-segment DuckDB cannot read after the ack would wedge the
  claim that holds it, since the merge skips only the inputs the manifest
  dropped (`Smolquery.StorageService.Merge`). A footer DuckDB parses is the
  check that the file is one it can open; the data pages are not read back.

  ## Nanosecond bounds

  A `TIMESTAMP_NS` column's bounds keep every digit in the `COPY`'s text
  (and in the text of `min`/`max` where a column is scanned) and are rounded
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
  alias Smolquery.StatText

  @type row :: %{optional(String.t()) => term()}

  @typedoc """
  A batch that is still bytes on disk: NDJSON files the API spooled without
  parsing. DuckDB reads them itself, sorts on the clustering key and writes the
  Parquet, so no row in the batch ever becomes an Elixir term.

  Needs `:engine` and a store whose staging path is a local file — DuckDB's
  `COPY` writes to a filesystem path, not to an object store. The row count
  and the stats come back from the `COPY` that writes that staged file and
  are the put's `meta`, so the store's own location never has to be
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
    with {:ok, written} <- copy_ndjson(engine, paths, staged, schema, compression),
         :ok <- footer_agrees(engine, staged, written.rows),
         {:ok, stats} <- written_stats(engine, staged, schema, written.columns) do
      {:ok, %{row_count: written.rows, stats: stats}}
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
    TO $#{count + 1} (FORMAT PARQUET, COMPRESSION #{codec(compression)}#{field_ids_option(schema)}, RETURN_STATS true)
    """

    case Engine.query(engine, sql, paths ++ [staged]) do
      {:ok, result} -> written(result, staged)
      {:error, error} -> {:error, classify(error, :ndjson_copy_failed)}
    end
  end

  defp written(%{columns: names, rows: [values]}, staged) do
    case Map.new(Enum.zip(names, values)) do
      %{"count" => rows, "column_statistics" => columns}
      when is_integer(rows) and is_map(columns) ->
        {:ok, %{rows: rows, columns: columns}}

      _other ->
        {:error, {:segment_facts_failed, "no write statistics for #{staged}"}}
    end
  end

  defp written(_result, staged),
    do: {:error, {:segment_facts_failed, "no write statistics for #{staged}"}}

  defp footer_agrees(engine, path, rows) do
    case Engine.query(engine, "SELECT num_rows FROM parquet_file_metadata($1)", [path]) do
      {:ok, %{rows: [[^rows] | _rest]}} ->
        :ok

      {:ok, %{rows: [[other] | _rest]}} ->
        {:error,
         {:segment_facts_failed,
          "footer of #{path} holds #{other} rows, the COPY reported #{rows}"}}

      {:ok, _result} ->
        {:error, {:segment_facts_failed, "no parquet footer at #{path}"}}

      {:error, error} ->
        {:error, {:segment_facts_failed, Exception.message(error)}}
    end
  end

  defp written_stats(engine, path, %Schema{fields: fields}, columns) do
    {described, scanned} =
      Enum.reduce(fields, {%{}, []}, fn %Field{} = field, {described, scanned} ->
        column = Map.get(columns, Identifier.quote_name!(field.name))

        case described_stats(field.type, column) do
          {:ok, stats} -> {Map.put(described, field.name, stats), scanned}
          :error -> {described, [field | scanned]}
        end
      end)

    with {:ok, scanned} <- scanned_stats(engine, path, Enum.reverse(scanned)) do
      {:ok, Map.merge(described, scanned)}
    end
  end

  defp described_stats(type, %{"null_count" => nulls} = column) do
    case Integer.parse(nulls) do
      {null_count, ""} ->
        {min, max} = described_bounds(type, column)
        {:ok, column_stats(min, max, null_count)}

      _invalid ->
        :error
    end
  end

  defp described_stats(_type, _column), do: :error

  defp described_bounds(:float64, %{"has_nan" => "true"} = column),
    do: {parsed(:float64, column["min"], :floor) || :nan, :nan}

  defp described_bounds(:float64, %{"has_nan" => "false"} = column),
    do: {parsed(:float64, column["min"], :floor), parsed(:float64, column["max"], :ceil)}

  defp described_bounds(:float64, _column), do: {nil, nil}

  defp described_bounds(:string, column),
    do: {text_bound(column["min"]), text_bound(column["max"])}

  defp described_bounds(type, column) do
    if ndjson_bounded?(type),
      do: {parsed(type, column["min"], :floor), parsed(type, column["max"], :ceil)},
      else: {nil, nil}
  end

  defp text_bound(text) when is_binary(text), do: if(String.valid?(text), do: text, else: nil)
  defp text_bound(_text), do: nil

  defp parsed(_type, nil, _direction), do: nil
  defp parsed(:int64, text, _direction), do: whole(Integer.parse(text))
  defp parsed(:float64, "inf", _direction), do: :infinity
  defp parsed(:float64, "-inf", _direction), do: :neg_infinity
  defp parsed(:float64, text, _direction), do: whole(Float.parse(text))

  defp parsed({:numeric, precision, _scale}, text, _direction),
    do: whole(Decimal.parse(text, max_digits: precision))

  defp parsed(:date, text, _direction), do: StatText.date(text)

  defp parsed(type, text, direction) when type in [:timestamp, :timestamp_ns],
    do: StatText.naive_datetime(text, direction)

  defp parsed(_type, _text, _direction), do: nil

  defp whole({value, ""}), do: value
  defp whole(_partial), do: nil

  defp scanned_stats(_engine, _path, []), do: {:ok, %{}}

  defp scanned_stats(engine, path, fields) do
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
    do:
      "CAST(min(#{name}) AS VARCHAR), CAST(max(#{name}) AS VARCHAR), " <>
        "count(*) - count(#{name})"

  defp stats_select(type, name) do
    if ndjson_bounded?(type),
      do: "min(#{name}), max(#{name}), count(*) - count(#{name})",
      else: "NULL, NULL, count(*) - count(#{name})"
  end

  defp bound(:timestamp_ns, text, direction) when is_binary(text),
    do: StatText.naive_datetime(text, direction)

  defp bound(_type, value, _direction), do: value

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
