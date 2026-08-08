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

  Needs `:engine` and a store whose `location/2` is a local file — DuckDB's
  `COPY` writes to a filesystem path, and the read-back this path does after
  the put has no object-store credentials to read one with. `write/3` refuses a
  shared store rather than upload a segment it cannot then describe.
  """
  @type ndjson :: {:ndjson, [Path.t()]}

  @type option ::
          {:store, Store.t()}
          | {:prefix, String.t()}
          | {:id, String.t()}
          | {:compression, atom() | {atom(), integer() | nil}}
          | {:engine, atom()}
          | {:timeout, timeout()}

  @orderable [:int64, :float64, :timestamp, :date]

  # Every DuckDB call a flush makes is a `GenServer.call` against one connection
  # that is a per-query mutex, and `Smolquery.Engine.Connection` defaults those to
  # 30 s — longer than the buffer's shipped `write_timeout_ms` of 15 s. The caller
  # would therefore give up first and the late exit would take the committer, and
  # every unacked batch it holds, with it. This default is under that 15 s so the
  # inner call fails first with an error the commit can report; a buffer that has
  # moved `write_timeout_ms` passes `Smolquery.BufferService.Runtime.engine_timeout/1`
  # here instead.
  @default_timeout 10_000

  # A URI scheme in front of a path means the bytes are not on this filesystem.
  @scheme ~r|^[A-Za-z][A-Za-z0-9+.-]*://|

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
    * `:timeout` — how long each of the `{:ndjson, paths}` path's DuckDB calls
      may take, defaulting to `#{@default_timeout}` ms. Keep it under the
      caller's own deadline: see the note beside `@default_timeout`.

  """
  @spec write(columns() | [row()] | DataFrame.t(), Schema.t(), [option()]) ::
          {:ok, Segment.t()} | {:error, term()}
  def write({:ndjson, paths}, %Schema{} = schema, opts) when is_list(paths) do
    store = Keyword.fetch!(opts, :store)
    engine = Keyword.fetch!(opts, :engine)
    prefix = Keyword.get(opts, :prefix, "")
    id = Keyword.get_lazy(opts, :id, &Id.generate/0)
    compression = Keyword.get(opts, :compression, :zstd)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    # `local_store/1` and `writable_names/1` run before `Store.put/3`, not after:
    # a check that fires once the put has returned has already uploaded a segment
    # nothing will ever name, and an orphan in a bucket is what this path has no
    # way to clean up.
    with :ok <- some_paths(paths),
         :ok <- local_store(store),
         :ok <- writable_names(schema),
         {:ok, key} <- Store.key(prefix, id),
         {:ok, put} <-
           Store.put(store, key, &copy_ndjson(engine, paths, &1, schema, compression, timeout)),
         {:ok, row_count, stats} <- read_stats(engine, put.location, schema, timeout) do
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

  # The `COPY` writes to a filesystem path and the stats read the file back
  # through the write pool, whose engines are started without an object store's
  # credential statements. A shared store therefore uploads the segment and
  # *then* fails, leaving an object no manifest, catalog or GC pass names.
  # `Smolquery.BufferService.Runtime.new/1` refuses that configuration at boot;
  # this is the same refusal one layer down, for a caller that built its own
  # store.
  defp local_store(%Store{} = store) do
    if Store.shared?(store) do
      {:error, {:ndjson_store_not_local, store.impl}}
    else
      :ok
    end
  end

  defp local_path(path) do
    if Regex.match?(@scheme, path) do
      {:error, {:ndjson_store_not_local, path}}
    else
      :ok
    end
  end

  # Every column name in this path's SQL is interpolated — `read_json`'s
  # `columns` map takes a string key, and DuckDB takes no parameter for an
  # identifier — so all of them are validated once, here, before any reaches a
  # statement. The caller cannot be trusted to have done it:
  # `Smolquery.Catalog.DuckLake.build_schema/2` builds `%Field{}` structs
  # straight from `information_schema`, bypassing `Field.new/3` and so
  # `Identifier.validate/1`, and that schema is what the cache serves the write
  # path. Failing here is also what keeps `Identifier.quote_name!/1` below from
  # raising out of a flush and killing the committer with it.
  defp writable_names(%Schema{fields: fields} = schema) do
    names = Enum.map(fields, & &1.name) ++ Schema.clustering_columns(schema)

    Enum.reduce_while(names, :ok, fn name, :ok ->
      case Identifier.validate(name) do
        {:ok, _name} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # One statement for the whole flush: DuckDB reads every spooled body, sorts the
  # union on the clustering key and writes one Parquet file. `read_json` with an
  # explicit `columns` map pins the types, so nothing is inferred per request and
  # the file matches the schema by construction.
  #
  # Paths are parameters rather than interpolated: a spool path is generated here,
  # but a quoted literal in a COPY is exactly the place a future caller-supplied
  # name would become an injection.
  defp copy_ndjson(engine, paths, staged, schema, compression, timeout) do
    # Once, not twice: the placeholder list and the destination's index are the
    # same count, and `length/1` walks the list to find it.
    count = length(paths)

    sql = """
    COPY (
      SELECT * FROM read_json([#{placeholders(count)}],
        format = 'newline_delimited',
        columns = {#{columns_spec(schema)}})#{order_clause(schema)}
    )
    TO $#{count + 1} (FORMAT PARQUET, COMPRESSION #{codec(compression)})
    """

    with :ok <- local_path(staged),
         {:ok, _result} <- query(engine, sql, paths ++ [staged], timeout) do
      :ok
    end
  end

  # The manifest's stats and its row count together, in one pass over a local
  # file still in the page cache. `count(*)` is the number
  # `parquet_file_metadata` used to be asked for in a query of its own — the
  # same count, one fewer round trip on a connection that is a per-query mutex,
  # and one fewer chance for a flush to die between two statements.
  #
  # The footer's own per-row-group statistics would make this a metadata read
  # rather than a scan, but `parquet_metadata` hands every bound back as a
  # VARCHAR: folding those into typed manifest values needs a per-type cast
  # this code cannot honestly promise for `DECIMAL`, and a bound that comes
  # back subtly wrong silently prunes a segment out of a query rather than
  # failing. The scan is the price of stats that are right.
  #
  # Unlike the Polars path this includes bounds for string columns:
  # `Explorer.Series.min/1` raises for `:string`, so `@orderable` cannot carry
  # them, and without them `Smolquery.QueryService.Pruner` cannot prune on a
  # tenant id — the first column of every clustering key this schema is used with.
  defp read_stats(engine, path, %Schema{fields: fields}, timeout) do
    selects =
      Enum.map_join(fields, ", ", fn %Field{} = field ->
        name = Identifier.quote_name!(field.name)

        if bounded?(field.type) do
          "min(#{name}), max(#{name}), count(*) - count(#{name})"
        else
          "NULL, NULL, count(*) - count(#{name})"
        end
      end)

    case query(engine, "SELECT count(*), #{selects} FROM read_parquet($1)", [path], timeout) do
      {:ok, %{rows: [[row_count | values]]}} -> {:ok, row_count, zip_stats(fields, values)}
      {:ok, _other} -> {:error, {:ndjson_copy_failed, "no stats row for #{path}"}}
      {:error, reason} -> {:error, reason}
    end
  end

  # `Engine.query/3` passes no timeout, so it inherits `Engine.Connection`'s
  # 30 s — see `@default_timeout` for why that is the wrong deadline for a
  # flush. Called through the connection directly because the timeout is the
  # point.
  defp query(engine, sql, params, timeout) do
    case Engine.Connection.query(Engine.connection_name(engine), sql, params, timeout) do
      {:ok, result} -> {:ok, result}
      {:error, error} -> {:error, {:ndjson_copy_failed, Exception.message(error)}}
    end
  catch
    # A `GenServer.call` that times out, or that lands on an engine the pool
    # never started, exits the *caller* — and the caller here is the committer,
    # whose death drops every batch waiting on this flush without a reply. Tag
    # it instead, so a slow or missing engine fails the commit the same way a
    # rejected line does.
    :exit, reason -> {:error, {:ndjson_engine_exit, reason}}
  end

  defp zip_stats(fields, values) do
    fields
    |> Enum.zip(Enum.chunk_every(values, 3))
    |> Map.new(fn {%Field{} = field, [min, max, nulls]} ->
      {field.name, column_stats(min, max, nulls)}
    end)
  end

  # The one place either encoder spells the per-column statistic out. The shape
  # is a contract that crosses two module boundaries — the writer produces it,
  # `Smolquery.BufferService.HotManifest.Entry` serialises it into the manifest
  # log and reads it back, and `Smolquery.QueryService.Pruner` decides on it — so
  # the two encoders having drifted apart would have shown up as segments that
  # prune differently depending on which one wrote them.
  defp column_stats(min, max, null_count) do
    %{min: min, max: max, null_count: null_count}
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

  # Escaped as well as validated by `writable_names/1`. Its two neighbours quote
  # through `Identifier.quote_name!/1` and this one did not, which is exactly the
  # asymmetry a name reaching here from outside `Field.new/3` would exploit — a
  # single quote closes the key and the rest of the `columns` map is attacker
  # SQL. Two guards, because only one of them is enforced by a caller.
  defp columns_spec(%Schema{fields: fields}) do
    Enum.map_join(fields, ", ", fn %Field{} = field ->
      "#{Identifier.sql_string(field.name)}: '#{sql_type(field.type)}'"
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
       column_stats(
         bound(series, field.type, &Series.min/1),
         bound(series, field.type, &Series.max/1),
         Series.nil_count(series)
       )}
    end)
  end

  defp bound(series, {:numeric, _precision, _scale}, fun), do: fun.(series)
  defp bound(series, type, fun) when type in @orderable, do: fun.(series)
  defp bound(_series, _type, _fun), do: nil
end
