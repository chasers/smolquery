defmodule Smolquery.Catalog.DuckLake do
  @moduledoc """
  `Smolquery.Catalog` over DuckLake — Parquet data files plus a SQL catalog.

  DuckLake is the architecture smolquery already wanted: immutable Parquet
  files registered in a transactional SQL catalog with snapshot isolation. The
  write path stays ours (`Smolquery.Segments.Writer` encodes the Parquet);
  registration hands DuckLake a path it adopts in place, without copying or
  rewriting the file.

  ## Tiering

  The metadata database is a connection string, so the same code runs a
  single-node dev lake and a cluster:

      metadata: "sqlite:/var/lib/smolquery/catalog.sqlite"
      metadata: "postgres:dbname=smolquery host=catalog.internal"

  A `"postgres:"` metadata string also loads DuckDB's `postgres` extension
  (Milestone 8 L2) — DuckLake needs it to talk to the metadata database
  itself, on top of `ducklake` — added automatically from the metadata
  string's own prefix, not a separate option. `config/runtime.exs` builds
  this string from `CATALOG_DATABASE_URL`, the same URL
  `Smolquery.Cluster` uses for node discovery (PL-11 D1); `SMOLQUERY_CATALOG`
  overrides it explicitly when the two need to differ.

  ## Setup

  A DuckLake catalog is an engine whose connection has the `ducklake` extension
  loaded and the lake attached. Both happen as connection bootstrap statements,
  so a connection is never observable un-attached and a restart re-attaches:

      {Smolquery.Catalog.DuckLake,
       name: MyLake,
       metadata: "sqlite:\#{data_dir}/catalog.sqlite",
       data_path: "\#{data_dir}/segments"}

      catalog = Smolquery.Catalog.DuckLake.new(engine: MyLake)

  ## What the spike found, and what this module does about it

  Registration is *not* idempotent in DuckLake — adding a path twice
  double-counts its rows — so `register_segments/3` diffs against the paths the
  catalog already holds and commits only the remainder. Commits use optimistic
  concurrency and DuckLake does not retry them, so a conflicting commit is
  retried here with backoff before surfacing `{:error, :commit_conflict}`.
  The diff runs *inside* the retry, against a fresh read each attempt —
  replaying the pre-conflict statement would re-add exactly the paths the
  winning commit just registered, turning a lost race into the double-count
  this diff exists to prevent (Milestone 8 L6: two storage nodes can each
  believe they own a table's seal work while a ring change propagates). A
  simultaneous two-committer race — both reading before either commits, and
  DuckLake accepting both appends — is not closed by this and remains the
  ring gate's residual window; see `Smolquery.StorageService.Routing`.

  DuckLake collects its own min-max statistics from the Parquet footer at
  registration, which is why nothing here passes stats: the sealed tier prunes
  on numbers DuckLake derives, and a segment's own stats serve the hot tier.

  `known_segments/1` reads DuckLake's own metadata schema
  (`__ducklake_metadata_<catalog>.ducklake_data_file`) rather than a documented
  function, because no documented function answers "every path any snapshot ever
  referenced" — `ducklake_list_files/2` answers per snapshot, and iterating every
  snapshot to union them costs more the longer a lake lives. That is a coupling to
  an internal name, taken deliberately and pinned by a test: if DuckLake renames
  it, the query fails loudly rather than reporting an empty set, which is the
  failure mode that would matter (garbage collection would treat every sealed
  segment as an orphan). A row whose `path_is_relative` is true fails the same
  way: a relative path returned as-is would match no store location, and GC would
  again see every committed segment as unreferenced.

  `ducklake_merge_adjacent_files/2` must never be called on a smolquery table:
  over externally-registered files it crashes DuckDB fatally (ducklake
  `67480b1d`, format 0.4), and a fatal error invalidates the whole database.
  Compaction stays `Smolquery.StorageService.Compactor`'s job, built on
  `replace_segments/4` — registration and retirement composed inside one
  `Smolquery.Engine.transaction/2`, so a single snapshot carries both. The
  swap runs its `DELETE` before its `ducklake_add_data_files`: the order is
  invisible inside the transaction, and it puts the statement most likely to
  fail (registering a merged file the table could reject) after one that has
  already applied, so the rollback path is the one a real failure exercises.
  """

  @behaviour Smolquery.Catalog

  import Bitwise

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @enforce_keys [:engine, :catalog]
  defstruct [:engine, :catalog]

  @type t :: %__MODULE__{engine: atom(), catalog: String.t()}

  @type option ::
          {:name, atom()}
          | {:metadata, String.t()}
          | {:data_path, String.t()}
          | {:catalog, String.t()}

  @default_catalog "lake"
  @commit_attempts 5

  @doc """
  A child spec starting the engine that backs this catalog.
  """
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Starts an engine with the `ducklake` extension loaded and the lake attached.

  ## Options

    * `:name` — engine name, and the handle `new/1` takes
    * `:metadata` (required) — DuckLake metadata database, e.g.
      `"sqlite:/var/lib/smolquery/catalog.sqlite"`
    * `:data_path` (required) — where DuckLake writes files it owns. smolquery
      registers segments from outside this directory, but DuckLake requires it.
    * `:catalog` — attached catalog name. Defaults to `#{inspect(@default_catalog)}`.
    * any `Smolquery.Engine` option — `:extensions` is extended with
      `:ducklake` rather than replaced

  """
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    config = Keyword.merge(Application.get_env(:smolquery, __MODULE__, []), opts)
    {catalog, config} = Keyword.pop(config, :catalog, @default_catalog)
    {metadata, config} = Keyword.pop!(config, :metadata)
    {data_path, config} = Keyword.pop!(config, :data_path)

    :ok = ensure_metadata_dir(metadata)

    extensions = Keyword.get(config, :extensions, engine_extensions())
    statements = Keyword.get(config, :statements, [])
    required = if postgres_metadata?(metadata), do: [:postgres, :ducklake], else: [:ducklake]

    config
    |> Keyword.put(:extensions, Enum.uniq(required ++ extensions))
    |> Keyword.put(:statements, [attach_statement(catalog, metadata, data_path) | statements])
    |> Engine.start_link()
  end

  @doc """
  A catalog handle for an engine started by `start_link/1`.

  ## Options

    * `:engine` — the engine name given to `start_link/1`
    * `:catalog` — the attached catalog name, if not the default

  """
  @spec new(keyword()) :: Catalog.t()
  def new(opts) do
    config = %__MODULE__{
      engine: Keyword.get(opts, :engine, __MODULE__),
      catalog: Keyword.get(opts, :catalog, @default_catalog)
    }

    %Catalog{impl: __MODULE__, config: config}
  end

  @doc """
  The catalog name a lake is attached under when configuration names none.

  Public because the name appears in SQL other modules build — the query
  planner's views read `#{@default_catalog}.<dataset>.<table>`, and the engine
  executing them must have attached the lake under the same name.
  """
  @spec default_catalog() :: String.t()
  def default_catalog, do: @default_catalog

  @doc """
  Resolves a service's `:catalog` configuration into a handle and the options
  its supervisor must start an engine with.

  Every service that reads or commits through a catalog accepts the same two
  shapes of configuration. A `%Smolquery.Catalog{}` given outright is used
  as-is and the options come back `nil` — the catalog is managed elsewhere and
  the service starts nothing. Options (or nothing) mean the service runs its
  own lake: the handle reads through `engine`, and the options are what
  `children/2` starts that engine with.
  """
  @spec resolve(Catalog.t() | keyword() | nil, atom()) :: {Catalog.t(), keyword() | nil}
  def resolve(%Catalog{} = catalog, _engine), do: {catalog, nil}

  def resolve(opts, engine) do
    opts = List.wrap(opts)

    {new([engine: engine] ++ Keyword.take(opts, [:catalog])), opts}
  end

  @doc """
  The children a supervisor starts for a catalog `resolve/2` returned —
  none when the handle was given outright.
  """
  @spec children(keyword() | nil, atom()) :: [{module(), keyword()}]
  def children(nil, _engine), do: []
  def children(opts, engine), do: [{__MODULE__, [name: engine] ++ opts}]

  @doc """
  The `ATTACH` statement that binds a metadata database and data path to a
  catalog name.

  Data inlining is switched off, and that is not a tuning choice. DuckLake
  otherwise keeps small writes and small deletes inside the metadata database
  until something flushes them, which breaks two things smolquery relies on:
  rows would live somewhere `segments/3` cannot see, and a delete covering a
  whole segment would leave the segment listed instead of retiring it — the
  spike measured exactly that at 10 rows per file, and correct retirement at
  5_000. With inlining off, a segment is always a file and a whole-segment
  delete always retires it.
  """
  @spec attach_statement(String.t(), String.t(), String.t()) :: String.t()
  def attach_statement(catalog, metadata, data_path) do
    "ATTACH IF NOT EXISTS #{Identifier.sql_string("ducklake:" <> metadata)} " <>
      "AS #{Identifier.quote_name!(catalog)} " <>
      "(DATA_PATH #{Identifier.sql_string(data_path)}, DATA_INLINING_ROW_LIMIT 0)"
  end

  defp ensure_metadata_dir("sqlite:" <> path), do: File.mkdir_p(Path.dirname(path))
  defp ensure_metadata_dir(_metadata), do: :ok

  defp postgres_metadata?("postgres:" <> _), do: true
  defp postgres_metadata?(_metadata), do: false

  @impl Catalog
  def create_dataset(%__MODULE__{} = config, dataset) do
    with {:ok, name} <- dataset_name(config, dataset),
         {:ok, _result} <- query(config, "CREATE SCHEMA IF NOT EXISTS #{name}") do
      :ok
    end
  end

  @impl Catalog
  def list_datasets(%__MODULE__{} = config) do
    column(
      config,
      "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = $1 " <>
        "ORDER BY schema_name",
      [config.catalog]
    )
  end

  @impl Catalog
  def create_table(%__MODULE__{} = config, table, %Schema{} = schema) do
    with {:ok, name} <- table_name(config, table),
         {:ok, columns} <- Schema.column_definitions(schema),
         {:ok, _result} <- query(config, "CREATE TABLE IF NOT EXISTS #{name} (#{columns})") do
      :ok
    end
  end

  @impl Catalog
  def list_tables(%__MODULE__{} = config, dataset) do
    with {:ok, dataset} <- Identifier.validate(dataset) do
      column(
        config,
        "SELECT table_name FROM information_schema.tables " <>
          "WHERE table_catalog = $1 AND table_schema = $2 ORDER BY table_name",
        [config.catalog, dataset]
      )
    end
  end

  @impl Catalog
  def table_schema(%__MODULE__{} = config, {dataset, table}) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, result} <-
           query(
             config,
             "SELECT column_name, data_type, is_nullable FROM information_schema.columns " <>
               "WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3 " <>
               "ORDER BY ordinal_position",
             [config.catalog, dataset, table]
           ) do
      build_schema(result.rows, {dataset, table})
    end
  end

  @impl Catalog
  def register_segments(%__MODULE__{} = config, {dataset, table}, segments) do
    paths = segments |> Enum.map(& &1.path) |> Enum.uniq()

    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         :ok <- with_commit_retries(fn -> add_missing(config, {dataset, table}, paths) end) do
      current_snapshot(config)
    end
  end

  defp add_missing(config, ref, paths) do
    with {:ok, registered} <- segments(config, ref, :current) do
      case paths -- registered do
        [] -> {:ok, :already_registered}
        pending -> query(config, add_statement(config, ref, pending))
      end
    end
  end

  @impl Catalog
  def segments(%__MODULE__{} = config, {dataset, table}, snapshot) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table) do
      arguments =
        [
          Identifier.sql_string(config.catalog),
          Identifier.sql_string(table),
          "schema => #{Identifier.sql_string(dataset)}"
        ] ++ snapshot_argument(snapshot)

      column(
        config,
        "SELECT data_file FROM ducklake_list_files(#{Enum.join(arguments, ", ")})"
      )
    end
  end

  @impl Catalog
  def registered_through(%__MODULE__{} = config, {dataset, table}, snapshot)
      when is_integer(snapshot) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, result} <-
           query(
             config,
             "SELECT DISTINCT df.path, df.path_is_relative " <>
               "FROM #{metadata_schema(config)}.ducklake_data_file df " <>
               "JOIN #{metadata_schema(config)}.ducklake_table t ON t.table_id = df.table_id " <>
               "JOIN #{metadata_schema(config)}.ducklake_schema s ON s.schema_id = t.schema_id " <>
               "WHERE s.schema_name = $1 AND t.table_name = $2 AND df.begin_snapshot <= $3",
             [dataset, table, snapshot]
           ) do
      absolute_paths(result.rows)
    end
  end

  @impl Catalog
  def drop_segments(%__MODULE__{} = config, _table, []), do: current_snapshot(config)

  def drop_segments(%__MODULE__{} = config, table, paths) do
    with {:ok, name} <- table_name(config, table),
         :ok <- commit(config, delete_statement(name, paths)) do
      current_snapshot(config)
    end
  end

  @impl Catalog
  def replace_segments(%__MODULE__{} = _config, _table, [], _paths), do: {:error, :no_segments}

  def replace_segments(%__MODULE__{} = config, {dataset, table}, segments, paths) do
    add = segments |> Enum.map(& &1.path) |> Enum.uniq()
    drop = Enum.uniq(paths)

    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         :ok <- with_commit_retries(fn -> swap_missing(config, {dataset, table}, add, drop) end) do
      current_snapshot(config)
    end
  end

  defp swap_missing(config, ref, add, drop) do
    with {:ok, registered} <- segments(config, ref, :current) do
      case add -- registered do
        [] -> {:ok, :already_swapped}
        pending -> swap(config, ref, pending, drop)
      end
    end
  end

  defp swap(config, ref, add, []), do: query(config, add_statement(config, ref, add))

  defp swap(config, ref, add, drop) do
    with {:ok, name} <- table_name(config, ref),
         :ok <-
           Engine.transaction(config.engine, [
             delete_statement(name, drop),
             add_statement(config, ref, add)
           ]) do
      {:ok, :committed}
    end
  end

  @impl Catalog
  def current_snapshot(%__MODULE__{} = config) do
    with {:ok, result} <-
           query(
             config,
             "SELECT id FROM ducklake_current_snapshot(#{Identifier.sql_string(config.catalog)})"
           ) do
      case result.rows do
        [[snapshot]] -> {:ok, snapshot}
        rows -> {:error, {:unexpected_snapshot_result, rows}}
      end
    end
  end

  @impl Catalog
  def known_segments(%__MODULE__{} = config) do
    with {:ok, result} <-
           query(
             config,
             "SELECT path, path_is_relative FROM #{metadata_schema(config)}.ducklake_data_file"
           ) do
      absolute_paths(result.rows)
    end
  end

  @impl Catalog
  def put_retention(%__MODULE__{} = config, {dataset, table}, nil) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         :ok <- ensure_retention_table(config),
         {:ok, _result} <- query(config, delete_retention_sql(config, dataset, table)) do
      :ok
    end
  end

  def put_retention(%__MODULE__{} = config, {dataset, table}, %{column: column, ttl_ms: ttl_ms})
      when is_binary(column) and is_integer(ttl_ms) and ttl_ms > 0 do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         :ok <- ensure_retention_table(config) do
      Engine.transaction(config.engine, [
        delete_retention_sql(config, dataset, table),
        "INSERT INTO #{retention_table(config)} VALUES (" <>
          "#{Identifier.sql_string(dataset)}, #{Identifier.sql_string(table)}, " <>
          "#{Identifier.sql_string(column)}, #{ttl_ms})"
      ])
    end
  end

  def put_retention(%__MODULE__{} = _config, _table, policy),
    do: {:error, {:invalid_retention, policy}}

  @impl Catalog
  def retention(%__MODULE__{} = config, {dataset, table}) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         :ok <- ensure_retention_table(config),
         {:ok, result} <-
           query(
             config,
             "SELECT column_name, ttl_ms FROM #{retention_table(config)} " <>
               "WHERE dataset = $1 AND table_name = $2",
             [dataset, table]
           ) do
      case result.rows do
        [] -> {:ok, nil}
        [[column, ttl_ms]] -> {:ok, %{column: column, ttl_ms: ttl_ms}}
        rows -> {:error, {:ambiguous_retention, rows}}
      end
    end
  end

  @impl Catalog
  def expire_snapshots(%__MODULE__{} = config, older_than_ms)
      when is_integer(older_than_ms) and older_than_ms > 0 do
    sql =
      "CALL ducklake_expire_snapshots(#{Identifier.sql_string(config.catalog)}, " <>
        "older_than => now() - INTERVAL #{Identifier.sql_string("#{older_than_ms} milliseconds")})"

    case query(config, sql) do
      {:ok, result} -> {:ok, result.num_rows}
      {:error, error} -> {:error, classify(error)}
    end
  end

  defp retention_table(config), do: "#{metadata_schema(config)}.smolquery_retention"

  defp ensure_retention_table(config) do
    sql =
      "CREATE TABLE IF NOT EXISTS #{retention_table(config)} (" <>
        "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, " <>
        "column_name VARCHAR NOT NULL, ttl_ms BIGINT NOT NULL)"

    with {:ok, _result} <- query(config, sql), do: :ok
  end

  defp delete_retention_sql(config, dataset, table) do
    "DELETE FROM #{retention_table(config)} WHERE dataset = " <>
      "#{Identifier.sql_string(dataset)} AND table_name = #{Identifier.sql_string(table)}"
  end

  defp absolute_paths(rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn
      [path, relative], {:ok, paths} when relative in [false, 0] ->
        {:cont, {:ok, [path | paths]}}

      [path, _relative], _acc ->
        {:halt, {:error, {:relative_segment_path, path}}}
    end)
    |> case do
      {:ok, paths} -> {:ok, Enum.reverse(paths)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp metadata_schema(%__MODULE__{catalog: catalog}),
    do: Identifier.quote_name!("__ducklake_metadata_" <> catalog)

  defp add_statement(config, {dataset, table}, paths) do
    literals = Enum.map_join(paths, ", ", &Identifier.sql_string/1)

    "CALL ducklake_add_data_files(" <>
      "#{Identifier.sql_string(config.catalog)}, #{Identifier.sql_string(table)}, " <>
      "[#{literals}], schema => #{Identifier.sql_string(dataset)})"
  end

  defp delete_statement(name, paths) do
    literals = paths |> Enum.uniq() |> Enum.map_join(", ", &Identifier.sql_string/1)

    "DELETE FROM #{name} WHERE filename IN (#{literals})"
  end

  defp commit(config, sql), do: with_commit_retries(fn -> query(config, sql) end)

  defp with_commit_retries(run, attempt \\ 1) do
    case run.() do
      {:ok, _result} ->
        :ok

      {:error, error} ->
        if conflict?(error) and attempt < @commit_attempts do
          Process.sleep(backoff(attempt))
          with_commit_retries(run, attempt + 1)
        else
          {:error, classify(error)}
        end
    end
  end

  defp backoff(attempt), do: (1 <<< attempt) * 5 + :rand.uniform(10)

  defp conflict?(%{__exception__: true} = error),
    do: Exception.message(error) =~ "Failed to commit"

  defp conflict?(_error), do: false

  defp classify(error) do
    if conflict?(error), do: :commit_conflict, else: error
  end

  defp snapshot_argument(:current), do: []

  defp snapshot_argument(snapshot) when is_integer(snapshot),
    do: ["snapshot_version => #{snapshot}"]

  defp build_schema([], ref), do: {:error, {:unknown_table, ref}}

  defp build_schema(rows, _ref) do
    rows
    |> Enum.reduce_while({:ok, []}, fn [name, type, nullable], {:ok, fields} ->
      case Schema.logical_from_duckdb(type) do
        {:ok, logical} ->
          field = %Field{name: name, type: logical, nullable: nullable == "YES"}
          {:cont, {:ok, [field | fields]}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, fields} -> Schema.new(Enum.reverse(fields))
      {:error, reason} -> {:error, reason}
    end
  end

  defp dataset_name(config, dataset) do
    with {:ok, dataset} <- Identifier.validate(dataset) do
      {:ok, "#{Identifier.quote_name!(config.catalog)}.#{Identifier.quote_name!(dataset)}"}
    end
  end

  defp table_name(config, {dataset, table}) do
    with {:ok, dataset} <- dataset_name(config, dataset),
         {:ok, table} <- Identifier.validate(table) do
      {:ok, "#{dataset}.#{Identifier.quote_name!(table)}"}
    end
  end

  defp column(config, sql, params \\ []) do
    with {:ok, result} <- query(config, sql, params) do
      {:ok, Enum.map(result.rows, &hd/1)}
    end
  end

  defp query(config, sql, params \\ []), do: Engine.query(config.engine, sql, params)

  defp engine_extensions do
    :smolquery
    |> Application.get_env(Engine, [])
    |> Keyword.get(:extensions, [])
  end
end
