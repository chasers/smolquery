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
  loaded, the lake attached, and the clustering side table created. All three
  happen as connection bootstrap statements, so a connection is never
  observable half-set-up and a restart redoes them:

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

  What that retry fires on is matched narrowly, against four markers. Two came
  from failures observed here: DuckLake's own `"Transaction conflict"`, and
  SQLite metadata's `"database is locked"`. Two are the metadata database's
  own ordinary transient commits — Postgres `"deadlock detected"` and
  `"could not serialize access"` — which arrive verbatim, a metadata error
  being passed through whole rather than summarised (that pass-through is what
  finally made the bug below readable). Those two are not hypothetical here:
  `replace_segments/4` sends a `DELETE` and an add in one transaction, and L6
  below has two nodes committing against one metadata database, which is the
  shape that deadlocks. Nothing else in smolquery retries them, so dropping
  them from this list would strand compaction on a failure that clears itself.

  DuckDB wraps *every* commit-time failure in `"Failed to commit"`, permanent
  ones included, so keying on that prefix spent all five attempts on errors no
  retry could clear and then reported them as `:commit_conflict` — which named
  a race that had not happened and hid the one thing that would have explained
  the failure. The case that found this was a Postgres metadata database whose
  `ducklake_table_stats` a `FOR ALL TABLES` publication covers and no primary
  key identifies: Postgres refuses the `UPDATE` for want of a replica identity,
  so every commit after a table's first one fails, the first having inserted
  the stats row each later one updates. A non-retryable commit now fails at
  once and carries the metadata database's own message.

  All four exhaust into `:commit_conflict`, which therefore names contention
  rather than strictly a lost race — a held SQLite file lock and a deadlock are
  neither of them races. Worth knowing when reading that atom out of a
  compaction log, where it is the whole of what an operator gets.

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
  alias Smolquery.Catalog.Connection
  alias Smolquery.Catalog.Dataset
  alias Smolquery.Engine
  alias Smolquery.EngineSecrets
  alias Smolquery.Identifier
  alias Smolquery.Partitions
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Store

  @enforce_keys [:engine, :catalog]
  defstruct [:engine, :catalog]

  @type t :: %__MODULE__{engine: atom(), catalog: String.t()}

  @type option ::
          {:name, atom()}
          | {:metadata, String.t()}
          | {:data_path, String.t()}
          | {:catalog, String.t()}
          | {:automatic_migration, boolean()}
          | {:store, Store.t()}

  @default_catalog "lake"
  @commit_attempts 5
  @retryable_markers [
    "Transaction conflict",
    "database is locked",
    "deadlock detected",
    "could not serialize access"
  ]

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
    * `:automatic_migration` — passed to `attach_statement/4`
    * `:store` — the sealed-tier store; a credential-chain S3 store adds
      the extensions its `CREATE SECRET` statement needs
    * any `Smolquery.Engine` option — `:extensions` is extended with
      `:ducklake` rather than replaced

  """
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    config = Keyword.merge(Application.get_env(:smolquery, __MODULE__, []), opts)
    {catalog, config} = Keyword.pop(config, :catalog, @default_catalog)
    {metadata, config} = Keyword.pop!(config, :metadata)
    {data_path, config} = Keyword.pop!(config, :data_path)
    {automatic_migration, config} = Keyword.pop(config, :automatic_migration, false)

    :ok = ensure_metadata_dir(metadata)

    {store, config} = Keyword.pop(config, :store)
    statements = Keyword.get(config, :statements, [])
    required = if postgres_metadata?(metadata), do: [:postgres, :ducklake], else: [:ducklake]

    extensions =
      config
      |> Keyword.get(:extensions, engine_extensions())
      |> then(&EngineSecrets.sealed_tier_extensions(store, &1))

    bootstrap = [
      attach_statement(catalog, metadata, data_path, automatic_migration: automatic_migration),
      create_clustering_statement(catalog),
      create_partitions_statement(catalog),
      create_connections_statement(catalog),
      create_datasets_statement(catalog)
    ]

    config
    |> Keyword.put(:extensions, Enum.uniq(required ++ extensions))
    |> Keyword.put(:statements, bootstrap ++ statements)
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

  ## Options

    * `:automatic_migration` — when `true`, adds `AUTOMATIC_MIGRATION TRUE`,
      so an extension carrying a newer DuckLake catalog version migrates the
      metadata database on attach instead of refusing to boot. Off by
      default: a migration rewrites the shared catalog one way, and a node
      still running the old extension cannot read the result, so the
      operator picks the moment (`SMOLQUERY_CATALOG_AUTOMATIC_MIGRATION`).
  """
  @spec attach_statement(String.t(), String.t(), String.t(), keyword()) :: String.t()
  def attach_statement(catalog, metadata, data_path, opts \\ []) do
    migration =
      if Keyword.get(opts, :automatic_migration, false) do
        ", AUTOMATIC_MIGRATION TRUE"
      else
        ""
      end

    "ATTACH IF NOT EXISTS #{Identifier.sql_string("ducklake:" <> metadata)} " <>
      "AS #{Identifier.quote_name!(catalog)} " <>
      "(DATA_PATH #{Identifier.sql_string(data_path)}, DATA_INLINING_ROW_LIMIT 0#{migration})"
  end

  defp ensure_metadata_dir("sqlite:" <> path), do: File.mkdir_p(Path.dirname(path))
  defp ensure_metadata_dir(_metadata), do: :ok

  defp postgres_metadata?("postgres:" <> _), do: true
  defp postgres_metadata?(_metadata), do: false

  @impl Catalog
  def create_dataset(%__MODULE__{} = config, %Dataset{} = dataset) do
    with {:ok, name} <- dataset_name(config, dataset.name),
         {:ok, existing} <- find_dataset(config, dataset.name),
         :ok <- creatable(existing, dataset),
         :ok <- catalog_unclaimed(config, dataset),
         :ok <- create_schema(config, name, dataset) do
      insert_dataset(config, dataset, existing)
    end
  end

  @impl Catalog
  def list_datasets(%__MODULE__{} = config) do
    with {:ok, schemata} <- schemata(config),
         {:ok, recorded} <- column(config, "SELECT name FROM #{datasets_table(config.catalog)}") do
      {:ok, Enum.sort(Enum.uniq(schemata ++ recorded))}
    end
  end

  @impl Catalog
  def dataset(%__MODULE__{} = config, name) do
    case find_dataset(config, name) do
      {:ok, nil} -> {:error, {:unknown_dataset, name}}
      found -> found
    end
  end

  @impl Catalog
  def update_dataset(%__MODULE__{} = config, %Dataset{} = dataset) do
    Engine.transaction(config.engine, [
      delete_dataset_sql(config, dataset.name),
      insert_dataset_sql(config, dataset, dataset.created_at)
    ])
  end

  defp schemata(config) do
    column(
      config,
      "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = $1",
      [config.catalog]
    )
  end

  defp find_dataset(config, name) do
    with {:ok, %{rows: rows}} <- query(config, select_dataset_sql(config, name)) do
      case rows do
        [row] -> {:ok, dataset_from_row(row)}
        [] -> find_schema(config, name)
      end
    end
  end

  defp find_schema(config, name) do
    with {:ok, schemata} <- schemata(config) do
      if name in schemata, do: Dataset.default(name), else: {:ok, nil}
    end
  end

  defp creatable(nil, _dataset), do: :ok

  defp creatable(%Dataset{} = existing, %Dataset{} = dataset) do
    if Dataset.same_settings?(existing, dataset),
      do: :ok,
      else: {:error, {:dataset_exists, dataset.name}}
  end

  defp catalog_unclaimed(_config, %Dataset{catalog: nil}), do: :ok

  defp catalog_unclaimed(config, %Dataset{name: name, catalog: catalog}) do
    sql =
      "SELECT name FROM #{datasets_table(config.catalog)} " <>
        "WHERE name <> #{Identifier.sql_string(name)} " <>
        "AND catalog_host = #{Identifier.sql_string(catalog.host)} " <>
        "AND catalog_port = #{catalog.port} " <>
        "AND catalog_database = #{Identifier.sql_string(catalog.database)}"

    case column(config, sql) do
      {:ok, []} -> :ok
      {:ok, [owner | _rest]} -> {:error, {:catalog_in_use, owner}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_schema(_config, _name, %Dataset{catalog: %Dataset.Catalog{}}), do: :ok

  defp create_schema(config, name, %Dataset{catalog: nil}) do
    with {:ok, _result} <- query(config, "CREATE SCHEMA IF NOT EXISTS #{name}"), do: :ok
  end

  defp insert_dataset(_config, _dataset, %Dataset{created_at: at}) when is_integer(at), do: :ok

  defp insert_dataset(config, dataset, _unrecorded) do
    with {:ok, _result} <- query(config, insert_dataset_sql(config, dataset, nil)), do: :ok
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
           ),
         {:ok, schema} <- build_schema(result.rows, {dataset, table}),
         {:ok, clustering, partitions} <- side_options(config, {dataset, table}) do
      {:ok, %{schema | clustering: clustering, partitions: partitions}}
    end
  end

  defp side_options(config, {dataset, table}) do
    sql =
      "SELECT 0 AS kind, column_name AS name, position AS value " <>
        "FROM #{clustering_table(config.catalog)} WHERE dataset = $1 AND table_name = $2 " <>
        "UNION ALL SELECT 1, NULL, partition_count " <>
        "FROM #{partitions_table(config.catalog)} WHERE dataset = $1 AND table_name = $2 " <>
        "ORDER BY kind, value"

    with {:ok, result} <- query(config, sql, [dataset, table]) do
      {clustering_rows, partition_rows} =
        Enum.split_with(result.rows, fn [kind, _name, _value] -> kind == 0 end)

      clustering = Enum.map(clustering_rows, fn [_kind, name, _position] -> name end)

      case partition_rows do
        [] -> {:ok, clustering, nil}
        [[_kind, _name, count]] -> {:ok, clustering, count}
        rows -> {:error, {:ambiguous_partitions, rows}}
      end
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
               "FROM #{metadata_schema(config.catalog)}.ducklake_data_file df " <>
               "JOIN #{metadata_schema(config.catalog)}.ducklake_table t " <>
               "ON t.table_id = df.table_id " <>
               "JOIN #{metadata_schema(config.catalog)}.ducklake_schema s " <>
               "ON s.schema_id = t.schema_id " <>
               "WHERE s.schema_name = $1 AND t.table_name = $2 AND df.begin_snapshot <= $3",
             [dataset, table, snapshot]
           ) do
      absolute_paths(result.rows)
    end
  end

  @impl Catalog
  def segment_stats(%__MODULE__{} = config, {dataset, table}, snapshot)
      when is_integer(snapshot) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, result} <-
           query(
             config,
             "SELECT CAST(COUNT(*) AS BIGINT), " <>
               "CAST(COALESCE(SUM(df.record_count), 0) AS BIGINT), " <>
               "CAST(COALESCE(SUM(df.file_size_bytes), 0) AS BIGINT) " <>
               "FROM #{metadata_schema(config.catalog)}.ducklake_data_file df " <>
               "JOIN #{metadata_schema(config.catalog)}.ducklake_table t " <>
               "ON t.table_id = df.table_id " <>
               "JOIN #{metadata_schema(config.catalog)}.ducklake_schema s " <>
               "ON s.schema_id = t.schema_id " <>
               "WHERE s.schema_name = $1 AND t.table_name = $2 " <>
               "AND df.begin_snapshot <= $3 " <>
               "AND (df.end_snapshot IS NULL OR df.end_snapshot > $3) " <>
               "AND t.begin_snapshot <= $3 " <>
               "AND (t.end_snapshot IS NULL OR t.end_snapshot > $3)",
             [dataset, table, snapshot]
           ) do
      case result.rows do
        [[files, rows, bytes]] -> {:ok, %{files: files, rows: rows, bytes: bytes}}
        rows -> {:error, {:unexpected_stats_result, rows}}
      end
    end
  end

  @impl Catalog
  def segment_files(%__MODULE__{} = config, {dataset, table}, snapshot)
      when is_integer(snapshot) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, result} <-
           query(
             config,
             "SELECT df.path, df.path_is_relative, " <>
               "CAST(COALESCE(df.record_count, 0) AS BIGINT), " <>
               "CAST(COALESCE(df.file_size_bytes, 0) AS BIGINT) " <>
               "FROM #{metadata_schema(config.catalog)}.ducklake_data_file df " <>
               "JOIN #{metadata_schema(config.catalog)}.ducklake_table t " <>
               "ON t.table_id = df.table_id " <>
               "JOIN #{metadata_schema(config.catalog)}.ducklake_schema s " <>
               "ON s.schema_id = t.schema_id " <>
               "WHERE s.schema_name = $1 AND t.table_name = $2 " <>
               "AND df.begin_snapshot <= $3 " <>
               "AND (df.end_snapshot IS NULL OR df.end_snapshot > $3) " <>
               "AND t.begin_snapshot <= $3 " <>
               "AND (t.end_snapshot IS NULL OR t.end_snapshot > $3) " <>
               "ORDER BY df.path",
             [dataset, table, snapshot]
           ) do
      segment_file_rows(result.rows)
    end
  end

  defp segment_file_rows(rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn
      [path, relative, rows, bytes], {:ok, files} when relative in [false, 0] ->
        {:cont, {:ok, [%{path: path, rows: rows, bytes: bytes} | files]}}

      [path, _relative, _rows, _bytes], _acc ->
        {:halt, {:error, {:relative_segment_path, path}}}
    end)
    |> case do
      {:ok, files} -> {:ok, Enum.reverse(files)}
      {:error, reason} -> {:error, reason}
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
             "SELECT path, path_is_relative FROM " <>
               "#{metadata_schema(config.catalog)}.ducklake_data_file"
           ) do
      absolute_paths(result.rows)
    end
  end

  @impl Catalog
  def put_retention(%__MODULE__{} = config, table_ref, policy),
    do: put_table_options(config, table_ref, %{retention: policy})

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
  def put_clustering(%__MODULE__{} = config, table_ref, columns),
    do: put_table_options(config, table_ref, %{clustering: columns})

  @impl Catalog
  def clustering(%__MODULE__{} = config, {dataset, table}) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, result} <-
           query(
             config,
             "SELECT column_name FROM #{clustering_table(config.catalog)} " <>
               "WHERE dataset = $1 AND table_name = $2 ORDER BY position",
             [dataset, table]
           ) do
      {:ok, Enum.map(result.rows, fn [column] -> column end)}
    end
  end

  @impl Catalog
  def put_partitions(%__MODULE__{} = config, table_ref, count),
    do: put_table_options(config, table_ref, %{partitions: count})

  @impl Catalog
  def partitions(%__MODULE__{} = config, {dataset, table}) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, result} <-
           query(
             config,
             "SELECT partition_count FROM #{partitions_table(config.catalog)} " <>
               "WHERE dataset = $1 AND table_name = $2",
             [dataset, table]
           ) do
      case result.rows do
        [] -> {:ok, nil}
        [[count]] -> {:ok, count}
        rows -> {:error, {:ambiguous_partitions, rows}}
      end
    end
  end

  defp clustering_insert_sqls(config, dataset, table, columns) do
    Enum.map(Enum.with_index(columns), fn {column, position} ->
      "INSERT INTO #{clustering_table(config.catalog)} VALUES (" <>
        "#{Identifier.sql_string(dataset)}, #{Identifier.sql_string(table)}, " <>
        "#{Identifier.sql_string(column)}, #{position})"
    end)
  end

  @impl Catalog
  def put_table_options(%__MODULE__{} = config, {dataset, table}, options)
      when is_map(options) do
    with :ok <- validate_options(options),
         {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         :ok <- maybe_ensure_retention_table(config, options) do
      case option_statements(config, dataset, table, options) do
        [] -> :ok
        statements -> Engine.transaction(config.engine, statements)
      end
    end
  end

  defp maybe_ensure_retention_table(config, options) do
    if Map.has_key?(options, :retention), do: ensure_retention_table(config), else: :ok
  end

  defp validate_options(options) do
    Enum.reduce_while(options, :ok, fn
      {:retention, nil}, :ok ->
        {:cont, :ok}

      {:retention, %{column: column, ttl_ms: ttl_ms}}, :ok
      when is_binary(column) and is_integer(ttl_ms) and ttl_ms > 0 ->
        {:cont, :ok}

      {:retention, policy}, :ok ->
        {:halt, {:error, {:invalid_retention, policy}}}

      {:clustering, []}, :ok ->
        {:cont, :ok}

      {:clustering, columns}, :ok when is_list(columns) ->
        if valid_clustering?(columns),
          do: {:cont, :ok},
          else: {:halt, {:error, {:invalid_clustering, columns}}}

      {:clustering, columns}, :ok ->
        {:halt, {:error, {:invalid_clustering, columns}}}

      {:partitions, count}, :ok when is_integer(count) and count > 0 ->
        if count <= Partitions.max_count(),
          do: {:cont, :ok},
          else: {:halt, {:error, {:invalid_partitions, count}}}

      {:partitions, count}, :ok ->
        {:halt, {:error, {:invalid_partitions, count}}}

      {key, value}, :ok ->
        {:halt, {:error, {:unknown_table_option, key, value}}}
    end)
  end

  defp option_statements(config, dataset, table, options) do
    retention_statements(config, dataset, table, options) ++
      clustering_statements(config, dataset, table, options) ++
      partitions_statements(config, dataset, table, options)
  end

  defp retention_statements(config, dataset, table, options) do
    case Map.fetch(options, :retention) do
      :error ->
        []

      {:ok, nil} ->
        [delete_retention_sql(config, dataset, table)]

      {:ok, %{column: column, ttl_ms: ttl_ms}} ->
        [
          delete_retention_sql(config, dataset, table),
          "INSERT INTO #{retention_table(config)} VALUES (" <>
            "#{Identifier.sql_string(dataset)}, #{Identifier.sql_string(table)}, " <>
            "#{Identifier.sql_string(column)}, #{ttl_ms})"
        ]
    end
  end

  defp clustering_statements(config, dataset, table, options) do
    case Map.fetch(options, :clustering) do
      :error ->
        []

      {:ok, columns} ->
        [
          delete_clustering_sql(config, dataset, table)
          | clustering_insert_sqls(config, dataset, table, columns)
        ]
    end
  end

  defp partitions_statements(config, dataset, table, options) do
    case Map.fetch(options, :partitions) do
      :error ->
        []

      {:ok, count} ->
        [
          delete_partitions_below_sql(config, dataset, table, count),
          "INSERT INTO #{partitions_table(config.catalog)} " <>
            "SELECT #{Identifier.sql_string(dataset)}, " <>
            "#{Identifier.sql_string(table)}, #{count} " <>
            "WHERE NOT EXISTS (SELECT 1 FROM #{partitions_table(config.catalog)} " <>
            "WHERE dataset = #{Identifier.sql_string(dataset)} " <>
            "AND table_name = #{Identifier.sql_string(table)})"
        ]
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

  defp retention_table(config), do: "#{metadata_schema(config.catalog)}.smolquery_retention"

  defp ensure_retention_table(config) do
    sql =
      "CREATE TABLE IF NOT EXISTS #{retention_table(config)} (" <>
        "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, " <>
        "column_name VARCHAR NOT NULL, ttl_ms BIGINT NOT NULL, " <>
        "PRIMARY KEY (dataset, table_name))"

    with {:ok, _result} <- query(config, sql), do: :ok
  end

  defp delete_retention_sql(config, dataset, table) do
    "DELETE FROM #{retention_table(config)} WHERE dataset = " <>
      "#{Identifier.sql_string(dataset)} AND table_name = #{Identifier.sql_string(table)}"
  end

  defp clustering_table(catalog), do: "#{metadata_schema(catalog)}.smolquery_clustering"

  @doc """
  The `CREATE TABLE IF NOT EXISTS` that gives a lake its clustering side table.

  Public for the same reason `attach_statement/3` is: it is bootstrap SQL, run
  once per connection right after the `ATTACH` that makes the metadata schema
  reachable.

  Retention creates its own side table lazily, on the retention path, and that
  is still right for it — retention is read when a sweep runs. Clustering is
  read by `table_schema/2`, which the query planner calls per query per table
  and does not cache, so a lazy `CREATE TABLE IF NOT EXISTS` would put DDL on
  the hottest catalog read in the system: measured at 1.5 ms of a 5.2 ms
  `table_schema/2` on sqlite, serialized behind the engine's single connection,
  and on Postgres metadata a concurrent first use can fail outright with a
  duplicate-relation error. Bootstrapping it costs one statement per
  connection instead.

  The primary key (here and on retention's table) is not for lookups — the
  tables are tiny — it is the replica identity. The moduledoc's retry story
  turns on Postgres refusing `UPDATE`s to a published table that has no
  identity to replicate rows by, and the same rule covers the `DELETE` every
  `put_clustering`/`put_retention` starts with. DuckLake's own PK-less tables
  make such a database unusable as metadata anyway, but that is DuckLake's
  bug to carry, not one to add to. DuckDB passes the constraint through both
  metadata attaches, verified: sqlite stores it, and on Postgres
  `pg_constraint` shows the key with `relreplident` defaulting to it. An
  `IF NOT EXISTS` no-ops on a table created before the key existed, so a lake
  from before this change keeps its PK-less side tables until they are
  recreated.
  """
  @spec create_clustering_statement(String.t()) :: String.t()
  def create_clustering_statement(catalog) do
    "CREATE TABLE IF NOT EXISTS #{clustering_table(catalog)} (" <>
      "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, " <>
      "column_name VARCHAR NOT NULL, position INTEGER NOT NULL, " <>
      "PRIMARY KEY (dataset, table_name, position))"
  end

  defp partitions_table(catalog), do: "#{metadata_schema(catalog)}.smolquery_partitions"

  @doc """
  The `CREATE TABLE IF NOT EXISTS` that gives a lake its partition-count side
  table (T-304).

  Bootstrap SQL like `create_clustering_statement/1`, and for the same
  reason: the count is read by `table_schema/2`, the hottest catalog read in
  the system, so a lazy `CREATE TABLE IF NOT EXISTS` there would put DDL on
  the ingest and query paths. The primary key is the replica identity for a
  published Postgres metadata DB — see `create_clustering_statement/1`.
  """
  @spec create_partitions_statement(String.t()) :: String.t()
  def create_partitions_statement(catalog) do
    "CREATE TABLE IF NOT EXISTS #{partitions_table(catalog)} (" <>
      "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, " <>
      "partition_count INTEGER NOT NULL, " <>
      "PRIMARY KEY (dataset, table_name))"
  end

  defp connections_table(catalog), do: "#{metadata_schema(catalog)}.smolquery_connections"

  @doc """
  The `CREATE TABLE IF NOT EXISTS` that gives a lake its federated-connection
  side table (T-322).

  Bootstrap SQL like `create_clustering_statement/1`, and for the same reason:
  the planner reads connections per query, to decide whether a
  catalog-qualified reference names a registered database or is the error it
  has always been. Creating the table lazily would put DDL on that read.

  The primary key on `name` is the replica identity for a published Postgres
  metadata database — see `create_clustering_statement/1` — and it is also the
  uniqueness the name needs on its own terms, since it becomes a DuckDB
  catalog alias.

  `secret` holds what `Smolquery.Secrets` sealed. The column never contains a
  password, so a metadata database that is dumped, replicated, or read by an
  operator yields ciphertext whose key lives only in the environment.
  """
  @spec create_connections_statement(String.t()) :: String.t()
  def create_connections_statement(catalog) do
    "CREATE TABLE IF NOT EXISTS #{connections_table(catalog)} (" <>
      "name VARCHAR NOT NULL, host VARCHAR NOT NULL, port INTEGER NOT NULL, " <>
      "database_name VARCHAR NOT NULL, username VARCHAR NOT NULL, " <>
      "secret VARCHAR NOT NULL, sslmode VARCHAR NOT NULL, " <>
      "created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, " <>
      "PRIMARY KEY (name))"
  end

  @impl Catalog
  def put_connection(%__MODULE__{} = config, %Connection{} = connection) do
    now = System.system_time(:millisecond)
    created_at = connection.created_at || now

    Engine.transaction(config.engine, [
      delete_connection_sql(config, connection.name),
      "INSERT INTO #{connections_table(config.catalog)} " <>
        "(name, host, port, database_name, username, secret, sslmode, created_at, updated_at) " <>
        "VALUES (#{Identifier.sql_string(connection.name)}, " <>
        "#{Identifier.sql_string(connection.host)}, #{connection.port}, " <>
        "#{Identifier.sql_string(connection.database)}, " <>
        "#{Identifier.sql_string(connection.username)}, " <>
        "#{Identifier.sql_string(connection.secret)}, " <>
        "#{Identifier.sql_string(connection.sslmode)}, #{created_at}, #{now})"
    ])
  end

  @impl Catalog
  def connection(%__MODULE__{} = config, name) do
    sql =
      "SELECT name, host, port, database_name, username, secret, sslmode, created_at, updated_at " <>
        "FROM #{connections_table(config.catalog)} WHERE name = #{Identifier.sql_string(name)}"

    case query(config, sql) do
      {:ok, %{rows: [row]}} -> {:ok, connection_from_row(row)}
      {:ok, %{rows: []}} -> {:error, {:unknown_connection, name}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Catalog
  def list_connections(%__MODULE__{} = config) do
    sql =
      "SELECT name, host, port, database_name, username, secret, sslmode, created_at, updated_at " <>
        "FROM #{connections_table(config.catalog)} ORDER BY name"

    with {:ok, %{rows: rows}} <- query(config, sql) do
      {:ok, Enum.map(rows, &connection_from_row/1)}
    end
  end

  @impl Catalog
  def delete_connection(%__MODULE__{} = config, name) do
    with {:ok, _result} <- query(config, delete_connection_sql(config, name)), do: :ok
  end

  defp delete_connection_sql(config, name) do
    "DELETE FROM #{connections_table(config.catalog)} " <>
      "WHERE name = #{Identifier.sql_string(name)}"
  end

  defp connection_from_row([
         name,
         host,
         port,
         database,
         username,
         secret,
         sslmode,
         created_at,
         updated_at
       ]) do
    %Connection{
      name: name,
      host: host,
      port: port,
      database: database,
      username: username,
      secret: secret,
      sslmode: sslmode,
      created_at: created_at,
      updated_at: updated_at
    }
  end

  defp datasets_table(catalog), do: "#{metadata_schema(catalog)}.smolquery_datasets"

  @dataset_columns ~w(name catalog_host catalog_port catalog_database catalog_username
    catalog_secret catalog_sslmode catalog_version storage_bucket storage_prefix storage_endpoint
    storage_region storage_url_style storage_access_key_id storage_secret
    created_at updated_at)

  @doc """
  The `CREATE TABLE IF NOT EXISTS` that gives a lake its dataset side table
  (PL-51).

  Bootstrap SQL like `create_connections_statement/1`: `list_datasets/1`
  reads it on every listing, and `create_dataset/2` on every create, so a
  lazy `CREATE TABLE IF NOT EXISTS` would put DDL on both. The primary key on
  `name` is the replica identity for a published Postgres metadata database —
  see `create_clustering_statement/1`.

  `catalog_version` is the DuckLake format the dataset's lake is pinned to
  (PL-51 D7); it is here from the first release of the table because adding
  it to a deployed table later means an `ALTER` on every default lake.

  A dataset on the deployment defaults stores `NULL` in every `catalog_*`
  and `storage_*` column; a dataset created before this table existed has no
  row at all, and reads back the same way. `catalog_secret` and
  `storage_secret` hold what `Smolquery.Secrets` sealed, never a credential.
  """
  @spec create_datasets_statement(String.t()) :: String.t()
  def create_datasets_statement(catalog) do
    "CREATE TABLE IF NOT EXISTS #{datasets_table(catalog)} (" <>
      "name VARCHAR NOT NULL, " <>
      "catalog_host VARCHAR, catalog_port INTEGER, catalog_database VARCHAR, " <>
      "catalog_username VARCHAR, catalog_secret VARCHAR, catalog_sslmode VARCHAR, " <>
      "catalog_version VARCHAR, " <>
      "storage_bucket VARCHAR, storage_prefix VARCHAR, storage_endpoint VARCHAR, " <>
      "storage_region VARCHAR, storage_url_style VARCHAR, " <>
      "storage_access_key_id VARCHAR, storage_secret VARCHAR, " <>
      "created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, " <>
      "PRIMARY KEY (name))"
  end

  @dataset_column_list Enum.join(@dataset_columns, ", ")

  defp select_dataset_sql(config, name) do
    "SELECT #{@dataset_column_list} FROM #{datasets_table(config.catalog)} " <>
      "WHERE name = #{Identifier.sql_string(name)}"
  end

  defp delete_dataset_sql(config, name) do
    "DELETE FROM #{datasets_table(config.catalog)} WHERE name = #{Identifier.sql_string(name)}"
  end

  defp insert_dataset_sql(config, %Dataset{} = dataset, created_at) do
    now = System.system_time(:millisecond)
    catalog = dataset.catalog || %{}
    storage = dataset.storage || %{}

    values = [
      Identifier.sql_string(dataset.name),
      nullable_string(Map.get(catalog, :host)),
      nullable_integer(Map.get(catalog, :port)),
      nullable_string(Map.get(catalog, :database)),
      nullable_string(Map.get(catalog, :username)),
      nullable_string(Map.get(catalog, :secret)),
      nullable_string(Map.get(catalog, :sslmode)),
      nullable_string(Map.get(catalog, :version)),
      nullable_string(Map.get(storage, :bucket)),
      nullable_string(Map.get(storage, :prefix)),
      nullable_string(Map.get(storage, :endpoint)),
      nullable_string(Map.get(storage, :region)),
      nullable_string(Map.get(storage, :url_style)),
      nullable_string(Map.get(storage, :access_key_id)),
      nullable_string(Map.get(storage, :secret)),
      Integer.to_string(created_at || now),
      Integer.to_string(now)
    ]

    row = Enum.join(values, ", ")

    "INSERT INTO #{datasets_table(config.catalog)} (#{@dataset_column_list}) VALUES (#{row})"
  end

  defp nullable_string(nil), do: "NULL"
  defp nullable_string(value), do: Identifier.sql_string(value)

  defp nullable_integer(nil), do: "NULL"
  defp nullable_integer(value), do: Integer.to_string(value)

  defp dataset_from_row([name | columns]) do
    {catalog_columns, [bucket, prefix, endpoint, region, url_style, key_id, secret, at, updated]} =
      Enum.split(columns, 7)

    %Dataset{
      name: name,
      catalog: catalog_from_columns(catalog_columns),
      storage: storage_from_columns(bucket, prefix, endpoint, region, url_style, key_id, secret),
      created_at: at,
      updated_at: updated
    }
  end

  defp catalog_from_columns([nil | _rest]), do: nil

  defp catalog_from_columns([host, port, database, username, secret, sslmode, version]) do
    %Dataset.Catalog{
      host: host,
      port: port,
      database: database,
      username: username,
      secret: secret,
      sslmode: sslmode,
      version: version
    }
  end

  defp storage_from_columns(nil, _prefix, _endpoint, _region, _url_style, _key_id, _secret),
    do: nil

  defp storage_from_columns(bucket, prefix, endpoint, region, url_style, key_id, secret) do
    %Dataset.Storage{
      bucket: bucket,
      prefix: prefix,
      endpoint: endpoint,
      region: region,
      url_style: url_style,
      access_key_id: key_id,
      secret: secret
    }
  end

  defp delete_partitions_below_sql(config, dataset, table, count) do
    "DELETE FROM #{partitions_table(config.catalog)} WHERE dataset = " <>
      "#{Identifier.sql_string(dataset)} AND table_name = #{Identifier.sql_string(table)} " <>
      "AND partition_count < #{count}"
  end

  defp delete_clustering_sql(config, dataset, table) do
    "DELETE FROM #{clustering_table(config.catalog)} WHERE dataset = " <>
      "#{Identifier.sql_string(dataset)} AND table_name = #{Identifier.sql_string(table)}"
  end

  defp valid_clustering?(columns) do
    columns != [] and Enum.all?(columns, &is_binary/1) and columns == Enum.uniq(columns)
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

  defp metadata_schema(catalog),
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
        if retryable?(error) and attempt < @commit_attempts do
          Process.sleep(backoff(attempt))
          with_commit_retries(run, attempt + 1)
        else
          {:error, classify(error)}
        end
    end
  end

  defp backoff(attempt), do: (1 <<< attempt) * 5 + :rand.uniform(10)

  @doc """
  Whether a failed commit is worth retrying.

  Public for the same reason `Smolquery.Engine.Connection.fatal?/1` is: it
  classifies DuckDB by message text, so the strings it keys on are pinned by a
  test rather than trusted. Four are retryable — DuckLake's own
  `"Transaction conflict"`, SQLite metadata's `"database is locked"`, and
  Postgres metadata's `"deadlock detected"` and `"could not serialize access"`.
  A commit that failed for any other reason is permanent, however much its
  wrapper reads like a lost race; see the moduledoc.
  """
  @spec retryable?(Exception.t() | term()) :: boolean()
  def retryable?(%{__exception__: true} = error) do
    message = Exception.message(error)

    Enum.any?(@retryable_markers, &String.contains?(message, &1))
  end

  def retryable?(_error), do: false

  defp classify(error) do
    if retryable?(error), do: :commit_conflict, else: error
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
