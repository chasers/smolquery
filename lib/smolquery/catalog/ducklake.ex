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

  What that retry fires on is matched narrowly, against six markers. Two came
  from failures observed here: DuckLake's own `"Transaction conflict"`, and
  SQLite metadata's `"database is locked"`. Two are the metadata database's
  own ordinary transient commits — Postgres `"deadlock detected"` and
  `"could not serialize access"` — which arrive verbatim, a metadata error
  being passed through whole rather than summarised (that pass-through is what
  finally made the bug below readable). Those two are not hypothetical here:
  `replace_segments/4` writes several metadata tables in one transaction, and
  L6 below has two nodes committing against one metadata database, which is the
  shape that deadlocks. Nothing else in smolquery retries them, so dropping
  them from this list would strand compaction on a failure that clears itself.
  The last two name the snapshot key a swap's move writes
  (`Smolquery.Catalog.DuckLake.Swap`): Postgres's `"ducklake_snapshot_pkey"`
  and SQLite's `"ducklake_snapshot.snapshot_id"`, a commit that landed first.

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

  All six exhaust into `:commit_conflict`, which therefore names contention
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

  ## A call that exits is an error here, never a crash upstream

  Every statement this module runs goes through `Smolquery.Engine.try_query/4`
  or `Smolquery.Engine.try_transaction/4`, so a call that times out on a busy
  connection, or finds the connection gone, comes back as
  `{:error, %Smolquery.Engine.CallExited{}}` like any other failure (T-464).
  Each is also one `[:smolquery, :catalog, :statement]` event, by kind and
  result (T-549): over the `[:smolquery, :catalog, :op]` events, how many
  statements one catalog operation costs, and how long each takes. The
  swap also times its parts, as kinds `:stage` (DuckLake registering the
  merged file into the twin) and `:move` and `:commit` (the metadata
  transaction, inside its one `:transaction`), so a slow registration, a
  slow move and a commit that lost the snapshot key are told apart (T-573,
  T-600). Every
  attempt `with_commit_retries/2` makes is one
  `[:smolquery, :catalog, :commit_attempt]` event, by attempt number and
  result (`:ok`, `:conflict` or `:error`), and a conflict it retries is
  logged at info: a retried conflict otherwise shows nowhere but a
  subtraction of statement counts.
  Every `Smolquery.Catalog` callback already promises `{:error, term()}`, and
  the callers that matter are sweeps: the compactor, retention and GC each
  visit every table in one long-lived process, and an exit from one table's
  read used to take the whole sweep down and, for GC, its grace-period
  bookkeeping with it. The abandoned statement keeps running on the
  connection either way; what changes is that the caller keeps its state.

  `ducklake_merge_adjacent_files/2` must never be called on a smolquery table:
  over externally-registered files it crashes DuckDB fatally (ducklake
  `67480b1d`, format 0.4), and a fatal error invalidates the whole database.
  Compaction stays `Smolquery.StorageService.Scheduler`'s job, built on
  `replace_segments/4`: registration and retirement in one metadata
  transaction, so a single snapshot carries both
  (`Smolquery.Catalog.DuckLake.Swap`).

  ## A swap whose inputs are not all live refuses

  Two compactions can merge overlapping groups: two nodes while the ring
  changes, or the two levels of `Smolquery.StorageService.Scheduler` if a
  merge outlasts the bucket between them. The loser's `DELETE` then finds its
  retired inputs already gone and removes nothing for them, while its add
  registers a merged file holding their rows a second time. So the swap,
  inside its commit retry and against the listing it already reads, refuses
  with `{:error, {:inputs_not_live, paths}}` when any input has left the
  table, and commits nothing. The winner's merge stands, and the loser's
  merged file is an orphan for GC. A retry of a swap that did commit still
  answers first, from its registered additions.

  ## The swap is a compaction, not a delete

  The swap used to be a `DELETE ... WHERE filename IN (...)` and a
  `ducklake_add_data_files` in one DuckLake transaction. It failed two ways
  on `metrics.samples`. DuckLake answers that `DELETE` by opening every file
  of the table (T-594), and even bounded by `ts` the transaction stayed open
  29 s. And DuckLake refuses to commit a delete from a table another
  transaction inserted into meanwhile. A seal is such an insert, so on a
  table that seals every 20 s nearly every swap lost (T-595, T-600).

  So the swap stages the merged file into a hidden twin table, where DuckLake
  itself registers it, then moves it onto the table and retires the inputs
  in one metadata transaction tagged `merge_adjacent`, as DuckLake's own
  compaction is. A seal in flight commits through it; a retention delete in
  flight still conflicts. No data file is opened. `Smolquery.Catalog.DuckLake.Swap`
  has the details. It also guards the inputs from the metadata, at the
  snapshot it writes over: each is live, none has a delete file, and their
  `record_count`s sum to the merged file's rows, so an input registered
  twice or partly deleted refuses the swap rather than double or lose rows.

  Only `replace_segments/4` swaps this way. `drop_segments/3` (retention)
  still retires through `DELETE`.
  """

  @behaviour Smolquery.Catalog

  import Bitwise

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Catalog.DuckLake.Swap
  alias Smolquery.Engine
  alias Smolquery.Engine.Connection, as: EngineConnection
  alias Smolquery.EngineSecrets
  alias Smolquery.Identifier
  alias Smolquery.Partitions
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Schema.Materialized
  alias Smolquery.Segments.Store

  @default_swap_timeout_ms 120_000

  @enforce_keys [:engine, :catalog]
  defstruct [:engine, :catalog, swap_timeout_ms: @default_swap_timeout_ms]

  @type t :: %__MODULE__{
          engine: Engine.handle(),
          catalog: String.t(),
          swap_timeout_ms: timeout()
        }

  @type option ::
          {:name, atom()}
          | {:metadata, String.t()}
          | {:data_path, String.t()}
          | {:catalog, String.t()}
          | {:automatic_migration, boolean()}
          | {:store, Store.t()}
          | {:swap_timeout_ms, timeout()}

  @default_catalog "lake"
  @commit_attempts 5
  @move_attempts 10
  @stage_abandon_ms 3_600_000
  @retryable_markers [
    "Transaction conflict",
    "database is locked",
    "deadlock detected",
    "could not serialize access",
    "ducklake_snapshot_pkey",
    "ducklake_snapshot.snapshot_id"
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
      create_materialized_statement(catalog),
      create_required_statement(catalog)
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
    * `:swap_timeout_ms` — how long `replace_segments/4` waits for its
      transaction, two minutes by default. The swap's
      `ducklake_add_data_files` reads the merged file's footer through the
      store, and over S3 that alone outlasted the engine's 30 s call default
      on every attempt of one table (T-460); nothing but compaction commits
      through this path, so the wait costs no seal anything.

  """
  @spec new(keyword()) :: Catalog.t()
  def new(opts) do
    config = %__MODULE__{
      engine: Keyword.get(opts, :engine, __MODULE__),
      catalog: Keyword.get(opts, :catalog, @default_catalog),
      swap_timeout_ms: Keyword.get(opts, :swap_timeout_ms, @default_swap_timeout_ms)
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

    {new([engine: engine] ++ Keyword.take(opts, [:catalog, :swap_timeout_ms])), opts}
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
      "(DATA_PATH #{Identifier.sql_string(data_path)}, DATA_INLINING_ROW_LIMIT 0" <>
      "#{migration})"
  end

  defp ensure_metadata_dir("sqlite:" <> path), do: File.mkdir_p(Path.dirname(path))
  defp ensure_metadata_dir(_metadata), do: :ok

  defp postgres_metadata?("postgres:" <> _), do: true
  defp postgres_metadata?(_metadata), do: false

  @impl Catalog
  def create_dataset(%__MODULE__{} = _config, "__smolquery_stage" = dataset),
    do: {:error, {:reserved_dataset, dataset}}

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
        "AND schema_name <> $2 ORDER BY schema_name",
      [config.catalog, Swap.stage_schema()]
    )
  end

  @impl Catalog
  def create_table(%__MODULE__{} = config, table, %Schema{} = schema) do
    with {:ok, name} <- table_name(config, table),
         {:ok, columns} <- Schema.column_definitions(schema),
         {:ok, definitions} <- validated_materialized(schema),
         {:ok, existed} <- exists?(config, table),
         {:ok, _result} <- query(config, "CREATE TABLE IF NOT EXISTS #{name} (#{columns})") do
      if existed, do: :ok, else: record_materialized(config, table, definitions)
    end
  end

  defp exists?(config, table) do
    case table_schema(config, table) do
      {:ok, _schema} -> {:ok, true}
      {:error, {:unknown_table, _ref}} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validated_materialized(%Schema{} = schema) do
    regular = %{schema | fields: Schema.regular_fields(schema)}

    schema
    |> Schema.materialized_fields()
    |> Enum.reduce_while({:ok, []}, fn %Field{} = field, {:ok, acc} ->
      case Materialized.validate(regular, field) do
        {:ok, definition} -> {:cont, {:ok, [{field, definition} | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp record_materialized(_config, _table, []), do: :ok

  defp record_materialized(config, table, definitions) do
    with {:ok, %Schema{} = created} <- table_schema(config, table) do
      definitions
      |> Enum.reject(fn {%Field{name: name}, _definition} -> recorded?(created, name) end)
      |> Enum.map(fn {field, definition} -> materialized_row(created, field, definition) end)
      |> materialized_inserts(config, table)
    end
  end

  defp recorded?(%Schema{} = schema, name) do
    match?(
      {:ok, %Field{materialized: %Materialized{canonical: canonical}}} when is_binary(canonical),
      Schema.field(schema, name)
    )
  end

  defp materialized_row(%Schema{} = schema, %Field{} = declared, %Materialized{} = definition) do
    {:ok, %Field{id: id}} = Schema.field(schema, declared.name)

    sources =
      Enum.flat_map(definition.sources, fn
        source when is_integer(source) ->
          [source]

        source when is_binary(source) ->
          case Schema.field(schema, source) do
            {:ok, %Field{id: source_id}} -> [source_id]
            :error -> []
          end
      end)

    {id, %{definition | sources: sources}, declared.nullable}
  end

  defp materialized_inserts([], _config, _table), do: :ok

  defp materialized_inserts(rows, config, {dataset, table}) do
    statements =
      Enum.flat_map(rows, fn {id, %Materialized{} = definition, nullable} ->
        values = [
          Identifier.sql_string(dataset),
          Identifier.sql_string(table),
          Integer.to_string(id),
          Identifier.sql_string(definition.expression),
          Identifier.sql_string(definition.canonical),
          Identifier.sql_string(Enum.map_join(definition.sources, ",", &Integer.to_string/1))
        ]

        key = values |> Enum.take(3) |> Enum.join(", ")
        required = "INSERT INTO #{required_table(config.catalog)} VALUES (#{key})"

        defined =
          "INSERT INTO #{materialized_table(config.catalog)} VALUES (#{Enum.join(values, ", ")})"

        if nullable, do: [defined], else: [defined, required]
      end)

    transaction(config, statements)
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
         {:ok, result} <- query(config, columns_sql(config), [config.catalog, dataset, table]),
         {:ok, schema} <- build_schema(result.rows, {dataset, table}),
         {:ok, clustering, partitions, materialized, required} <-
           side_options(config, {dataset, table}) do
      {:ok,
       %{
         schema
         | fields:
             schema.fields |> attach_materialized(materialized) |> attach_required(required),
           clustering: clustering,
           partitions: partitions
       }}
    end
  end

  defp side_options(config, {dataset, table}) do
    sql =
      "SELECT 0 AS kind, column_name AS name, position AS value, NULL AS expression, " <>
        "NULL AS canonical, NULL AS sources " <>
        "FROM #{clustering_table(config.catalog)} WHERE dataset = $1 AND table_name = $2 " <>
        "UNION ALL SELECT 1, NULL, partition_count, NULL, NULL, NULL " <>
        "FROM #{partitions_table(config.catalog)} WHERE dataset = $1 AND table_name = $2 " <>
        "UNION ALL SELECT 2, NULL, column_id, expression, canonical, sources " <>
        "FROM #{materialized_table(config.catalog)} WHERE dataset = $1 AND table_name = $2 " <>
        "UNION ALL SELECT 3, NULL, column_id, NULL, NULL, NULL " <>
        "FROM #{required_table(config.catalog)} WHERE dataset = $1 AND table_name = $2 " <>
        "ORDER BY kind, value"

    with {:ok, result} <- query(config, sql, [dataset, table]) do
      rows = Enum.group_by(result.rows, &hd/1)
      clustering = Enum.map(Map.get(rows, 0, []), fn [_kind, name | _rest] -> name end)

      materialized =
        Map.new(Map.get(rows, 2, []), fn [_kind, _name, id, expression, canonical, sources] ->
          {id,
           %Materialized{
             expression: expression,
             canonical: canonical,
             sources: source_ids(sources)
           }}
        end)

      required = MapSet.new(Map.get(rows, 3, []), fn [_kind, _name, id | _rest] -> id end)

      case Map.get(rows, 1, []) do
        [] -> {:ok, clustering, nil, materialized, required}
        [[_kind, _name, count | _rest]] -> {:ok, clustering, count, materialized, required}
        partition_rows -> {:error, {:ambiguous_partitions, partition_rows}}
      end
    end
  end

  defp source_ids(""), do: []
  defp source_ids(sources), do: sources |> String.split(",") |> Enum.map(&String.to_integer/1)

  defp attach_materialized(fields, materialized) when map_size(materialized) == 0, do: fields

  defp attach_materialized(fields, materialized),
    do: Enum.map(fields, &%{&1 | materialized: Map.get(materialized, &1.id)})

  defp attach_required(fields, required) do
    Enum.map(fields, fn %Field{} = field ->
      if Schema.materialized?(field) and MapSet.member?(required, field.id),
        do: %{field | nullable: false},
        else: field
    end)
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
  def segment_files(%__MODULE__{} = config, table, :current) do
    with {:ok, snapshot} <- current_snapshot(config), do: segment_files(config, table, snapshot)
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
               "CAST(COALESCE(df.file_size_bytes, 0) AS BIGINT), df.begin_snapshot, " <>
               "(SELECT list(fcs.column_id ORDER BY fcs.column_id) " <>
               "FROM #{metadata_schema(config.catalog)}.ducklake_file_column_stats fcs " <>
               "WHERE fcs.data_file_id = df.data_file_id AND fcs.table_id = df.table_id) " <>
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
      [path, relative, rows, bytes, snapshot, column_ids], {:ok, files}
      when relative in [false, 0] ->
        file = %{path: path, rows: rows, bytes: bytes, snapshot: snapshot, column_ids: column_ids}

        {:cont, {:ok, [file | files]}}

      [path, _relative, _rows, _bytes, _snapshot, _column_ids], _acc ->
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
         :ok <- commit(config, delete_statement(name, by_file(paths))) do
      current_snapshot(config)
    end
  end

  @impl Catalog
  def replace_segments(%__MODULE__{} = _config, _table, [], _paths), do: {:error, :no_segments}

  def replace_segments(%__MODULE__{} = config, {dataset, table}, segments, paths) do
    drop = Enum.uniq(paths)

    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table),
         {:ok, name} <- table_name(config, {dataset, table}),
         :ok <-
           with_commit_retries(fn ->
             swap_missing(config, {dataset, table}, name, segments, drop)
           end) do
      current_snapshot(config)
    end
  end

  defp swap_missing(config, ref, name, segments, drop) do
    add = segments |> Enum.map(& &1.path) |> Enum.uniq()

    with {:ok, registered} <- segments(config, ref, :current) do
      case {add -- registered, drop -- registered} do
        {[], _retired} ->
          {:ok, :already_swapped}

        {pending, []} ->
          swap(config, ref, name, segments, pending, drop)

        {_pending, retired} ->
          {:error, {:inputs_not_live, retired}}
      end
    end
  end

  defp swap(config, ref, _name, _segments, add, []),
    do: query(config, add_statement(config, ref, add), [], config.swap_timeout_ms)

  defp swap(config, ref, name, [merged], [_path], drop) do
    with {:ok, table_id} <- table_id(config, ref),
         {:ok, stage} <- ensure_stage(config, name, table_id),
         :ok <- stage_merged(config, stage, merged.path) do
      move(config, table_id, stage, merged, drop, @move_attempts)
    end
  end

  defp swap(_config, _ref, _name, segments, _add, _drop),
    do: {:error, {:swap_expects_one_file, length(segments)}}

  defp table_id(config, {dataset, table}) do
    sql =
      "SELECT t.table_id FROM #{metadata_schema(config.catalog)}.ducklake_table t " <>
        "JOIN #{metadata_schema(config.catalog)}.ducklake_schema s ON s.schema_id = t.schema_id " <>
        "WHERE s.schema_name = $1 AND t.table_name = $2 " <>
        "AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL"

    case query(config, sql, [dataset, table]) do
      {:ok, %{rows: [[id]]}} -> {:ok, id}
      {:ok, %{rows: []}} -> {:error, {:unknown_table, {dataset, table}}}
      {:ok, %{rows: rows}} -> {:error, {:ambiguous_table, {dataset, table}, rows}}
      {:error, _error} = failed -> failed
    end
  end

  defp ensure_stage(config, name, table_id) do
    stage_ref = {Swap.stage_schema(), Swap.stage_table(table_id)}

    with {:ok, columns} <- columns(config, table_id),
         {:ok, stage} <- current_stage(config, stage_ref, columns) do
      case stage do
        {:ok, id, stage_columns} ->
          {:ok, %{ref: stage_ref, id: id, columns: stage_columns, table_columns: columns}}

        :recreate ->
          recreate_stage(config, name, stage_ref, columns)
      end
    end
  end

  defp current_stage(config, stage_ref, columns) do
    case table_id(config, stage_ref) do
      {:ok, id} -> matching_stage(config, id, columns)
      {:error, {:unknown_table, _ref}} -> {:ok, :recreate}
      {:error, _reason} = failed -> failed
    end
  end

  defp matching_stage(config, id, columns) do
    with {:ok, stage_columns} <- columns(config, id) do
      if Swap.matches?(stage_columns, columns),
        do: {:ok, {:ok, id, stage_columns}},
        else: {:ok, :recreate}
    end
  end

  defp recreate_stage(config, name, {schema, table} = stage_ref, columns) do
    stage = "#{Identifier.quote_name!(config.catalog)}.#{Identifier.quote_name!(schema)}"
    twin = "#{stage}.#{Identifier.quote_name!(table)}"

    with :ok <-
           transact(config, [
             "CREATE SCHEMA IF NOT EXISTS #{stage}",
             "DROP TABLE IF EXISTS #{twin}",
             "CREATE TABLE #{twin} AS SELECT * FROM #{name} LIMIT 0"
           ]),
         {:ok, id} <- table_id(config, stage_ref),
         {:ok, stage_columns} <- columns(config, id) do
      {:ok, %{ref: stage_ref, id: id, columns: stage_columns, table_columns: columns}}
    end
  end

  defp columns(config, table_id) do
    sql =
      "SELECT column_id, column_name, parent_column, column_type " <>
        "FROM #{metadata_schema(config.catalog)}.ducklake_column " <>
        "WHERE table_id = #{table_id} AND end_snapshot IS NULL"

    with {:ok, result} <- query(config, sql) do
      {:ok, Enum.map(result.rows, &List.to_tuple/1)}
    end
  end

  defp stage_merged(config, stage, path) do
    case staged_file(config, stage.id, path) do
      {:ok, _staged} ->
        :ok

      {:error, :not_staged} ->
        statement(:stage, fn ->
          Engine.try_query(
            config.engine,
            add_statement(config, stage.ref, [path]),
            [],
            config.swap_timeout_ms
          )
        end)
        |> case do
          {:ok, _result} -> :ok
          {:error, _error} = failed -> failed
        end

      {:error, _reason} = failed ->
        failed
    end
  end

  defp staged_file(config, stage_id, path) do
    sql =
      "SELECT data_file_id, record_count, file_size_bytes, mapping_id " <>
        "FROM #{metadata_schema(config.catalog)}.ducklake_data_file " <>
        "WHERE table_id = #{stage_id} AND end_snapshot IS NULL AND path = $1"

    case query(config, sql, [path]) do
      {:ok, %{rows: [[id, rows, bytes, mapping]]}} ->
        {:ok, %{data_file_id: id, rows: rows, bytes: bytes, mapping_id: mapping}}

      {:ok, %{rows: []}} ->
        {:error, :not_staged}

      {:error, _error} = failed ->
        failed
    end
  end

  defp move(config, table_id, stage, merged, drop, attempts) do
    with {:ok, snapshot} <- latest_snapshot(config),
         {:ok, plan} <- move_plan(config, table_id, stage, merged, drop, snapshot) do
      write_move(config, plan, attempts)
    end
  end

  defp move_plan(config, table_id, stage, merged, drop, snapshot) do
    with {:ok, retire} <- live_inputs(config, table_id, drop, merged.row_count),
         {:ok, staged} <- staged_file(config, stage.id, merged.path),
         {:ok, abandoned} <- abandoned_stage_files(config, stage.id, merged.path),
         {:ok, column_ids} <- Swap.column_ids(stage.columns, stage.table_columns),
         {:ok, mappings} <- mappings(config, [table_id, stage.id]),
         {:ok, next_row_id} <- next_row_id(config, table_id) do
      {:ok,
       %{
         snapshot: snapshot,
         table_id: table_id,
         stage_table_id: stage.id,
         next_row_id: next_row_id,
         staged: Map.take(staged, [:data_file_id, :rows]),
         retire: retire,
         abandoned: abandoned,
         column_ids: column_ids,
         mapping:
           Swap.mapping(
             staged.mapping_id && Map.get(mappings, staged.mapping_id),
             Map.filter(mappings, fn {_id, mapping} -> mapping.table_id == table_id end)
             |> Map.new(fn {id, mapping} -> {id, Map.delete(mapping, :table_id)} end),
             column_ids
           )
       }}
    end
  end

  defp live_inputs(config, table_id, drop, expected_rows) do
    sql =
      "SELECT df.data_file_id, df.path, CAST(df.record_count AS BIGINT), " <>
        "(SELECT count(*) FROM #{metadata_schema(config.catalog)}.ducklake_delete_file del " <>
        "WHERE del.table_id = #{table_id} AND del.data_file_id = df.data_file_id " <>
        "AND del.end_snapshot IS NULL) " <>
        "FROM #{metadata_schema(config.catalog)}.ducklake_data_file df " <>
        "WHERE df.table_id = #{table_id} AND df.end_snapshot IS NULL " <>
        "AND df.path IN (#{placeholders(1, length(drop))})"

    with {:ok, result} <- query(config, sql, drop) do
      live = Enum.map(result.rows, fn [_id, path | _rest] -> path end)
      found = Enum.sum_by(result.rows, fn [_id, _path, rows, _deletes] -> rows end)

      cond do
        drop -- live != [] -> {:error, {:inputs_not_live, drop -- live}}
        Enum.any?(result.rows, &(List.last(&1) > 0)) -> {:error, {:inputs_have_deletes, drop}}
        found != expected_rows -> {:error, {:row_count_mismatch, expected_rows, found}}
        true -> {:ok, Enum.map(result.rows, &hd/1)}
      end
    end
  end

  defp abandoned_stage_files(config, stage_id, path) do
    sql =
      "SELECT data_file_id, begin_snapshot " <>
        "FROM #{metadata_schema(config.catalog)}.ducklake_data_file " <>
        "WHERE table_id = #{stage_id} AND end_snapshot IS NULL AND path <> $1"

    case query(config, sql, [path]) do
      {:ok, %{rows: []}} -> {:ok, []}
      {:ok, %{rows: others}} -> staged_before(config, others)
      {:error, _error} = failed -> failed
    end
  end

  defp staged_before(config, others) do
    cutoff =
      DateTime.utc_now()
      |> DateTime.add(-@stage_abandon_ms, :millisecond)
      |> DateTime.to_iso8601()

    snapshots = others |> Enum.map(&List.last/1) |> Enum.uniq()

    sql =
      "SELECT snapshot_id FROM #{metadata_schema(config.catalog)}.ducklake_snapshot " <>
        "WHERE snapshot_id IN (#{Enum.map_join(snapshots, ", ", &Integer.to_string/1)}) " <>
        "AND CAST(snapshot_time AS TIMESTAMPTZ) < CAST($1 AS TIMESTAMPTZ)"

    with {:ok, old} <- column(config, sql, [cutoff]) do
      {:ok, for([id, snapshot] <- others, snapshot in old, do: id)}
    end
  end

  defp mappings(config, table_ids) do
    sql =
      "SELECT m.mapping_id, m.table_id, m.type, n.column_id, n.source_name, " <>
        "n.target_field_id, n.parent_column, n.is_partition " <>
        "FROM #{metadata_schema(config.catalog)}.ducklake_column_mapping m " <>
        "JOIN #{metadata_schema(config.catalog)}.ducklake_name_mapping n " <>
        "ON n.mapping_id = m.mapping_id " <>
        "WHERE m.table_id IN (#{Enum.map_join(table_ids, ", ", &Integer.to_string/1)})"

    with {:ok, result} <- query(config, sql) do
      {:ok,
       result.rows
       |> Enum.group_by(&hd/1)
       |> Map.new(fn {id, [[_id, table_id, type | _row] | _more] = rows} ->
         {id,
          %{
            table_id: table_id,
            type: type,
            rows:
              Enum.map(rows, fn [_id, _table, _type, column, source, target, parent, partition] ->
                {column, source, target, parent, partition in [true, 1]}
              end)
          }}
       end)}
    end
  end

  defp next_row_id(config, table_id) do
    sql =
      "SELECT next_row_id FROM #{metadata_schema(config.catalog)}.ducklake_table_stats " <>
        "WHERE table_id = #{table_id}"

    case query(config, sql) do
      {:ok, %{rows: [[next]]}} -> {:ok, next}
      {:ok, %{rows: rows}} -> {:error, {:unexpected_table_stats, rows}}
      {:error, _error} = failed -> failed
    end
  end

  defp latest_snapshot(config) do
    with {:ok, id} <- current_snapshot(config) do
      sql =
        "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id " <>
          "FROM #{metadata_schema(config.catalog)}.ducklake_snapshot WHERE snapshot_id = #{id}"

      case query(config, sql) do
        {:ok, %{rows: [[^id, version, catalog_id, file_id]]}} ->
          {:ok,
           %{id: id, schema_version: version, next_catalog_id: catalog_id, next_file_id: file_id}}

        {:ok, %{rows: rows}} ->
          {:error, {:unexpected_snapshot_result, rows}}

        {:error, _error} = failed ->
          failed
      end
    end
  end

  defp write_move(config, plan, attempts) do
    case transaction(config, move_statements(config, plan), config.swap_timeout_ms) do
      :ok ->
        {:ok, :committed}

      {:error, error} ->
        if Swap.lost_snapshot?(error) and attempts > 1,
          do: rebase_move(config, plan, error, attempts),
          else: {:error, error}
    end
  end

  defp rebase_move(config, plan, error, attempts) do
    with {:ok, snapshot} <- latest_snapshot(config),
         {:ok, changes} <- changes_since(config, plan.snapshot.id, snapshot.id),
         :ok <- rebasable(changes, plan, error),
         {:ok, next_row_id} <- next_row_id(config, plan.table_id) do
      write_move(config, %{plan | snapshot: snapshot, next_row_id: next_row_id}, attempts - 1)
    end
  end

  defp rebasable(changes, plan, error) do
    if Swap.rebase?(changes, plan.table_id, plan.stage_table_id),
      do: :ok,
      else: {:error, error}
  end

  defp changes_since(config, from, through) do
    sql =
      "SELECT changes_made FROM #{metadata_schema(config.catalog)}.ducklake_snapshot_changes " <>
        "WHERE snapshot_id > #{from} AND snapshot_id <= #{through}"

    column(config, sql)
  end

  defp move_statements(config, plan) do
    case metadata_type(config) do
      "postgres" ->
        batch = plan |> Swap.statements(~s("public")) |> Enum.join(";\n")

        [
          {:move,
           "CALL postgres_execute(#{Identifier.sql_string("__ducklake_metadata_" <> config.catalog)}, " <>
             "#{Identifier.sql_string(batch)})"}
        ]

      _through_duckdb ->
        Enum.map(Swap.statements(plan, metadata_schema(config.catalog)), &{:move, &1})
    end
  end

  defp metadata_type(config) do
    sql = "SELECT type FROM duckdb_databases() WHERE database_name = $1"

    case query(config, sql, ["__ducklake_metadata_" <> config.catalog]) do
      {:ok, %{rows: [[type]]}} -> type
      _unknown -> nil
    end
  end

  defp placeholders(first, count), do: Enum.map_join(first..(first + count - 1), ", ", &"$#{&1}")

  @impl Catalog
  def on_connection(%__MODULE__{engine: {name, _slot}} = config, slot),
    do: %{config | engine: {name, slot}}

  def on_connection(%__MODULE__{engine: name} = config, slot),
    do: %{config | engine: {name, slot}}

  @impl Catalog
  def schema_version(%__MODULE__{} = config) do
    sql =
      "SELECT schema_version FROM #{metadata_schema(config.catalog)}.ducklake_snapshot " <>
        "ORDER BY snapshot_id DESC LIMIT 1"

    with {:ok, result} <- query(config, sql) do
      case result.rows do
        [[version]] -> {:ok, version}
        rows -> {:error, {:unexpected_snapshot_result, rows}}
      end
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
        statements -> transaction(config, statements)
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
  def alter_table(%__MODULE__{} = config, ref, {:add_column, %Field{materialized: nil} = field}) do
    with {:ok, name} <- table_name(config, ref),
         {:ok, definition} <- Schema.column_definition(field) do
      transact(config, ["ALTER TABLE #{name} ADD COLUMN #{definition}"])
    end
  end

  def alter_table(%__MODULE__{} = config, ref, {:add_column, %Field{} = field}) do
    with {:ok, name} <- table_name(config, ref),
         {:ok, definition} <- Schema.column_definition(field),
         {:ok, %Schema{} = before} <- table_schema(config, ref),
         {:ok, validated} <- Materialized.validate(before, field),
         :ok <- transact(config, ["ALTER TABLE #{name} ADD COLUMN #{definition}"]) do
      case record_materialized(config, ref, [{field, validated}]) do
        :ok ->
          :ok

        {:error, _reason} = failure ->
          _compensated = alter_table(config, ref, {:drop_column, field.name})
          failure
      end
    end
  end

  def alter_table(%__MODULE__{} = config, ref, {:drop_column, column}) do
    with {:ok, name} <- table_name(config, ref),
         {:ok, column} <- Identifier.validate(column),
         {:ok, %Schema{} = before} <- table_schema(config, ref),
         :ok <-
           transact(config, ["ALTER TABLE #{name} DROP COLUMN #{Identifier.quote_name!(column)}"]) do
      forget_materialized(config, ref, Schema.field(before, column))
    end
  end

  defp forget_materialized(
         config,
         {dataset, table},
         {:ok, %Field{materialized: %Materialized{}, id: id}}
       ) do
    where = "WHERE dataset = $1 AND table_name = $2 AND column_id = $3"
    key = [dataset, table, id]

    with {:ok, _defined} <-
           query(config, "DELETE FROM #{materialized_table(config.catalog)} " <> where, key),
         {:ok, _required} <-
           query(config, "DELETE FROM #{required_table(config.catalog)} " <> where, key) do
      :ok
    end
  end

  defp forget_materialized(_config, _ref, _field), do: :ok

  defp transact(config, statements) do
    with_commit_retries(fn ->
      case transaction(config, statements) do
        :ok -> {:ok, :committed}
        {:error, _error} = failure -> failure
      end
    end)
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

  defp materialized_table(catalog), do: "#{metadata_schema(catalog)}.smolquery_materialized"

  @doc """
  The `CREATE TABLE IF NOT EXISTS` that gives a lake its materialized-column
  side table (PL-61 L4). Bootstrap SQL like `create_clustering_statement/1`,
  and for the same reason: `table_schema/2` reads it on the query path.

  A row is keyed by the column's id, not its name (PL-62): a name can be
  dropped and given to a plain column, and a row keyed by the name would
  attach the old expression to the new column. `sources` is the ids of the
  columns the expression reads, comma-joined.
  """
  @spec create_materialized_statement(String.t()) :: String.t()
  def create_materialized_statement(catalog) do
    "CREATE TABLE IF NOT EXISTS #{materialized_table(catalog)} (" <>
      "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, " <>
      "column_id BIGINT NOT NULL, expression VARCHAR NOT NULL, " <>
      "canonical VARCHAR NOT NULL, sources VARCHAR NOT NULL, " <>
      "PRIMARY KEY (dataset, table_name, column_id))"
  end

  defp required_table(catalog), do: "#{metadata_schema(catalog)}.smolquery_required_columns"

  @doc """
  The `CREATE TABLE IF NOT EXISTS` that gives a lake its side table of
  materialized columns declared `nullable: false` (T-515).

  DuckLake cannot add a constrained column, so the column it holds stays
  nullable and the declaration is smolquery's own, kept here by column id
  like the expression it belongs to. It is true of every row because every
  evaluation stores the type's default where the expression gives nothing
  (`Smolquery.Schema.computed_expression/1`). A table of its own rather
  than a column on the materialized one: a bootstrap that only ever
  creates what is missing needs no migration of a lake that already has it.
  """
  @spec create_required_statement(String.t()) :: String.t()
  def create_required_statement(catalog) do
    "CREATE TABLE IF NOT EXISTS #{required_table(catalog)} (" <>
      "dataset VARCHAR NOT NULL, table_name VARCHAR NOT NULL, column_id BIGINT NOT NULL, " <>
      "PRIMARY KEY (dataset, table_name, column_id))"
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

    transaction(config, [
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
      "[#{literals}], schema => #{Identifier.sql_string(dataset)}, " <>
      "allow_missing => true, ignore_extra_columns => true)"
  end

  defp delete_statement(name, where), do: "DELETE FROM #{name} WHERE #{where}"

  defp by_file(paths) do
    literals = paths |> Enum.uniq() |> Enum.map_join(", ", &Identifier.sql_string/1)

    "filename IN (#{literals})"
  end

  defp commit(config, sql), do: with_commit_retries(fn -> query(config, sql) end)

  defp with_commit_retries(run, attempt \\ 1) do
    case run.() do
      {:ok, _result} ->
        attempted(attempt, :ok)
        :ok

      {:error, error} ->
        attempted(attempt, if(retryable?(error), do: :conflict, else: :error))

        if retryable?(error) and attempt < @commit_attempts do
          Logger.info(
            "catalog commit attempt #{attempt} of #{@commit_attempts} conflicted, retrying: " <>
              Exception.message(error)
          )

          Process.sleep(backoff(attempt, error))
          with_commit_retries(run, attempt + 1)
        else
          gave_up(error, attempt)
          {:error, classify(error)}
        end
    end
  end

  defp gave_up(error, attempt) do
    if retryable?(error) do
      Logger.warning(
        "catalog commit gave up after #{attempt} attempts, answering :commit_conflict: " <>
          Exception.message(error)
      )
    end
  end

  defp attempted(attempt, result),
    do:
      :telemetry.execute([:smolquery, :catalog, :commit_attempt], %{count: 1}, %{
        attempt: attempt,
        result: result
      })

  defp backoff(attempt, error) do
    if EngineConnection.locked?(error),
      do: (1 <<< attempt) * 50 + :rand.uniform(50),
      else: (1 <<< attempt) * 5 + :rand.uniform(10)
  end

  @doc """
  Whether a failed commit is worth retrying.

  Public for the same reason `Smolquery.Engine.Connection.fatal?/1` is: it
  classifies DuckDB by message text, so the strings it keys on are pinned by a
  test rather than trusted. DuckLake's own `"Transaction conflict"`, SQLite
  metadata's `"database is locked"`, and Postgres metadata's
  `"deadlock detected"` and `"could not serialize access"` are retryable, and
  so is a swap's move that lost the snapshot key to a concurrent commit
  (`Smolquery.Catalog.DuckLake.Swap.lost_snapshot?/1`) after its own rebases.
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

  defp columns_sql(config) do
    metadata = metadata_schema(config.catalog)

    "SELECT ic.column_name, ic.data_type, ic.is_nullable, dc.column_id, dc.begin_snapshot " <>
      "FROM information_schema.columns ic " <>
      "JOIN #{metadata}.ducklake_schema ds ON ds.schema_name = ic.table_schema " <>
      "AND ds.end_snapshot IS NULL " <>
      "JOIN #{metadata}.ducklake_table dt ON dt.schema_id = ds.schema_id " <>
      "AND dt.table_name = ic.table_name AND dt.end_snapshot IS NULL " <>
      "JOIN #{metadata}.ducklake_column dc ON dc.table_id = dt.table_id " <>
      "AND dc.column_name = ic.column_name AND dc.end_snapshot IS NULL " <>
      "AND dc.parent_column IS NULL " <>
      "WHERE ic.table_catalog = $1 AND ic.table_schema = $2 AND ic.table_name = $3 " <>
      "ORDER BY ic.ordinal_position"
  end

  defp build_schema([], ref), do: {:error, {:unknown_table, ref}}

  defp build_schema(rows, _ref) do
    rows
    |> Enum.reduce_while({:ok, []}, fn [name, type, nullable, id, since], {:ok, fields} ->
      case Schema.logical_from_duckdb(type) do
        {:ok, logical} ->
          field = %Field{
            name: name,
            type: logical,
            nullable: nullable == "YES",
            id: id,
            since: since
          }

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

  defp query(config, sql, params \\ [], timeout \\ 30_000),
    do: statement(:query, fn -> Engine.try_query(config.engine, sql, params, timeout) end)

  defp transaction(config, statements),
    do: statement(:transaction, fn -> Engine.try_transaction(config.engine, statements) end)

  defp transaction(config, statements, timeout),
    do:
      statement(:transaction, fn ->
        Engine.try_transaction(config.engine, statements, timeout,
          span: [:smolquery, :catalog, :statement]
        )
      end)

  defp statement(kind, run) do
    Smolquery.Telemetry.span(
      [:smolquery, :catalog, :statement],
      &{%{}, %{kind: kind, result: Smolquery.Telemetry.outcome(&1)}},
      run
    )
  end

  defp engine_extensions do
    :smolquery
    |> Application.get_env(Engine, [])
    |> Keyword.get(:extensions, [])
  end
end
