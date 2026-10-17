defmodule Smolquery.QueryService.PartialWorker do
  @moduledoc """
  Runs one shard of a scattered query on whichever node it lands on (PL-49).

  `Smolquery.QueryService.WorkerTransport` calls `run/2`: directly on the
  coordinator's own node, and over gen_rpc from a peer (T-364). Neither
  path stops the worker when its caller gives up, so the request carries
  the job's deadline, and the partial query runs under it: a shard whose
  coordinator gave up stops at that deadline, and the `after` below kills
  its engine. The request also carries everything shard-specific: the table
  and its planned schema, this shard's files, the partial SQL that reads the
  planned table name, and the hot-tier URLs the shard may fetch. The view
  that defines the table over the files is rendered here, not on the
  coordinator, because a sealed file's column ids live in the file (PL-62):
  the worker reads them with one `parquet_schema()` over its shard —
  footers the scan is about to read anyway — and projects each file by id,
  or a file without ids as of the snapshot it was registered at, through
  `Smolquery.QueryService.Views.sources_select/2`, exactly as the planner's
  own view and the compactor do. A hot micro-segment brings its ids in its
  manifest entry and is not asked. Everything node-local comes from this node's own
  published `Smolquery.QueryService.Runtime`: the engine extensions and the
  same hot-tier and sealed-tier secrets a job engine gets.

  The engine is private and disposable
  (`Smolquery.QueryService.JobEngine.acquire/1` — warm from the node's
  pool when one is ready), then sized by the runtime's
  `distributed.worker_memory_limit` and `worker_threads`, which DuckDB
  accepts before `lock_configuration`. The partial
  result leaves DuckDB as a parquet file (`COPY`), not through Arrow →
  Polars — DuckDB intermittently fails to read Polars-written parquet
  (PL-48) — and returns to the coordinator as the file's bytes.

  ## A standalone shard (PL-77)

  A burst runner (`Smolquery.Burst`) has no `Runtime`: no catalog, no
  warm pool, no file cache, no internal secret. `run_standalone/1` runs the
  same shard from a request that also carries `engine`, the engine
  configuration the coordinator resolved with `engine_spec/1`: extensions,
  memory limit, threads, the sealed store's settings, lockdown and the
  directories it allows. Credentials never travel in it. The store arrives
  without keys, and the engine reads S3 with the runner's own: static keys
  from the runner's `SMOLQUERY_S3_*` configuration when it has them for
  that same bucket and endpoint, the AWS credential chain (the Lambda
  execution role, Pod Identity) otherwise. The engine is always cold
  (`JobEngine.start_standalone/1`), loads no `ducklake`, attaches no lake,
  loads no `cache_httpfs`, and renders no hot-tier secret: a runner is
  handed sealed units only (D4), so it never fetches a hot URL. Its spill
  directory is the host's own, the connection's default under the system
  temporary directory. `standalone_options/1` is the bootstrap, public so a
  test can read it.

  A spec for an S3 store allows no local directories but the shard's own
  output: the coordinator's data and cache directories do not exist on the
  runner. A spec for a local store keeps them, since a local store is only
  readable from a VM that shares the filesystem.

  `run/2` and `run_standalone/1` share everything after the engine exists:
  the settings, the field-id description, the view, the lockdown and the
  `COPY` out. Both build the statements they will run before they take an
  engine, so nothing can raise between the engine's start and the `after`
  that stops it.

  ## File cache

  The shard decides whether to read through this node's file cache itself
  (T-630): the request carries the job's `file_cache_mode` and the sealed
  files of every shard the job runs on this node (`node_files`), and
  `Smolquery.QueryService.FileCache.shard_decision/3` weighs them against
  this node's own index, which is where their blocks land. The reply
  carries the decision, and a shard that answered counts it in
  `[:smolquery, :query, :file_cache, :shard]`. A request from a
  coordinator older than T-630 has no mode, only that coordinator's own
  verdict as `file_cache`, and the shard follows it. A standalone shard has
  no cache and reports `nil`.

  ## Lockdown

  The partial SQL derives from user SQL, so the engine is confined the way
  a job engine is: `allowed_directories` is exactly this shard's output
  directory, the node's own allowed data directories, and the sealed
  store's prefix; `allowed_paths` is the shard's hot URLs; and
  `Smolquery.QueryService.Lockdown` orders the rest, DuckLake's exempted
  options before the lock. `enable_external_access` stays on —
  `COPY ... TO` needs it — and while it is on DuckDB applies neither list
  (T-690), so what the lock seals today is the configuration, honoring the
  runtime's `lockdown` flag the same way `Runner` does.
  """

  alias Smolquery.Engine.Connection
  alias Smolquery.EngineSecrets
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.QueryService.FileCache
  alias Smolquery.QueryService.JobEngine
  alias Smolquery.QueryService.Lockdown
  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.Views
  alias Smolquery.Segments.FieldIds
  alias Smolquery.Segments.Store

  @type request :: %{
          required(:table_ref) => Smolquery.Catalog.table_ref(),
          required(:schema) => Smolquery.Schema.t(),
          required(:files) => [map()],
          required(:partial_sql) => String.t(),
          required(:allowed_paths) => [String.t()],
          optional(:timeout_ms) => timeout(),
          optional(:file_cache) => FileCache.decision(),
          optional(:file_cache_mode) => :auto | boolean(),
          optional(:node_files) => [map()],
          optional(:params) => [term()]
        }

  @typedoc """
  The engine a standalone shard builds, resolved on the coordinator from its
  `Runtime` by `engine_spec/1`. `store` carries no credentials.
  """
  @type engine_spec :: %{
          extensions: [atom() | String.t()],
          memory_limit: String.t(),
          threads: pos_integer() | nil,
          store: Store.t() | nil,
          lockdown: boolean(),
          allowed_directories: [String.t()],
          clickhouse_functions: boolean()
        }

  @type standalone_request :: %{
          required(:engine) => engine_spec(),
          required(:table_ref) => Smolquery.Catalog.table_ref(),
          required(:schema) => Smolquery.Schema.t(),
          required(:files) => [map()],
          required(:partial_sql) => String.t(),
          required(:allowed_paths) => [String.t()],
          optional(:timeout_ms) => timeout(),
          optional(:params) => [term()]
        }

  @typedoc """
  What a shard answers: the partial as parquet bytes, its row count, what
  the shard did with the file cache, the node that ran it, and how long the
  engine, the field-id description and the scan took on that node (T-691).
  """
  @type reply :: %{
          parquet: binary(),
          rows: non_neg_integer(),
          file_cache: FileCache.decision(),
          node: node(),
          timings: %{
            engine_us: non_neg_integer(),
            describe_us: non_neg_integer(),
            scan_us: non_neg_integer()
          }
        }

  @doc """
  Whether query service instance `name` runs on this node.
  """
  @spec available?(atom()) :: boolean()
  def available?(name), do: match?({:ok, _runtime}, Runtime.fetch(name))

  @doc """
  Runs `request`'s partial query over its shard and returns the result as
  parquet bytes with its row count and what the shard did with the file
  cache.
  """
  @spec run(atom(), request()) :: {:ok, reply()} | {:error, term()}
  def run(name, request) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> with_engine(runtime, request)
      :error -> {:error, :query_service_unavailable}
    end
  end

  @doc """
  Runs `request`'s partial query on a cold engine built from
  `request.engine`, with nothing from this node but its S3 credentials.
  """
  @spec run_standalone(standalone_request()) :: {:ok, reply()} | {:error, term()}
  def run_standalone(%{engine: spec} = request) do
    plan = %{
      settings: [],
      functions: functions(spec.clickhouse_functions, request.partial_sql),
      lockdown: &lockdown(spec.lockdown, spec.allowed_directories, spec.store, &1, &2)
    }

    case timed(fn -> JobEngine.start_standalone(standalone_options(spec)) end) do
      {engine_us, {:ok, engine}} -> execute(engine, request, nil, plan, engine_us)
      {_engine_us, {:error, reason}} -> {:error, {:engine_failed, reason}}
    end
  end

  @doc """
  The engine a standalone shard of a job on `runtime` builds: the worker's
  limits, the sealed store without its credentials, and the lockdown the
  runtime applies.
  """
  @spec engine_spec(Runtime.t()) :: engine_spec()
  def engine_spec(%Runtime{} = runtime) do
    limits = limits(runtime)

    %{
      extensions: runtime.engine_extensions,
      memory_limit: Keyword.fetch!(limits, :memory_limit),
      threads: Keyword.get(limits, :threads),
      store: without_credentials(runtime.store),
      lockdown: runtime.lockdown,
      allowed_directories: shared_directories(runtime),
      clickhouse_functions: runtime.clickhouse_functions
    }
  end

  @doc """
  The bootstrap a standalone shard's engine runs: `spec`'s extensions plus
  what its store's credential needs, its limits as settings, and the sealed
  tier's secret with this node's own keys when it has them for that bucket.
  No lake, no hot tier, no file cache.
  """
  @spec standalone_options(engine_spec()) :: keyword()
  def standalone_options(spec) do
    store = with_local_credentials(spec.store)

    [
      extensions: EngineSecrets.sealed_tier_extensions(store, spec.extensions),
      settings:
        Enum.reject([memory_limit: spec.memory_limit, threads: spec.threads], &nil_value?/1),
      statements: EngineSecrets.sealed_tier(store),
      max_rows: :infinity
    ]
  end

  defp nil_value?({_setting, value}), do: is_nil(value)

  defp shared_directories(%Runtime{store: %Store{impl: Store.S3}}), do: []
  defp shared_directories(%Runtime{allowed_directories: directories}), do: directories

  defp without_credentials(%Store{impl: Store.S3, config: config} = store),
    do: %{store | config: Store.S3.without_credentials(config)}

  defp without_credentials(store), do: store

  defp with_local_credentials(
         %Store{impl: Store.S3, config: %Store.S3{access_key_id: nil} = config} = store
       ) do
    case local_s3_options() do
      {bucket, endpoint, id, secret}
      when bucket == config.bucket and endpoint == config.endpoint and is_binary(id) and
             is_binary(secret) ->
        %{store | config: %{config | access_key_id: id, secret_access_key: secret}}

      _other_or_chain ->
        store
    end
  end

  defp with_local_credentials(store), do: store

  defp local_s3_options do
    case Keyword.get(Application.get_env(:smolquery, Smolquery.QueryService, []), :store) do
      {Store.S3, options} when is_list(options) ->
        {options[:bucket], options[:endpoint], options[:access_key_id],
         options[:secret_access_key]}

      %Store{impl: Store.S3, config: %Store.S3{} = config} ->
        {config.bucket, config.endpoint, config.access_key_id, config.secret_access_key}

      _none_or_local ->
        nil
    end
  end

  defp with_engine(runtime, request) do
    decision = cache_decision(runtime, request)

    plan = %{
      settings: set_statements(limits(runtime)) ++ FileCache.statements(decision),
      functions: functions(runtime.clickhouse_functions, request.partial_sql),
      lockdown: &lockdown(runtime.lockdown, runtime.allowed_directories, runtime.store, &1, &2)
    }

    case timed(fn -> JobEngine.acquire(runtime) end) do
      {engine_us, {:ok, engine, _source}} -> execute(engine, request, decision, plan, engine_us)
      {_engine_us, {:error, reason}} -> {:error, {:engine_failed, reason}}
    end
  end

  defp execute(engine, request, decision, plan, engine_us) do
    path =
      Path.join(
        System.tmp_dir!(),
        "smolquery-partial-#{System.unique_integer([:positive])}.parquet"
      )

    try do
      with :ok <- apply_statements(engine.connection, plan.settings),
           :ok <- apply_statements(engine.connection, plan.functions),
           {describe_us, {:ok, files}} <-
             timed(fn -> described(engine.connection, request.files) end),
           :ok <-
             apply_statements(
               engine.connection,
               view(request, files) ++ plan.lockdown.(path, request.allowed_paths)
             ),
           {scan_us, {:ok, partial}} <-
             timed(fn ->
               copy_out(
                 engine.connection,
                 request.partial_sql,
                 Map.get(request, :params, []),
                 path,
                 Map.get(request, :timeout_ms, :infinity)
               )
             end) do
        if decision do
          :telemetry.execute([:smolquery, :query, :file_cache, :shard], %{}, %{
            decision: decision
          })
        end

        {:ok,
         Map.merge(partial, %{
           file_cache: decision,
           node: node(),
           timings: %{engine_us: engine_us, describe_us: describe_us, scan_us: scan_us}
         })}
      else
        {_elapsed_us, {:error, reason}} -> {:error, reason}
        {:error, reason} -> {:error, reason}
      end
    after
      JobEngine.stop(engine)
      File.rm(path)
    end
  end

  defp timed(fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()

    {System.monotonic_time(:microsecond) - started, result}
  end

  defp view(_request, []), do: []

  defp view(%{table_ref: ref, schema: schema}, files),
    do:
      Views.table_view(
        ref,
        schema,
        Views.sources_select(schema, files),
        Views.recomputed(schema, files)
      )

  defp described(connection, files) do
    {unknown, known} =
      Enum.split_with(files, &(Map.get(&1, "field_ids") == nil and Map.has_key?(&1, "snapshot")))

    case unknown do
      [] ->
        {:ok, files}

      sealed ->
        with(
          {:ok, described} <- read_ids(connection, sealed),
          do: attach_ids(known, sealed, described)
        )
    end
  end

  defp read_ids(connection, sealed) do
    urls = Enum.map(sealed, & &1["url"])

    with {:ok, result} <-
           Connection.query(connection, FieldIds.sql(length(urls)), urls, :infinity) do
      {:ok, FieldIds.by_file(result.rows)}
    end
  end

  defp attach_ids(known, sealed, described) do
    case Enum.find(sealed, &(not Map.has_key?(described, &1["url"]))) do
      nil -> {:ok, known ++ Enum.map(sealed, &describe_file(&1, described))}
      %{"url" => url} -> {:error, {:undescribed_file, url}}
    end
  end

  # A file the query did not describe is an error, never a by-name read: the
  # silent fallback would be the leak this projection exists to close.
  defp describe_file(%{"url" => url} = file, described) do
    %{ids: ids, columns: columns} = Map.fetch!(described, url)

    file |> Map.put("field_ids", ids) |> Map.put("columns", columns)
  end

  defp cache_decision(%Runtime{file_cache: %{directory: nil}}, _request), do: nil

  defp cache_decision(%Runtime{} = runtime, %{file_cache_mode: mode, node_files: node_files}),
    do: FileCache.shard_decision(runtime.name, %{runtime.file_cache | mode: mode}, node_files)

  defp cache_decision(_runtime, request), do: Map.get(request, :file_cache)

  defp limits(%Runtime{} = runtime) do
    memory_limit = runtime.distributed.worker_memory_limit || runtime.job_memory_limit

    case runtime.distributed.worker_threads || runtime.read_engine_threads do
      nil -> [memory_limit: memory_limit]
      threads -> [memory_limit: memory_limit, threads: threads]
    end
  end

  defp set_statements(settings) do
    Enum.map(settings, fn
      {setting, value} when is_binary(value) -> "SET #{setting} = #{Identifier.sql_string(value)}"
      {setting, value} -> "SET #{setting} = #{value}"
    end)
  end

  defp functions(false, _sql), do: []
  defp functions(true, sql), do: ClickHouseFunctions.statements_for(sql)

  defp apply_statements(connection, statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case Connection.query(connection, statement, [], :infinity) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:statement_failed, reason}}}
      end
    end)
  end

  defp lockdown(false, _allowed_directories, _store, _path, _allowed_paths), do: []

  defp lockdown(true, allowed_directories, store, path, allowed_paths) do
    directories =
      [Path.dirname(path) | allowed_directories] ++ EngineSecrets.sealed_prefixes(store)

    Lockdown.statements(directories, allowed_paths, external_access: true)
  end

  defp copy_out(connection, partial_sql, params, path, timeout_ms) do
    statement = "COPY (#{partial_sql}) TO #{Identifier.sql_string(path)} (FORMAT parquet)"

    with {:ok, result} <- Connection.query(connection, statement, params, timeout_ms) do
      {:ok, %{parquet: File.read!(path), rows: copied_rows(result)}}
    end
  end

  defp copied_rows(%{rows: [[count]]}) when is_integer(count), do: count
  defp copied_rows(_result), do: 0
end
