defmodule Smolquery.QueryService.PartialWorker do
  @moduledoc """
  Runs one shard of a scattered query on whichever node it lands on (PL-49).

  `Smolquery.QueryService.Scatter` calls `run/2` through `:erpc.call/4` —
  the local node included, so one code path serves both, and `:erpc` kills
  the spawned worker when its caller dies, so a cancelled or timed-out job
  takes its in-flight shards down with it. The request carries everything
  shard-specific: the view statements that define the planned table name
  over this shard's files, the partial SQL that reads it, and the hot-tier
  URLs the shard may fetch. Everything node-local comes from this node's own
  published `Smolquery.QueryService.Runtime`: the engine extensions and the
  same hot-tier and sealed-tier secrets a job engine gets.

  The engine is private and disposable
  (`Smolquery.QueryService.JobEngine`), sized by the runtime's
  `distributed.worker_memory_limit` and `worker_threads`. The partial
  result leaves DuckDB as a parquet file (`COPY`), not through Arrow →
  Polars — DuckDB intermittently fails to read Polars-written parquet
  (PL-48) — and returns to the coordinator as the file's bytes.

  ## Lockdown

  The partial SQL derives from user SQL, so the engine is confined the way
  a job engine is: `allowed_directories` is exactly this shard's output
  directory, the node's own allowed data directories, and the sealed
  store's prefix; `allowed_paths` is the shard's hot URLs; and
  `lock_configuration = true` seals it. `enable_external_access` stays on —
  `COPY ... TO` needs it — but the directory and path lists bound what it
  can reach, honoring the runtime's `lockdown` flag the same way `Runner`
  does.
  """

  alias Smolquery.Engine.Connection
  alias Smolquery.EngineSecrets
  alias Smolquery.Identifier
  alias Smolquery.QueryService.JobEngine
  alias Smolquery.QueryService.Runtime

  @type request :: %{
          statements: [String.t()],
          partial_sql: String.t(),
          allowed_paths: [String.t()]
        }

  @doc """
  Whether query service instance `name` runs on this node.
  """
  @spec available?(atom()) :: boolean()
  def available?(name), do: match?({:ok, _runtime}, Runtime.fetch(name))

  @doc """
  Runs `request`'s partial query over its shard and returns the result as
  parquet bytes with its row count.
  """
  @spec run(atom(), request()) ::
          {:ok, %{parquet: binary(), rows: non_neg_integer()}} | {:error, term()}
  def run(name, request) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> with_engine(runtime, request)
      :error -> {:error, :query_service_unavailable}
    end
  end

  defp with_engine(runtime, request) do
    path =
      Path.join(
        System.tmp_dir!(),
        "smolquery-partial-#{System.unique_integer([:positive])}.parquet"
      )

    statements =
      secrets(runtime) ++ request.statements ++ lockdown(runtime, path, request.allowed_paths)

    engine =
      JobEngine.start(
        extensions:
          EngineSecrets.sealed_tier_extensions(runtime.store, runtime.engine_extensions),
        settings: settings(runtime),
        statements: statements,
        max_rows: :infinity
      )

    case engine do
      {:ok, engine} ->
        try do
          copy_out(engine.connection, request.partial_sql, path)
        after
          JobEngine.stop(engine)
          File.rm(path)
        end

      {:error, reason} ->
        {:error, {:engine_failed, reason}}
    end
  end

  defp settings(%Runtime{} = runtime) do
    memory_limit = runtime.distributed.worker_memory_limit || runtime.job_memory_limit

    case runtime.distributed.worker_threads || runtime.read_engine_threads do
      nil -> [memory_limit: memory_limit]
      threads -> [memory_limit: memory_limit, threads: threads]
    end
  end

  defp secrets(%Runtime{} = runtime) do
    EngineSecrets.hot_tier(runtime.engine_extensions, runtime.buffer_base_url) ++
      EngineSecrets.sealed_tier(runtime.store)
  end

  defp lockdown(%Runtime{lockdown: false}, _path, _allowed_paths), do: []

  defp lockdown(%Runtime{} = runtime, path, allowed_paths) do
    directories =
      [Path.dirname(path) | runtime.allowed_directories] ++
        EngineSecrets.sealed_prefixes(runtime.store)

    [
      "SET allowed_directories = #{sql_list(directories)}",
      "SET allowed_paths = #{sql_list(allowed_paths)}",
      "SET lock_configuration = true"
    ]
  end

  defp sql_list(values) do
    "[" <> Enum.map_join(values, ", ", &Identifier.sql_string/1) <> "]"
  end

  defp copy_out(connection, partial_sql, path) do
    statement = "COPY (#{partial_sql}) TO #{Identifier.sql_string(path)} (FORMAT parquet)"

    with {:ok, result} <- Connection.query(connection, statement, [], :infinity) do
      {:ok, %{parquet: File.read!(path), rows: copied_rows(result)}}
    end
  end

  defp copied_rows(%{rows: [[count]]}) when is_integer(count), do: count
  defp copied_rows(_result), do: 0
end
