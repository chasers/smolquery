defmodule Smolquery.QueryService.PartialWorker do
  @moduledoc """
  Runs one shard of a scattered query on whichever node it lands on (PL-49).

  `Smolquery.QueryService.WorkerTransport` calls `run/2`: directly on the
  coordinator's own node, and over gen_rpc from a peer (T-364). Neither
  path stops the worker when its caller gives up, so the request carries
  the job's deadline, and the partial query runs under it: a shard whose
  coordinator gave up stops at that deadline, and the `after` below kills
  its engine. The request also carries everything shard-specific: the view statements that define the planned table name
  over this shard's files, the partial SQL that reads it, and the hot-tier
  URLs the shard may fetch. Everything node-local comes from this node's own
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
          required(:statements) => [String.t()],
          required(:partial_sql) => String.t(),
          required(:allowed_paths) => [String.t()],
          optional(:timeout_ms) => timeout(),
          optional(:params) => [term()]
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
      settings(runtime) ++ request.statements ++ lockdown(runtime, path, request.allowed_paths)

    case JobEngine.acquire(runtime) do
      {:ok, engine, _source} ->
        try do
          with :ok <- apply_statements(engine.connection, statements) do
            copy_out(
              engine.connection,
              request.partial_sql,
              Map.get(request, :params, []),
              path,
              Map.get(request, :timeout_ms, :infinity)
            )
          end
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
    limit = ["SET memory_limit = #{Identifier.sql_string(memory_limit)}"]

    case runtime.distributed.worker_threads || runtime.read_engine_threads do
      nil -> limit
      threads -> limit ++ ["SET threads = #{threads}"]
    end
  end

  defp apply_statements(connection, statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case Connection.query(connection, statement, [], :infinity) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:statement_failed, reason}}}
      end
    end)
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

  defp copy_out(connection, partial_sql, params, path, timeout_ms) do
    statement = "COPY (#{partial_sql}) TO #{Identifier.sql_string(path)} (FORMAT parquet)"

    with {:ok, result} <- Connection.query(connection, statement, params, timeout_ms) do
      {:ok, %{parquet: File.read!(path), rows: copied_rows(result)}}
    end
  end

  defp copied_rows(%{rows: [[count]]}) when is_integer(count), do: count
  defp copied_rows(_result), do: 0
end
