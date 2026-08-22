defmodule Smolquery.QueryService.PartialWorker do
  @moduledoc """
  Runs one shard of a scattered query on whichever node it lands on (PL-49).

  `Smolquery.QueryService.Scatter` calls `run/2` through `:erpc.call/4` —
  the local node included, so one code path serves both. The request
  carries everything shard-specific: the view statements that define the
  planned table name over this shard's files, and the partial SQL that
  reads it. Everything node-local comes from this node's own published
  `Smolquery.QueryService.Runtime`: the engine extensions, and the same
  hot-tier and sealed-tier secrets a job engine gets, so the shard can read
  peer buffer URLs and the object store exactly as a local scan would.

  The engine is private and disposable, the way `Runner`'s is: an
  `Adbc.Database` and a bootstrapped connection, killed when the shard
  finishes either way. The partial result leaves DuckDB as a parquet file
  (`COPY`), not through Arrow → Polars — DuckDB intermittently fails to
  read Polars-written parquet (PL-48) — and returns to the coordinator as
  the file's bytes.

  ## PoC limits

  The worker applies no lockdown: `COPY ... TO` is external access, so the
  engine keeps it enabled. The partial SQL derives from user SQL, so this
  is a trusted-posture PoC path behind a default-off flag; hardening
  (a scoped `allowed_directories`, or handing back Arrow without touching
  disk) is follow-up work before this leaves PoC.
  """

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.EngineSecrets
  alias Smolquery.QueryService.Runtime

  @type request :: %{statements: [String.t()], partial_sql: String.t()}

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
    case start_engine(runtime, request.statements) do
      {:ok, engine} ->
        try do
          copy_out(engine.connection, request.partial_sql)
        after
          Process.exit(engine.connection, :kill)
          Process.exit(engine.database, :kill)
        end

      {:error, reason} ->
        {:error, {:engine_failed, reason}}
    end
  end

  defp start_engine(runtime, statements) do
    with {:ok, database} <- DuckDB.start_link() do
      connection =
        Connection.start_link(
          database: database,
          extensions:
            EngineSecrets.sealed_tier_extensions(runtime.store, runtime.engine_extensions),
          settings: settings(runtime),
          statements: secrets(runtime) ++ statements,
          max_rows: :infinity
        )

      case connection do
        {:ok, connection} ->
          {:ok, %{database: database, connection: connection}}

        {:error, reason} ->
          Process.exit(database, :kill)
          {:error, reason}
      end
    end
  end

  defp settings(%Runtime{read_engine_threads: nil} = runtime),
    do: [memory_limit: runtime.job_memory_limit]

  defp settings(%Runtime{read_engine_threads: threads} = runtime),
    do: [memory_limit: runtime.job_memory_limit, threads: threads]

  defp secrets(%Runtime{} = runtime) do
    EngineSecrets.hot_tier(runtime.engine_extensions, runtime.buffer_base_url) ++
      EngineSecrets.sealed_tier(runtime.store)
  end

  defp copy_out(connection, partial_sql) do
    path =
      Path.join(
        System.tmp_dir!(),
        "smolquery-partial-#{System.unique_integer([:positive])}.parquet"
      )

    statement =
      "COPY (#{partial_sql}) TO #{Smolquery.Identifier.sql_string(path)} (FORMAT parquet)"

    try do
      with {:ok, result} <- Connection.query(connection, statement, [], :infinity) do
        {:ok, %{parquet: File.read!(path), rows: copied_rows(result)}}
      end
    after
      File.rm(path)
    end
  end

  defp copied_rows(%{rows: [[count]]}) when is_integer(count), do: count
  defp copied_rows(_result), do: 0
end
