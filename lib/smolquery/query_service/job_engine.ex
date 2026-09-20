defmodule Smolquery.QueryService.JobEngine do
  @moduledoc """
  The lifecycle of one disposable DuckDB engine: an `Adbc.Database` and a
  bootstrapped `Smolquery.Engine.Connection`, linked to the caller, killed
  on teardown.

  `Smolquery.QueryService.Runner` (the job engine) and
  `Smolquery.QueryService.PartialWorker` (a shard engine) share this
  mechanic and the bootstrap `options/1` composes — the extensions, the
  settings, the tier secrets, and the lake attach. Keeping the start/kill
  choreography in one place is what stops the two from drifting (PL-49
  review): a half-started engine is killed rather than leaked, and `stop/1`
  unlinks before it kills, so a non-trapping caller — the worker runs inside
  a scatter task or a gen_rpc acceptor — never races its own teardown's
  exit signal against delivering its result.

  ## Warm engines (PL-50)

  That bootstrap costs ~650 ms on a production node — three extension loads
  and an `ATTACH` to a Postgres catalog in another availability zone — and
  every job used to pay it on the request path. `acquire/1` asks the
  instance's `Smolquery.QueryService.EnginePool` for an engine it already
  built, probes it with one cheap statement, and only starts a cold one when
  the pool is empty, disabled, or the probe fails. Either way the caller ends
  up with a private, linked engine it alone owns; the pool never blocks a
  job. `[:smolquery, :query, :engine]` says which path served it.
  """

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.EngineSecrets
  alias Smolquery.QueryService.EnginePool
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Telemetry

  @probe_timeout_ms 5_000

  @type t :: %{database: pid(), connection: pid()}
  @type source :: :warm | :cold

  @doc """
  The bootstrap a job engine for `runtime` runs: extensions, settings,
  secrets, and the lake attach — `Smolquery.Engine.Connection.start_link/1`
  options minus `:database`.
  """
  @spec options(Runtime.t()) :: keyword()
  def options(%Runtime{} = runtime) do
    [
      extensions: extensions(runtime),
      settings: settings(runtime),
      statements: secrets(runtime) ++ runtime.job_bootstrap,
      max_rows: :infinity
    ]
  end

  @doc """
  A bootstrapped, private engine for `runtime`, linked to the caller — warm
  from the pool when one is ready, cold otherwise.
  """
  @spec acquire(Runtime.t()) :: {:ok, t(), source()} | {:error, term()}
  def acquire(%Runtime{} = runtime) do
    Telemetry.span([:smolquery, :query, :engine], &acquired/1, fn ->
      case warm(runtime) do
        {:ok, engine} -> {:ok, engine, :warm}
        :cold -> cold(runtime)
      end
    end)
  end

  defp acquired({:ok, _engine, source}), do: {%{}, %{source: source}}
  defp acquired(_failed_or_raised), do: {%{}, %{source: :failed}}

  @doc """
  Starts a database and a connection bootstrapped with `opts`
  (`Smolquery.Engine.Connection.start_link/1` options, minus `:database`).

  A connection that fails to bootstrap takes the database with it.
  """
  @spec start(keyword()) :: {:ok, t()} | {:error, term()}
  def start(opts) do
    with {:ok, database} <- DuckDB.start_link() do
      case Connection.start_link([{:database, database} | opts]) do
        {:ok, connection} ->
          {:ok, %{database: database, connection: connection}}

        {:error, reason} ->
          Process.exit(database, :kill)

          {:error, reason}
      end
    end
  end

  @doc """
  Links the caller to both engine processes — how ownership transfers out
  of the pool.
  """
  @spec link(t()) :: :ok
  def link(%{database: database, connection: connection}) do
    Process.link(database)
    Process.link(connection)

    :ok
  end

  @doc """
  Unlinks and kills both engine processes; DuckDB's in-flight work dies with
  its connection.
  """
  @spec stop(t() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(%{database: database, connection: connection}) do
    Process.unlink(connection)
    Process.unlink(database)
    Process.exit(connection, :kill)
    Process.exit(database, :kill)

    :ok
  end

  defp warm(%Runtime{warm_engines: 0}), do: :cold

  defp warm(%Runtime{} = runtime) do
    case EnginePool.checkout(runtime.name) do
      {:ok, engine} ->
        link(engine)
        probe(runtime, engine)

      :empty ->
        :cold
    end
  catch
    :exit, _pool_unavailable -> :cold
  end

  defp probe(%Runtime{} = runtime, engine) do
    case Connection.query(engine.connection, runtime.warm_probe, [], @probe_timeout_ms) do
      {:ok, _result} ->
        {:ok, engine}

      {:error, _stale} ->
        stop(engine)

        :cold
    end
  catch
    :exit, _reason ->
      stop(engine)

      :cold
  end

  defp cold(runtime) do
    with {:ok, engine} <- start(options(runtime)) do
      {:ok, engine, :cold}
    end
  end

  defp extensions(%Runtime{job_bootstrap: [], engine_extensions: extensions} = runtime),
    do: EngineSecrets.sealed_tier_extensions(runtime.store, extensions)

  defp extensions(%Runtime{engine_extensions: extensions} = runtime),
    do: EngineSecrets.sealed_tier_extensions(runtime.store, Enum.uniq([:ducklake | extensions]))

  defp settings(%Runtime{read_engine_threads: nil} = runtime),
    do: [memory_limit: runtime.job_memory_limit]

  defp settings(%Runtime{read_engine_threads: threads} = runtime),
    do: [memory_limit: runtime.job_memory_limit, threads: threads]

  defp secrets(%Runtime{} = runtime) do
    EngineSecrets.hot_tier(runtime.engine_extensions, runtime.buffer_base_url) ++
      EngineSecrets.sealed_tier(runtime.store)
  end
end
