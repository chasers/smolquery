defmodule Smolquery.QueryService.Runner do
  @moduledoc """
  One job, as a process: plan, execute, hold the result, die on schedule.

  The runner owns a private engine — an `Adbc.Database` and a bootstrapped
  `Smolquery.Engine.Connection`, both linked, neither registered under a name.
  That privacy is the point (see PL-7 D8): two jobs' views never collide
  because they live in different in-memory catalogs, cancellation is killing
  the engine processes — DuckDB's work dies with its connection — and the
  job's `memory_limit` binds only itself. The engine is not supervised for
  restart: a job whose engine died is a failed job, not a retried one, so the
  runner traps the exit and records it.

  The query runs in a `Task`, not in the runner itself, so `cancel/1`,
  `fetch/1`, and `await/2` stay answerable mid-scan. A runner past its
  deadline (`:timeout_ms`, defaulting to the runtime's `default_timeout_ms`)
  cancels itself the same way a caller would.

  A finished runner holds its `Explorer.DataFrame` result until `result_ttl_ms`
  expires, then stops normally — the registry entry goes with it, and a later
  fetch is honestly `:not_found`. The registry value tracks `:active` versus
  `:settled`, which is what lets admission count only jobs still doing work.
  """

  use GenServer, restart: :temporary

  alias Explorer.DataFrame
  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService.History
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime

  @type option :: {:timeout_ms, pos_integer()}

  @doc """
  Starts a runner for `job`, registered by the job's id.
  """
  @spec start_link({Runtime.t(), Job.t(), [option()]}) :: GenServer.on_start()
  def start_link({runtime, job, opts}) do
    GenServer.start_link(__MODULE__, {runtime, job, opts}, name: via(runtime.name, job.id))
  end

  @doc """
  The registered name of the runner for `job_id` on instance `name`.
  """
  @spec via(atom(), String.t()) :: {:via, Registry, {atom(), String.t(), :active}}
  def via(name, job_id), do: {:via, Registry, {Runtime.registry(name), job_id, :active}}

  @doc """
  Blocks until the job reaches a terminal state, then returns it with its result.
  """
  @spec await(GenServer.server(), timeout()) ::
          {:ok, Job.t(), DataFrame.t() | nil} | {:error, term()}
  def await(server, timeout), do: GenServer.call(server, :await, timeout)

  @doc """
  The job as it stands, and its result frame if it has one.
  """
  @spec fetch(GenServer.server()) :: {:ok, Job.t(), DataFrame.t() | nil}
  def fetch(server), do: GenServer.call(server, :fetch)

  @doc """
  Cancels a running job; cancelling a finished one changes nothing.
  """
  @spec cancel(GenServer.server()) :: :ok
  def cancel(server), do: GenServer.call(server, :cancel)

  @impl GenServer
  def init({runtime, job, opts}) do
    Process.flag(:trap_exit, true)

    Process.send_after(
      self(),
      :deadline,
      Keyword.get(opts, :timeout_ms, runtime.default_timeout_ms)
    )

    state = %{
      runtime: runtime,
      job: job,
      engine: nil,
      task: nil,
      result: nil,
      waiters: []
    }

    {:ok, state, {:continue, :run}}
  end

  @impl GenServer
  def handle_continue(:run, state) do
    case start_engine(state.runtime) do
      {:ok, engine} ->
        runtime = state.runtime
        sql = state.job.sql
        connection = engine.connection
        task = Task.async(fn -> execute(runtime, connection, sql) end)

        {:noreply, %{state | engine: engine, task: task, job: Job.running(state.job)}}

      {:error, reason} ->
        {:noreply, settle(state, Job.failed(state.job, {:engine_failed, reason}))}
    end
  end

  @impl GenServer
  def handle_call(:await, from, state) do
    if Job.terminal?(state.job) do
      {:reply, {:ok, state.job, state.result}, state}
    else
      {:noreply, %{state | waiters: [from | state.waiters]}}
    end
  end

  def handle_call(:fetch, _from, state) do
    {:reply, {:ok, state.job, state.result}, state}
  end

  def handle_call(:cancel, _from, state) do
    if Job.terminal?(state.job) do
      {:reply, :ok, state}
    else
      {:reply, :ok, state |> halt() |> settle(Job.cancelled(state.job, :cancelled))}
    end
  end

  @impl GenServer
  def handle_info({ref, outcome}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])

    {job, result} =
      case outcome do
        {:ok, %{snapshot: snapshot, frame: frame, duration_ms: duration}} ->
          {Job.done(state.job, snapshot, DataFrame.n_rows(frame), duration), frame}

        {:error, reason} ->
          {Job.failed(state.job, reason), nil}
      end

    {:noreply, settle(%{state | task: nil, result: result}, job)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    {:noreply, settle(%{state | task: nil}, Job.failed(state.job, {:query_crashed, reason}))}
  end

  def handle_info(:deadline, state) do
    if Job.terminal?(state.job) do
      {:noreply, state}
    else
      {:noreply, state |> halt() |> settle(Job.cancelled(state.job, :timeout))}
    end
  end

  def handle_info(:expire, state), do: {:stop, :normal, state}

  def handle_info({:EXIT, pid, reason}, state) do
    if engine_pid?(state.engine, pid) and not Job.terminal?(state.job) do
      {:noreply, state |> halt() |> settle(Job.failed(state.job, {:engine_exit, reason}))}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    stop_engine(state.engine)
  end

  defp start_engine(runtime) do
    with {:ok, database} <- Adbc.Database.start_link(driver: :duckdb) do
      connection =
        Connection.start_link(
          database: database,
          extensions: extensions(runtime),
          settings: [memory_limit: runtime.job_memory_limit],
          statements: runtime.job_bootstrap,
          max_rows: :infinity
        )

      case connection do
        {:ok, connection} -> {:ok, %{database: database, connection: connection}}
        {:error, reason} -> stop_and_report(database, reason)
      end
    end
  end

  defp stop_and_report(database, reason) do
    Process.exit(database, :kill)

    {:error, reason}
  end

  defp extensions(%Runtime{job_bootstrap: [], engine_extensions: extensions}), do: extensions

  defp extensions(%Runtime{engine_extensions: extensions}),
    do: Enum.uniq([:ducklake | extensions])

  defp execute(runtime, connection, sql) do
    started = System.monotonic_time(:millisecond)

    with {:ok, plan} <- Planner.plan(runtime, connection, sql),
         :ok <- run_statements(connection, plan.statements),
         {:ok, frame} <- Connection.frame(connection, plan.sql, [], :infinity) do
      duration = System.monotonic_time(:millisecond) - started

      {:ok, %{snapshot: plan.snapshot, frame: frame, duration_ms: duration}}
    end
  end

  defp run_statements(connection, statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case Connection.query(connection, statement, [], :infinity) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp halt(%{task: nil} = state), do: state

  defp halt(%{task: task} = state) do
    Task.shutdown(task, :brutal_kill)

    %{state | task: nil}
  end

  defp settle(state, job) do
    stop_engine(state.engine)
    record_history(state.runtime, job)
    Enum.each(state.waiters, &GenServer.reply(&1, {:ok, job, state.result}))

    Registry.update_value(
      Runtime.registry(state.runtime.name),
      job.id,
      fn _active -> :settled end
    )

    Process.send_after(self(), :expire, state.runtime.result_ttl_ms)

    %{state | job: job, engine: nil, waiters: []}
  end

  defp stop_engine(nil), do: :ok

  defp stop_engine(%{database: database, connection: connection}) do
    Process.exit(connection, :kill)
    Process.exit(database, :kill)

    :ok
  end

  defp record_history(%Runtime{history_metadata: nil}, _job), do: :ok
  defp record_history(%Runtime{name: name}, job), do: History.record(name, job)

  defp engine_pid?(nil, _pid), do: false

  defp engine_pid?(%{database: database, connection: connection}, pid),
    do: pid in [database, connection]
end
