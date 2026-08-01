defmodule Smolquery.QueryService.History do
  @moduledoc """
  Terminal jobs, durably — the PL-8 D8 store.

  v1 held finished jobs in the registry until `result_ttl_ms` because history
  without an HTTP API had no consumer; `GET /v1/jobs/:id` is the consumer, and
  this is where it looks when the registry misses. History is metadata only —
  id, sql, terminal state, stats — never results; `result_ttl_ms` still
  governs data.

  Rows live in a `smolquery_jobs` table inside the same SQL database that
  backs the DuckLake catalog (SQLite in dev), written through this process's
  own engine via DuckDB's `sqlite` extension — one metadata database to
  operate and replicate, but the `Smolquery.Catalog` behaviour stays about
  datasets and segments. Postgres lands with the M8 catalog move.

  Recording is a cast and failures only log: a job's history row is worth
  having and never worth failing the job over. A history whose engine could
  not start (no `sqlite` extension offline, unwritable metadata database)
  disables itself rather than crash-looping the query service.
  """

  use GenServer

  require Logger

  alias Smolquery.Engine.Connection
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Runtime

  @table "smolquery_history.smolquery_jobs"

  @doc """
  Starts the history store for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.history(runtime.name))
  end

  @doc """
  Records a terminal job. A history that is not running loses the row, and
  the job does not care.
  """
  @spec record(atom(), Job.t()) :: :ok
  def record(name, %Job{} = job) do
    GenServer.cast(Runtime.history(name), {:record, job})
  end

  @doc """
  The recorded job, when the registry no longer knows it.
  """
  @spec fetch(atom(), String.t()) :: {:ok, Job.t()} | {:error, :not_found}
  def fetch(name, job_id) do
    GenServer.call(Runtime.history(name), {:fetch, job_id})
  catch
    :exit, {:noproc, _call} -> {:error, :not_found}
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    {:ok, %{runtime: runtime, connection: nil}, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state) do
    case connect(state.runtime) do
      {:ok, connection} ->
        {:noreply, %{state | connection: connection}}

      {:error, reason} ->
        Logger.warning(
          "job history disabled: could not open #{state.runtime.history_metadata}: " <>
            inspect(reason)
        )

        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast({:record, _job}, %{connection: nil} = state), do: {:noreply, state}

  def handle_cast({:record, %Job{} = job}, state) do
    params = [
      job.id,
      job.sql,
      state_string(job.state),
      error_string(job.error),
      job.snapshot,
      job.row_count,
      job.duration_ms,
      job.submitted_at,
      job.finished_at
    ]

    insert =
      "INSERT INTO #{@table} VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

    case Connection.query(state.connection, insert, params, 15_000) do
      {:ok, _result} -> :ok
      {:error, reason} -> Logger.warning("job history write failed: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:fetch, _job_id}, _from, %{connection: nil} = state) do
    {:reply, {:error, :not_found}, state}
  end

  def handle_call({:fetch, job_id}, _from, state) do
    select =
      "SELECT id, sql, state, error, snapshot, row_count, duration_ms, " <>
        "submitted_at, finished_at FROM #{@table} WHERE id = $1"

    case Connection.query(state.connection, select, [job_id], 15_000) do
      {:ok, %{rows: [row]}} -> {:reply, {:ok, from_row(row)}, state}
      {:ok, _empty} -> {:reply, {:error, :not_found}, state}
      {:error, _reason} -> {:reply, {:error, :not_found}, state}
    end
  end

  defp connect(%Runtime{history_metadata: "sqlite:" <> path}) do
    with {:ok, database} <- Adbc.Database.start_link(driver: :duckdb) do
      Connection.start_link(
        database: database,
        extensions: [:sqlite],
        statements: [
          "ATTACH IF NOT EXISTS #{Identifier.sql_string(path)} " <>
            "AS smolquery_history (TYPE sqlite)",
          "CREATE TABLE IF NOT EXISTS #{@table} (" <>
            "id TEXT, sql TEXT, state TEXT, error TEXT, snapshot BIGINT, " <>
            "row_count BIGINT, duration_ms BIGINT, submitted_at BIGINT, " <>
            "finished_at BIGINT)"
        ],
        max_rows: 1_000
      )
    end
  end

  defp connect(%Runtime{history_metadata: other}), do: {:error, {:unsupported_metadata, other}}

  defp from_row([id, sql, state, error, snapshot, row_count, duration_ms, submitted, finished]) do
    %Job{
      id: id,
      sql: sql,
      state: state_from_string(state),
      error: error,
      snapshot: snapshot,
      row_count: row_count,
      duration_ms: duration_ms,
      submitted_at: submitted,
      finished_at: finished
    }
  end

  defp state_string(:done), do: "done"
  defp state_string(:error), do: "error"
  defp state_string(:cancelled), do: "cancelled"
  defp state_string(:pending), do: "pending"
  defp state_string(:running), do: "running"

  defp state_from_string("done"), do: :done
  defp state_from_string("error"), do: :error
  defp state_from_string("cancelled"), do: :cancelled
  defp state_from_string(_other), do: :error

  defp error_string(nil), do: nil
  defp error_string(error) when is_binary(error), do: error
  defp error_string(error), do: inspect(error)
end
