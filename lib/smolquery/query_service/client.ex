defmodule Smolquery.QueryService.Client do
  @moduledoc """
  The only way in or out of the query service.

  Same rule as every other service: no `GenServer.call` into a runner from
  outside, no Registry lookup, no reading the runner supervisor — this module
  is the seam that lets the query service move to its own deployment without
  a rewrite.

  ## Sync and async are the same job

  `query/3` is `submit/3` plus `await/3`: the job exists either way, with the
  same id, lifecycle, and result TTL. A sync caller that gives up waiting has
  not stopped the query — `query/3` cancels the job before returning
  `{:error, :timeout}`, so a timed-out call never leaves work running behind
  the caller's back.

  ## Admission is a bound, not a queue

  `max_concurrent_jobs` counts jobs still doing work (finished ones holding
  results for their TTL do not count). A submission past the bound is refused
  with `{:error, :too_many_jobs}` — the caller knows immediately, instead of
  queueing invisibly behind an unbounded backlog.

  ## v1 trust boundary

  SQL given to this module is trusted. A SELECT can read any file or URL the
  node can reach (`read_csv('/etc/passwd')` is a SELECT), and DuckDB's
  external access cannot be disabled without also breaking the hot-tier reads
  the plan itself needs. Single-tenant self-hosted is the v1 posture, the
  same one `HotServer`'s unauthenticated routes take; scoping
  `allowed_directories` lands with auth in Milestone 6.
  """

  alias Explorer.DataFrame
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Runner
  alias Smolquery.QueryService.Runtime

  @type option :: {:timeout_ms, pos_integer()}

  @doc """
  Runs `sql` and waits for its result.

  Returns the finished job and its result frame. The job may have finished
  badly — `job.state` is `:error` or `:cancelled` and the frame `nil` — which
  is still `{:ok, ...}`: the question this function answers is "what happened
  to my query", and an answer is not itself a failure. `{:error, ...}` means
  the question could not be answered: the service is not running here, the
  job was refused, or the wait timed out (in which case the job has been
  cancelled).
  """
  @spec query(atom(), String.t(), [option()]) ::
          {:ok, Job.t(), DataFrame.t() | nil} | {:error, term()}
  def query(name, sql, opts \\ []) do
    with {:ok, runtime} <- runtime(name),
         {:ok, job} <- submit(name, sql, opts) do
      await(name, job.id, Keyword.get(opts, :timeout_ms, runtime.default_timeout_ms))
    end
  end

  @doc """
  Submits `sql` as an async job and returns it, pending.

  The job is refused when the service is not running here
  (`{:error, :query_service_unavailable}`) or the node is at
  `max_concurrent_jobs` (`{:error, :too_many_jobs}`).
  """
  @spec submit(atom(), String.t(), [option()]) :: {:ok, Job.t()} | {:error, term()}
  def submit(name, sql, opts \\ []) do
    with {:ok, runtime} <- runtime(name),
         :ok <- admit(runtime) do
      job = Job.new(sql)
      spec = {Runner, {runtime, job, Keyword.take(opts, [:timeout_ms])}}

      case DynamicSupervisor.start_child(
             {:via, PartitionSupervisor, {Runtime.runners(name), job.id}},
             spec
           ) do
        {:ok, _pid} -> {:ok, job}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Blocks until job `job_id` finishes, then returns it with its result.

  Waiting out `timeout` cancels the job — see `query/3`.
  """
  @spec await(atom(), String.t(), timeout()) ::
          {:ok, Job.t(), DataFrame.t() | nil} | {:error, term()}
  def await(name, job_id, timeout) do
    case whereis(name, job_id) do
      {:ok, pid} -> do_await(name, job_id, pid, timeout)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The job as it stands right now, and its result frame if it has one.

  `{:error, :not_found}` covers both a job that never existed and one whose
  result TTL has expired — to a caller they are the same answer.
  """
  @spec fetch(atom(), String.t()) :: {:ok, Job.t(), DataFrame.t() | nil} | {:error, term()}
  def fetch(name, job_id) do
    with {:ok, pid} <- whereis(name, job_id) do
      Runner.fetch(pid)
    end
  catch
    :exit, _reason -> {:error, :not_found}
  end

  @doc """
  Cancels job `job_id`. Cancelling a finished or unknown job is `:ok` —
  the state a caller wanted is the state they have.
  """
  @spec cancel(atom(), String.t()) :: :ok
  def cancel(name, job_id) do
    case whereis(name, job_id) do
      {:ok, pid} -> Runner.cancel(pid)
      {:error, _reason} -> :ok
    end
  catch
    :exit, _reason -> :ok
  end

  defp do_await(name, job_id, pid, timeout) do
    Runner.await(pid, timeout)
  catch
    :exit, {:timeout, _call} ->
      cancel(name, job_id)

      {:error, :timeout}

    :exit, {reason, _call} when reason in [:noproc, :normal, :shutdown] ->
      {:error, :not_found}
  end

  defp runtime(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> {:ok, runtime}
      :error -> {:error, :query_service_unavailable}
    end
  end

  defp admit(%Runtime{} = runtime) do
    active =
      Runtime.registry(runtime.name)
      |> Registry.select([{{:_, :_, :active}, [], [true]}])
      |> length()

    if active < runtime.max_concurrent_jobs, do: :ok, else: {:error, :too_many_jobs}
  end

  defp whereis(name, job_id) do
    case Registry.lookup(Runtime.registry(name), job_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end
end
