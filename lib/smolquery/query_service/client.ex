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

  ## Trust boundary

  SQL given to this module is untrusted by default (PL-8 D7): after planning,
  each job engine disables DuckDB's external access for the user's SQL,
  leaving readable exactly the runtime's `allowed_directories`, the plan's
  own micro-segment URLs, and the sealed tier's object-store prefix when the
  runtime's `store` is S3 — `read_csv('/etc/passwd')` is a permission error,
  not a data source. `lockdown: false` restores the old trusted posture for
  deployments that want it.
  """

  alias Explorer.DataFrame
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Runner
  alias Smolquery.QueryService.Runtime

  @type option ::
          {:timeout_ms, pos_integer()}
          | {:explain, :plan | :analyze}
          | {:describe, boolean()}
          | {:trace, boolean()}
          | {:distributed, boolean()}
          | {:snapshot, Smolquery.Catalog.snapshot()}
          | {:hot_before_ms, pos_integer()}
          | {:hot_ids, %{Smolquery.Catalog.table_ref() => [String.t()]}}
          | {:params, [term()]}
          | {:result_max_rows, pos_integer()}

  @submit_option_keys [
    :timeout_ms,
    :explain,
    :describe,
    :trace,
    :distributed,
    :snapshot,
    :hot_before_ms,
    :hot_ids,
    :params,
    :result_max_rows
  ]

  @service_failures [
    :engine_failed,
    :engine_exit,
    :query_crashed,
    :worker_unreachable,
    :statement_failed,
    :extension_failed,
    :setting_failed,
    :hot_tier_unavailable,
    :pinned_hot_retired,
    :pinned_hot_expired
  ]

  @server_message ~r/\A(IO|HTTP|Connection|Internal|Out of Memory) Error|INTERNAL Error/i

  @doc """
  Runs `sql` and waits for its result.

  With `explain: :plan` or `explain: :analyze` the job finishes with the
  engine's plan text on `job.explain` and no result frame — see
  `Smolquery.QueryService.Job.explained/5`. With `describe: true` the job
  plans for real, then answers DuckDB's `DESCRIBE` of the query as the
  result frame — `column_name` and `column_type` per result column, without
  executing the query (PL-58: a Postgres `Describe` must name a prepared
  statement's columns before it binds). With `trace: true` it settles
  with its phase spans on `job.trace` (`Smolquery.QueryService.Trace`).
  `distributed: true | false` overrides the runtime's distributed default
  for this job only (PL-49); a distributed answer settles with the shard
  count on `job.scatter`, and a refusal or failure falls back silently.
  `snapshot:` pins the sealed tier at that catalog version instead of the
  current one; `hot_ids:` pins each named table's hot tier to exactly
  those micro-segment ids; and `hot_before_ms:` excludes micro-segments
  stamped after the bound from any table `hot_ids:` does not name.
  Together they are a caller's repeatable read across several jobs
  (PL-58 layers 7 and 8): pass the first job's `job.snapshot` and its
  submit time to every later job, and each table's `job.hot_members`
  from the job that first touched it. A pinned id the hot tier no longer
  holds fails the job with `{:pinned_hot_retired, ref, ids}`.
  `params:` binds the query's `$n` placeholders positionally (T-410) —
  integers, floats, strings, booleans, `Decimal`, `Date`, `NaiveDateTime`,
  or an `Adbc.Column` for a blob — as engine parameters, never as SQL
  text; the pruner and the Top-N bound read them where the `WHERE` names
  a `$n`, and an `explain:` job binds them to its `EXPLAIN`.
  `result_max_rows:` replaces the runtime's result budget for this job
  only (T-564): a caller whose own SQL ends in a `LIMIT`, and which holds
  its own ceiling on what it reads, as the VictoriaMetrics edge's samples
  query does, sets the budget to that ceiling instead of the page-sized
  default the API's results are held to.

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
      spec = {Runner, {runtime, job, Keyword.take(opts, @submit_option_keys)}}

      case DynamicSupervisor.start_child(
             {:via, PartitionSupervisor, {Runtime.runners(name), job.id}},
             spec
           ) do
        {:ok, _pid} -> {:ok, job}
        {:error, reason} -> {:error, reason}
      end
    end
  rescue
    ArgumentError -> {:error, :query_service_unavailable}
  catch
    :exit, _restarting -> {:error, :query_service_unavailable}
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

  @doc """
  Ends job `job_id` now instead of when its result TTL expires: its result
  frame is dropped from the service and a later `fetch/2` is `:not_found`.
  A caller that has taken the frame it needed calls this so a large result
  is not held twice, once by the caller and once by the service, for the
  TTL. A job still running is cancelled first; releasing an unknown job is
  `:ok`.
  """
  @spec release(atom(), String.t()) :: :ok
  def release(name, job_id) do
    case whereis(name, job_id) do
      {:ok, pid} -> Runner.release(pid)
      {:error, _reason} -> :ok
    end
  catch
    :exit, _reason -> :ok
  end

  @doc """
  Whether a failed job's `error` is the service's rather than the
  statement's, so that sending the same statement again later can succeed:
  an engine that did not start or died, a worker or a buffer node that
  could not be reached, a bootstrap statement that failed, a pinned read
  the hot tier no longer holds, or an engine error whose text is one
  `server_failure_message?/1` recognises. A statement the engine refused
  (a parser, binder or regular-expression error) is not.
  """
  @spec service_failure?(term()) :: boolean()
  def service_failure?(error) when is_tuple(error) and tuple_size(error) >= 2,
    do: elem(error, 0) in @service_failures or invalid_query_message?(error)

  def service_failure?(error) when is_exception(error),
    do: error |> Exception.message() |> server_failure_message?()

  def service_failure?(_error), do: false

  defp invalid_query_message?({:invalid_query, message}) when is_binary(message),
    do: server_failure_message?(message)

  defp invalid_query_message?(_error), do: false

  @doc """
  Whether an engine error's text is the server's failure — the disk, memory,
  a connection the engine could not make, an internal error — rather than
  the statement's.

      iex> Smolquery.QueryService.Client.server_failure_message?("Out of Memory Error: failed to allocate")
      true
      iex> Smolquery.QueryService.Client.server_failure_message?("Binder Error: column x not found")
      false
  """
  @spec server_failure_message?(String.t()) :: boolean()
  def server_failure_message?(message) when is_binary(message),
    do: Regex.match?(@server_message, message)

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
