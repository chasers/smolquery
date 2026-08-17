defmodule SmolqueryApi.JobController do
  @moduledoc """
  The async-job routes' logic, over `Smolquery.QueryService.Client`.

  A job's HTTP shape mirrors `Smolquery.QueryService.Job` — id, state, sql,
  timestamps, stats, the pinned snapshot — plus `resultsAvailable`, which is
  what separates a finished job whose frame is still held from one the
  registry has let go. When the registry misses, `GET /v1/jobs/:id` falls back
  to `QueryService.History` (PL-8 D8): status and stats survive the result
  TTL; results do not.

  Results page from the frame the runner materialized (PL-8 D9): an opaque
  `pageToken` carries the offset, `maxResults` bounds the page, and paging a
  job never extends its TTL. A finished-but-expired job answers 410, an
  unknown one 404 — a client can tell "too late" from "never existed".
  """

  use SmolqueryApi, :controller

  alias Explorer.DataFrame
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.History
  alias Smolquery.QueryService.Job
  alias Smolquery.QueryService.Statistics
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json
  alias SmolqueryApi.Runtime

  @default_max_results 1_000
  @max_max_results 10_000

  @doc """
  Submits the body's query as an async job.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, _params) do
    with {:ok, sql} <- sql(conn.body_params),
         {:ok, opts} <- submit_opts(conn.body_params),
         {:ok, job} <- Client.submit(query_name(conn), sql, opts) do
      Json.send_json(conn, 200, job_json(job, false))
    else
      {:error, reason} -> query_error(conn, reason)
    end
  end

  @doc """
  A job's status and stats — from the registry, or from history once the
  registry has let it go.
  """
  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"id" => job_id}) do
    case Client.fetch(query_name(conn), job_id) do
      {:ok, %Job{} = job, frame} ->
        Json.send_json(conn, 200, job_json(job, job.state == :done and frame != nil))

      {:error, :not_found} ->
        from_history(conn, job_id)
    end
  end

  @doc """
  One page of a finished job's rows.
  """
  @spec results(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def results(conn, %{"id" => job_id}) do
    conn = fetch_query_params(conn)

    with {:ok, max_results} <- max_results(conn.query_params),
         {:ok, offset} <- offset(conn.query_params, job_id) do
      case Client.fetch(query_name(conn), job_id) do
        {:ok, %Job{state: :done} = job, %DataFrame{} = frame} ->
          Json.send_json(conn, 200, page_body(job, frame, offset, max_results))

        {:ok, %Job{state: :done, explain: explain}, _frame} when is_binary(explain) ->
          Errors.send_error(
            conn,
            409,
            "FAILED_PRECONDITION",
            "an explain job has no result rows; read explain from GET /v1/jobs/:id"
          )

        {:ok, %Job{state: state}, _frame} when state in [:pending, :running] ->
          Json.send_json(conn, 200, %{"complete" => false, "rows" => []})

        {:ok, %Job{} = job, _frame} ->
          failed_job(conn, job)

        {:error, :not_found} ->
          expired_or_unknown(conn, job_id)
      end
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  @doc """
  Cancels a job. Cancelling a finished or unknown job succeeds — the state
  the caller wanted is the state they have.
  """
  @spec delete(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def delete(conn, %{"id" => job_id}) do
    :ok = Client.cancel(query_name(conn), job_id)

    Json.send_json(conn, 200, %{"id" => job_id})
  end

  @doc """
  The JSON shape of a job.
  """
  @spec job_json(Job.t(), boolean()) :: map()
  def job_json(%Job{} = job, results_available) do
    %{
      "id" => job.id,
      "state" => Atom.to_string(job.state),
      "sql" => job.sql,
      "submittedAt" => iso8601(job.submitted_at),
      "finishedAt" => iso8601(job.finished_at),
      "snapshot" => job.snapshot,
      "rowCount" => job.row_count,
      "durationMs" => job.duration_ms,
      "statistics" => statistics_json(job.statistics),
      "explain" => job.explain,
      "error" => error_json(job.error),
      "resultsAvailable" => results_available
    }
  end

  defp statistics_json(nil), do: nil

  defp statistics_json(%Statistics{} = statistics) do
    %{
      "filesTotal" => Statistics.files_total(statistics),
      "filesScanned" => Statistics.files_scanned(statistics),
      "rowsScanned" => Statistics.rows_scanned(statistics),
      "bytesScanned" => Statistics.bytes_scanned(statistics),
      "mibScanned" => Statistics.mib_scanned(statistics),
      "hot" => tier_json(statistics.hot),
      "sealed" => tier_json(statistics.sealed)
    }
  end

  defp tier_json(tier) do
    %{
      "filesTotal" => tier.files_total,
      "filesScanned" => tier.files_scanned,
      "rowsScanned" => tier.rows_scanned,
      "bytesScanned" => tier.bytes_scanned
    }
  end

  @doc """
  One page of `frame` as a response body, with the token that continues it.
  """
  @spec page_body(Job.t(), DataFrame.t(), non_neg_integer(), pos_integer()) :: map()
  def page_body(%Job{} = job, %DataFrame{} = frame, offset, max_results) do
    total = DataFrame.n_rows(frame)

    rows =
      frame
      |> DataFrame.slice(offset, max_results)
      |> DataFrame.to_rows()
      |> Enum.map(&row_json/1)

    body = %{
      "complete" => true,
      "totalRows" => total,
      "rows" => rows
    }

    case offset + length(rows) do
      past_the_end when past_the_end >= total -> body
      next -> Map.put(body, "nextPageToken", token(job.id, next))
    end
  end

  @doc """
  Maps a query-service refusal to the envelope; shared with the sync route.
  """
  @spec query_error(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def query_error(conn, :too_many_jobs) do
    Errors.send_resource_exhausted(conn, 1, "too many jobs in flight, retry later")
  end

  def query_error(conn, :query_service_unavailable) do
    Errors.send_error(conn, 503, "UNAVAILABLE", "the query service is not available here")
  end

  def query_error(conn, reason), do: Errors.from_reason(conn, reason)

  @doc """
  The query-service instance this API talks to.
  """
  @spec query_name(Plug.Conn.t()) :: atom()
  def query_name(conn) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    runtime.query_name
  end

  @doc """
  Submit options a request body may carry.

  `timeoutMs` bounds the job; `explain` (`"plan"` or `"analyze"`) asks for
  the engine's plan instead of rows. Any other `explain` value is refused —
  silently running the query a caller asked to explain would be the worst
  possible reading of a typo.
  """
  @spec submit_opts(map()) :: {:ok, keyword()} | {:error, {:invalid_param, String.t()}}
  def submit_opts(body) do
    with {:ok, explain} <- explain_opt(body) do
      {:ok, timeout_opt(body) ++ explain}
    end
  end

  defp timeout_opt(%{"timeoutMs" => timeout}) when is_integer(timeout) and timeout > 0,
    do: [timeout_ms: timeout]

  defp timeout_opt(_body), do: []

  defp explain_opt(%{"explain" => "plan"}), do: {:ok, [explain: :plan]}
  defp explain_opt(%{"explain" => "analyze"}), do: {:ok, [explain: :analyze]}
  defp explain_opt(%{"explain" => _other}), do: {:error, {:invalid_param, "explain"}}
  defp explain_opt(_body), do: {:ok, []}

  @doc """
  The body's `query` field.
  """
  @spec sql(map()) :: {:ok, String.t()} | {:error, {:missing_field, String.t()}}
  def sql(%{"query" => sql}) when is_binary(sql) and sql != "", do: {:ok, sql}
  def sql(_body), do: {:error, {:missing_field, "query"}}

  defp from_history(conn, job_id) do
    case History.fetch(query_name(conn), job_id) do
      {:ok, %Job{} = job} -> Json.send_json(conn, 200, job_json(job, false))
      {:error, :not_found} -> Errors.send_error(conn, 404, "NOT_FOUND", "no such job")
    end
  end

  defp expired_or_unknown(conn, job_id) do
    case History.fetch(query_name(conn), job_id) do
      {:ok, %Job{state: :done}} ->
        Errors.send_error(conn, 410, "GONE", "results expired; re-run the query")

      {:ok, %Job{} = job} ->
        failed_job(conn, job)

      {:error, :not_found} ->
        Errors.send_error(conn, 404, "NOT_FOUND", "no such job")
    end
  end

  defp failed_job(conn, %Job{} = job) do
    Errors.send_error(
      conn,
      409,
      "FAILED_PRECONDITION",
      "job finished #{job.state}: #{error_json(job.error) || "no error recorded"}"
    )
  end

  defp max_results(%{"max_results" => value}) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, min(n, @max_max_results)}
      _invalid -> {:error, {:invalid_param, "max_results"}}
    end
  end

  defp max_results(_params), do: {:ok, @default_max_results}

  defp offset(%{"page_token" => token}, job_id) do
    with {:ok, decoded} <- Base.url_decode64(token, padding: false),
         {:ok, [^job_id, offset]} when is_integer(offset) and offset >= 0 <- decode(decoded) do
      {:ok, offset}
    else
      _invalid -> {:error, {:invalid_param, "page_token"}}
    end
  end

  defp offset(_params, _job_id), do: {:ok, 0}

  defp decode(binary) do
    case JSON.decode(binary) do
      {:ok, value} -> {:ok, value}
      {:error, _reason} -> :error
    end
  end

  defp token(job_id, offset) do
    Base.url_encode64(JSON.encode!([job_id, offset]), padding: false)
  end

  defp iso8601(nil), do: nil

  defp iso8601(milliseconds) do
    milliseconds |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
  end

  defp error_json(nil), do: nil
  defp error_json({:invalid_query, message}) when is_binary(message), do: message
  defp error_json(error) when is_binary(error), do: error
  defp error_json(error), do: inspect(error)

  defp row_json(row), do: Map.new(row, fn {name, value} -> {name, value_json(value)} end)

  defp value_json(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp value_json(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp value_json(%Date{} = value), do: Date.to_iso8601(value)
  defp value_json(%Time{} = value), do: Time.to_iso8601(value)
  defp value_json(%Decimal{} = value), do: Decimal.to_string(value)
  defp value_json(value) when is_struct(value), do: inspect(value)
  defp value_json(value), do: value
end
