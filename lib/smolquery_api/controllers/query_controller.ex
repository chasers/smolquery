defmodule SmolqueryApi.QueryController do
  @moduledoc """
  The synchronous query route: submit, wait, answer with the first page.

  Sync and async are the same job underneath (`QueryService.Client.query/3`),
  so the response carries the job alongside its rows — the job reference is
  how a caller pages the rest through `GET /v1/jobs/:id/results`. A query
  that outlives `timeoutMs` has been cancelled by the client layer; the 504
  says so and points long queries at `POST /v1/jobs`.

  A query that *finished* badly is usually the caller's error, not the server's:
  the planner's message comes back in a 400. Unreachable hot tier is the
  exception, and answers 503 — the SQL was fine, a buffer node holding rows the
  answer needs could not be asked (`Smolquery.QueryService.Planner`'s failure
  honesty, T-94). The distinction is the difference between "fix your query" and
  "retry this", which is not something a client should have to infer from an
  error string.
  """

  use SmolqueryApi, :controller

  alias Explorer.DataFrame
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias SmolqueryApi.Errors
  alias SmolqueryApi.JobController
  alias SmolqueryApi.Json

  @doc """
  Runs the body's query and answers with the finished job and its first page.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, _params) do
    with {:ok, sql} <- JobController.sql(conn.body_params),
         {:ok, opts} <- JobController.submit_opts(conn.body_params),
         {:ok, job, frame} <- Client.query(JobController.query_name(conn), sql, opts) do
      answer(conn, job, frame)
    else
      {:error, :timeout} ->
        Errors.send_error(
          conn,
          504,
          "DEADLINE_EXCEEDED",
          "query cancelled after its timeout; submit long queries via POST /v1/jobs"
        )

      {:error, reason} ->
        JobController.query_error(conn, reason)
    end
  end

  defp answer(conn, %Job{state: :done, explain: explain} = job, nil) when is_binary(explain) do
    Json.send_json(conn, 200, %{"job" => JobController.job_json(job, false)})
  end

  defp answer(conn, %Job{state: :done} = job, %DataFrame{} = frame) do
    body =
      job
      |> JobController.page_body(frame, 0, max_results(conn.body_params))
      |> Map.put("job", JobController.job_json(job, true))

    Json.send_json(conn, 200, body)
  end

  defp answer(conn, %Job{state: :error, error: error} = job, _frame) do
    if hot_tier_unavailable?(error) do
      Errors.send_error(
        conn,
        503,
        "UNAVAILABLE",
        "a buffer node holding unsealed rows for this query could not be reached"
      )
    else
      Errors.send_error(conn, 400, "INVALID_QUERY", error_message(job))
    end
  end

  defp answer(conn, %Job{} = job, _frame) do
    Errors.send_error(conn, 409, "FAILED_PRECONDITION", "job finished #{job.state}")
  end

  defp error_message(%Job{error: {:invalid_query, message}}) when is_binary(message), do: message
  defp error_message(%Job{error: error}), do: inspect(error)

  defp hot_tier_unavailable?({:hot_tier_unavailable, _reason}), do: true
  defp hot_tier_unavailable?({:hot_tier_unavailable, _ref, _reason}), do: true
  defp hot_tier_unavailable?(_other), do: false

  defp max_results(body) do
    case body do
      %{"maxResults" => n} when is_integer(n) and n > 0 -> min(n, 10_000)
      _default -> 1_000
    end
  end
end
