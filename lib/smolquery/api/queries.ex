defmodule Smolquery.Api.Queries do
  @moduledoc """
  The synchronous query route: submit, wait, answer with the first page.

  Sync and async are the same job underneath (`QueryService.Client.query/3`),
  so the response carries the job alongside its rows — the job reference is
  how a caller pages the rest through `GET /v1/jobs/:id/results`. A query
  that outlives `timeoutMs` has been cancelled by the client layer; the 504
  says so and points long queries at `POST /v1/jobs`.

  A query that *finished* badly is the caller's error, not the server's: the
  planner's message comes back in a 400.
  """

  alias Explorer.DataFrame
  alias Smolquery.Api.Errors
  alias Smolquery.Api.Jobs
  alias Smolquery.Api.Json
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job

  @doc """
  Runs the body's query and answers with the finished job and its first page.
  """
  @spec create(Plug.Conn.t()) :: Plug.Conn.t()
  def create(conn) do
    with {:ok, sql} <- Jobs.sql(conn.body_params),
         {:ok, job, frame} <-
           Client.query(Jobs.query_name(conn), sql, Jobs.submit_opts(conn.body_params)) do
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
        Jobs.query_error(conn, reason)
    end
  end

  defp answer(conn, %Job{state: :done} = job, %DataFrame{} = frame) do
    body =
      job
      |> Jobs.page_body(frame, 0, max_results(conn.body_params))
      |> Map.put("job", Jobs.job_json(job, true))

    Json.send_json(conn, 200, body)
  end

  defp answer(conn, %Job{state: :error} = job, _frame) do
    Errors.send_error(conn, 400, "INVALID_QUERY", error_message(job))
  end

  defp answer(conn, %Job{} = job, _frame) do
    Errors.send_error(conn, 409, "FAILED_PRECONDITION", "job finished #{job.state}")
  end

  defp error_message(%Job{error: {:invalid_query, message}}) when is_binary(message), do: message
  defp error_message(%Job{error: error}), do: inspect(error)

  defp max_results(body) do
    case body do
      %{"maxResults" => n} when is_integer(n) and n > 0 -> min(n, 10_000)
      _default -> 1_000
    end
  end
end
