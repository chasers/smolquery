defmodule SmolqueryVictoriaMetrics.Errors do
  @moduledoc """
  Answers a failure the way the Prometheus HTTP API does (PL-70, T-562).

      {"status":"error","errorType":"bad_data","error":"..."}

  One form for every refusal on this edge, from authentication to a refused
  write, with `retry-after` when resending later can succeed. vmagent reads
  only the status: it retries a 429 or a 5xx and keeps the block, and drops
  the block on any other 4xx. Grafana shows `error`.
  """

  import Plug.Conn

  @typedoc """
  HTTP status, the Prometheus `errorType`, the message, and the `retry-after`
  seconds or `nil`.
  """
  @type t :: {Plug.Conn.int_status(), String.t(), String.t(), pos_integer() | nil}

  @doc """
  Sends `error` as a Prometheus API error body.
  """
  @spec send_error(Plug.Conn.t(), t()) :: Plug.Conn.t()
  def send_error(conn, {status, type, message, retry_after}) do
    body = JSON.encode!(%{"status" => "error", "errorType" => type, "error" => message})

    conn
    |> retry_after(retry_after)
    |> put_resp_content_type("application/json")
    |> send_resp(status, body)
  end

  defp retry_after(conn, nil), do: conn

  defp retry_after(conn, seconds),
    do: put_resp_header(conn, "retry-after", Integer.to_string(seconds))
end
