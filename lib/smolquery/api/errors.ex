defmodule Smolquery.Api.Errors do
  @moduledoc """
  The one JSON error envelope every route answers failures with (PL-8 D11).

      {"error": {"code": 404, "status": "NOT_FOUND", "message": "no such route"}}

  `code` repeats the HTTP status so a client parsing only the body still knows;
  `status` is a stable machine-readable symbol; `message` is for humans and
  never carries internals.
  """

  import Plug.Conn

  @doc """
  Sends the envelope and returns the conn.
  """
  @spec send_error(Plug.Conn.t(), pos_integer(), String.t(), String.t()) :: Plug.Conn.t()
  def send_error(conn, code, status, message) do
    body = JSON.encode!(%{"error" => %{"code" => code, "status" => status, "message" => message}})

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(code, body)
  end
end
