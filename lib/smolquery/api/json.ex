defmodule Smolquery.Api.Json do
  @moduledoc """
  The one way a route sends a JSON success body.
  """

  import Plug.Conn

  @doc """
  Sends `body` as JSON with `status`.
  """
  @spec send_json(Plug.Conn.t(), pos_integer(), term()) :: Plug.Conn.t()
  def send_json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(body))
  end
end
