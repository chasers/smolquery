defmodule Smolquery.Api.Auth do
  @moduledoc """
  Bearer-key authentication for every `/v1` route (PL-8 D5).

  One static key, compared in constant time. `/healthz` is the only exemption —
  a load balancer probing liveness holds no credentials. The plug sits between
  `:match` and `:dispatch`, so an unauthenticated request learns nothing about
  which routes exist: it is a 401 whether the path matches or not.

  The instance name arrives in `conn.private.smolquery_api`, placed there by
  `Smolquery.Api.Router` before the pipeline runs. No published runtime for
  that name means the API is not actually up here, and the answer is the same
  401 — never an open door.
  """

  @behaviour Plug

  import Plug.Conn

  alias Smolquery.Api.Errors
  alias Smolquery.Api.Runtime

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(%Plug.Conn{path_info: ["healthz"]} = conn, _opts), do: conn

  def call(conn, _opts) do
    if authenticated?(conn) do
      conn
    else
      conn
      |> Errors.send_error(401, "UNAUTHENTICATED", "missing or invalid API key")
      |> halt()
    end
  end

  defp authenticated?(conn) do
    with ["Bearer " <> key] <- get_req_header(conn, "authorization"),
         {:ok, runtime} <- Runtime.fetch(conn.private.smolquery_api) do
      Plug.Crypto.secure_compare(key, runtime.api_key)
    else
      _unauthenticated -> false
    end
  end
end
