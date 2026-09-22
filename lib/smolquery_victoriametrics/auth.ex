defmodule SmolqueryVictoriaMetrics.Auth do
  @moduledoc """
  Checks the password a remote-write or Prometheus API client presents
  (PL-70, T-562).

  vmagent sends `-remoteWrite.bearerToken` as `Authorization: Bearer <token>`
  and `-remoteWrite.basicAuth.username/password` as HTTP basic auth; Grafana's
  Prometheus datasource sends basic auth. Either form is taken, the token or
  the basic-auth password being the edge's password. The basic-auth user
  name is accepted as given. The comparison is constant-time, and a request
  with no credential is refused like a wrong one.
  """

  import Plug.Conn, only: [get_req_header: 2]

  @doc """
  Whether `conn` presents `password`.
  """
  @spec authenticated?(Plug.Conn.t(), String.t()) :: boolean()
  def authenticated?(conn, password),
    do: conn |> presented() |> SmolqueryApi.Auth.matches?(password)

  defp presented(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token | _rest] -> {:ok, token}
      [_other | _rest] -> basic(conn)
      [] -> :error
    end
  end

  defp basic(conn) do
    case Plug.BasicAuth.parse_basic_auth(conn) do
      {_user, password} -> {:ok, password}
      :error -> :error
    end
  end
end
