defmodule SmolqueryClickHouse.Auth do
  @moduledoc """
  Checks the password a ClickHouse HTTP client presents (T-477).

  ClickHouse takes a password three ways, and so does this edge: the
  `X-ClickHouse-Key` header, HTTP basic auth, or the `password` query
  parameter. A `Bearer` token is taken as well, so a client of the API's
  insert keeps its header. The first form present wins, in that order, and
  the comparison is constant-time.

  The user name (`X-ClickHouse-User`, basic auth's user, or `user`) is
  accepted as given, as the Postgres wire edge accepts its user. A request
  with no password is refused like a wrong one: there is no passwordless
  `default` user here.

  Reads `conn.query_params`, so the caller fetches them first.
  """

  import Plug.Conn, only: [get_req_header: 2]

  @doc """
  Whether `conn` presents `password`.
  """
  @spec authenticated?(Plug.Conn.t(), String.t()) :: boolean()
  def authenticated?(conn, password),
    do: conn |> presented() |> SmolqueryApi.Auth.matches?(password)

  defp presented(conn) do
    with :error <- key_header(conn),
         :error <- basic(conn),
         :error <- parameter(conn) do
      bearer(conn)
    end
  end

  defp key_header(conn) do
    case get_req_header(conn, "x-clickhouse-key") do
      [key | _rest] -> {:ok, key}
      [] -> :error
    end
  end

  defp basic(conn) do
    case Plug.BasicAuth.parse_basic_auth(conn) do
      {_user, password} -> {:ok, password}
      :error -> :error
    end
  end

  defp parameter(%Plug.Conn{query_params: %{"password" => password}}) when is_binary(password),
    do: {:ok, password}

  defp parameter(_conn), do: :error

  defp bearer(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token | _rest] -> {:ok, token}
      _other -> :error
    end
  end
end
