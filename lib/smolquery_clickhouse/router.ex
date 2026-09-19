defmodule SmolqueryClickHouse.Router do
  @moduledoc """
  The ClickHouse HTTP edge's routes, a plug over one instance name (T-477).

      GET  /                                     Ok., no password
      GET  /ping                                 Ok., no password
      POST /?query=INSERT ... FORMAT RowBinary   SmolqueryClickHouse.Insert

  The insert's order is the API's: the password (`SmolqueryClickHouse.Auth`),
  then ingest admission (`SmolqueryApi.Admission`), then the body. A request
  without the password is refused before admission counts it and before a
  byte of its body is read, and it is refused on every path, so a 404 never
  tells a stranger which paths exist. The health checks are the exception,
  as ClickHouse's are and as `/healthz` is on the API.

  Every URL parameter holds one string. `database[x]=1` or `query[]=...`
  parses to a map or a list, which nothing downstream takes, so it is a 400
  `BAD_ARGUMENTS` here rather than a crash there.

  A query in a `GET`, ClickHouse's read path, is a 501: this edge only
  writes. No published runtime for the instance means the edge is not up
  here, and the answer is the same refusal as a wrong password.

  Every request emits `[:smolquery, :clickhouse, :start | :stop]` through
  `Plug.Telemetry`, which `Smolquery.Telemetry` counts into
  `smolquery_clickhouse_requests_total`.
  """

  @behaviour Plug

  import Plug.Conn

  alias SmolqueryApi.Admission
  alias SmolqueryClickHouse.Auth
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Insert
  alias SmolqueryClickHouse.Runtime

  @telemetry Plug.Telemetry.init(event_prefix: [:smolquery, :clickhouse])

  @unauthenticated {401, 516, "AUTHENTICATION_FAILED",
                    "Authentication failed: password is incorrect, or there is no user with such name",
                    nil}

  @admission_full {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES",
                   "too many insert bytes in flight, retry later", 1}

  @repeated_parameter {400, 36, "BAD_ARGUMENTS",
                       "a URL parameter takes one value, not a list or a map", nil}

  @read_only {501, 48, "NOT_IMPLEMENTED",
              "this endpoint runs INSERT ... FORMAT statements, sent with POST", nil}

  @impl Plug
  def init(name) when is_atom(name), do: name

  @impl Plug
  def call(conn, name) do
    conn
    |> Plug.Telemetry.call(@telemetry)
    |> fetch_query_params()
    |> route(name)
  end

  defp route(%Plug.Conn{method: method, path_info: ["ping"]} = conn, _name)
       when method in ["GET", "HEAD"],
       do: ok(conn)

  defp route(%Plug.Conn{method: method, path_info: [], query_params: params} = conn, _name)
       when method in ["GET", "HEAD"] and not is_map_key(params, "query"),
       do: ok(conn)

  defp route(conn, name) do
    with {:ok, runtime} <- Runtime.fetch(name),
         true <- Auth.authenticated?(conn, runtime.password),
         :ok <- single_valued(conn.query_params) do
      authorized(conn, runtime)
    else
      :repeated -> Errors.send_exception(conn, @repeated_parameter)
      _refused -> Errors.send_exception(conn, @unauthenticated)
    end
  end

  defp single_valued(params) do
    if Enum.all?(params, fn {_name, value} -> is_binary(value) end),
      do: :ok,
      else: :repeated
  end

  defp authorized(%Plug.Conn{method: "POST", path_info: []} = conn, runtime) do
    case Admission.admit_body(conn, runtime.name, runtime.max_ndjson_bytes) do
      {:ok, conn} -> Insert.call(conn, runtime)
      {:error, :admission_full} -> Errors.send_exception(conn, @admission_full)
    end
  end

  defp authorized(%Plug.Conn{path_info: []} = conn, _runtime),
    do: Errors.send_exception(conn, @read_only)

  defp authorized(conn, _runtime) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(
      404,
      "There is no handle #{conn.request_path}\n\nUse / or /ping for health checks.\n"
    )
  end

  defp ok(conn) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "Ok.\n")
  end
end
