defmodule SmolqueryClickHouse.Router do
  @moduledoc """
  The ClickHouse HTTP edge's routes, a plug over one instance name (T-477).

      GET  /                                     Ok., no password
      GET  /ping                                 Ok., no password
      POST /?query=INSERT ... FORMAT RowBinary   SmolqueryClickHouse.Insert
      POST /  with a statement                   SmolqueryClickHouse.Query
      GET  /?query=SELECT ...                    SmolqueryClickHouse.Query, read-only

  A `POST` whose `query` parameter holds an `INSERT` is an insert, and its
  body is the rows. Any other `POST` is a query (T-478): the statement is the
  `query` parameter followed by the body, as ClickHouse joins them, and it
  is read up to #{262_144} bytes, ClickHouse's `max_query_size`. An `INSERT`
  in the body alone is refused, since its rows would have to follow it
  there.

  The insert's order is the API's: the password (`SmolqueryClickHouse.Auth`),
  then ingest admission (`SmolqueryApi.Admission`), then the body. A query
  skips admission: its body is bounded, and a result is bounded by the query
  service. A request without the password is refused before either, and it
  is refused on every path, so a 404 never tells a stranger which paths
  exist. The health checks are the exception, as ClickHouse's are and as
  `/healthz` is on the API.

  Every URL parameter holds one string. `database[x]=1` or `query[]=...`
  parses to a map or a list, which nothing downstream takes, so it is a 400
  `BAD_ARGUMENTS` here rather than a crash there.

  No published runtime for the instance means the edge is not up here, and
  the answer is the same refusal as a wrong password.

  Every request emits `[:smolquery, :clickhouse, :start | :stop]` through
  `Plug.Telemetry`, which `Smolquery.Telemetry` counts into
  `smolquery_clickhouse_requests_total`.
  """

  @behaviour Plug

  import Plug.Conn

  alias SmolqueryApi.Admission
  alias SmolqueryApi.Body
  alias SmolqueryClickHouse.Auth
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Insert
  alias SmolqueryClickHouse.Query
  alias SmolqueryClickHouse.Runtime
  alias SmolqueryClickHouse.Statement

  @telemetry Plug.Telemetry.init(event_prefix: [:smolquery, :clickhouse])

  @max_query_bytes 262_144

  @unauthenticated {401, 516, "AUTHENTICATION_FAILED",
                    "Authentication failed: password is incorrect, or there is no user with such name",
                    nil}

  @admission_full {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES",
                   "too many insert bytes in flight, retry later", 1}

  @repeated_parameter {400, 36, "BAD_ARGUMENTS",
                       "a URL parameter takes one value, not a list or a map", nil}

  @query_too_large {400, 62, "SYNTAX_ERROR",
                    "Max query size exceeded: the statement is over #{@max_query_bytes} bytes",
                    nil}

  @body_insert {501, 48, "NOT_IMPLEMENTED",
                "an INSERT is sent in the query parameter, with its rows in the body", nil}

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
    if Statement.insert?(conn.query_params["query"] || ""),
      do: insert(conn, runtime),
      else: post_query(conn, runtime)
  end

  defp authorized(%Plug.Conn{path_info: [], query_params: %{"query" => sql}} = conn, runtime),
    do: Query.call(conn, runtime, sql, read_only: true)

  defp authorized(conn, _runtime) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(
      404,
      "There is no handle #{conn.request_path}\n\nUse / or /ping for health checks.\n"
    )
  end

  defp insert(conn, runtime) do
    case Admission.admit_body(conn, runtime.name, runtime.max_ndjson_bytes) do
      {:ok, conn} -> Insert.call(conn, runtime)
      {:error, :admission_full} -> Errors.send_exception(conn, @admission_full)
    end
  end

  defp post_query(conn, runtime) do
    case Body.read(conn, @max_query_bytes) do
      {:ok, body, conn} ->
        sql = join(conn.query_params["query"], body)

        if Statement.insert?(sql),
          do: Errors.send_exception(conn, @body_insert),
          else: Query.call(conn, runtime, sql)

      {:error, :too_large} ->
        Errors.send_exception(conn, @query_too_large)
    end
  end

  defp join(nil, body), do: body
  defp join(query, ""), do: query
  defp join(query, body), do: query <> "\n" <> body

  defp ok(conn) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "Ok.\n")
  end
end
