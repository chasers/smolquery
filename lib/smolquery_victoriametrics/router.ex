defmodule SmolqueryVictoriaMetrics.Router do
  @moduledoc """
  The VictoriaMetrics edge's routes, a plug over one instance name
  (PL-70, T-562).

      GET  /health, /-/healthy, /-/ready                  OK, no password
      POST /api/v1/write                                  SmolqueryVictoriaMetrics.Write
      POST /prometheus/api/v1/write                       the same
      POST /insert/<account>/prometheus/api/v1/write      the same; the account is ignored
      GET|POST /api/v1/query                              SmolqueryVictoriaMetrics.Query, instant
      GET|POST /api/v1/query_range                        SmolqueryVictoriaMetrics.Query, range
      GET|POST /api/v1/labels                             SmolqueryVictoriaMetrics.Metadata
      GET|POST /api/v1/label/<name>/values                SmolqueryVictoriaMetrics.Metadata
      GET|POST /api/v1/series                             SmolqueryVictoriaMetrics.Metadata
      GET|POST /api/v1/status/buildinfo, /api/v1/metadata,
               /api/v1/rules, /api/v1/alerts,
               /api/v1/notifiers, /api/v1/query_exemplars SmolqueryVictoriaMetrics.Status

  Every read route also answers under `/prometheus` and under
  `/select/<account>/prometheus`, the prefixes Grafana's datasources use for
  a VictoriaMetrics behind a proxy or a cluster's `vmselect`; the account
  is ignored. As in a cluster, `/insert/...` only writes and `/select/...`
  only reads.

  `/api/v1/write` is a single-node VictoriaMetrics' remote-write path, and
  the other two are the prefixes vmagent is pointed at for a VictoriaMetrics
  behind a `/prometheus` proxy or for a cluster's `vminsert`. Tenancy is not
  this edge's, so the account id is accepted and ignored.

  A write's order is the ClickHouse insert's: the password
  (`SmolqueryVictoriaMetrics.Auth`), then ingest admission
  (`SmolqueryApi.Admission`) on the body as sent, compressed, then the body.
  A request without the password is refused before either, and it is
  refused on every path, so a 404 never tells a stranger which paths exist.
  The health checks are the exception, as VictoriaMetrics' are.

  No published runtime for the instance means the edge is not up here, and
  the answer is the same refusal as a wrong password.

  Every request emits `[:smolquery, :victoriametrics, :start | :stop]`
  through `Plug.Telemetry`, which `Smolquery.Telemetry` counts into
  `smolquery_victoriametrics_requests_total` and times by the `kind` this
  router puts in `conn.private`: `write`, `query`, `labels` for the three
  metadata routes that scan the table, `health`, or `other`, which the
  fixed answers of `SmolqueryVictoriaMetrics.Status` are.
  Queries are not held to ingest admission: they read, and the query
  service bounds its own jobs.
  """

  @behaviour Plug

  import Plug.Conn

  alias SmolqueryApi.Admission
  alias SmolqueryVictoriaMetrics.Auth
  alias SmolqueryVictoriaMetrics.Errors
  alias SmolqueryVictoriaMetrics.Metadata
  alias SmolqueryVictoriaMetrics.Query
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Status
  alias SmolqueryVictoriaMetrics.Write

  @telemetry Plug.Telemetry.init(event_prefix: [:smolquery, :victoriametrics])

  @kind :smolquery_victoriametrics_kind

  @health [["health"], ["-", "healthy"], ["-", "ready"]]

  @unauthenticated {401, "unauthorized",
                    "authentication failed: present the password as a Bearer token or with basic auth",
                    nil}

  @admission_full {429, "unavailable", "too many write bytes in flight, retry later", 1}

  @impl Plug
  def init(name) when is_atom(name), do: name

  @impl Plug
  def call(conn, name) do
    conn
    |> Plug.Telemetry.call(@telemetry)
    |> route(name)
  end

  defp route(%Plug.Conn{method: method, path_info: path} = conn, _name)
       when method in ["GET", "HEAD"] and path in @health,
       do: conn |> kind(:health) |> ok()

  defp route(conn, name) do
    with {:ok, runtime} <- Runtime.fetch(name),
         true <- Auth.authenticated?(conn, runtime.password) do
      authorized(conn, runtime)
    else
      _refused -> Errors.send_error(conn, @unauthenticated)
    end
  end

  defp authorized(conn, runtime) do
    case destination(conn.method, conn.path_info) do
      :write -> write(conn, runtime)
      {:query, kind} -> conn |> kind(:query) |> Query.call(runtime, kind)
      {:metadata, route} -> conn |> kind(:labels) |> Metadata.call(runtime, route)
      {:status, route} -> conn |> kind(:other) |> Status.call(route)
      :unknown -> not_found(conn)
    end
  end

  defp destination(method, ["prometheus" | path]), do: endpoint(method, path)

  defp destination(method, ["insert", _account, "prometheus" | path]) do
    case endpoint(method, path) do
      :write -> :write
      _query_or_unknown -> :unknown
    end
  end

  defp destination(method, ["select", _account, "prometheus" | path]) do
    case endpoint(method, path) do
      :write -> :unknown
      read_or_unknown -> read_or_unknown
    end
  end

  defp destination(method, path), do: endpoint(method, path)

  defp endpoint("POST", ["api", "v1", "write"]), do: :write

  defp endpoint(method, ["api", "v1", "query"]) when method in ["GET", "POST"],
    do: {:query, :instant}

  defp endpoint(method, ["api", "v1", "query_range"]) when method in ["GET", "POST"],
    do: {:query, :range}

  defp endpoint(method, ["api", "v1" | path]) when method in ["GET", "POST"], do: read(path)
  defp endpoint(_method, _path), do: :unknown

  defp read(["labels"]), do: {:metadata, :labels}
  defp read(["label", name, "values"]), do: {:metadata, {:label_values, name}}
  defp read(["series"]), do: {:metadata, :series}

  defp read(path) do
    case Status.route(path) do
      nil -> :unknown
      route -> {:status, route}
    end
  end

  defp write(conn, runtime) do
    conn = kind(conn, :write)

    case Admission.admit_body(conn, runtime.name, runtime.max_ndjson_bytes) do
      {:ok, conn} -> Write.call(conn, runtime)
      {:error, :admission_full} -> Errors.send_error(conn, @admission_full)
    end
  end

  defp not_found(conn) do
    Errors.send_error(
      conn,
      {404, "not_found", "unsupported path requested: #{conn.method} #{conn.request_path}", nil}
    )
  end

  defp kind(conn, kind), do: put_private(conn, @kind, kind)

  defp ok(conn) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, "OK")
  end
end
