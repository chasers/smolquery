defmodule SmolqueryVictoriaMetrics.Router do
  @moduledoc """
  The VictoriaMetrics edge's routes, a plug over one instance name
  (PL-70, T-562).

      GET  /health, /-/healthy, /-/ready                  OK, no password
      POST /api/v1/write                                  SmolqueryVictoriaMetrics.Write
      POST /prometheus/api/v1/write                       the same
      POST /insert/<account>/prometheus/api/v1/write      the same; the account is ignored

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
  router puts in `conn.private`: `write`, `health`, or `other`.
  """

  @behaviour Plug

  import Plug.Conn

  alias SmolqueryApi.Admission
  alias SmolqueryVictoriaMetrics.Auth
  alias SmolqueryVictoriaMetrics.Errors
  alias SmolqueryVictoriaMetrics.Runtime
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
    case endpoint(conn.method, api_path(conn.path_info)) do
      :write -> write(conn, runtime)
      :unknown -> not_found(conn)
    end
  end

  defp api_path(["prometheus" | path]), do: path
  defp api_path(["insert", _account, "prometheus" | path]), do: path
  defp api_path(path), do: path

  defp endpoint("POST", ["api", "v1", "write"]), do: :write
  defp endpoint(_method, _path), do: :unknown

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
