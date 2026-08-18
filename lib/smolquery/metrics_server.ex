defmodule Smolquery.MetricsServer do
  @moduledoc """
  Serves `GET /metrics` on every node, whatever its roles (T-302).

  `Smolquery.Telemetry` counts events on every node, but only the `:api`
  role's endpoint served them — a buffer-only or storage-only node held
  counters nothing could scrape. This listener is its own Bandit child in
  `Smolquery.Application`, outside every role subtree, so a scraper reaches
  each node the same way. The API endpoint keeps its `/metrics` route; this
  listener is additive.

  The one route answers to the internal secret (`Smolquery.InternalSecret`),
  like the API's `/metrics` and the hot tier: metrics are for operators, not
  tenants. An unauthenticated request is a 401 before any routing happens.

  The bind comes from `config :smolquery, Smolquery.MetricsServer` (`ip` and
  `port`, default 4003) — `SMOLQUERY_METRICS_IP` / `SMOLQUERY_METRICS_PORT`
  in a release.
  """

  @behaviour Plug

  import Plug.Conn

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_opts) do
    config = Application.get_env(:smolquery, __MODULE__, [])

    Supervisor.child_spec(
      {Bandit,
       plug: __MODULE__,
       ip: Keyword.get(config, :ip, {127, 0, 0, 1}),
       port: Keyword.get(config, :port, 4003),
       startup_log: false},
      id: __MODULE__
    )
  end

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    if Smolquery.InternalSecret.proven?(conn) do
      route(conn)
    else
      send_resp(conn, 401, "missing or invalid internal secret")
    end
  end

  @doc """
  The base URL this node's listener answers on.

  Not needed to serve scrapes — only for a test discovering the port Bandit
  bound when the configured port is `0`.
  """
  @spec base_url() :: String.t()
  def base_url do
    listener =
      Smolquery.Supervisor
      |> Supervisor.which_children()
      |> Enum.find_value(fn
        {__MODULE__, pid, _type, _modules} -> pid
        _child -> nil
      end)

    {:ok, {_address, port}} = ThousandIsland.listener_info(listener)

    "http://127.0.0.1:#{port}"
  end

  defp route(%Plug.Conn{method: "GET", path_info: ["metrics"]} = conn) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, Smolquery.Telemetry.render())
  end

  defp route(conn), do: send_resp(conn, 404, "not found")
end
