defmodule SmolqueryApi.Endpoint do
  @moduledoc """
  The API's HTTP listener — Phoenix, like `SmolqueryWeb.Endpoint`, so both
  surfaces serve web requests the same way (PL-16).

  Deliberately bare next to the web endpoint: no static files, no session, no
  LiveView socket, and — load-bearing — no `Plug.Parsers`. Parsing lives in
  `SmolqueryApi.Router`'s pipeline *after* `SmolqueryApi.Auth`, so an
  unauthenticated body is never read; an endpoint-level parser would invert
  that order.

  `Plug.Telemetry` keeps the `[:smolquery, :api, :start | :stop]` events the
  Plug.Router emitted — `Smolquery.Telemetry` counts them into
  `smolquery_api_requests_total` for `/metrics`.
  """

  use Phoenix.Endpoint, otp_app: :smolquery

  plug Plug.Telemetry, event_prefix: [:smolquery, :api]
  plug SmolqueryApi.Router

  @doc """
  The base URL a running listener answers on.

  Reads the port Bandit actually bound, which is how tests run against
  `port: 0`.
  """
  @spec base_url() :: String.t()
  def base_url do
    {:ok, {_address, port}} = Bandit.PhoenixAdapter.server_info(__MODULE__, :http)

    "http://127.0.0.1:#{port}"
  end
end
