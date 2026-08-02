defmodule SmolqueryApi.MetricsController do
  @moduledoc """
  Prometheus text for operators — `SmolqueryApi.Auth` admits only the
  internal secret here, never the tenant API key.
  """

  use SmolqueryApi, :controller

  def show(conn, _params) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, Smolquery.Telemetry.render())
  end
end
