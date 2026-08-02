defmodule SmolqueryApi.HealthController do
  @moduledoc """
  Liveness, no auth — a load balancer probing `/healthz` holds no credentials.
  """

  use SmolqueryApi, :controller

  alias SmolqueryApi.Json

  def show(conn, _params) do
    Json.send_json(conn, 200, %{"status" => "ok"})
  end
end
