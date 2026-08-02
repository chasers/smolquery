defmodule SmolqueryApi.NoRouteController do
  @moduledoc """
  The catch-all — a real route inside the authed pipeline, not Phoenix's
  `NoRouteError`, so 404 only ever answers a caller who already proved a key
  and route existence never leaks to anyone else.
  """

  use SmolqueryApi, :controller

  alias SmolqueryApi.Errors

  def not_found(conn, _params) do
    Errors.send_error(conn, 404, "NOT_FOUND", "no such route")
  end
end
