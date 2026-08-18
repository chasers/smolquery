defmodule SmolqueryApi.DocsController do
  @moduledoc """
  Serves `SmolqueryApi.Docs.spec/0` as JSON at `GET /v1/docs.json`.
  """

  use SmolqueryApi, :controller

  alias SmolqueryApi.Docs
  alias SmolqueryApi.Json

  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, _params), do: Json.send_json(conn, 200, Docs.spec())
end
