defmodule SmolqueryWeb.DocsController do
  @moduledoc """
  Serves the API description (`SmolqueryApi.Docs.spec/0`) on the UI's own
  host at `GET /v1/docs.json`, so the header link resolves without the UI
  knowing the API listener's public address. Basic auth guards it like every
  other UI route; the API listener serves the same document unauthenticated
  for agents.
  """

  use SmolqueryWeb, :controller

  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, _params), do: json(conn, SmolqueryApi.Docs.spec())
end
