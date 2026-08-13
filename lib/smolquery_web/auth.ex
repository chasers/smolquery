defmodule SmolqueryWeb.Auth do
  @moduledoc """
  Basic authentication for every UI route, and for the socket behind them.

  The peer of `SmolqueryApi.Auth`: one static credential, compared in constant
  time, resolved from the published runtime rather than from compile-time
  options. A node holding the `:web` role with no credential refuses to boot
  (`SmolqueryWeb.Runtime`), so there is no configuration that serves an open
  UI.

  The UI needs this more than the API does, not less. `/query` runs SQL through
  `Smolquery.QueryService`, and `/cluster` kills, drains, and restarts nodes.

  ## Two layers, because there are two doors

  `call/2` guards the HTTP request from the `:browser` pipeline. It runs after
  `:fetch_session`, because a success writes the session marker the second
  layer reads.

  `on_mount/4` guards the LiveView socket. The endpoint's `socket "/live"`
  declaration never runs the router's pipelines, so the plug alone does not
  cover the upgrade. A browser usually resends cached credentials on a
  same-origin upgrade, but "usually" is not a fence — the hook requires the
  marker instead, which only an authenticated request could have written.

  Static assets sit ahead of the router in the endpoint and stay public.
  `priv/static` holds no data.
  """

  @behaviour Plug

  import Plug.Conn

  alias Phoenix.LiveView
  alias SmolqueryWeb.Runtime

  @realm "smolquery"
  @marker :authenticated

  @impl Plug
  def init(opts), do: Keyword.get(opts, :name, SmolqueryWeb)

  @impl Plug
  def call(conn, name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> authenticate(conn, runtime)
      :error -> challenge(conn)
    end
  end

  defp authenticate(conn, runtime) do
    conn
    |> Plug.BasicAuth.basic_auth(
      username: runtime.username,
      password: runtime.password,
      realm: @realm
    )
    |> mark()
  end

  defp mark(%Plug.Conn{halted: true} = conn), do: conn
  defp mark(conn), do: put_session(conn, @marker, true)

  defp challenge(conn) do
    conn
    |> Plug.BasicAuth.request_basic_auth(realm: @realm)
    |> halt()
  end

  @doc """
  Requires the marker `call/2` writes before a LiveView mounts.

  An absent marker sends the caller back to `/`, where the plug asks for the
  credential again.
  """
  @spec on_mount(:require_authenticated, LiveView.unsigned_params(), map(), LiveView.Socket.t()) ::
          {:cont | :halt, LiveView.Socket.t()}
  def on_mount(:require_authenticated, _params, session, socket) do
    if session[Atom.to_string(@marker)] do
      {:cont, socket}
    else
      {:halt, LiveView.redirect(socket, to: "/")}
    end
  end
end
