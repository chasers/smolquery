defmodule SmolqueryWeb.Auth do
  @moduledoc """
  Basic authentication for every UI route and for the LiveView socket.

  This module is the web peer of `SmolqueryApi.Auth`. It holds one static
  credential. It compares the credential in constant time. It reads the
  credential from the published runtime, not from compile-time options. A node
  with the `:web` role and no credential refuses to boot
  (`SmolqueryWeb.Runtime`), so no configuration serves an open UI.

  The UI needs authentication at least as much as the API. `/query` runs SQL
  through `Smolquery.QueryService`. `/cluster` kills, drains, and restarts
  nodes.

  ## Two layers

  `call/2` guards the HTTP request from the `:browser` pipeline. It runs after
  `:fetch_session`, because a success writes the session marker that the
  second layer reads.

  `on_mount/4` guards the LiveView socket. The endpoint's `socket "/live"`
  declaration does not run the router's pipelines, so the plug does not see
  the websocket upgrade. A browser usually resends cached credentials on a
  same-origin upgrade, but the hook does not rely on that. The hook requires
  the session marker. Only an authenticated request can write the marker. The
  marker value is bound to the live credential and the session secret
  (`SmolqueryWeb.Runtime`), so a credential rotation revokes old sessions.

  The endpoint serves static assets before the router runs, so they stay
  public. `priv/static` holds no data.
  """

  @behaviour Plug

  import Plug.Conn

  alias Phoenix.LiveView
  alias Smolquery.Auth
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
    |> mark(runtime)
  end

  defp mark(%Plug.Conn{halted: true} = conn, _runtime), do: conn

  defp mark(conn, runtime) do
    conn =
      if get_session(conn, @marker) == runtime.session_marker do
        conn
      else
        put_session(conn, @marker, runtime.session_marker)
      end

    Auth.assign_context(conn, runtime.context)
  end

  defp challenge(conn) do
    conn
    |> Plug.BasicAuth.request_basic_auth(realm: @realm)
    |> halt()
  end

  @doc """
  Requires the session marker that `call/2` writes before a LiveView mounts.

  The marker is bound to the live credential, so a rotated credential revokes
  every session written under the old one. If the marker is absent or stale,
  the hook redirects the caller to `/`. There the plug asks for the credential
  again.
  """
  @spec on_mount(:require_authenticated, LiveView.unsigned_params(), map(), LiveView.Socket.t()) ::
          {:cont | :halt, LiveView.Socket.t()}
  def on_mount(:require_authenticated, _params, session, socket) do
    with {:ok, runtime} <- Runtime.fetch(SmolqueryWeb),
         marker when marker == runtime.session_marker <- session[Atom.to_string(@marker)] do
      {:cont, Auth.assign_context(socket, runtime.context)}
    else
      _unauthenticated -> {:halt, LiveView.redirect(socket, to: "/")}
    end
  end
end
