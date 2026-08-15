defmodule SmolqueryWeb.Auth do
  @moduledoc """
  Authentication for the UI and its LiveView socket.

  Static mode retains Basic authentication and its marker rotation semantics.
  OIDC mode reconstructs a minimal encrypted session identity on every request
  and mount. Browser entry and every socket require only `:web_access`; route
  and operation-specific capabilities are enforced by `SmolqueryWeb.Authorization`.
  """

  @behaviour Plug

  import Plug.Conn

  alias Phoenix.LiveView
  alias Smolquery.Auth
  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Policy
  alias SmolqueryWeb.{Authorization, Runtime, Session}

  @realm "smolquery"
  @marker :authenticated

  @impl Plug
  def init(opts), do: Keyword.get(opts, :name, SmolqueryWeb)

  @impl Plug
  def call(conn, name) do
    case Runtime.fetch(name) do
      {:ok, %{auth_mode: :static} = runtime} -> static(conn, runtime)
      {:ok, %{auth_mode: :oidc} = runtime} -> oidc(conn, runtime)
      :error -> challenge(conn)
    end
  end

  defp static(conn, runtime) do
    conn
    |> Plug.BasicAuth.basic_auth(
      username: runtime.username,
      password: runtime.password,
      realm: @realm
    )
    |> mark_static(runtime)
  end

  defp mark_static(%Plug.Conn{halted: true} = conn, _runtime), do: conn

  defp mark_static(conn, runtime) do
    conn =
      if get_session(conn, @marker) == runtime.session_marker,
        do: conn,
        else: put_session(conn, @marker, runtime.session_marker)

    Auth.assign_context(conn, runtime.context)
  end

  defp oidc(conn, runtime) do
    case Session.decode(get_session(conn, Session.key())) do
      {:ok, context} ->
        with true <- context.principal.issuer == runtime.oidc.issuer,
             :ok <- Policy.authorize(context, :web_access),
             {:ok, identity} <- Session.encode(context) do
          conn
          |> configure_session(renew: true)
          |> put_session(Session.key(), identity)
          |> Auth.assign_context(context)
        else
          _failure -> redirect_login(conn)
        end

      :error ->
        redirect_login(conn)
    end
  end

  defp redirect_login(conn) do
    conn |> put_resp_header("location", "/auth/login") |> send_resp(302, "") |> halt()
  end

  defp challenge(conn) do
    conn |> Plug.BasicAuth.request_basic_auth(realm: @realm) |> halt()
  end

  @doc "Requires a valid static marker or reconstructed OIDC session."
  @spec on_mount(:require_authenticated, LiveView.unsigned_params(), map(), LiveView.Socket.t()) ::
          {:cont | :halt, LiveView.Socket.t()}
  def on_mount(:require_authenticated, _params, session, socket) do
    case Runtime.fetch(SmolqueryWeb) do
      {:ok, %{auth_mode: :static} = runtime} -> mount_static(runtime, session, socket)
      {:ok, %{auth_mode: :oidc} = runtime} -> mount_oidc(runtime, session, socket)
      :error -> {:halt, LiveView.redirect(socket, to: "/")}
    end
  end

  defp mount_static(runtime, session, socket) do
    with marker when is_binary(marker) <- session[Atom.to_string(@marker)],
         true <- marker == runtime.session_marker,
         %Context{} = context <- runtime.context do
      socket = socket |> Auth.assign_context(context) |> Authorization.attach_static(marker)
      {:cont, socket}
    else
      _ -> {:halt, LiveView.redirect(socket, to: "/")}
    end
  end

  defp mount_oidc(runtime, session, socket) do
    with {:ok, context} <- Session.decode(session[Session.key()]),
         true <- context.principal.issuer == runtime.oidc.issuer,
         :ok <- Policy.authorize(context, :web_access) do
      socket = Auth.assign_context(socket, context)
      {:cont, Authorization.attach(socket, :web_access)}
    else
      _ -> {:halt, LiveView.redirect(socket, to: "/auth/login")}
    end
  end

  defp put_identity(conn, context) do
    case Session.encode(context) do
      {:ok, identity} ->
        conn =
          conn
          |> configure_session(renew: true)
          |> put_session(Session.key(), identity)
          |> Auth.assign_context(context)

        {:ok, conn}

      :error ->
        :error
    end
  end

  @doc false
  def assign_identity(conn, context), do: put_identity(conn, context)
end
