defmodule SmolqueryWeb.AuthTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Phoenix.LiveView
  alias Smolquery.Auth, as: AuthContext
  alias Smolquery.Test.MapCatalog
  alias SmolqueryWeb.Auth
  alias SmolqueryWeb.Runtime

  defp unauthenticated, do: Phoenix.ConnTest.build_conn()

  defp credential do
    config = Application.get_env(:smolquery, SmolqueryWeb, [])
    {Keyword.fetch!(config, :username), Keyword.fetch!(config, :password)}
  end

  defp basic_auth(conn, username, password) do
    put_req_header(conn, "authorization", Plug.BasicAuth.encode_basic_auth(username, password))
  end

  describe "the HTTP request" do
    setup do
      {:ok, runtime: start_web!()}
    end

    test "a request with no credential is challenged, not served" do
      conn = get(unauthenticated(), ~p"/")

      assert conn.status == 401
      assert get_resp_header(conn, "www-authenticate") == [~s(Basic realm="smolquery")]
      refute conn.resp_body =~ "datasets"
    end

    test "a wrong password is challenged" do
      {username, _password} = credential()

      conn = get(basic_auth(unauthenticated(), username, "wrong"), ~p"/")

      assert conn.status == 401
    end

    test "a wrong username is challenged" do
      {_username, password} = credential()

      conn = get(basic_auth(unauthenticated(), "wrong", password), ~p"/")

      assert conn.status == 401
    end

    test "the correct credential is served with a normalized operator context", %{conn: conn} do
      conn = get(conn, ~p"/")

      assert conn.status == 200
      assert {:ok, context} = AuthContext.fetch_context(conn)
      assert context.principal.authn == :basic
      assert context.principal.kind == :user

      assert MapSet.equal?(
               context.capabilities,
               MapSet.new([:web_access, :query, :catalog_manage, :platform_operate])
             )

      {username, password} = credential()
      refute inspect(context) =~ username
      refute inspect(context) =~ password
    end

    test "every route requires the credential" do
      for path <- [~p"/", ~p"/tables", ~p"/query", ~p"/cluster"] do
        assert get(unauthenticated(), path).status == 401, "#{path} answered without a credential"
      end
    end

    test "a served request writes the marker the socket requires", %{
      conn: conn,
      runtime: runtime
    } do
      conn = get(conn, ~p"/")

      assert get_session(conn, :authenticated) == runtime.session_marker
    end

    test "a request whose marker is current does not rewrite the session", %{conn: conn} do
      first = get(conn, ~p"/")
      assert get_resp_header(first, "set-cookie") != []

      second = first |> recycle() |> authenticate() |> get(~p"/")

      assert get_resp_header(second, "set-cookie") == []
    end
  end

  describe "when the web role is not published here" do
    test "the request is challenged, not served", %{conn: conn} do
      start_web!()
      Runtime.delete(SmolqueryWeb)

      assert get(conn, ~p"/").status == 401
    end
  end

  describe "the LiveView socket" do
    test "mounts when the request was authenticated", %{conn: conn} do
      start_web!()

      assert {:ok, _lv, _html} = live(conn, ~p"/tables")
    end

    test "the hook redirects a mount whose session carries no marker" do
      start_web!()

      assert {:halt, socket} =
               Auth.on_mount(:require_authenticated, %{}, %{}, %LiveView.Socket{})

      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "the hook redirects a mount whose marker is stale" do
      runtime = start_web!()

      assert {:halt, socket} =
               Auth.on_mount(
                 :require_authenticated,
                 %{},
                 %{"authenticated" => "stale-" <> runtime.session_marker},
                 %LiveView.Socket{}
               )

      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "the hook redirects when no runtime is published" do
      assert {:halt, socket} =
               Auth.on_mount(
                 :require_authenticated,
                 %{},
                 %{"authenticated" => "any-marker"},
                 %LiveView.Socket{}
               )

      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "the hook redirects cleanly while OIDC browser login is not implemented" do
      runtime =
        Runtime.new(
          catalog: MapCatalog.new(),
          auth_mode: :oidc,
          secret_key_base: String.duplicate("s", 64),
          web_host: "ui.example",
          oidc: [
            issuer: "https://issuer.example/",
            web_client_id: "web",
            web_client_auth_method: :none,
            web_origin: "https://ui.example",
            web_redirect_uri: "https://ui.example/auth/callback"
          ]
        )

      Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(SmolqueryWeb) end)

      assert {:halt, socket} = Auth.on_mount(:require_authenticated, %{}, %{}, %LiveView.Socket{})
      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "a rotated password revokes the marker old sessions carry and preserves identity" do
      runtime = start_web!()
      first_id = runtime.context.principal.id

      Runtime.put(Runtime.new(catalog: MapCatalog.new(), password: "rotated-password"))
      {:ok, rotated} = Runtime.fetch(SmolqueryWeb)
      assert rotated.context.principal.id == first_id

      assert {:halt, socket} =
               Auth.on_mount(
                 :require_authenticated,
                 %{},
                 %{"authenticated" => runtime.session_marker},
                 %LiveView.Socket{}
               )

      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "a rotated username revokes the marker while preserving identity" do
      runtime = start_web!()
      rotated = Runtime.new(catalog: MapCatalog.new(), username: "rotated-user")

      assert rotated.context.principal.id == runtime.context.principal.id
      refute rotated.session_marker == runtime.session_marker
      Runtime.put(rotated)

      assert {:halt, socket} =
               Auth.on_mount(
                 :require_authenticated,
                 %{},
                 %{"authenticated" => runtime.session_marker},
                 %LiveView.Socket{}
               )

      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "the hook admits a mount whose session carries the marker and assigns context" do
      runtime = start_web!()

      assert {:cont, socket} =
               Auth.on_mount(
                 :require_authenticated,
                 %{},
                 %{"authenticated" => runtime.session_marker},
                 %LiveView.Socket{}
               )

      assert {:ok, context} = AuthContext.fetch_context(socket)
      assert context.principal.id == runtime.context.principal.id
      refute socket.redirected
    end
  end
end
