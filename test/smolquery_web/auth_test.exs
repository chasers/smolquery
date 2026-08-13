defmodule SmolqueryWeb.AuthTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Phoenix.LiveView
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
      start_web!()
      :ok
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

    test "the correct credential is served", %{conn: conn} do
      conn = get(conn, ~p"/")

      assert conn.status == 200
    end

    test "every route is behind the fence" do
      for path <- [~p"/", ~p"/tables", ~p"/query", ~p"/cluster"] do
        assert get(unauthenticated(), path).status == 401, "#{path} answered without a credential"
      end
    end

    test "a served request writes the marker the socket requires", %{conn: conn} do
      conn = get(conn, ~p"/")

      assert get_session(conn, :authenticated) == true
    end
  end

  describe "when the web role is not published here" do
    test "the answer is a challenge, never an open door", %{conn: conn} do
      start_web!()
      Runtime.delete(SmolqueryWeb)

      assert get(conn, ~p"/").status == 401
    end
  end

  describe "the LiveView socket" do
    test "mounts through the fence when the request was authenticated", %{conn: conn} do
      start_web!()

      assert {:ok, _lv, _html} = live(conn, ~p"/tables")
    end

    test "the hook redirects a mount whose session carries no marker" do
      assert {:halt, socket} =
               Auth.on_mount(:require_authenticated, %{}, %{}, %LiveView.Socket{})

      assert {:redirect, %{to: "/"}} = socket.redirected
    end

    test "the hook admits a mount whose session carries the marker" do
      assert {:cont, socket} =
               Auth.on_mount(
                 :require_authenticated,
                 %{},
                 %{"authenticated" => true},
                 %LiveView.Socket{}
               )

      refute socket.redirected
    end
  end
end
