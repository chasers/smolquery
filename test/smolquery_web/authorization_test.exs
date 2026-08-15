defmodule SmolqueryWeb.AuthorizationTest do
  use ExUnit.Case, async: true

  alias Phoenix.LiveView.{Lifecycle, Socket}
  alias Smolquery.Auth
  alias Smolquery.Auth.{Context, Principal, Static}
  alias SmolqueryWeb.Authorization
  alias SmolqueryWeb.ClusterLive.Index, as: ClusterLive
  alias SmolqueryWeb.QueryLive.Index, as: QueryLive
  alias SmolqueryWeb.TableLive.Index, as: TableIndex
  alias SmolqueryWeb.TableLive.Show, as: TableShow

  test "uses only normalized contexts and distinguishes forbidden from unauthenticated" do
    {:ok, principal} = Principal.oidc("https://issuer.example", "subject", :user)

    {:ok, query_context} =
      Context.single_tenant(principal, [:web_access, :query], expires_at: 2_000_000_000)

    {:ok, expired_context} =
      Context.single_tenant(principal, [:web_access, :query], expires_at: 1)

    query_socket = Auth.assign_context(%Socket{}, query_context)
    expired_socket = Auth.assign_context(%Socket{}, expired_context)

    assert Authorization.authorize(query_socket, :query) == :ok
    assert Authorization.authorize(query_socket, :platform_operate) == {:error, :forbidden}
    assert Authorization.authorize(expired_socket, :query) == {:error, :unauthenticated}
    assert Authorization.authorize(%Socket{}, :query) == {:error, :unauthenticated}
  end

  test "mount requirements are closed and generic" do
    {:ok, principal} = Principal.oidc("https://issuer.example", "subject", :user)
    {:ok, context} = Context.single_tenant(principal, [:web_access], expires_at: 2_000_000_000)
    socket = Auth.assign_context(%Socket{}, context)

    assert {:halt, denied} = Authorization.on_mount(:query, %{}, %{}, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:halt, denied} = Authorization.on_mount(:query, %{}, %{}, %Socket{})
    assert {:redirect, %{to: "/auth/login"}} = denied.redirected
  end

  test "direct event guard denies before malformed input can be inspected" do
    {:ok, principal} = Principal.oidc("https://issuer.example", "subject", :user)

    {:ok, context} =
      Context.single_tenant(principal, [:web_access, :query], expires_at: 2_000_000_000)

    socket = Auth.assign_context(%Socket{}, context)

    assert {:error, :forbidden, denied} = Authorization.event(socket, :platform_operate)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:error, :forbidden, _denied} = Authorization.event(socket, :catalog_manage)
  end

  test "direct LiveView handlers deny before malformed params or side effects" do
    {:ok, principal} = Principal.oidc("https://issuer.example", "subject", :user)

    {:ok, context} =
      Context.single_tenant(principal, [:web_access], expires_at: 2_000_000_000)

    socket = Auth.assign_context(%Socket{}, context)

    assert {:noreply, denied} = QueryLive.handle_event("run", :malformed, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:noreply, denied} = TableIndex.handle_event("create_dataset", :malformed, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:noreply, denied} = TableShow.handle_event("save_retention", :malformed, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:noreply, denied} = ClusterLive.handle_event("kill", :malformed, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:noreply, denied} = ClusterLive.handle_event("restart", :malformed, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
    assert {:noreply, denied} = ClusterLive.handle_event("drain", :malformed, socket)
    assert {:redirect, %{to: "/cluster"}} = denied.redirected
  end

  test "expired sockets are denied on events and scheduled expiry redirects" do
    {:ok, principal} = Principal.oidc("https://issuer.example", "subject", :user)
    now = System.system_time(:second)
    {:ok, context} = Context.single_tenant(principal, [:web_access, :query], expires_at: now)
    socket = Auth.assign_context(%Socket{}, context)

    assert {:error, :unauthenticated, denied} = Authorization.event(socket, :query)
    assert {:redirect, %{to: "/auth/login"}} = denied.redirected

    socket = %{socket | private: %{lifecycle: %Lifecycle{}}}
    socket = Authorization.attach(socket, :web_access)
    assert_receive :smolquery_auth_expiry, 100
    assert {:halt, denied} = Lifecycle.handle_info(:smolquery_auth_expiry, socket)
    assert {:redirect, %{to: "/auth/login"}} = denied.redirected
  end

  test "static operator contexts retain every web capability" do
    context = Static.web_context()
    socket = Auth.assign_context(%Socket{}, context)

    for capability <- [:web_access, :query, :catalog_manage, :platform_operate] do
      assert Authorization.authorize(socket, capability) == :ok
    end
  end
end
