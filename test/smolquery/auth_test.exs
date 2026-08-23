defmodule Smolquery.AuthTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Phoenix.LiveView.Socket
  alias Smolquery.Auth
  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Principal

  defp context do
    {:ok, principal} = Principal.local("static:test", :api_key, :service)
    {:ok, context} = Context.single_tenant(principal, :query)
    context
  end

  test "uses the stable direct assign key" do
    assert Auth.assign_key() == :smolquery_auth_context
  end

  test "round-trips a context on a Plug connection" do
    conn = Auth.assign_context(conn(:get, "/"), context())

    assert {:ok, fetched} = Auth.fetch_context(conn)
    assert fetched == context()
    assert conn.assigns[Auth.assign_key()] == fetched
  end

  test "round-trips a context on a LiveView socket" do
    socket = Auth.assign_context(%Socket{}, context())

    assert {:ok, fetched} = Auth.fetch_context(socket)
    assert fetched == context()
    assert socket.assigns[Auth.assign_key()] == fetched
  end

  test "rejects a malformed assignment" do
    conn = Plug.Conn.assign(conn(:get, "/"), Auth.assign_key(), %{principal: :invalid})

    assert :error = Auth.fetch_context(conn)
    assert_raise ArgumentError, fn -> Auth.assign_context(conn, %{principal: :invalid}) end
  end

  test "rejects malformed context internals without raising" do
    malformed_contexts = [
      %{context() | capabilities: %MapSet{map: :forged}},
      %{context() | principal: %{__struct__: Principal}}
    ]

    for malformed <- malformed_contexts do
      conn = Plug.Conn.assign(conn(:get, "/"), Auth.assign_key(), malformed)
      socket = Phoenix.Component.assign(%Socket{}, Auth.assign_key(), malformed)

      assert :error = Auth.fetch_context(conn)
      assert :error = Auth.fetch_context(socket)
      assert_raise ArgumentError, fn -> Auth.assign_context(conn, malformed) end
      assert_raise ArgumentError, fn -> Auth.assign_context(socket, malformed) end
    end
  end

  test "rejects unsupported targets" do
    assert :error = Auth.fetch_context(%{})
    assert_raise ArgumentError, fn -> Auth.assign_context(%{}, context()) end
  end
end
