defmodule SmolqueryApi.ConnectionControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @key "connections-test-key"

  setup do
    previous = Application.get_env(:smolquery, :credential_key)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    name = :"api_connections_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, api_key: @key, catalog: MapCatalog.new()))

    on_exit(fn ->
      Runtime.delete(name)

      if previous do
        Application.put_env(:smolquery, :credential_key, previous)
      else
        Application.delete_env(:smolquery, :credential_key)
      end
    end)

    %{name: name}
  end

  defp request(name, conn) do
    conn
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp send_json(name, method, path, body) do
    request(
      name,
      conn(method, path, JSON.encode!(body)) |> put_req_header("content-type", "application/json")
    )
  end

  defp get(name, path), do: request(name, conn(:get, path))

  defp body(response), do: JSON.decode!(response.resp_body)

  defp valid(overrides \\ %{}) do
    Map.merge(
      %{
        "name" => "warehouse",
        "host" => "db.internal",
        "database" => "app",
        "username" => "reader",
        "password" => "hunter2"
      },
      overrides
    )
  end

  describe "POST /v1/connections" do
    test "registers a connection and answers 201", %{name: name} do
      response = send_json(name, :post, "/v1/connections", valid())

      assert response.status == 201
      assert body(response)["name"] == "warehouse"
      assert body(response)["port"] == 5432
      assert body(response)["sslmode"] == "require"
    end

    test "never returns the password or the sealed secret", %{name: name} do
      response = send_json(name, :post, "/v1/connections", valid())

      refute response.resp_body =~ "hunter2"
      refute response.resp_body =~ "password"
      refute response.resp_body =~ "secret"
    end

    test "a second put of the same name replaces it and answers 200", %{name: name} do
      assert send_json(name, :post, "/v1/connections", valid()).status == 201

      response = send_json(name, :post, "/v1/connections", valid(%{"host" => "db2.internal"}))

      assert response.status == 200
      assert body(response)["host"] == "db2.internal"

      assert [_one] = body(get(name, "/v1/connections"))["connections"]
    end

    test "a name that is not an identifier is a 400", %{name: name} do
      response = send_json(name, :post, "/v1/connections", valid(%{"name" => "bad name"}))

      assert response.status == 400
      assert body(response)["error"]["status"] == "INVALID_ARGUMENT"
    end

    test "a missing field is a 400 naming it", %{name: name} do
      response = send_json(name, :post, "/v1/connections", Map.delete(valid(), "password"))

      assert response.status == 400
      assert body(response)["error"]["message"] =~ "password"
    end

    test "an invalid port or sslmode is a 400", %{name: name} do
      assert send_json(name, :post, "/v1/connections", valid(%{"port" => 0})).status == 400

      assert send_json(name, :post, "/v1/connections", valid(%{"sslmode" => "maybe"})).status ==
               400
    end

    test "a node with no credential key answers 503", %{name: name} do
      Application.delete_env(:smolquery, :credential_key)

      response = send_json(name, :post, "/v1/connections", valid())

      assert response.status == 503
      assert body(response)["error"]["status"] == "UNAVAILABLE"
      assert body(response)["error"]["message"] =~ "SMOLQUERY_CREDENTIAL_KEY"
    end
  end

  describe "GET /v1/connections" do
    test "starts empty and sorts by name", %{name: name} do
      assert body(get(name, "/v1/connections"))["connections"] == []

      send_json(name, :post, "/v1/connections", valid(%{"name" => "zeta"}))
      send_json(name, :post, "/v1/connections", valid(%{"name" => "alpha"}))

      names = Enum.map(body(get(name, "/v1/connections"))["connections"], & &1["name"])

      assert names == ["alpha", "zeta"]
    end

    test "a listing carries no secrets", %{name: name} do
      send_json(name, :post, "/v1/connections", valid())

      response = get(name, "/v1/connections")

      refute response.resp_body =~ "hunter2"
      refute response.resp_body =~ "secret"
    end
  end

  describe "GET /v1/connections/:name" do
    test "answers the connection without its password", %{name: name} do
      send_json(name, :post, "/v1/connections", valid())

      response = get(name, "/v1/connections/warehouse")

      assert response.status == 200
      assert body(response)["username"] == "reader"
      refute response.resp_body =~ "hunter2"
    end

    test "an unknown name is a 404 naming it", %{name: name} do
      response = get(name, "/v1/connections/nope")

      assert response.status == 404
      assert body(response)["error"]["message"] =~ "nope"
    end
  end

  describe "PATCH /v1/connections/:name" do
    test "changes the fields the body names", %{name: name} do
      send_json(name, :post, "/v1/connections", valid())

      response = send_json(name, :patch, "/v1/connections/warehouse", %{"host" => "db2.internal"})

      assert response.status == 200
      assert body(response)["host"] == "db2.internal"
      assert body(response)["username"] == "reader"
    end

    test "an absent password keeps the stored one", %{name: name} do
      send_json(name, :post, "/v1/connections", valid())
      send_json(name, :patch, "/v1/connections/warehouse", %{"port" => 6543})

      response = send_json(name, :post, "/v1/connections/warehouse/test", %{})

      refute response.status == 404
    end

    test "an empty password is a 400, not a clear", %{name: name} do
      send_json(name, :post, "/v1/connections", valid())

      response = send_json(name, :patch, "/v1/connections/warehouse", %{"password" => ""})

      assert response.status == 400
    end

    test "patching an unknown name is a 404", %{name: name} do
      assert send_json(name, :patch, "/v1/connections/nope", %{"host" => "x"}).status == 404
    end
  end

  describe "DELETE /v1/connections/:name" do
    test "removes it, and removing an absent one is still 200", %{name: name} do
      send_json(name, :post, "/v1/connections", valid())

      assert request(name, conn(:delete, "/v1/connections/warehouse")).status == 200
      assert body(get(name, "/v1/connections"))["connections"] == []
      assert request(name, conn(:delete, "/v1/connections/warehouse")).status == 200
    end
  end

  describe "POST /v1/connections/:name/test" do
    test "an unknown connection is a 404", %{name: name} do
      assert send_json(name, :post, "/v1/connections/nope/test", %{}).status == 404
    end

    @tag :integration
    test "an unreachable database is a 422 that never quotes the password", %{name: name} do
      send_json(
        name,
        :post,
        "/v1/connections",
        valid(%{"host" => "127.0.0.1", "port" => 1, "password" => "sup3rsecret"})
      )

      response = send_json(name, :post, "/v1/connections/warehouse/test", %{})

      assert response.status == 422
      assert body(response)["error"]["message"] =~ "warehouse"
      refute response.resp_body =~ "sup3rsecret"
    end
  end

  describe "auth" do
    test "every connection route needs the key", %{name: name} do
      for {method, path} <- [
            {:get, "/v1/connections"},
            {:post, "/v1/connections"},
            {:get, "/v1/connections/warehouse"},
            {:patch, "/v1/connections/warehouse"},
            {:delete, "/v1/connections/warehouse"},
            {:post, "/v1/connections/warehouse/test"}
          ] do
        assert ApiEndpoint.request(name, conn(method, path)).status == 401
      end
    end
  end
end
