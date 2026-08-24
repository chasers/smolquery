defmodule SmolqueryApi.DatasetControllerTest do
  @moduledoc """
  The dataset routes over `Smolquery.Test.MapCatalog` (PL-51 L2): a dataset
  with its own catalog and storage round-trips through the API with no secret
  in any response, credentials rotate through `PATCH`, and everything else
  about a dataset is immutable.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @key "datasets-test-key"

  setup do
    previous = Application.get_env(:smolquery, :credential_key)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    name = :"api_datasets_#{:erlang.unique_integer([:positive])}"
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

  @catalog %{
    "host" => "db.abc.supabase.co",
    "database" => "postgres",
    "username" => "postgres",
    "password" => "hunter2"
  }

  @storage %{
    "bucket" => "lake",
    "prefix" => "analytics",
    "endpoint" => "https://abc.storage.supabase.co/storage/v1/s3",
    "region" => "eu-west-1",
    "url_style" => "path",
    "access_key_id" => "AKIA",
    "secret_access_key" => "shh"
  }

  defp valid(overrides \\ %{}) do
    Map.merge(%{"id" => "analytics", "catalog" => @catalog, "storage" => @storage}, overrides)
  end

  describe "POST /v1/datasets" do
    test "creates a dataset with its own catalog and storage, returning no secret",
         %{name: name} do
      response = send_json(name, :post, "/v1/datasets", valid())

      assert response.status == 200
      json = body(response)
      assert json["id"] == "analytics"
      assert json["name"] == "analytics"
      assert json["catalog"]["host"] == "db.abc.supabase.co"
      assert json["catalog"]["port"] == 5432
      assert json["catalog"]["sslmode"] == "require"
      assert json["catalog"]["version"] == "1.0"
      assert json["storage"]["bucket"] == "lake"
      assert json["storage"]["prefix"] == "analytics"
      assert json["storage"]["access_key_id"] == "AKIA"
      assert is_integer(json["createdAt"])
      refute response.resp_body =~ "hunter2"
      refute response.resp_body =~ "shh"
      refute Map.has_key?(json["catalog"], "password")
      refute Map.has_key?(json["storage"], "secret_access_key")
    end

    test "a name alone is a dataset on the deployment defaults", %{name: name} do
      response = send_json(name, :post, "/v1/datasets", %{"id" => "plain"})

      assert response.status == 200
      assert %{"id" => "plain", "catalog" => nil, "storage" => nil} = body(response)
    end

    test "re-creating with the same settings is a 200, with different ones a 409",
         %{name: name} do
      assert send_json(name, :post, "/v1/datasets", valid()).status == 200
      assert send_json(name, :post, "/v1/datasets", valid()).status == 200

      response =
        send_json(name, :post, "/v1/datasets", valid(%{"storage" => %{"bucket" => "other"}}))

      assert response.status == 409
      assert %{"error" => %{"status" => "ALREADY_EXISTS"}} = body(response)
    end

    test "a missing catalog field names itself, with its axis", %{name: name} do
      response =
        send_json(
          name,
          :post,
          "/v1/datasets",
          valid(%{"catalog" => Map.delete(@catalog, "host")})
        )

      assert response.status == 400
      assert %{"error" => %{"message" => message}} = body(response)
      assert message =~ "catalog.host"
    end

    test "half a storage key pair is a 400", %{name: name} do
      response =
        send_json(
          name,
          :post,
          "/v1/datasets",
          valid(%{"storage" => Map.delete(@storage, "secret_access_key")})
        )

      assert response.status == 400
      assert %{"error" => %{"message" => message}} = body(response)
      assert message =~ "storage.secret_access_key"
    end

    test "a node without a credential key answers 503 when a body carries a secret",
         %{name: name} do
      Application.delete_env(:smolquery, :credential_key)

      assert send_json(name, :post, "/v1/datasets", valid()).status == 503
      assert send_json(name, :post, "/v1/datasets", %{"id" => "plain"}).status == 200
    end
  end

  describe "GET /v1/datasets/:dataset" do
    test "answers the settings without secrets", %{name: name} do
      send_json(name, :post, "/v1/datasets", valid())

      response = get(name, "/v1/datasets/analytics")

      assert response.status == 200
      assert %{"id" => "analytics", "catalog" => %{"username" => "postgres"}} = body(response)
      refute response.resp_body =~ "hunter2"
    end

    test "an unknown dataset is a 404", %{name: name} do
      response = get(name, "/v1/datasets/nope")

      assert response.status == 404
      assert %{"error" => %{"status" => "NOT_FOUND"}} = body(response)
    end
  end

  describe "PATCH /v1/datasets/:dataset" do
    test "rotates credentials and keeps everything else", %{name: name} do
      send_json(name, :post, "/v1/datasets", valid())

      response =
        send_json(name, :patch, "/v1/datasets/analytics", %{
          "catalog" => %{"username" => "reader", "password" => "new"},
          "storage" => %{"access_key_id" => "AKIB", "secret_access_key" => "newer"}
        })

      assert response.status == 200
      json = body(response)
      assert json["catalog"]["username"] == "reader"
      assert json["catalog"]["host"] == "db.abc.supabase.co"
      assert json["storage"]["access_key_id"] == "AKIB"
      assert json["storage"]["bucket"] == "lake"
      refute response.resp_body =~ "new"
    end

    test "every other field is immutable", %{name: name} do
      send_json(name, :post, "/v1/datasets", valid())

      response =
        send_json(name, :patch, "/v1/datasets/analytics", %{"catalog" => %{"host" => "x"}})

      assert send_json(name, :patch, "/v1/datasets/analytics", %{
               "catalog" => %{"version" => "1.1"}
             }).status == 400

      assert response.status == 400

      assert %{"error" => %{"status" => "INVALID_ARGUMENT", "message" => message}} =
               body(response)

      assert message =~ "catalog.host"
    end

    test "a default axis cannot gain credentials", %{name: name} do
      send_json(name, :post, "/v1/datasets", %{"id" => "plain"})

      response =
        send_json(name, :patch, "/v1/datasets/plain", %{"catalog" => %{"password" => "x"}})

      assert response.status == 400
    end

    test "an unknown dataset is a 404", %{name: name} do
      response =
        send_json(name, :patch, "/v1/datasets/nope", %{"catalog" => %{"password" => "x"}})

      assert response.status == 404
    end
  end
end
