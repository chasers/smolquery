defmodule Smolquery.Api.TablesTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Api.Router
  alias Smolquery.Api.Runtime
  alias Smolquery.Test.MapCatalog

  @key "tables-test-key"
  @schema_json [
    %{"name" => "id", "type" => "INT64", "nullable" => false},
    %{"name" => "ts", "type" => "TIMESTAMP", "nullable" => true},
    %{"name" => "amount", "type" => "NUMERIC(38,2)", "nullable" => true}
  ]

  setup do
    name = :"api_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, api_key: @key, catalog: MapCatalog.new())
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  defp request(name, conn) do
    conn
    |> put_req_header("authorization", "Bearer #{@key}")
    |> Router.call(Router.init(name))
  end

  defp post_json(name, path, body) do
    request(
      name,
      conn(:post, path, JSON.encode!(body))
      |> put_req_header("content-type", "application/json")
    )
  end

  defp get_json(name, path), do: request(name, conn(:get, path))

  defp create_dataset(name, id), do: post_json(name, "/v1/datasets", %{"id" => id})

  describe "datasets" do
    test "create then list", %{name: name} do
      assert create_dataset(name, "analytics").status == 200

      response = get_json(name, "/v1/datasets")

      assert response.status == 200
      assert JSON.decode!(response.resp_body) == %{"datasets" => ["analytics"]}
    end

    test "creating an existing dataset is idempotent", %{name: name} do
      assert create_dataset(name, "analytics").status == 200
      assert create_dataset(name, "analytics").status == 200
    end

    test "a body without an id is a 400", %{name: name} do
      response = post_json(name, "/v1/datasets", %{"name" => "analytics"})

      assert response.status == 400

      assert %{"error" => %{"status" => "INVALID_ARGUMENT", "message" => message}} =
               JSON.decode!(response.resp_body)

      assert message =~ "id"
    end

    test "a body that is not json is a 400 in the envelope", %{name: name} do
      conn =
        conn(:post, "/v1/datasets", "id=analytics")
        |> put_req_header("content-type", "application/json")
        |> put_req_header("authorization", "Bearer #{@key}")

      assert_raise Plug.Parsers.ParseError, fn -> Router.call(conn, Router.init(name)) end

      assert {400, _headers, body} = sent_resp(conn)
      assert %{"error" => %{"status" => "INVALID_ARGUMENT"}} = JSON.decode!(body)
    end

    test "a non-json content type is a 415", %{name: name} do
      conn =
        conn(:post, "/v1/datasets", "<dataset/>")
        |> put_req_header("content-type", "application/xml")
        |> put_req_header("authorization", "Bearer #{@key}")

      assert_raise Plug.Parsers.UnsupportedMediaTypeError, fn ->
        Router.call(conn, Router.init(name))
      end

      assert {415, _headers, body} = sent_resp(conn)
      assert %{"error" => %{"status" => "UNSUPPORTED_MEDIA_TYPE"}} = JSON.decode!(body)
    end
  end

  describe "tables" do
    test "create, list, and read the schema back", %{name: name} do
      create_dataset(name, "analytics")

      created =
        post_json(name, "/v1/datasets/analytics/tables", %{
          "id" => "events",
          "schema" => @schema_json
        })

      assert created.status == 200
      assert JSON.decode!(created.resp_body) == %{"id" => "events", "schema" => @schema_json}

      listed = get_json(name, "/v1/datasets/analytics/tables")
      assert JSON.decode!(listed.resp_body) == %{"tables" => ["events"]}

      fetched = get_json(name, "/v1/datasets/analytics/tables/events")
      assert JSON.decode!(fetched.resp_body) == %{"id" => "events", "schema" => @schema_json}
    end

    test "re-creating with the same schema is idempotent", %{name: name} do
      create_dataset(name, "analytics")
      body = %{"id" => "events", "schema" => @schema_json}

      assert post_json(name, "/v1/datasets/analytics/tables", body).status == 200
      assert post_json(name, "/v1/datasets/analytics/tables", body).status == 200
    end

    test "re-creating with a different schema is a 409, not a silent no-op", %{name: name} do
      create_dataset(name, "analytics")

      assert post_json(name, "/v1/datasets/analytics/tables", %{
               "id" => "events",
               "schema" => @schema_json
             }).status == 200

      response =
        post_json(name, "/v1/datasets/analytics/tables", %{
          "id" => "events",
          "schema" => [%{"name" => "other", "type" => "STRING"}]
        })

      assert response.status == 409

      assert %{"error" => %{"status" => "ALREADY_EXISTS", "message" => message}} =
               JSON.decode!(response.resp_body)

      assert message =~ "different schema"
    end

    test "creating in a missing dataset is a 404", %{name: name} do
      response =
        post_json(name, "/v1/datasets/nope/tables", %{"id" => "events", "schema" => @schema_json})

      assert response.status == 404
      assert %{"error" => %{"status" => "NOT_FOUND"}} = JSON.decode!(response.resp_body)
    end

    test "listing a missing dataset is a 404", %{name: name} do
      assert get_json(name, "/v1/datasets/nope/tables").status == 404
    end

    test "an unknown table is a 404", %{name: name} do
      create_dataset(name, "analytics")

      response = get_json(name, "/v1/datasets/analytics/tables/nope")

      assert response.status == 404

      assert %{"error" => %{"message" => "table analytics.nope does not exist"}} =
               JSON.decode!(response.resp_body)
    end

    test "an unsupported type is a 400", %{name: name} do
      create_dataset(name, "analytics")

      response =
        post_json(name, "/v1/datasets/analytics/tables", %{
          "id" => "events",
          "schema" => [%{"name" => "payload", "type" => "GEOGRAPHY"}]
        })

      assert response.status == 400

      assert %{"error" => %{"status" => "INVALID_ARGUMENT", "message" => message}} =
               JSON.decode!(response.resp_body)

      assert message =~ "GEOGRAPHY"
    end

    test "an empty schema is a 400", %{name: name} do
      create_dataset(name, "analytics")

      response =
        post_json(name, "/v1/datasets/analytics/tables", %{"id" => "events", "schema" => []})

      assert response.status == 400
    end

    test "an invalid column name is a 400", %{name: name} do
      create_dataset(name, "analytics")

      response =
        post_json(name, "/v1/datasets/analytics/tables", %{
          "id" => "events",
          "schema" => [%{"name" => "bad name!", "type" => "STRING"}]
        })

      assert response.status == 400
    end
  end
end
