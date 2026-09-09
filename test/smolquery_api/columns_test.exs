defmodule SmolqueryApi.ColumnControllerTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @key "columns-test-key"

  @schema_json [
    %{"name" => "id", "type" => "INT64", "nullable" => false},
    %{"name" => "ts", "type" => "TIMESTAMP", "nullable" => true}
  ]

  @columns "/v1/datasets/analytics/tables/events/columns"

  setup do
    name = :"api_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, api_key: @key, catalog: MapCatalog.new())
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    post_json(name, "/v1/datasets", %{"id" => "analytics"})

    post_json(name, "/v1/datasets/analytics/tables", %{
      "id" => "events",
      "schema" => @schema_json
    })

    %{name: name}
  end

  defp request(name, conn) do
    conn
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp json(name, method, path, body) do
    request(
      name,
      conn(method, path, JSON.encode!(body))
      |> put_req_header("content-type", "application/json")
    )
  end

  defp post_json(name, path, body), do: json(name, :post, path, body)
  defp patch_json(name, path, body), do: json(name, :patch, path, body)
  defp get_json(name, path), do: request(name, conn(:get, path))
  defp delete(name, path), do: request(name, conn(:delete, path))

  defp add(name, field), do: post_json(name, @columns, field)
  defp drop(name, column), do: delete(name, @columns <> "/" <> column)

  defp schema_of(response), do: JSON.decode!(response.resp_body)["schema"]

  defp error_of(response) do
    %{"error" => %{"status" => status, "message" => message}} =
      JSON.decode!(response.resp_body)

    {response.status, status, message}
  end

  describe "POST .../columns" do
    test "appends the column last, nullable, and answers the table body", %{name: name} do
      response = add(name, %{"name" => "country", "type" => "STRING"})

      assert response.status == 200

      assert JSON.decode!(response.resp_body) == %{
               "id" => "events",
               "schema" =>
                 @schema_json ++ [%{"name" => "country", "type" => "STRING", "nullable" => true}],
               "retention" => nil,
               "clustering" => [],
               "partitions" => nil
             }

      assert get_json(name, "/v1/datasets/analytics/tables/events") |> schema_of() |> length() ==
               3
    end

    test "refuses a column that is not nullable", %{name: name} do
      response = add(name, %{"name" => "flag", "type" => "BOOL", "nullable" => false})

      assert {422, "INVALID_ARGUMENT", message} = error_of(response)
      assert message =~ "nullable"
    end

    test "a name the table has is a 409", %{name: name} do
      assert {409, "ALREADY_EXISTS", message} =
               error_of(add(name, %{"name" => "ts", "type" => "STRING"}))

      assert message =~ "already exists"
    end

    test "a name the table once had is a 409 that says why", %{name: name} do
      assert drop(name, "ts").status == 200

      assert {409, "ALREADY_EXISTS", message} =
               error_of(add(name, %{"name" => "ts", "type" => "INT64"}))

      assert message =~ "dropped"
      assert add(name, %{"name" => "ts2", "type" => "INT64"}).status == 200
    end

    test "a malformed field, an unsupported type, and a bad name are 400s", %{name: name} do
      assert {400, "INVALID_ARGUMENT", _message} = error_of(add(name, %{"type" => "STRING"}))

      assert {400, "INVALID_ARGUMENT", _message} =
               error_of(add(name, %{"name" => "x", "type" => "GEOGRAPHY"}))

      assert {400, "INVALID_ARGUMENT", _message} =
               error_of(add(name, %{"name" => "bad name", "type" => "STRING"}))
    end

    test "an unknown table is a 404, a partition-shaped one a 422", %{name: name} do
      assert {404, "NOT_FOUND", _message} =
               error_of(
                 post_json(name, "/v1/datasets/analytics/tables/nope/columns", %{
                   "name" => "x",
                   "type" => "STRING"
                 })
               )

      assert {422, "INVALID_ARGUMENT", message} =
               error_of(
                 post_json(name, "/v1/datasets/analytics/tables/events__p1/columns", %{
                   "name" => "x",
                   "type" => "STRING"
                 })
               )

      assert message =~ "partition"
    end
  end

  describe "DELETE .../columns/:column" do
    test "drops the column and answers the table body without it", %{name: name} do
      response = drop(name, "ts")

      assert response.status == 200
      assert schema_of(response) == [%{"name" => "id", "type" => "INT64", "nullable" => false}]

      assert get_json(name, "/v1/datasets/analytics/tables/events") |> schema_of() |> length() ==
               1
    end

    test "an unknown column is a 404", %{name: name} do
      assert {404, "NOT_FOUND", message} = error_of(drop(name, "nope"))
      assert message =~ "nope"
    end

    test "the last column cannot be dropped", %{name: name} do
      assert drop(name, "ts").status == 200
      assert {422, "FAILED_PRECONDITION", message} = error_of(drop(name, "id"))
      assert message =~ "at least one column"
    end

    test "a clustering column cannot be dropped until the key is cleared", %{name: name} do
      patch = "/v1/datasets/analytics/tables/events"
      assert patch_json(name, patch, %{"clustering" => ["ts"]}).status == 200

      assert {422, "FAILED_PRECONDITION", message} = error_of(drop(name, "ts"))
      assert message =~ "clustering"

      assert patch_json(name, patch, %{"clustering" => []}).status == 200
      assert drop(name, "ts").status == 200
    end

    test "the retention column cannot be dropped until the policy is cleared", %{name: name} do
      patch = "/v1/datasets/analytics/tables/events"

      assert patch_json(name, patch, %{"retention" => %{"column" => "ts", "ttlMs" => 1_000}}).status ==
               200

      assert {422, "FAILED_PRECONDITION", message} = error_of(drop(name, "ts"))
      assert message =~ "retention"

      assert patch_json(name, patch, %{"retention" => nil}).status == 200
      assert drop(name, "ts").status == 200
    end
  end
end
