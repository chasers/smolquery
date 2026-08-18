defmodule SmolqueryApi.TableControllerTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

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
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp post_json(name, path, body) do
    request(
      name,
      conn(:post, path, JSON.encode!(body))
      |> put_req_header("content-type", "application/json")
    )
  end

  defp get_json(name, path), do: request(name, conn(:get, path))

  defp patch_json(name, path, body) do
    request(
      name,
      conn(:patch, path, JSON.encode!(body))
      |> put_req_header("content-type", "application/json")
    )
  end

  defp create_dataset(name, id), do: post_json(name, "/v1/datasets", %{"id" => id})

  defp create_events(name) do
    create_dataset(name, "analytics")

    post_json(name, "/v1/datasets/analytics/tables", %{
      "id" => "events",
      "schema" => @schema_json
    })
  end

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

      response = ApiEndpoint.request(name, conn)

      assert response.status == 400
      assert response.halted
      assert %{"error" => %{"status" => "INVALID_ARGUMENT"}} = JSON.decode!(response.resp_body)
    end

    test "a non-json content type is a 415", %{name: name} do
      conn =
        conn(:post, "/v1/datasets", "<dataset/>")
        |> put_req_header("content-type", "application/xml")
        |> put_req_header("authorization", "Bearer #{@key}")

      response = ApiEndpoint.request(name, conn)

      assert response.status == 415
      assert response.halted

      assert %{"error" => %{"status" => "UNSUPPORTED_MEDIA_TYPE"}} =
               JSON.decode!(response.resp_body)
    end
  end

  describe "tables" do
    test "a partition-shaped id is refused — reserved for the write path (T-170)", %{
      name: name
    } do
      create_dataset(name, "analytics")

      response =
        post_json(name, "/v1/datasets/analytics/tables", %{
          "id" => "events__p1",
          "schema" => @schema_json
        })

      assert response.status == 400
    end

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

      assert JSON.decode!(fetched.resp_body) == %{
               "id" => "events",
               "schema" => @schema_json,
               "retention" => nil,
               "clustering" => [],
               "partitions" => nil
             }
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

  describe "retention" do
    test "sets, reads back, and clears a policy", %{name: name} do
      create_events(name)
      policy = %{"column" => "ts", "ttlMs" => 86_400_000}

      updated =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"retention" => policy})

      assert updated.status == 200
      assert JSON.decode!(updated.resp_body)["retention"] == policy

      fetched = get_json(name, "/v1/datasets/analytics/tables/events")
      assert JSON.decode!(fetched.resp_body)["retention"] == policy

      cleared =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"retention" => nil})

      assert cleared.status == 200
      assert JSON.decode!(cleared.resp_body)["retention"] == nil
    end

    test "refuses a column the schema does not have", %{name: name} do
      create_events(name)

      response =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "retention" => %{"column" => "created", "ttlMs" => 1_000}
        })

      assert response.status == 400
      assert JSON.decode!(response.resp_body)["error"]["message"] =~ "does not exist"
    end

    test "refuses a column that is not a timestamp or date", %{name: name} do
      create_events(name)

      response =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "retention" => %{"column" => "amount", "ttlMs" => 1_000}
        })

      assert response.status == 400
      assert JSON.decode!(response.resp_body)["error"]["message"] =~ "timestamp or date"
    end

    test "refuses a malformed policy and a missing field", %{name: name} do
      create_events(name)

      zero_ttl =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "retention" => %{"column" => "ts", "ttlMs" => 0}
        })

      assert zero_ttl.status == 400

      missing = patch_json(name, "/v1/datasets/analytics/tables/events", %{})
      assert missing.status == 400
    end

    test "a table the catalog does not hold is a 404", %{name: name} do
      create_dataset(name, "analytics")

      response =
        patch_json(name, "/v1/datasets/analytics/tables/missing", %{
          "retention" => %{"column" => "ts", "ttlMs" => 1_000}
        })

      assert response.status == 404
    end
  end

  describe "clustering" do
    test "sets, reads back, and clears a clustering key", %{name: name} do
      create_events(name)
      key = ["amount", "ts"]

      updated =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"clustering" => key})

      assert updated.status == 200
      assert JSON.decode!(updated.resp_body)["clustering"] == key

      fetched = get_json(name, "/v1/datasets/analytics/tables/events")
      assert JSON.decode!(fetched.resp_body)["clustering"] == key

      cleared =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"clustering" => []})

      assert cleared.status == 200
      assert JSON.decode!(cleared.resp_body)["clustering"] == []
    end

    test "refuses a column the schema does not have with 422", %{name: name} do
      create_events(name)

      response =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "clustering" => ["missing"]
        })

      assert response.status == 422
      assert JSON.decode!(response.resp_body)["error"]["message"] =~ "does not exist"
    end

    test "refuses duplicates and a non-list", %{name: name} do
      create_events(name)

      duped =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "clustering" => ["ts", "ts"]
        })

      assert duped.status == 400

      bad = patch_json(name, "/v1/datasets/analytics/tables/events", %{"clustering" => "ts"})
      assert bad.status == 400
    end

    test "can patch clustering alongside retention", %{name: name} do
      create_events(name)
      policy = %{"column" => "ts", "ttlMs" => 1_000}

      response =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "retention" => policy,
          "clustering" => ["id", "ts"]
        })

      assert response.status == 200
      body = JSON.decode!(response.resp_body)
      assert body["retention"] == policy
      assert body["clustering"] == ["id", "ts"]
    end

    test "a rejected patch applies none of it (T-304 partitions included)", %{name: name} do
      create_events(name)
      kept = %{"column" => "ts", "ttlMs" => 1_000}

      assert patch_json(name, "/v1/datasets/analytics/tables/events", %{"retention" => kept}).status ==
               200

      rejected =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "retention" => %{"column" => "ts", "ttlMs" => 2_000},
          "clustering" => ["no_such_column"],
          "partitions" => 4
        })

      assert rejected.status == 422

      shown = JSON.decode!(get_json(name, "/v1/datasets/analytics/tables/events").resp_body)
      assert shown["retention"] == kept
      assert shown["clustering"] == []
      assert shown["partitions"] == nil
    end
  end

  describe "partitions (T-304)" do
    test "sets and reads back a count, raise-only", %{name: name} do
      create_events(name)

      shown = JSON.decode!(get_json(name, "/v1/datasets/analytics/tables/events").resp_body)
      assert shown["partitions"] == nil

      raised =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"partitions" => 3})

      assert raised.status == 200
      assert JSON.decode!(raised.resp_body)["partitions"] == 3

      again =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"partitions" => 6})

      assert again.status == 200
      assert JSON.decode!(again.resp_body)["partitions"] == 6

      lowered =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{"partitions" => 2})

      assert lowered.status == 422
      assert JSON.decode!(lowered.resp_body)["error"]["message"] =~ "raise-only"

      shown = JSON.decode!(get_json(name, "/v1/datasets/analytics/tables/events").resp_body)
      assert shown["partitions"] == 6
    end

    test "refuses a malformed count", %{name: name} do
      create_events(name)

      for bad <- [0, -1, "3", nil, 1.5] do
        response =
          patch_json(name, "/v1/datasets/analytics/tables/events", %{"partitions" => bad})

        assert response.status == 400
      end
    end

    test "can patch partitions alongside retention and clustering", %{name: name} do
      create_events(name)
      policy = %{"column" => "ts", "ttlMs" => 1_000}

      response =
        patch_json(name, "/v1/datasets/analytics/tables/events", %{
          "retention" => policy,
          "clustering" => ["id", "ts"],
          "partitions" => 3
        })

      assert response.status == 200
      body = JSON.decode!(response.resp_body)
      assert body["retention"] == policy
      assert body["clustering"] == ["id", "ts"]
      assert body["partitions"] == 3
    end
  end
end
