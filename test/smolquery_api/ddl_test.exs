defmodule SmolqueryApi.DdlTest do
  @moduledoc """
  `ALTER TABLE` over the query routes (PL-61 L3): the job answers with
  `statementType: "ALTER_TABLE"` and its `ddl` outcome, and its refusals take
  the column routes' statuses.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @key "ddl-test-key"
  @table {"analytics", "events"}

  setup do
    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        @table,
        Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
      )

    query = :"api_ddl_query_#{:erlang.unique_integer([:positive])}"
    start_supervised!({QueryService.Supervisor, name: query, catalog: catalog}, id: query)
    on_exit(fn -> QueryService.Runtime.delete(query) end)

    name = :"api_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, api_key: @key, catalog: catalog, query_name: query))
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, catalog: catalog}
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

  defp get_json(name, path), do: name |> request(conn(:get, path)) |> decode()

  defp decode(response), do: {response.status, JSON.decode!(response.resp_body)}

  defp query(name, sql), do: decode(post_json(name, "/v1/queries", %{"query" => sql}))

  test "a sync ALTER TABLE answers the job with its ddl outcome and no rows", %{
    name: name,
    catalog: catalog
  } do
    assert {200, body} = query(name, "ALTER TABLE analytics.events ADD COLUMN label STRING")

    assert %{
             "job" => %{
               "state" => "done",
               "statementType" => "ALTER_TABLE",
               "rowCount" => nil,
               "snapshot" => nil,
               "resultsAvailable" => false,
               "ddl" => %{
                 "operation" => "ADD_COLUMN",
                 "targetTable" => "analytics.events",
                 "column" => "label",
                 "performed" => true
               }
             }
           } = body

    refute Map.has_key?(body, "rows")

    {:ok, schema} = Catalog.table_schema(catalog, @table)
    assert Schema.names(schema) == ["id", "ts", "label"]

    assert {200, %{"job" => %{"ddl" => %{"operation" => "DROP_COLUMN", "performed" => false}}}} =
             query(name, "ALTER TABLE analytics.events DROP COLUMN IF EXISTS nope")
  end

  test "an async ALTER TABLE carries the outcome on the job, and its results route is a 409",
       %{name: name} do
    response =
      post_json(name, "/v1/jobs", %{"query" => "ALTER TABLE analytics.events DROP COLUMN ts"})

    assert %{"id" => id, "state" => "pending", "statementType" => "ALTER_TABLE", "ddl" => nil} =
             JSON.decode!(response.resp_body)

    assert Eventually.until(fn ->
             match?({200, %{"state" => "done"}}, get_json(name, "/v1/jobs/#{id}"))
           end)

    assert {200, %{"ddl" => %{"operation" => "DROP_COLUMN", "column" => "ts"}}} =
             get_json(name, "/v1/jobs/#{id}")

    assert {409, %{"error" => %{"status" => "FAILED_PRECONDITION", "message" => message}}} =
             get_json(name, "/v1/jobs/#{id}/results")

    assert message =~ "DDL job has no result rows"
  end

  test "the refusals take the column routes' statuses", %{name: name} do
    assert {409,
            %{"error" => %{"status" => "ALREADY_EXISTS", "message" => "column id already exists"}}} =
             query(name, "ALTER TABLE analytics.events ADD COLUMN id INT64")

    assert {404,
            %{"error" => %{"status" => "NOT_FOUND", "message" => "column nope does not exist"}}} =
             query(name, "ALTER TABLE analytics.events DROP COLUMN nope")

    assert {404, %{"error" => %{"status" => "NOT_FOUND"}}} =
             query(name, "ALTER TABLE analytics.nope DROP COLUMN id")

    assert {422, %{"error" => %{"status" => "INVALID_ARGUMENT"}}} =
             query(name, "ALTER TABLE analytics.events__p1 DROP COLUMN id")
  end

  test "a statement the parser does not accept is the caller's 400", %{name: name} do
    assert {400, %{"error" => %{"status" => "INVALID_QUERY", "message" => message}}} =
             query(name, "ALTER TABLE analytics.events ADD COLUMN n BIGINT NOT NULL")

    assert message == "NOT NULL is not supported in ALTER TABLE"

    assert {400, %{"error" => %{"message" => "events: a table is named dataset.table"}}} =
             query(name, "ALTER TABLE events DROP COLUMN ts")

    assert {200, %{"job" => %{"ddl" => %{"column" => "t", "performed" => true}}}} =
             query(
               name,
               "ALTER TABLE analytics.events ADD COLUMN t TIMESTAMP MATERIALIZED epoch_ms(id)"
             )
  end

  test "explain and DDL do not mix", %{name: name} do
    assert {400, %{"error" => %{"status" => "INVALID_ARGUMENT", "message" => message}}} =
             decode(
               post_json(name, "/v1/queries", %{
                 "query" => "ALTER TABLE analytics.events DROP COLUMN ts",
                 "explain" => "plan"
               })
             )

    assert message == "ALTER TABLE cannot be explained or described"
  end
end
