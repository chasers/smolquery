defmodule SmolqueryApi.JobControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.QueryService
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog
  alias SmolqueryApi.Runtime

  @key "jobs-test-key"

  setup do
    query = :"api_jobs_query_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    name = :"api_#{:erlang.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        name: name,
        api_key: @key,
        catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
        query_name: query
      )

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

  defp get_json(name, path), do: name |> request(conn(:get, path)) |> decode()

  defp decode(response), do: {response.status, JSON.decode!(response.resp_body)}

  describe "POST /v1/queries (sync)" do
    test "answers with the finished job and its rows", %{name: name} do
      response = post_json(name, "/v1/queries", %{"query" => "SELECT 1 + 1 AS n"})

      assert response.status == 200

      assert %{
               "complete" => true,
               "totalRows" => 1,
               "rows" => [%{"n" => 2}],
               "job" => %{"state" => "done", "resultsAvailable" => true, "id" => _id}
             } = JSON.decode!(response.resp_body)
    end

    test "temporal and decimal values arrive as strings", %{name: name} do
      response =
        post_json(name, "/v1/queries", %{
          "query" =>
            "SELECT TIMESTAMP '2026-08-01 10:00:00' AS ts, DATE '2026-08-01' AS d, " <>
              "CAST('12.50' AS DECIMAL(38,2)) AS amount"
        })

      assert %{"rows" => [row]} = JSON.decode!(response.resp_body)
      assert row["ts"] == "2026-08-01T10:00:00.000000"
      assert row["d"] == "2026-08-01"
      assert row["amount"] == "12.50"
    end

    test "a first page bounded by maxResults carries a token for the rest", %{name: name} do
      response =
        post_json(name, "/v1/queries", %{
          "query" => "SELECT range AS n FROM range(10) ORDER BY n",
          "maxResults" => 4
        })

      assert %{
               "totalRows" => 10,
               "rows" => rows,
               "nextPageToken" => token,
               "job" => %{"id" => id}
             } = JSON.decode!(response.resp_body)

      assert Enum.map(rows, & &1["n"]) == [0, 1, 2, 3]

      {200, page2} = get_json(name, "/v1/jobs/#{id}/results?max_results=4&page_token=#{token}")
      assert Enum.map(page2["rows"], & &1["n"]) == [4, 5, 6, 7]

      {200, page3} =
        get_json(
          name,
          "/v1/jobs/#{id}/results?max_results=4&page_token=#{page2["nextPageToken"]}"
        )

      assert Enum.map(page3["rows"], & &1["n"]) == [8, 9]
      refute Map.has_key?(page3, "nextPageToken")
    end

    test "a query that finishes badly is the caller's 400, with the planner's message",
         %{name: name} do
      response = post_json(name, "/v1/queries", %{"query" => "DROP TABLE analytics.events"})

      assert response.status == 400
      assert %{"error" => %{"status" => "INVALID_QUERY"}} = JSON.decode!(response.resp_body)
    end

    test "an unreachable buffer node is a 503, not the caller's 400" do
      table = {"analytics", "events"}

      catalog =
        FixedCatalog.new(%{
          snapshot: 1,
          schemas: %{table => Smolquery.Schema.new!([{"id", :int64}])},
          segments: %{}
        })

      query = :"api_jobs_down_query_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {QueryService.Supervisor,
         name: query,
         catalog: catalog,
         buffer_base_url: "http://127.0.0.1:1",
         buffer_timeout_ms: 500},
        id: query
      )

      on_exit(fn -> QueryService.Runtime.delete(query) end)

      name = :"api_down_#{:erlang.unique_integer([:positive])}"
      Runtime.put(Runtime.new(name: name, api_key: @key, catalog: catalog, query_name: query))
      on_exit(fn -> Runtime.delete(name) end)

      response = post_json(name, "/v1/queries", %{"query" => "SELECT * FROM analytics.events"})

      assert response.status == 503
      assert %{"error" => %{"status" => "UNAVAILABLE"}} = JSON.decode!(response.resp_body)
    end

    test "a missing query field is a 400", %{name: name} do
      assert post_json(name, "/v1/queries", %{"sql" => "SELECT 1"}).status == 400
    end

    test "a query service that is not running is a 503", %{name: name} do
      {:ok, runtime} = Runtime.fetch(name)
      Runtime.put(%{runtime | query_name: :never_started_query})

      assert post_json(name, "/v1/queries", %{"query" => "SELECT 1"}).status == 503
    end
  end

  describe "POST /v1/jobs + GET /v1/jobs/:id (async)" do
    test "submit, poll to done, page the results", %{name: name} do
      submitted = post_json(name, "/v1/jobs", %{"query" => "SELECT 40 + 2 AS n"})

      assert submitted.status == 200
      assert %{"id" => id, "state" => "pending"} = JSON.decode!(submitted.resp_body)

      assert Eventually.until(fn ->
               {200, job} = get_json(name, "/v1/jobs/#{id}")
               job["state"] == "done"
             end)

      {200, job} = get_json(name, "/v1/jobs/#{id}")
      assert job["resultsAvailable"] == true
      assert job["rowCount"] == 1

      {200, results} = get_json(name, "/v1/jobs/#{id}/results")
      assert results["rows"] == [%{"n" => 42}]
    end

    test "results before the job finishes say incomplete", %{name: name} do
      slow = "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"
      %{"id" => id} = JSON.decode!(post_json(name, "/v1/jobs", %{"query" => slow}).resp_body)

      {200, body} = get_json(name, "/v1/jobs/#{id}/results")

      assert body == %{"complete" => false, "rows" => []}

      request(name, conn(:delete, "/v1/jobs/#{id}"))
    end

    test "cancelling a running job settles it cancelled", %{name: name} do
      slow = "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"
      %{"id" => id} = JSON.decode!(post_json(name, "/v1/jobs", %{"query" => slow}).resp_body)

      assert request(name, conn(:delete, "/v1/jobs/#{id}")).status == 200

      {200, job} = get_json(name, "/v1/jobs/#{id}")
      assert job["state"] == "cancelled"

      {409, body} = get_json(name, "/v1/jobs/#{id}/results")
      assert body["error"]["status"] == "FAILED_PRECONDITION"
    end

    test "an unknown job is a 404", %{name: name} do
      assert {404, _body} = get_json(name, "/v1/jobs/01UNKNOWNJOBID0000000000")
    end

    test "an invalid page token is a 400", %{name: name} do
      %{"job" => %{"id" => id}} =
        JSON.decode!(post_json(name, "/v1/queries", %{"query" => "SELECT 1"}).resp_body)

      assert {400, _body} = get_json(name, "/v1/jobs/#{id}/results?page_token=junk")
    end

    test "a token for another job is a 400", %{name: name} do
      %{"job" => %{"id" => id}} =
        JSON.decode!(
          post_json(name, "/v1/queries", %{
            "query" => "SELECT range AS n FROM range(10)",
            "maxResults" => 2
          }).resp_body
        )

      token = Base.url_encode64(JSON.encode!(["someone-else", 2]), padding: false)

      assert {400, _body} = get_json(name, "/v1/jobs/#{id}/results?page_token=#{token}")
    end
  end
end
