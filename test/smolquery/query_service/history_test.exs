defmodule Smolquery.QueryService.HistoryTest do
  @moduledoc """
  The PL-8 D8 store, proven against a real SQLite metadata database: terminal
  jobs survive the registry's result TTL, and `GET /v1/jobs/:id` has somewhere
  to look. Tagged `:integration` because DuckDB loads the `sqlite` extension.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.History
  alias Smolquery.QueryService.Job
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

  @moduletag :integration
  @moduletag :tmp_dir

  @key "history-test-key"

  defp start_query_service(context, opts \\ []) do
    name = :"history_query_#{:erlang.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [
          name: name,
          catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
          history_metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
        ],
        opts
      )

    start_supervised!({QueryService.Supervisor, opts}, id: name)
    on_exit(fn -> QueryService.Runtime.delete(name) end)

    name
  end

  test "a terminal job is recorded and fetchable", context do
    name = start_query_service(context)

    {:ok, job, _frame} = Client.query(name, "SELECT 1 + 1 AS n")

    assert Eventually.until(fn -> match?({:ok, _job}, History.fetch(name, job.id)) end)

    {:ok, recorded} = History.fetch(name, job.id)
    assert %Job{state: :done, row_count: 1, sql: "SELECT 1 + 1 AS n"} = recorded
    assert recorded.id == job.id
    assert recorded.snapshot == 1
  end

  test "a failed job's error survives as a string", context do
    name = start_query_service(context)

    {:ok, job, nil} = Client.query(name, "DROP TABLE analytics.events")
    assert job.state == :error

    assert Eventually.until(fn -> match?({:ok, _job}, History.fetch(name, job.id)) end)

    {:ok, recorded} = History.fetch(name, job.id)
    assert recorded.state == :error
    assert recorded.error =~ "invalid_query"
  end

  test "an unknown id is not found", context do
    name = start_query_service(context)

    assert History.fetch(name, "01NOSUCHJOB0000000000000") == {:error, :not_found}
  end

  test "the api falls back to history after the result ttl", context do
    query = start_query_service(context, result_ttl_ms: 50)

    api = :"history_api_#{:erlang.unique_integer([:positive])}"

    api
    |> then(
      &SmolqueryApi.Runtime.new(
        name: &1,
        api_key: @key,
        catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
        query_name: query
      )
    )
    |> SmolqueryApi.Runtime.put()

    on_exit(fn -> SmolqueryApi.Runtime.delete(api) end)

    {:ok, job, _frame} = Client.query(query, "SELECT 1 AS n")

    assert Eventually.until(fn ->
             Client.fetch(query, job.id) == {:error, :not_found}
           end)

    status =
      conn(:get, "/v1/jobs/#{job.id}")
      |> put_req_header("authorization", "Bearer #{@key}")
      |> then(&ApiEndpoint.request(api, &1))

    assert status.status == 200

    assert %{"state" => "done", "resultsAvailable" => false, "rowCount" => 1} =
             JSON.decode!(status.resp_body)

    results =
      conn(:get, "/v1/jobs/#{job.id}/results")
      |> put_req_header("authorization", "Bearer #{@key}")
      |> then(&ApiEndpoint.request(api, &1))

    assert results.status == 410
    assert %{"error" => %{"status" => "GONE"}} = JSON.decode!(results.resp_body)
  end
end
