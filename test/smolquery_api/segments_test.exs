defmodule SmolqueryApi.SegmentControllerTest do
  @moduledoc """
  `DELETE /v1/datasets/:dataset/tables/:table/segments` (T-310).

  `Smolquery.Test.PathCatalog` stands in for the catalog: unlike `MapCatalog`
  (dataset/table surface only), it actually implements `drop_segments/3` —
  the one behaviour this route exists to drive.
  """

  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.PathCatalog
  alias SmolqueryApi.Runtime

  @key "segments-test-key"

  setup do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    name = :"api_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, api_key: @key, catalog: PathCatalog.new(agent))
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, agent: agent}
  end

  defp request(name, conn) do
    conn
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp delete_json(name, path, body) do
    request(
      name,
      conn(:delete, path, JSON.encode!(body))
      |> put_req_header("content-type", "application/json")
    )
  end

  defp drop(name, paths),
    do: delete_json(name, "/v1/datasets/analytics/tables/events/segments", %{"paths" => paths})

  test "drops the named segments and answers the snapshot", %{name: name, agent: agent} do
    PathCatalog.register(agent, "analytics/events/a.parquet")
    PathCatalog.register(agent, "analytics/events/b.parquet")

    response = drop(name, ["analytics/events/a.parquet"])

    assert response.status == 200
    body = JSON.decode!(response.resp_body)
    assert body["dropped"] == ["analytics/events/a.parquet"]
    assert is_integer(body["snapshot"])

    {:ok, current} =
      Smolquery.Catalog.segments(PathCatalog.new(agent), {"analytics", "events"}, :current)

    assert current == ["analytics/events/b.parquet"]
  end

  test "dropping a path the table does not hold is not an error", %{name: name, agent: agent} do
    PathCatalog.register(agent, "analytics/events/a.parquet")

    first = drop(name, ["analytics/events/never-existed.parquet"])
    second = drop(name, ["analytics/events/never-existed.parquet"])

    assert first.status == 200
    assert second.status == 200

    body = JSON.decode!(first.resp_body)
    assert body["dropped"] == []
    assert body["notFound"] == ["analytics/events/never-existed.parquet"]
  end

  test "the response separates what matched from what did not", %{name: name, agent: agent} do
    PathCatalog.register(agent, "analytics/events/a.parquet")

    response = drop(name, ["analytics/events/a.parquet", "analytics/events/typo.parquet"])

    assert response.status == 200
    body = JSON.decode!(response.resp_body)
    assert body["dropped"] == ["analytics/events/a.parquet"]
    assert body["notFound"] == ["analytics/events/typo.parquet"]
  end

  test "a missing paths field is a 400", %{name: name} do
    response = delete_json(name, "/v1/datasets/analytics/tables/events/segments", %{})

    assert response.status == 400
    body = JSON.decode!(response.resp_body)
    assert body["error"]["status"] == "INVALID_ARGUMENT"
  end

  test "an empty paths list is a 400", %{name: name} do
    response = drop(name, [])

    assert response.status == 400
  end

  test "a non-string path is a 400", %{name: name} do
    response = drop(name, [1])

    assert response.status == 400
  end

  test "requires the bearer key", %{name: name} do
    response =
      ApiEndpoint.request(
        name,
        conn(
          :delete,
          "/v1/datasets/analytics/tables/events/segments",
          JSON.encode!(%{"paths" => ["x"]})
        )
        |> put_req_header("content-type", "application/json")
      )

    assert response.status == 401
  end
end
