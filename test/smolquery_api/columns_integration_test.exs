defmodule SmolqueryApi.ColumnsIntegrationTest do
  @moduledoc """
  The column routes over a real listener and a real DuckLake catalog (PL-61
  L2): curl-shaped requests, and answers that come back from the lake's own
  `information_schema`, tombstones included.

  Tagged `:integration` because it downloads the `ducklake` extension on
  first use and writes a catalog database to disk.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Test.ApiEndpoint
  alias SmolqueryApi.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @key "columns-integration-key"
  @table "/v1/datasets/analytics/tables/events"

  setup context do
    ApiEndpoint.stop_shared!()
    config = Application.get_env(:smolquery, SmolqueryApi.Endpoint)
    Application.put_env(:smolquery, SmolqueryApi.Endpoint, Keyword.put(config, :server, true))

    on_exit(fn ->
      Application.put_env(:smolquery, SmolqueryApi.Endpoint, config)
      ApiEndpoint.start_shared!()
    end)

    start_supervised!(
      {SmolqueryApi.Supervisor,
       api_key: @key,
       catalog: [
         metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
         data_path: Path.join(context.tmp_dir, "data")
       ]}
    )

    on_exit(fn -> Runtime.delete(SmolqueryApi) end)

    req = Req.new(base_url: SmolqueryApi.Endpoint.base_url(), auth: {:bearer, @key}, retry: false)
    Req.post!(req, url: "/v1/datasets", json: %{"id" => "analytics"})

    Req.post!(req,
      url: "/v1/datasets/analytics/tables",
      json: %{
        "id" => "events",
        "schema" => [
          %{"name" => "id", "type" => "INT64", "nullable" => false},
          %{"name" => "ts", "type" => "TIMESTAMP"}
        ]
      }
    )

    %{req: req}
  end

  defp names(response), do: Enum.map(response.body["schema"], & &1["name"])

  test "add, drop, and the tombstone, all through the lake", %{req: req} do
    added =
      Req.post!(req, url: @table <> "/columns", json: %{"name" => "country", "type" => "STRING"})

    assert added.status == 200
    assert names(added) == ["id", "ts", "country"]
    assert names(Req.get!(req, url: @table)) == ["id", "ts", "country"]

    dropped = Req.delete!(req, url: @table <> "/columns/ts")
    assert dropped.status == 200
    assert names(dropped) == ["id", "country"]
    assert names(Req.get!(req, url: @table)) == ["id", "country"]

    again = Req.post!(req, url: @table <> "/columns", json: %{"name" => "ts", "type" => "INT64"})
    assert again.status == 409
    assert again.body["error"]["message"] =~ "dropped"
  end
end
