defmodule Smolquery.Api.CrudIntegrationTest do
  @moduledoc """
  The L2 exit proof: datasets/tables CRUD over a real listener and a real
  DuckLake catalog — curl-shaped requests, catalog-backed answers.

  Tagged `:integration` because it downloads the `ducklake` extension on first
  use and writes a catalog database to disk.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Api
  alias Smolquery.Api.Router
  alias Smolquery.Api.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @key "crud-integration-key"
  @schema_json [
    %{"name" => "id", "type" => "INT64", "nullable" => false},
    %{"name" => "ts", "type" => "TIMESTAMP", "nullable" => true},
    %{"name" => "amount", "type" => "NUMERIC(38,2)", "nullable" => true}
  ]

  setup context do
    name = :"api_crud_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {Api.Supervisor,
       name: name,
       api_key: @key,
       port: 0,
       catalog: [
         metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
         data_path: Path.join(context.tmp_dir, "data")
       ]}
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{base: Router.base_url(name)}
  end

  defp req(base) do
    Req.new(base_url: base, auth: {:bearer, @key}, retry: false)
  end

  test "create dataset and table, list both, read the schema back", %{base: base} do
    req = req(base)

    assert Req.post!(req, url: "/v1/datasets", json: %{"id" => "analytics"}).status == 200

    datasets = Req.get!(req, url: "/v1/datasets")
    assert "analytics" in datasets.body["datasets"]

    created =
      Req.post!(req,
        url: "/v1/datasets/analytics/tables",
        json: %{"id" => "events", "schema" => @schema_json}
      )

    assert created.status == 200
    assert created.body == %{"id" => "events", "schema" => @schema_json}

    assert Req.get!(req, url: "/v1/datasets/analytics/tables").body == %{"tables" => ["events"]}

    fetched = Req.get!(req, url: "/v1/datasets/analytics/tables/events")
    assert fetched.status == 200
    assert fetched.body == %{"id" => "events", "schema" => @schema_json}
  end

  test "the schema read back through DuckLake survives a re-create check", %{base: base} do
    req = req(base)
    Req.post!(req, url: "/v1/datasets", json: %{"id" => "analytics"})

    body = %{"id" => "events", "schema" => @schema_json}
    assert Req.post!(req, url: "/v1/datasets/analytics/tables", json: body).status == 200
    assert Req.post!(req, url: "/v1/datasets/analytics/tables", json: body).status == 200

    conflicted =
      Req.post!(req,
        url: "/v1/datasets/analytics/tables",
        json: %{"id" => "events", "schema" => [%{"name" => "other", "type" => "STRING"}]}
      )

    assert conflicted.status == 409
    assert conflicted.body["error"]["status"] == "ALREADY_EXISTS"
  end

  test "a missing table and a missing dataset answer 404 from the real catalog", %{base: base} do
    req = req(base)
    Req.post!(req, url: "/v1/datasets", json: %{"id" => "analytics"})

    assert Req.get!(req, url: "/v1/datasets/analytics/tables/nope").status == 404

    missing_dataset =
      Req.post!(req,
        url: "/v1/datasets/nope/tables",
        json: %{"id" => "events", "schema" => @schema_json}
      )

    assert missing_dataset.status == 404
  end
end
