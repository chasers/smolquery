defmodule SmolqueryApi.DocsTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Docs
  alias SmolqueryApi.Runtime

  setup do
    name = :"api_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, api_key: "docs-test-key", catalog: MapCatalog.new())
    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  test "GET /v1/docs.json answers without a credential", %{name: name} do
    response = ApiEndpoint.request(name, conn(:get, "/v1/docs.json"))

    assert response.status == 200

    body = JSON.decode!(response.resp_body)
    assert body["name"] == "smolquery HTTP API"
    assert body["repository"] == "https://github.com/chasers/smolquery"
    assert is_list(body["routes"])
  end

  test "the spec documents every routed path, and no other" do
    routed =
      SmolqueryApi.Router
      |> Phoenix.Router.routes()
      |> Enum.reject(&(&1.path == "/*path"))
      |> MapSet.new(&{&1.verb |> Atom.to_string() |> String.upcase(), &1.path})

    documented =
      MapSet.new(Docs.spec()["routes"], &{&1["method"], &1["path"]})

    assert MapSet.equal?(documented, routed)
  end

  test "every documented route carries an auth statement and a summary" do
    for route <- Docs.spec()["routes"] do
      assert is_binary(route["auth"]) and route["auth"] != ""
      assert is_binary(route["summary"]) and route["summary"] != ""
    end
  end
end
