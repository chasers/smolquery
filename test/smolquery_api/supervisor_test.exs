defmodule SmolqueryApi.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @key "supervisor-test-key"

  setup do
    ApiEndpoint.stop_shared!()
    config = Application.get_env(:smolquery, SmolqueryApi.Endpoint)
    Application.put_env(:smolquery, SmolqueryApi.Endpoint, Keyword.put(config, :server, true))

    on_exit(fn ->
      Application.put_env(:smolquery, SmolqueryApi.Endpoint, config)
      ApiEndpoint.start_shared!()
    end)
  end

  defp start_api(opts \\ []) do
    opts = Keyword.merge([api_key: @key, catalog: MapCatalog.new()], opts)

    start_supervised!({SmolqueryApi.Supervisor, opts})
    on_exit(fn -> Runtime.delete(SmolqueryApi) end)

    SmolqueryApi.Endpoint.base_url()
  end

  test "serves the api over a real listener" do
    base = start_api()

    response = Req.get!(base <> "/healthz", retry: false)

    assert response.status == 200
    assert response.body == %{"status" => "ok"}
  end

  test "enforces auth over the wire" do
    base = start_api()

    assert Req.get!(base <> "/v1/datasets", retry: false).status == 401
    assert Req.get!(base <> "/v1/datasets", auth: {:bearer, @key}, retry: false).status == 200

    response = Req.get!(base <> "/v1/no/such/route", auth: {:bearer, @key}, retry: false)

    assert response.status == 404
    assert %{"error" => %{"status" => "NOT_FOUND"}} = response.body
  end

  test "refuses to boot without an api key" do
    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      SmolqueryApi.Supervisor.start_link(name: :api_no_key, catalog: MapCatalog.new())
    end
  end
end
