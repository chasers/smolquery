defmodule SmolqueryApi.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.OIDCProvider
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
    on_exit(fn -> Runtime.delete(Keyword.get(opts, :name, SmolqueryApi)) end)

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

  test "does not start the endpoint when the OIDC provider fails to boot" do
    name = :api_oidc_boot_failure
    Process.flag(:trap_exit, true)

    assert {:error, {:shutdown, _reason}} =
             SmolqueryApi.Supervisor.start_link(oidc_options(name, OIDCProvider.failing_client()))

    refute Process.whereis(SmolqueryApi.Endpoint)
    refute Process.whereis(Module.concat(name, "OIDCProvider"))
    Runtime.delete(name)
  end

  test "restarts the endpoint subtree when the OIDC provider dies" do
    name = :api_oidc_restart
    start_api(oidc_options(name, OIDCProvider.client()))
    provider_name = Module.concat(name, "OIDCProvider")
    provider = Process.whereis(provider_name)
    endpoint = Process.whereis(SmolqueryApi.Endpoint)

    Process.exit(provider, :kill)

    assert Eventually.until(fn ->
             replacement_provider = Process.whereis(provider_name)
             replacement_endpoint = Process.whereis(SmolqueryApi.Endpoint)

             is_pid(replacement_provider) and replacement_provider != provider and
               is_pid(replacement_endpoint) and replacement_endpoint != endpoint
           end)

    replacement_base = SmolqueryApi.Endpoint.base_url()
    assert Req.get!(replacement_base <> "/healthz", retry: false).status == 200
  end

  defp oidc_options(name, client) do
    [
      name: name,
      auth_mode: :oidc,
      catalog: MapCatalog.new(),
      oidc_provider_http_client: client,
      oidc: [issuer: "https://issuer.example", api_audience: "smolquery-api"]
    ]
  end
end
