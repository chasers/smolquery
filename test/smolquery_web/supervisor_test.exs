defmodule SmolqueryWeb.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.Eventually
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.OIDCProvider
  alias SmolqueryWeb.Runtime

  test "refuses to start without a session secret" do
    assert_raise ArgumentError, ~r/at least 64 bytes \(got 0\)/, fn ->
      SmolqueryWeb.Supervisor.start_link(catalog: MapCatalog.new(), secret_key_base: nil)
    end
  end

  test "does not start the endpoint when the OIDC provider fails to boot" do
    name = :web_oidc_boot_failure
    Process.flag(:trap_exit, true)

    assert {:error, {:shutdown, _reason}} =
             SmolqueryWeb.Supervisor.start_link(oidc_options(name, OIDCProvider.failing_client()))

    refute Process.whereis(SmolqueryWeb.Endpoint)
    refute Process.whereis(Module.concat(name, "OIDCProvider"))
    Runtime.delete(name)
  end

  test "restarts the endpoint subtree when the OIDC provider dies" do
    name = :web_oidc_restart

    start_supervised!({SmolqueryWeb.Supervisor, oidc_options(name, OIDCProvider.client())})
    on_exit(fn -> Runtime.delete(name) end)

    provider_name = Module.concat(name, "OIDCProvider")
    provider = Process.whereis(provider_name)
    endpoint = Process.whereis(SmolqueryWeb.Endpoint)

    Process.exit(provider, :kill)

    assert Eventually.until(fn ->
             replacement_provider = Process.whereis(provider_name)
             replacement_endpoint = Process.whereis(SmolqueryWeb.Endpoint)

             is_pid(replacement_provider) and replacement_provider != provider and
               is_pid(replacement_endpoint) and replacement_endpoint != endpoint
           end)
  end

  defp oidc_options(name, client) do
    [
      name: name,
      auth_mode: :oidc,
      catalog: MapCatalog.new(),
      secret_key_base: String.duplicate("s", 64),
      web_host: "ui.example",
      oidc_provider_http_client: client,
      oidc: [
        issuer: "https://issuer.example",
        web_client_id: "smolquery-web",
        web_client_secret: "secret",
        web_origin: "https://ui.example",
        web_redirect_uri: "https://ui.example/auth/callback"
      ]
    ]
  end
end
