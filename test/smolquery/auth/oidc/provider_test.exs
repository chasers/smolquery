defmodule Smolquery.Auth.OIDC.ProviderTest do
  use ExUnit.Case, async: true

  alias Smolquery.Auth.OIDC.{Config, Provider}

  @metadata %{
    "issuer" => "https://issuer.example/",
    "authorization_endpoint" => "https://login.example/authorize",
    "token_endpoint" => "https://login.example/token",
    "jwks_uri" => "https://keys.example/keys",
    "id_token_signing_alg_values_supported" => ["RS256"]
  }
  @jwks %{"keys" => [%{"kid" => "one", "kty" => "RSA", "n" => "AQ", "e" => "AQAB"}]}

  test "loads discovery and JWKS before becoming available" do
    test_pid = self()
    client = client(@metadata, @jwks, test_pid)
    config = Config.new([oidc: [issuer: "https://issuer.example/", api_audience: "api"]], :api)
    name = unique_name()

    assert {:ok, pid} = Provider.start_link(name: name, config: config, http_client: client)
    assert {:ok, @metadata} = Provider.metadata(pid)
    assert {:ok, @jwks} = Provider.jwks(pid)
    assert_received {:request, "https://issuer.example/.well-known/openid-configuration"}
    assert_received {:request, "https://keys.example/keys"}
    Process.exit(pid, :normal)
  end

  test "provider outage fails startup and never returns an authorization-ready cache" do
    Process.flag(:trap_exit, true)
    client = fn _, _ -> {:error, :timeout} end
    config = Config.new([oidc: [issuer: "https://issuer.example/", api_audience: "api"]], :api)

    assert {:error, {:oidc_provider_failed, {:http_error, :timeout}}} =
             Provider.start_link(name: unique_name(), config: config, http_client: client)
  end

  test "provider calls return tagged results after a delayed injected refresh" do
    client = fn url, _options ->
      if String.ends_with?(url, "/openid-configuration"), do: Process.sleep(20)

      case url do
        "https://issuer.example/.well-known/openid-configuration" -> {:ok, response(@metadata)}
        "https://keys.example/keys" -> {:ok, response(@jwks)}
      end
    end

    config =
      Config.new(
        [oidc: [issuer: "https://issuer.example/", api_audience: "api", discovery_max_age_ms: 0]],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)
    assert {:ok, @metadata} = Provider.metadata(pid)
    Process.exit(pid, :normal)
  end

  test "metadata refresh atomically replaces keys when the JWKS URI changes" do
    metadata = Map.put(@metadata, "jwks_uri", "https://keys.example/rotated")
    rotated = %{"keys" => [%{"kid" => "two", "kty" => "RSA", "n" => "Ag", "e" => "AQAB"}]}
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          count = Agent.get_and_update(counter, fn value -> {value, value + 1} end)
          {:ok, response(if(count == 0, do: @metadata, else: metadata))}

        "https://keys.example/keys" ->
          {:ok, response(@jwks)}

        "https://keys.example/rotated" ->
          {:ok, response(rotated)}
      end
    end

    config =
      Config.new(
        [oidc: [issuer: "https://issuer.example/", api_audience: "api", discovery_max_age_ms: 0]],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)

    assert {:ok, rotated} = Provider.jwks(pid)
    refute rotated == @jwks
    assert {:ok, metadata} = Provider.metadata(pid)
    assert metadata["jwks_uri"] == "https://keys.example/rotated"
    Process.exit(pid, :normal)
  end

  test "metadata refresh observes key rotation even when the JWKS URI is unchanged" do
    rotated = %{"keys" => [%{"kid" => "two", "kty" => "RSA", "n" => "Ag", "e" => "AQAB"}]}
    {:ok, key_counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://keys.example/keys" ->
          count = Agent.get_and_update(key_counter, fn value -> {value, value + 1} end)
          {:ok, response(if(count == 0, do: @jwks, else: rotated))}
      end
    end

    config =
      Config.new(
        [oidc: [issuer: "https://issuer.example/", api_audience: "api", discovery_max_age_ms: 0]],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)

    assert {:ok, rotated} = Provider.jwks(pid)
    refute rotated == @jwks
    Process.exit(pid, :normal)
  end

  test "forced refresh returns unavailable errors and never accepts unknown keys" do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      count =
        if url == "https://keys.example/keys" do
          Agent.get_and_update(counter, fn value -> {value, value + 1} end)
        else
          0
        end

      case {url, count} do
        {"https://issuer.example/.well-known/openid-configuration", _} ->
          {:ok, response(@metadata)}

        {"https://keys.example/keys", 0} ->
          {:ok, response(@jwks)}

        {"https://keys.example/keys", _} ->
          {:error, :timeout}
      end
    end

    config =
      Config.new(
        [
          oidc: [
            issuer: "https://issuer.example/",
            api_audience: "api",
            jwks_max_age_ms: 0
          ]
        ],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)

    assert {:error, {:jwks_unavailable, {:http_error, :timeout}}} = Provider.refresh_jwks(pid)

    Process.exit(pid, :normal)
  end

  defp client(metadata, jwks, test_pid) do
    fn url, _options ->
      send(test_pid, {:request, url})

      case url do
        "https://issuer.example/.well-known/openid-configuration" -> {:ok, response(metadata)}
        "https://keys.example/keys" -> {:ok, response(jwks)}
      end
    end
  end

  defp unique_name, do: {:global, {__MODULE__, System.unique_integer([:positive])}}

  defp response(body) do
    %Req.Response{
      status: 200,
      headers: %{"content-type" => ["application/json"]},
      body: JSON.encode!(body)
    }
  end
end
