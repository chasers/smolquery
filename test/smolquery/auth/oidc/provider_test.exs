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
  @public_key JOSE.JWK.generate_key({:rsa, 2048})
              |> JOSE.JWK.to_public()
              |> JOSE.JWK.to_map()
              |> elem(1)
              |> Map.put("kid", "one")
  @rotated_public_key JOSE.JWK.generate_key({:rsa, 2048})
                      |> JOSE.JWK.to_public()
                      |> JOSE.JWK.to_map()
                      |> elem(1)
                      |> Map.put("kid", "two")
  @jwks %{"keys" => [@public_key]}
  @rotated_jwks %{"keys" => [@rotated_public_key]}

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

  test "provider outage or unusable keys fail startup" do
    Process.flag(:trap_exit, true)
    config = Config.new([oidc: [issuer: "https://issuer.example/", api_audience: "api"]], :api)

    assert {:error, {:oidc_provider_failed, {:http_error, :timeout}}} =
             Provider.start_link(
               name: unique_name(),
               config: config,
               http_client: fn _, _ -> {:error, :timeout} end
             )

    ec_only = %{
      "keys" => [
        %{
          "kid" => "ec",
          "kty" => "EC",
          "crv" => "P-256",
          "x" => Base.url_encode64(:binary.copy(<<1>>, 32), padding: false),
          "y" => Base.url_encode64(:binary.copy(<<2>>, 32), padding: false)
        }
      ]
    }

    assert {:error, {:oidc_provider_failed, :jwks_no_compatible_signing_key}} =
             Provider.start_link(
               name: unique_name(),
               config: config,
               http_client: client(@metadata, ec_only, self())
             )

    mixed_config = %{config | algorithms: ["RS256", "ES256"]}
    mismatched = %{"keys" => [Map.put(hd(@jwks["keys"]), "alg", "ES256")]}

    assert {:error, {:oidc_provider_failed, :jwks_no_compatible_signing_key}} =
             Provider.start_link(
               name: unique_name(),
               config: mixed_config,
               http_client: client(@metadata, mismatched, self())
             )

    assert {:error, {:oidc_provider_failed, :jwks_duplicate_kid}} =
             Provider.start_link(
               name: unique_name(),
               config: config,
               http_client: client(@metadata, %{"keys" => @jwks["keys"] ++ @jwks["keys"]}, self())
             )
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
    rotated = @rotated_jwks
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
    rotated = @rotated_jwks
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

  test "forced refresh accepts rotation once and then reuses the cache during cooldown" do
    rotated = @rotated_jwks
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://keys.example/keys" ->
          count = Agent.get_and_update(counter, fn value -> {value, value + 1} end)
          {:ok, response(if(count == 0, do: @jwks, else: rotated))}
      end
    end

    config =
      Config.new(
        [
          oidc: [
            issuer: "https://issuer.example/",
            api_audience: "api",
            forced_refresh_cooldown_ms: 100_000
          ]
        ],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)

    assert {:ok, ^rotated} = Provider.refresh_jwks(pid)
    assert {:ok, ^rotated} = Provider.refresh_jwks(pid)
    assert Agent.get(counter, & &1) == 2
    Process.exit(pid, :normal)
  end

  test "concurrent forced refreshes perform at most one network fetch per cooldown" do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://keys.example/keys" ->
          Agent.update(counter, &(&1 + 1))
          Process.sleep(10)
          {:ok, response(@jwks)}
      end
    end

    config =
      Config.new(
        [
          oidc: [
            issuer: "https://issuer.example/",
            api_audience: "api",
            forced_refresh_cooldown_ms: 100_000
          ]
        ],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)

    results =
      1..8
      |> Enum.map(fn _ -> Task.async(fn -> Provider.refresh_jwks(pid) end) end)
      |> Enum.map(&Task.await(&1, 5_000))

    assert Enum.all?(results, &match?({:ok, @jwks}, &1))
    assert Agent.get(counter, & &1) == 2
    Process.exit(pid, :normal)
  end

  test "fresh cache reads remain responsive while a forced refresh is in flight" do
    test = self()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://keys.example/keys" ->
          case Agent.get_and_update(counter, fn value -> {value, value + 1} end) do
            0 ->
              {:ok, response(@jwks)}

            _ ->
              send(test, {:refresh_started, self()})

              receive do
                :finish_refresh -> {:ok, response(@jwks)}
              end
          end
      end
    end

    config = Config.new([oidc: [issuer: "https://issuer.example/", api_audience: "api"]], :api)
    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)
    refresh = Task.async(fn -> Provider.refresh_jwks(pid) end)

    assert_receive {:refresh_started, worker}
    assert {:ok, @jwks} = Provider.jwks(pid)
    send(worker, :finish_refresh)
    assert {:ok, @jwks} = Task.await(refresh)
    Process.exit(pid, :normal)
  end

  test "stopping the provider terminates an in-flight refresh worker" do
    test = self()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    client = fn url, _options ->
      case url do
        "https://issuer.example/.well-known/openid-configuration" ->
          {:ok, response(@metadata)}

        "https://keys.example/keys" ->
          case Agent.get_and_update(counter, fn value -> {value, value + 1} end) do
            0 ->
              {:ok, response(@jwks)}

            _ ->
              send(test, {:refresh_worker, self()})

              receive do
                :blocked -> {:ok, response(@jwks)}
              end
          end
      end
    end

    config = Config.new([oidc: [issuer: "https://issuer.example/", api_audience: "api"]], :api)
    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)
    spawn(fn -> Provider.refresh_jwks(pid) end)

    assert_receive {:refresh_worker, worker}
    monitor = Process.monitor(worker)
    GenServer.stop(pid)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}
  end

  test "failed refreshes are backoff-limited and never return a stale success" do
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
          Process.sleep(20)
          {:error, :timeout}
      end
    end

    config =
      Config.new(
        [
          oidc: [
            issuer: "https://issuer.example/",
            api_audience: "api",
            jwks_max_age_ms: 0,
            forced_refresh_cooldown_ms: 1,
            refresh_failure_backoff_ms: 100_000
          ]
        ],
        :api
      )

    {:ok, pid} = Provider.start_link(name: unique_name(), config: config, http_client: client)

    assert {:error, {:jwks_unavailable, {:http_error, :timeout}}} = Provider.refresh_jwks(pid)
    assert {:error, {:jwks_unavailable, {:http_error, :timeout}}} = Provider.jwks(pid)
    assert Agent.get(counter, & &1) == 2

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
