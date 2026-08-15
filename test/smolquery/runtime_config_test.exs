defmodule Smolquery.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias Smolquery.RuntimeConfig

  doctest RuntimeConfig

  test "parses positive integers" do
    assert RuntimeConfig.positive_integer!("VALUE", "42") == 42
  end

  test "rejects malformed, zero, and negative integers" do
    for value <- ["nope", "0", "-1", "1.5"] do
      assert_raise ArgumentError, ~r/VALUE.*#{Regex.escape(value)}.*at least 1/, fn ->
        RuntimeConfig.positive_integer!("VALUE", value)
      end
    end
  end

  test "parses integers with an explicit lower bound" do
    assert RuntimeConfig.integer_at_least!("REPLICATION", "2", 2) == 2

    for value <- ["0", "1"] do
      assert_raise ArgumentError, ~r/REPLICATION.*#{value}.*at least 2/, fn ->
        RuntimeConfig.integer_at_least!("REPLICATION", value, 2)
      end
    end
  end

  test "parses non-negative integers" do
    assert RuntimeConfig.non_negative_integer!("COUNT", "0") == 0
    assert RuntimeConfig.non_negative_integer!("COUNT", "42") == 42

    assert_raise ArgumentError, ~r/COUNT.*-1.*at least 0/, fn ->
      RuntimeConfig.non_negative_integer!("COUNT", "-1")
    end
  end

  test "parses explicitly bounded integers" do
    assert RuntimeConfig.integer_in_range!("POOL", "1", 1, 32) == 1
    assert RuntimeConfig.integer_in_range!("POOL", "32", 1, 32) == 32

    assert_raise ArgumentError, ~r/POOL.*33.*1\.\.32/, fn ->
      RuntimeConfig.integer_in_range!("POOL", "33", 1, 32)
    end
  end

  test "parses bounded ports" do
    assert RuntimeConfig.port!("PORT", "1") == 1
    assert RuntimeConfig.port!("PORT", "65535") == 65_535
  end

  test "rejects ports outside the valid range" do
    for value <- ["0", "65536", "bad"] do
      assert_raise ArgumentError, ~r/PORT.*#{Regex.escape(value)}.*1\.\.65535/, fn ->
        RuntimeConfig.port!("PORT", value)
      end
    end
  end

  test "parses IP addresses" do
    assert RuntimeConfig.ip!("IP", "127.0.0.1") == {127, 0, 0, 1}
    assert RuntimeConfig.ip!("IP", "::1") == {0, 0, 0, 0, 0, 0, 0, 1}
  end

  test "rejects malformed IP addresses" do
    assert_raise ArgumentError, ~r/IP.*not-an-ip.*IPv4 or IPv6 address/, fn ->
      RuntimeConfig.ip!("IP", "not-an-ip")
    end
  end

  test "parses positive integers or infinity" do
    assert RuntimeConfig.positive_integer_or_infinity!("ROWS", "12") == 12
    assert RuntimeConfig.positive_integer_or_infinity!("ROWS", "infinity") == :infinity

    assert_raise ArgumentError, ~r/ROWS.*0.*at least 1/, fn ->
      RuntimeConfig.positive_integer_or_infinity!("ROWS", "0")
    end
  end

  test "parses release booleans" do
    assert RuntimeConfig.boolean!("TLS", "true")
    assert RuntimeConfig.boolean!("TLS", "1")
    refute RuntimeConfig.boolean!("TLS", "false")
    refute RuntimeConfig.boolean!("TLS", "0")

    assert_raise ArgumentError, ~r/TLS.*maybe.*true, 1, false, or 0/, fn ->
      RuntimeConfig.boolean!("TLS", "maybe")
    end
  end

  test "parses OIDC bounded values and claim mappings" do
    assert RuntimeConfig.csv!("ALGORITHMS", "RS256, ES256") == ["RS256", "ES256"]
    assert RuntimeConfig.bounded_non_negative_integer!("SKEW", "30", 300) == 30

    assert RuntimeConfig.string_lists!("REQUIRED", ~s({"token_use":["access"]})) == %{
             "token_use" => ["access"]
           }

    assert RuntimeConfig.capability_mapping!(
             "CLAIMS",
             ~s({"roles":{"reader":["query"],"operator":["web_access","platform_operate"]}})
           ) == %{
             "roles" => %{"reader" => [:query], "operator" => [:web_access, :platform_operate]}
           }

    for {function, value} <- [
          {&RuntimeConfig.csv!("ALGORITHMS", &1), ""},
          {&RuntimeConfig.string_lists!("REQUIRED", &1), "[]"},
          {&RuntimeConfig.capability_mapping!("CLAIMS", &1), ""},
          {&RuntimeConfig.capability_mapping!("CLAIMS", &1), "roles=unknown"}
        ] do
      assert_raise ArgumentError, ~r/ALGORITHMS|CLAIMS|REQUIRED/, fn -> function.(value) end
    end
  end

  test "maps supported enum values without creating atoms" do
    choices = [{"polars", :polars}, {"duckdb", :duckdb}]

    assert RuntimeConfig.enum!("WRITER", "duckdb", choices) == :duckdb

    assert_raise ArgumentError, ~r/WRITER.*other.*polars, duckdb/, fn ->
      RuntimeConfig.enum!("WRITER", "other", choices)
    end
  end

  test "parses bounded node names without creating atoms" do
    assert RuntimeConfig.node_names!(
             "NODES",
             "smolquery@buffer-0.example, smolquery@buffer-1.example"
           ) == ["smolquery@buffer-0.example", "smolquery@buffer-1.example"]

    assert_raise ArgumentError, ~r/NODES.*name@host/, fn ->
      RuntimeConfig.node_names!("NODES", "not-a-node")
    end
  end

  test "runtime config parses OIDC settings under the OIDC config namespace" do
    with_env(
      %{
        "SMOLQUERY_OIDC_ISSUER" => "https://issuer.example/",
        "SMOLQUERY_OIDC_API_AUDIENCE" => "api",
        "SMOLQUERY_OIDC_ALGORITHMS" => "RS256,PS256",
        "SMOLQUERY_OIDC_CLOCK_SKEW" => "30",
        "SMOLQUERY_OIDC_CLAIM_CAPABILITIES" =>
          ~s({"roles":{"reader":["query"],"operator":["web_access"]}}),
        "SMOLQUERY_OIDC_TOKEN_TYPES" => "at+jwt",
        "SMOLQUERY_OIDC_REQUIRED_CLAIMS" => ~s({"token_use":["access"]}),
        "SMOLQUERY_OIDC_MAX_TOKEN_BYTES" => "2048",
        "SMOLQUERY_OIDC_MAX_TOKEN_SEGMENT_BYTES" => "1024",
        "SMOLQUERY_OIDC_IAT_FUTURE_SECONDS" => "60",
        "SMOLQUERY_OIDC_FORCED_REFRESH_COOLDOWN_MS" => "250"
      },
      fn ->
        runtime = Config.Reader.read!("config/runtime.exs", env: :prod, target: :host)
        oidc = Keyword.fetch!(Keyword.fetch!(runtime, :smolquery), Smolquery.Auth.OIDC.Config)

        assert oidc[:issuer] == "https://issuer.example/"
        assert oidc[:algorithms] == ["RS256", "PS256"]
        assert oidc[:typ_allowlist] == ["at+jwt"]
        assert oidc[:required_claims] == %{"token_use" => ["access"]}
        assert oidc[:max_token_bytes] == 2048
        assert oidc[:max_segment_bytes] == 1024
        assert oidc[:iat_future_seconds] == 60
        assert oidc[:forced_refresh_cooldown_ms] == 250

        assert oidc[:claim_capabilities] == %{
                 "roles" => %{"reader" => [:query], "operator" => [:web_access]}
               }
      end
    )
  end

  test "runtime config applies parsed values without creating node atoms" do
    with_env(
      %{
        "SMOLQUERY_API_PORT" => "65535",
        "SMOLQUERY_BUFFER_NODES" => "smolquery@buffer-0.example",
        "SMOLQUERY_BUFFER_REPLICATION" => "2"
      },
      fn ->
        runtime = Config.Reader.read!("config/runtime.exs", env: :prod, target: :host)
        smolquery = Keyword.fetch!(runtime, :smolquery)

        assert get_in(smolquery, [SmolqueryApi.Endpoint, :http, :port]) == 65_535

        buffer = Keyword.fetch!(smolquery, Smolquery.BufferService)
        assert buffer[:expected_node_names] == ["smolquery@buffer-0.example"]

        assert buffer[:replicator] ==
                 {Smolquery.BufferService.Replicator.SegmentShipping, replication_factor: 2}
      end
    )
  end

  test "runtime config reports the actual replication and port bounds" do
    with_env(%{"SMOLQUERY_BUFFER_REPLICATION" => "1"}, fn ->
      assert_raise ArgumentError, ~r/SMOLQUERY_BUFFER_REPLICATION.*at least 2/, fn ->
        Config.Reader.read!("config/runtime.exs", env: :prod, target: :host)
      end
    end)

    with_env(%{"SMOLQUERY_API_PORT" => "0"}, fn ->
      assert_raise ArgumentError, ~r/SMOLQUERY_API_PORT.*1\.\.65535/, fn ->
        Config.Reader.read!("config/runtime.exs", env: :prod, target: :host)
      end
    end)
  end

  defp with_env(values, function) do
    previous = Map.new(values, fn {name, _value} -> {name, System.get_env(name)} end)
    Enum.each(values, fn {name, value} -> System.put_env(name, value) end)

    try do
      function.()
    after
      Enum.each(previous, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end
  end
end
