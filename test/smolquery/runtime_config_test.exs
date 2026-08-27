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

  test "maps supported enum values without creating atoms" do
    choices = [{"zstd", :zstd}, {"snappy", :snappy}]

    assert RuntimeConfig.enum!("CODEC", "snappy", choices) == :snappy

    assert_raise ArgumentError, ~r/CODEC.*other.*zstd, snappy/, fn ->
      RuntimeConfig.enum!("CODEC", "other", choices)
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
