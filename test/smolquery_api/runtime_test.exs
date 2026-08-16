defmodule SmolqueryApi.RuntimeTest do
  use ExUnit.Case, async: true

  alias SmolqueryApi.Runtime

  describe "new/1" do
    test "resolves options over application config" do
      runtime = Runtime.new(name: :api_runtime_test, api_key: "k", load_max_bytes: 1024)

      assert runtime.name == :api_runtime_test
      assert runtime.api_key == "k"
      assert runtime.load_max_bytes == 1024
    end

    test "defaults the instance name" do
      assert Runtime.new(api_key: "k").name == SmolqueryApi
    end

    test "refuses to resolve without an api_key" do
      assert_raise ArgumentError, ~r/refuses to boot without an API key/, fn ->
        Runtime.new(name: :api_runtime_test)
      end
    end

    test "refuses an empty api_key" do
      assert_raise ArgumentError, fn ->
        Runtime.new(name: :api_runtime_test, api_key: "")
      end
    end
  end

  describe "publication" do
    test "put/fetch/delete round-trip" do
      runtime = Runtime.new(name: :api_runtime_roundtrip, api_key: "k")

      Runtime.put(runtime)
      assert Runtime.fetch(:api_runtime_roundtrip) == {:ok, runtime}

      Runtime.delete(:api_runtime_roundtrip)
      assert Runtime.fetch(:api_runtime_roundtrip) == :error
    end
  end

  describe "insert_max_in_flight_bytes/2 (T-245)" do
    test "an explicit limit wins over the cgroup" do
      runtime =
        Runtime.new(name: :api_admission_explicit, api_key: "k", insert_max_in_flight_bytes: 42)

      assert Runtime.insert_max_in_flight_bytes(runtime, {:ok, 4_294_967_296}) == 42
    end

    test "derives a quarter of the cgroup limit" do
      runtime = Runtime.new(name: :api_admission_derived, api_key: "k")

      assert Runtime.insert_max_in_flight_bytes(runtime, {:ok, 4_294_967_296}) == 1_073_741_824
    end

    test "a tiny cgroup limit still admits one NDJSON body" do
      runtime = Runtime.new(name: :api_admission_floor, api_key: "k")

      assert Runtime.insert_max_in_flight_bytes(runtime, {:ok, 1_000_000}) == 8_000_000
    end

    test "without a cgroup limit the fallback applies" do
      runtime = Runtime.new(name: :api_admission_fallback, api_key: "k")

      assert Runtime.insert_max_in_flight_bytes(runtime, :none) == 268_435_456
    end

    test "an explicit non-positive limit refuses to boot" do
      assert_raise ArgumentError, ~r/insert_max_in_flight_bytes/, fn ->
        Runtime.new(name: :api_admission_zero, api_key: "k", insert_max_in_flight_bytes: 0)
      end
    end

    test "an explicit non-integer limit refuses to boot" do
      assert_raise ArgumentError, ~r/insert_max_in_flight_bytes/, fn ->
        Runtime.new(
          name: :api_admission_string,
          api_key: "k",
          insert_max_in_flight_bytes: "256MB"
        )
      end
    end
  end
end
