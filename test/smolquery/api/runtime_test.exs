defmodule Smolquery.Api.RuntimeTest do
  use ExUnit.Case, async: true

  alias Smolquery.Api.Runtime

  describe "new/1" do
    test "resolves options over application config" do
      runtime = Runtime.new(name: :api_runtime_test, api_key: "k", port: 8080)

      assert runtime.name == :api_runtime_test
      assert runtime.api_key == "k"
      assert runtime.ip == {127, 0, 0, 1}
      assert runtime.port == 8080
    end

    test "defaults the instance name" do
      assert Runtime.new(api_key: "k").name == Smolquery.Api
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
end
