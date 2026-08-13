defmodule SmolqueryWeb.RuntimeTest do
  use ExUnit.Case, async: true

  alias SmolqueryWeb.Runtime

  describe "new/1" do
    test "resolves options over application config" do
      runtime = Runtime.new(name: :web_runtime_test, username: "u", password: "p")

      assert runtime.name == :web_runtime_test
      assert runtime.username == "u"
      assert runtime.password == "p"
    end

    test "defaults the instance name" do
      assert Runtime.new().name == SmolqueryWeb
    end

    test "refuses to resolve without a username" do
      assert_raise ArgumentError, ~r/SMOLQUERY_WEB_USERNAME/, fn ->
        Runtime.new(name: :web_runtime_test, username: nil)
      end
    end

    test "refuses to resolve without a password" do
      assert_raise ArgumentError, ~r/SMOLQUERY_WEB_PASSWORD/, fn ->
        Runtime.new(name: :web_runtime_test, password: nil)
      end
    end

    test "refuses an empty credential" do
      assert_raise ArgumentError, fn ->
        Runtime.new(name: :web_runtime_test, password: "")
      end
    end
  end

  describe "publication" do
    test "put/fetch/delete round-trip" do
      runtime = Runtime.new(name: :web_runtime_roundtrip, username: "u", password: "p")

      Runtime.put(runtime)
      assert Runtime.fetch(:web_runtime_roundtrip) == {:ok, runtime}

      Runtime.delete(:web_runtime_roundtrip)
      assert Runtime.fetch(:web_runtime_roundtrip) == :error
    end
  end
end
