defmodule SmolqueryWeb.RuntimeTest do
  use ExUnit.Case, async: true

  alias SmolqueryWeb.Runtime

  @secret String.duplicate("s", 64)

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

    test "refuses to resolve without an auth mode" do
      assert_raise ArgumentError, ~r/SMOLQUERY_AUTH_MODE/, fn ->
        Runtime.new(name: :web_runtime_test, auth_mode: nil)
      end
    end

    test "refuses oidc and malformed auth modes without falling back" do
      assert_raise ArgumentError, ~r/not supported/, fn ->
        Runtime.new(name: :web_runtime_test, auth_mode: :oidc)
      end

      assert_raise ArgumentError, ~r/invalid value/, fn ->
        Runtime.new(name: :web_runtime_test, auth_mode: :invalid)
      end
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

    test "refuses to resolve without a session secret" do
      assert_raise ArgumentError, ~r/at least 64 bytes \(got 0\)/, fn ->
        Runtime.new(name: :web_runtime_test, secret_key_base: nil)
      end
    end

    test "refuses a session secret the cookie store would reject" do
      assert_raise ArgumentError, ~r/at least 64 bytes \(got 63\)/, fn ->
        Runtime.new(name: :web_runtime_test, secret_key_base: String.duplicate("a", 63))
      end
    end
  end

  describe "session_marker" do
    test "changes when the password rotates" do
      runtime = Runtime.new(name: :web_runtime_test, password: "p", secret_key_base: @secret)
      rotated = Runtime.new(name: :web_runtime_test, password: "q", secret_key_base: @secret)

      assert runtime.session_marker != rotated.session_marker
    end

    test "changes when the session secret rotates" do
      runtime = Runtime.new(name: :web_runtime_test, secret_key_base: @secret)
      rotated = Runtime.new(name: :web_runtime_test, secret_key_base: String.duplicate("t", 64))

      assert runtime.session_marker != rotated.session_marker
    end

    test "is stable for the same credential and secret" do
      runtime = Runtime.new(name: :web_runtime_test, secret_key_base: @secret)
      same = Runtime.new(name: :web_runtime_test, secret_key_base: @secret)

      assert runtime.session_marker == same.session_marker
    end
  end

  describe "inspect" do
    test "redacts the credential" do
      runtime =
        Runtime.new(
          name: :web_runtime_test,
          username: "operator-name-7739",
          password: "operator-secret-7739"
        )

      refute inspect(runtime) =~ "operator-name-7739"
      refute inspect(runtime) =~ "operator-secret-7739"
      refute inspect(runtime) =~ runtime.session_marker
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
