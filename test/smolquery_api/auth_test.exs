defmodule SmolqueryApi.AuthTest do
  use ExUnit.Case, async: true

  alias SmolqueryApi.Auth

  describe "matches?/2" do
    test "a presented credential matches only the expected one" do
      assert Auth.matches?({:ok, "the-key"}, "the-key")
      refute Auth.matches?({:ok, "the-kez"}, "the-key")
      refute Auth.matches?({:ok, ""}, "the-key")
    end

    test "nothing presented never matches" do
      refute Auth.matches?(:error, "the-key")
    end
  end
end
