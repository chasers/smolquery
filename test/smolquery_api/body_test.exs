defmodule SmolqueryApi.BodyTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias SmolqueryApi.Body

  test "reads a body within the limit whole" do
    assert {:ok, "hello world", %Plug.Conn{}} = Body.read(conn(:post, "/", "hello world"), 11)
  end

  test "refuses a body past the limit" do
    assert Body.read(conn(:post, "/", String.duplicate("x", 12)), 11) == {:error, :too_large}
  end
end
