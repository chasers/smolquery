defmodule SmolqueryApi.BodyTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias SmolqueryApi.Body

  test "reads a body within the limit whole" do
    assert {:ok, "hello world", %Plug.Conn{}} = Body.read(conn(:post, "/", "hello world"), 11)
  end

  test "refuses a body past the limit with the conn it read" do
    conn = conn(:post, "/", String.duplicate("x", 12))

    assert {:error, :too_large, %Plug.Conn{} = read} = Body.read(conn, 11)
    assert read.adapter != conn.adapter
  end
end
