defmodule Smolquery.Cluster.RpcProbeTest do
  use ExUnit.Case, async: true

  alias Smolquery.Cluster.RpcProbe

  test "answers :pong" do
    assert RpcProbe.pong() == :pong
  end
end
