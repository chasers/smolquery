defmodule Smolquery.BufferService.Transport.GenRpcDestinationTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Transport.GenRpc

  @node :"buffer1@elsewhere.invalid"

  describe "destination/2" do
    test "control is one connection" do
      assert GenRpc.destination(@node, :control) == {@node, :control}
    end

    test "a bulk key always lands on the same slot" do
      table = {"analytics", "events"}

      assert {@node, {:bulk, slot}} = GenRpc.destination(@node, {:bulk, table})
      assert slot in 1..GenRpc.bulk_channels()
      assert GenRpc.destination(@node, {:bulk, table}) == {@node, {:bulk, slot}}
    end

    test "distinct keys spread over the pool" do
      slots =
        for i <- 1..64, uniq: true do
          {@node, {:bulk, slot}} = GenRpc.destination(@node, {:bulk, {"analytics", "t#{i}"}})
          slot
        end

      assert [_, _ | _] = slots
      assert Enum.all?(slots, &(&1 in 1..GenRpc.bulk_channels()))
    end
  end

  describe "invoke/5" do
    test "a node outside the cluster is refused before any connect" do
      assert GenRpc.invoke(@node, :control, :flush, [:never, {"a", "b"}], 5_000) ==
               {:error, {:badrpc, :nodedown}}
    end
  end
end
