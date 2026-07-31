defmodule Smolquery.BufferService.RingTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Ring

  @nodes [:buffer1@host, :buffer2@host, :buffer3@host, :buffer4@host]

  defp keys(count), do: for(i <- 1..count, do: {"analytics", "events-#{i}"})

  defp distribution(ring, keys) do
    Enum.frequencies_by(keys, &Ring.owner(ring, &1))
  end

  describe "new/1" do
    test "refuses a ring with no nodes" do
      assert Ring.new([]) == {:error, :empty_ring}
    end

    test "collapses duplicates and ignores order" do
      {:ok, one} = Ring.new([:a, :b, :a])
      {:ok, two} = Ring.new([:b, :a])

      assert Ring.nodes(one) == [:a, :b]
      assert one == two
    end
  end

  describe "new!/1" do
    test "raises on an empty node list" do
      assert_raise ArgumentError, ~r/empty_ring/, fn -> Ring.new!([]) end
    end
  end

  describe "owner/2" do
    test "sends everything to the only node in a single-node ring" do
      ring = Ring.new!([node()])

      assert Enum.all?(keys(100), &(Ring.owner(ring, &1) == node()))
    end

    test "is deterministic for the same ring and key" do
      ring = Ring.new!(@nodes)

      for key <- keys(50) do
        assert Ring.owner(ring, key) == Ring.owner(ring, key)
      end
    end

    test "does not depend on how the node list was ordered" do
      ring = Ring.new!(@nodes)
      shuffled = Ring.new!(Enum.reverse(@nodes))

      for key <- keys(50) do
        assert Ring.owner(ring, key) == Ring.owner(shuffled, key)
      end
    end

    test "spreads keys across the fleet" do
      ring = Ring.new!(@nodes)
      keys = keys(20_000)

      counts = distribution(ring, keys)

      assert map_size(counts) == length(@nodes)

      for {_node, count} <- counts do
        assert count > 0.15 * length(keys)
        assert count < 0.35 * length(keys)
      end
    end

    test "accepts any term as a routing key, so partition-level ownership fits" do
      ring = Ring.new!(@nodes)

      assert Ring.owner(ring, {"analytics", "events"}) in @nodes
      assert Ring.owner(ring, {"analytics", "events", 7}) in @nodes
      assert Ring.owner(ring, "analytics/events") in @nodes
    end

    test "moves roughly one node's share when the fleet grows" do
      ring = Ring.new!(@nodes)
      grown = Ring.new!([:buffer5@host | @nodes])
      keys = keys(5_000)

      moved = Enum.count(keys, &(Ring.owner(ring, &1) != Ring.owner(grown, &1)))

      assert moved > 0
      assert moved < 0.35 * length(keys)
    end

    test "only moves the lost node's keys when the fleet shrinks" do
      ring = Ring.new!(@nodes)
      lost = :buffer4@host
      shrunk = Ring.new!(@nodes -- [lost])

      for key <- keys(5_000), Ring.owner(ring, key) != lost do
        assert Ring.owner(shrunk, key) == Ring.owner(ring, key)
      end
    end
  end

  describe "own?/2" do
    test "is true when this node owns the key" do
      ring = Ring.new!([node()])

      assert Ring.own?(ring, {"analytics", "events"})
    end

    test "is false for a ring this node is not part of" do
      ring = Ring.new!(@nodes)

      refute Ring.own?(ring, {"analytics", "events"})
    end
  end
end
