defmodule Smolquery.PartitionsTest do
  use ExUnit.Case, async: true

  alias Smolquery.Partitions

  @table {"analytics", "events"}

  test "one partition is the bare ref, and more add reserved siblings" do
    assert Partitions.refs(@table, 1) == [@table]

    assert Partitions.refs(@table, 3) == [
             @table,
             {"analytics", "events__p1"},
             {"analytics", "events__p2"}
           ]
  end

  test "a batch with an insertId always lands on the same partition" do
    targets = for _try <- 1..20, do: Partitions.write_ref(@table, 4, "req-1")

    assert [_one] = Enum.uniq(targets)
  end

  test "id-less batches spread over every partition" do
    targets = for _try <- 1..64, uniq: true, do: Partitions.write_ref(@table, 4, nil)

    assert Enum.sort(targets) == Enum.sort(Partitions.refs(@table, 4))
  end

  test "count takes the catalog's value, never below the deployment default" do
    assert Partitions.count(nil, 1) == 1
    assert Partitions.count(nil, 3) == 3
    assert Partitions.count(6, 3) == 6
    assert Partitions.count(2, 3) == 3
  end

  test "parent inverts every partition ref, and leaves real tables alone" do
    for ref <- Partitions.refs(@table, 5) do
      assert Partitions.parent(ref) == @table
    end

    assert Partitions.parent({"analytics", "events__something"}) ==
             {"analytics", "events__something"}

    assert Partitions.parent({"analytics", "events__p0"}) == {"analytics", "events__p0"}
  end

  test "reserved? matches exactly the names refs can generate" do
    assert Partitions.reserved?("events__p1")
    assert Partitions.reserved?("events__p12")
    refute Partitions.reserved?("events")
    refute Partitions.reserved?("events__p0")
    refute Partitions.reserved?("events__page")
  end
end
