defmodule Smolquery.StorageService.Scheduler.PlannerTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Id
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Planner

  @now 1_790_000_000_000
  @settled_ms @now - 86_400_000

  defp files(count, bytes, at_ms),
    do: for(n <- 1..count//1, do: %{path: "#{Id.generate(at_ms + n)}.parquet", bytes: bytes})

  describe "by_need/4 (T-603)" do
    test "puts the table with the most settled span candidates first, then orders by name" do
      runtime = Runtime.new(name: __MODULE__.Need, compact_target_bytes: 1_073_741_824)
      quiet = {"bench", "a_quiet"}
      busy = {"metrics", "samples"}
      settled_big = {"bench", "b_settled_big"}
      recent = {"bench", "c_recent"}
      tie = {"bench", "d_tie"}

      listings = %{
        quiet => files(3, 10, @settled_ms),
        busy => files(50, 10, @settled_ms) ++ files(5, 900_000_000, @settled_ms),
        settled_big => files(40, 900_000_000, @settled_ms),
        recent => files(2_000, 10, @now - 60_000),
        tie => files(3, 10, @settled_ms)
      }

      assert Planner.by_need([quiet, settled_big, recent, tie, busy], listings, runtime, @now) ==
               [busy, quiet, tie, settled_big, recent]
    end

    test "leaves the order alone with the span level off" do
      runtime = Runtime.new(name: __MODULE__.Off, compact_target_bytes: nil)
      tables = [{"b", "t"}, {"a", "t"}]

      assert Planner.by_need(tables, %{}, runtime) == tables
    end
  end
end
