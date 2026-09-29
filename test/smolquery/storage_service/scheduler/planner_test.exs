defmodule Smolquery.StorageService.Scheduler.PlannerTest do
  use ExUnit.Case, async: true

  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Planner

  defp files(count, bytes), do: for(n <- 1..count//1, do: %{path: "#{n}.parquet", bytes: bytes})

  describe "by_need/3 (T-603)" do
    test "puts the table with the most span candidates first, then orders by name" do
      runtime = Runtime.new(name: __MODULE__.Need, compact_target_bytes: 1_073_741_824)
      quiet = {"bench", "a_quiet"}
      busy = {"metrics", "samples"}
      settled = {"bench", "b_settled"}
      tie = {"bench", "c_tie"}

      listings = %{
        quiet => files(3, 10),
        busy => files(50, 10) ++ files(5, 900_000_000),
        settled => files(40, 900_000_000),
        tie => files(3, 10)
      }

      assert Planner.by_need([quiet, settled, tie, busy], listings, runtime) ==
               [busy, quiet, tie, settled]
    end

    test "leaves the order alone with the span level off" do
      runtime = Runtime.new(name: __MODULE__.Off, compact_target_bytes: nil)
      tables = [{"b", "t"}, {"a", "t"}]

      assert Planner.by_need(tables, %{}, runtime) == tables
    end
  end
end
