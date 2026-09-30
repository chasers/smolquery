defmodule Smolquery.StorageService.Scheduler.PlannerTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Smolquery.Engine
  alias Smolquery.Segments.Id
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Planner

  @now 1_790_000_000_000
  @settled_ms @now - 86_400_000
  @day_ms 86_400_000
  @older_day div(@now, @day_ms) * @day_ms - 3 * @day_ms
  @newer_day @older_day + @day_ms

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

  describe "plan/6 on the span lane (T-606)" do
    @describetag :tmp_dir

    setup context do
      name = :"planner_#{System.unique_integer([:positive])}"
      start_supervised!({Engine, name: Runtime.compact_engine(name)})

      runtime =
        Runtime.new(name: name, merge_inputs_per_call: 1, compact_span_decoded_bytes: 1 <<< 40)

      %{runtime: runtime, dir: context.tmp_dir}
    end

    test "sizes the oldest files by name past the valve, whatever the catalog's order", context do
      sealed =
        for n <- 1..80 do
          partition = if rem(n, 2) == 1, do: "samples__p1", else: "samples__p2"
          seal(context, partition, @older_day + n * 1_000, 1)
        end

      {first, second} = Enum.split_with(sealed, &(&1.path =~ "samples__p1"))

      assert {:ok, %{paths: paths}} = plan(context.runtime, first ++ second)
      assert paths == sealed |> Enum.take(64) |> Enum.map(& &1.path)
    end

    test "sizes each span alone, so a span that forms no group leaves the next its turn",
         context do
      runtime = %{context.runtime | compact_span_decoded_bytes: 30_000}
      for n <- 1..70, do: seal(context, "samples", @older_day + n * 1_000, 20)
      newer = for n <- 1..3, do: seal(context, "samples", @newer_day + n * 1_000, 1)
      listing = Path.wildcard(Path.join(context.dir, "samples/*.parquet"))

      assert {:ok, %{paths: paths}} =
               plan(runtime, Enum.map(listing, &%{path: &1, bytes: 0}), 1_000)

      assert paths == Enum.map(newer, & &1.path)
    end

    test "merges a span's small files among themselves instead of rewriting its big one",
         context do
      big = seal(context, "samples", @older_day + 1_000, 10_000)
      small = for n <- 2..4, do: seal(context, "samples", @older_day + n * 1_000, 10)

      assert {:ok, %{paths: paths, row_count: 30}} = plan(context.runtime, [big | small])
      assert paths == Enum.map(small, & &1.path)
    end

    test "yields to the next span when only its big file would grow", context do
      big = seal(context, "samples", @older_day + 1_000, 10_000)
      lone = seal(context, "samples", @older_day + 2_000, 10)
      newer = for n <- 1..2, do: seal(context, "samples", @newer_day + n * 1_000, 1)

      assert {:ok, %{paths: paths}} = plan(context.runtime, [big, lone | newer])
      assert paths == Enum.map(newer, & &1.path)
    end

    test "folds the big file in once the small files add a tenth of its rows", context do
      big = seal(context, "samples", @older_day + 1_000, 1_000)
      small = for n <- 2..3, do: seal(context, "samples", @older_day + n * 1_000, 50)

      assert {:ok, %{paths: paths, row_count: 1_100}} = plan(context.runtime, [big | small])
      assert paths == Enum.map([big | small], & &1.path)
    end
  end

  defp plan(runtime, files, learned_width \\ nil) do
    planning = %{
      routing: Routing.resolve(runtime.name),
      quarantined_groups: MapSet.new(),
      learned_width: learned_width,
      files: files
    }

    Planner.plan(runtime, planning, {"metrics", "samples"}, 1 <<< 40, @now, :span)
  end

  defp seal(context, directory, at_ms, rows) do
    path = Path.join([context.dir, directory, "#{Id.generate(at_ms)}.parquet"])
    File.mkdir_p!(Path.dirname(path))

    Engine.query!(
      Runtime.compact_engine(context.runtime.name),
      "COPY (SELECT range AS id FROM range(#{rows})) TO '#{String.replace(path, "'", "''")}' (FORMAT PARQUET)"
    )

    %{path: path, bytes: 0}
  end
end
