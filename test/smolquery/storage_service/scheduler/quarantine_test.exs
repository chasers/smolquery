defmodule Smolquery.StorageService.Scheduler.QuarantineTest do
  @moduledoc "The groups a node stops planning because their inputs read as corrupt."

  use ExUnit.Case, async: true

  alias Smolquery.Engine.CallExited
  alias Smolquery.StorageService.Scheduler.Quarantine

  @table {"analytics", "events"}

  defp oom_failure(table) do
    {:failed,
     %{
       table: table,
       reason:
         {:put_failed, "analytics/events/x.parquet",
          {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}}}
     }}
  end

  defp compacted(table, rows) do
    {:ok, %{table: table, key: "k", replaced: 2, rows: rows, snapshot: 1}}
  end

  describe "adjusted_quarantine/4 (T-310)" do
    @paths ["analytics/events/a.parquet", "analytics/events/b.parquet"]

    defp corrupt_failure(table, paths) do
      {:failed,
       %{table: table, reason: {:sizing_failed, %Adbc.Error{message: "bad footer"}}, paths: paths}}
    end

    test "a failure below the threshold only counts, it does not quarantine" do
      {:failed, %{reason: reason}} = corrupt_failure(@table, @paths)

      {quarantine, quarantined} =
        Quarantine.adjusted_quarantine(%{}, MapSet.new(), [corrupt_failure(@table, @paths)], 3)

      assert quarantine == %{Enum.sort(@paths) => %{reason: reason, streak: 1}}
      assert quarantined == MapSet.new()
    end

    test "the Nth identical failure quarantines every path and drops the streak" do
      {quarantine, quarantined} =
        Enum.reduce(1..3, {%{}, MapSet.new()}, fn _sweep, {quarantine, quarantined} ->
          Quarantine.adjusted_quarantine(
            quarantine,
            quarantined,
            [corrupt_failure(@table, @paths)],
            3
          )
        end)

      assert quarantine == %{}
      assert quarantined == MapSet.new([Enum.sort(@paths)])
    end

    test "a merge OOM never counts toward quarantine, however often it repeats" do
      {quarantine, quarantined} =
        Enum.reduce(1..10, {%{}, MapSet.new()}, fn _sweep, {quarantine, quarantined} ->
          {:failed, failure} = oom_failure(@table)
          failure = Map.put(failure, :paths, @paths)

          Quarantine.adjusted_quarantine(quarantine, quarantined, [{:failed, failure}], 3)
        end)

      assert quarantine == %{}
      assert quarantined == MapSet.new()
    end

    test "an engine call exit never counts toward quarantine, however often it repeats" do
      exited =
        {:failed,
         %{
           table: @table,
           reason: {:sizing_failed, %CallExited{reason: :timeout}},
           paths: @paths
         }}

      {quarantine, quarantined} =
        Enum.reduce(1..10, {%{}, MapSet.new()}, fn _sweep, {quarantine, quarantined} ->
          Quarantine.adjusted_quarantine(quarantine, quarantined, [exited], 3)
        end)

      assert quarantine == %{}
      assert quarantined == MapSet.new()
    end

    test "a failure with no known paths never counts toward quarantine" do
      unattributed = {:failed, %{table: @table, reason: :mystery, paths: []}}

      {quarantine, quarantined} =
        Quarantine.adjusted_quarantine(%{}, MapSet.new(), [unattributed], 1)

      assert quarantine == %{}
      assert quarantined == MapSet.new()
    end

    test "a success or skip changes nothing" do
      assert Quarantine.adjusted_quarantine(%{}, MapSet.new(), [compacted(@table, 100)], 3) ==
               {%{}, MapSet.new()}

      assert Quarantine.adjusted_quarantine(%{}, MapSet.new(), [{:skip, @table}], 3) ==
               {%{}, MapSet.new()}
    end

    test "quarantine is keyed by the exact path set, not the table" do
      other_paths = ["analytics/events/c.parquet"]
      {:failed, %{reason: reason}} = corrupt_failure(@table, @paths)

      {quarantine, _quarantined} =
        Quarantine.adjusted_quarantine(
          %{},
          MapSet.new(),
          [corrupt_failure(@table, @paths), corrupt_failure(@table, other_paths)],
          3
        )

      assert quarantine == %{
               Enum.sort(@paths) => %{reason: reason, streak: 1},
               other_paths => %{reason: reason, streak: 1}
             }
    end

    test "a changed reason restarts the streak instead of accumulating" do
      first = corrupt_failure(@table, @paths)

      changed =
        {:failed,
         %{table: @table, reason: {:sizing_failed, %Adbc.Error{message: "other"}}, paths: @paths}}

      {quarantine, quarantined} =
        Enum.reduce([first, changed, first], {%{}, MapSet.new()}, fn outcome, acc ->
          Quarantine.adjusted_quarantine(elem(acc, 0), elem(acc, 1), [outcome], 3)
        end)

      assert %{streak: 1} = quarantine[Enum.sort(@paths)]
      assert quarantined == MapSet.new()
    end

    test "a store put failure never counts toward quarantine" do
      outage =
        {:failed,
         %{
           table: @table,
           reason: {:put_failed, "analytics/events/x.parquet", {:s3_status, 503, "slow down"}},
           paths: @paths
         }}

      {quarantine, quarantined} =
        Enum.reduce(1..10, {%{}, MapSet.new()}, fn _sweep, {quarantine, quarantined} ->
          Quarantine.adjusted_quarantine(quarantine, quarantined, [outage], 3)
        end)

      assert quarantine == %{}
      assert quarantined == MapSet.new()
    end

    test "a catalog conflict or a swap invariant failure never counts toward quarantine" do
      for reason <- [:commit_conflict, {:inputs_survived_swap, @paths}] do
        failure = {:failed, %{table: @table, reason: reason, paths: @paths}}

        assert Quarantine.adjusted_quarantine(%{}, MapSet.new(), [failure], 1) ==
                 {%{}, MapSet.new()}
      end
    end
  end

  describe "active_quarantined_paths/2 (T-310)" do
    test "a group binds while the listing holds every member" do
      groups = MapSet.new([Enum.sort(@paths)])
      listed = @paths ++ ["analytics/events/d.parquet"]

      assert Quarantine.active_quarantined_paths(groups, listed) == MapSet.new(@paths)
    end

    test "a group releases its survivors once any member leaves the listing" do
      groups = MapSet.new([Enum.sort(@paths)])
      [dropped | survivors] = @paths

      assert Quarantine.active_quarantined_paths(groups, survivors) == MapSet.new()
      refute dropped in survivors
    end
  end
end
