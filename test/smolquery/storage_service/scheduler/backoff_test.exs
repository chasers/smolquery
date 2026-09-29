defmodule Smolquery.StorageService.Scheduler.BackoffTest do
  @moduledoc "How long a failing table waits, and why a conflict is not a failure."

  use ExUnit.Case, async: true

  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Backoff

  import ExUnit.CaptureLog

  @table {"analytics", "events"}

  describe "adjusted_cooldowns/6 (T-458)" do
    @other {"analytics", "other"}

    defp backoff_runtime(base, max) do
      Runtime.with_compact_max_rows(
        Runtime.new(
          name: __MODULE__.Backoff,
          compact_backoff_base_ms: base,
          compact_backoff_max_ms: max
        )
      )
    end

    defp failed(table_ref), do: {:failed, %{table: table_ref, reason: :boom, paths: []}}

    test "the wait doubles per consecutive failure of a table, up to the ceiling" do
      runtime = backoff_runtime(100, 250)

      cooldowns =
        Enum.reduce(1..3, %{}, fn _n, acc ->
          Backoff.adjusted_cooldowns(acc, [@table], [failed(@table)], runtime, %{}, 1_000)
        end)

      assert cooldowns == %{@table => %{consecutive: 3, retry_at: 1_250}}

      assert Backoff.adjusted_cooldowns(%{}, [@table], [failed(@table)], runtime, %{}, 1_000) ==
               %{@table => %{consecutive: 1, retry_at: 1_100}}
    end

    test "a swept table that did not fail is cleared; a table the sweep left out keeps its entry" do
      runtime = backoff_runtime(100, 250)

      before = %{
        @table => %{consecutive: 2, retry_at: 5},
        @other => %{consecutive: 1, retry_at: 9}
      }

      ok = [{:ok, %{table: @table}}]

      assert Backoff.adjusted_cooldowns(before, [@table], ok, runtime, %{}, 0) ==
               %{@other => %{consecutive: 1, retry_at: 9}}

      assert Backoff.adjusted_cooldowns(before, [@table], [:skip], runtime, %{}, 0) ==
               %{@other => %{consecutive: 1, retry_at: 9}}
    end

    test "a failure with a recovery of its own is left to it, and clears the cooldown" do
      runtime = backoff_runtime(100, 250)
      cooling = %{@table => %{consecutive: 2, retry_at: 5}}

      oom =
        {:failed,
         %{
           table: @table,
           reason:
             {:put_failed, "k", {:merge_failed, %Adbc.Error{message: "Out of Memory Error"}}},
           paths: ["a"]
         }}

      corrupt =
        {:failed,
         %{
           table: @table,
           reason: {:sizing_failed, %Adbc.Error{message: "Invalid Input Error: No magic bytes"}},
           paths: ["a"]
         }}

      # The cap is above the floor, so the OOM halves it and must be seen again.
      assert Backoff.adjusted_cooldowns(cooling, [@table], [oom], runtime, %{}, 0) == %{}
      # A corrupt input is counted toward quarantine, which is the stop for it.
      assert Backoff.adjusted_cooldowns(cooling, [@table], [corrupt], runtime, %{}, 0) == %{}

      # At the floor there is nothing left to halve, so the OOM backs off.
      at_floor = %{@table => %{cap: 65_536}}

      assert Backoff.adjusted_cooldowns(%{}, [@table], [oom], runtime, at_floor, 0) ==
               %{@table => %{consecutive: 1, retry_at: 100}}
    end

    test "a commit conflict waits one sweep interval and never reaches the max wait (T-595)" do
      runtime = %{backoff_runtime(100, 250) | compact_interval_ms: 40}
      conflict = [{:failed, %{table: @table, reason: :commit_conflict, paths: []}}]

      conflicts =
        Enum.reduce(1..10, %{}, fn _n, acc ->
          Backoff.adjusted_cooldowns(acc, [@table], conflict, runtime, %{}, 1_000)
        end)

      assert conflicts == %{@table => %{consecutive: 0, conflicts: 10, retry_at: 1_040}}

      assert Backoff.adjusted_cooldowns(conflicts, [@table], [failed(@table)], runtime, %{}, 0) ==
               %{@table => %{consecutive: 1, retry_at: 100}}
    end

    test "a conflict keeps a failure streak's count, and a success clears both (T-595)" do
      runtime = %{backoff_runtime(100, 250) | compact_interval_ms: 40}
      conflict = [{:failed, %{table: @table, reason: :commit_conflict, paths: []}}]
      failing = %{@table => %{consecutive: 3, retry_at: 0}}

      after_conflict = Backoff.adjusted_cooldowns(failing, [@table], conflict, runtime, %{}, 0)
      assert after_conflict == %{@table => %{consecutive: 3, conflicts: 1, retry_at: 40}}

      assert Backoff.adjusted_cooldowns(
               after_conflict,
               [@table],
               [failed(@table)],
               runtime,
               %{},
               0
             ) ==
               %{@table => %{consecutive: 4, retry_at: 250}}

      ok = [{:ok, %{table: @table}}]
      assert Backoff.adjusted_cooldowns(after_conflict, [@table], ok, runtime, %{}, 0) == %{}
    end

    test "each conflict is an event, and the third in a row warns (T-595)" do
      runtime = %{backoff_runtime(100, 250) | compact_interval_ms: 40}
      conflict = [{:failed, %{table: @table, reason: :commit_conflict, paths: []}}]
      ref = :telemetry_test.attach_event_handlers(self(), [[:smolquery, :compact, :conflict]])

      log =
        capture_log(fn ->
          Enum.reduce(1..3, %{}, fn _n, acc ->
            Backoff.adjusted_cooldowns(acc, [@table], conflict, runtime, %{}, 0)
          end)
        end)

      assert_receive {[:smolquery, :compact, :conflict], ^ref, %{conflicts: 1, wait_ms: 40},
                      %{table_ref: @table}}

      assert_receive {[:smolquery, :compact, :conflict], ^ref, %{conflicts: 3}, _meta}
      assert log =~ "[warning]"
      assert log =~ "3 sweeps in a row"
      refute log =~ "stalled"
    end

    test "every deferral is an event, and the log escalates at five consecutive failures" do
      runtime = backoff_runtime(100, 250)
      ref = :telemetry_test.attach_event_handlers(self(), [[:smolquery, :compact, :backoff]])

      log =
        capture_log(fn ->
          Enum.reduce(1..5, %{}, fn _n, acc ->
            Backoff.adjusted_cooldowns(acc, [@table], [failed(@table)], runtime, %{}, 0)
          end)
        end)

      assert_receive {[:smolquery, :compact, :backoff], ^ref, %{consecutive: 1, wait_ms: 100},
                      %{table_ref: @table}}

      assert_receive {[:smolquery, :compact, :backoff], ^ref, %{consecutive: 5, wait_ms: 250}, _}
      assert log =~ "compaction of #{inspect(@table)} backs off 100 ms (1 consecutive failure(s))"
      assert log =~ "compaction on this table is stalled (T-458)"
    end
  end
end
