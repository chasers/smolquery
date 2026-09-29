defmodule Smolquery.StorageService.Scheduler.CapsTest do
  @moduledoc "The per-table row and span caps a node learns from its failures."

  use ExUnit.Case, async: true

  alias Smolquery.Engine.CallExited
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Backoff
  alias Smolquery.StorageService.Scheduler.Caps

  import ExUnit.CaptureLog

  @table {"analytics", "events"}

  describe "the span level's caps (T-592)" do
    defp span_failure(reason, cap),
      do: {:failed, %{table: @table, reason: reason, paths: [], level: :span, span_cap: cap}}

    defp span_oom(cap),
      do:
        span_failure(
          {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}},
          cap
        )

    test "a span merge that runs out of memory or time halves the table's span cap" do
      runtime = Runtime.new(name: __MODULE__.SpanCaps)

      for reason <- [
            {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}},
            CallExited.new(:timeout),
            {:call_exited, CallExited.new(:timeout)}
          ] do
        log =
          capture_log(fn ->
            assert Caps.adjusted_span_caps(
                     %{},
                     [span_failure(reason, 1_073_741_824)],
                     runtime
                   ) ==
                     %{@table => 536_870_912}
          end)

        assert log =~ "its groups shrink to 536870912 bytes"
      end
    end

    test "the span cap never shrinks below compact_max_bytes, and other failures leave it" do
      runtime = Runtime.new(name: __MODULE__.SpanFloor, compact_max_bytes: 100_000_000)

      capture_log(fn ->
        assert Caps.adjusted_span_caps(%{}, [span_oom(150_000_000)], runtime) ==
                 %{@table => 100_000_000}
      end)

      assert Caps.adjusted_span_caps(
               %{},
               [span_failure(:commit_conflict, 150_000_000)],
               runtime
             ) ==
               %{}
    end

    test "a span failure leaves the hour level's row cap alone" do
      assert Caps.adjusted_row_caps(%{}, [span_oom(1_073_741_824)], 4_194_304) == %{}
    end

    test "a span failure backs off only once its cap cannot shrink; a lost race never does" do
      runtime =
        Runtime.with_compact_max_rows(
          Runtime.new(name: __MODULE__.SpanBackoff, compact_backoff_base_ms: 100)
        )

      shrinkable = [span_oom(1_073_741_824)]
      at_floor = [span_oom(runtime.compact_max_bytes)]
      lost = [{:failed, %{table: @table, reason: {:inputs_not_live, ["a"]}, paths: []}}]

      assert Backoff.adjusted_cooldowns(%{}, [@table], shrinkable, runtime, %{}, 0) == %{}

      assert %{@table => _backoff} =
               Backoff.adjusted_cooldowns(%{}, [@table], at_floor, runtime, %{}, 0)

      assert Backoff.adjusted_cooldowns(%{}, [@table], lost, runtime, %{}, 0) == %{}
    end
  end

  describe "adjusted_row_caps/3 (T-262)" do
    @resolved 4_194_304

    defp oom_failure(table) do
      {:failed,
       %{
         table: table,
         reason:
           {:put_failed, "analytics/events/x.parquet",
            {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}}}
       }}
    end

    defp staging_oom_failure(table) do
      {:failed,
       %{
         table: table,
         reason: {:merge_failed, %Adbc.Error{message: "Out of Memory Error: failed to pin block"}}
       }}
    end

    defp compacted(table, rows) do
      {:ok, %{table: table, key: "k", replaced: 2, rows: rows, snapshot: 1}}
    end

    test "a merge OOM halves the table's cap" do
      caps = Caps.adjusted_row_caps(%{}, [oom_failure(@table)], @resolved)

      assert caps == %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2, probe: false}}
    end

    test "an OOM failure carrying the group's rows tightens the cap the same way" do
      {:failed, failure} = oom_failure(@table)

      caps =
        Caps.adjusted_row_caps(%{}, [{:failed, Map.put(failure, :rows, 300_000)}], @resolved)

      assert caps == %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2, probe: false}}
    end

    test "a staging-phase OOM tightens the cap the same way" do
      caps = Caps.adjusted_row_caps(%{}, [staging_oom_failure(@table)], @resolved)

      assert caps == %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2, probe: false}}
    end

    test "repeated OOMs keep halving, never below the floor, and grow the patience" do
      caps =
        Enum.reduce(1..30, %{}, fn _sweep, caps ->
          Caps.adjusted_row_caps(caps, [oom_failure(@table)], @resolved)
        end)

      assert caps == %{@table => %{cap: 65_536, streak: 0, patience: 64, probe: false}}
    end

    test "cap-filling successes raise the cap after the patience and shed at the resolved cap" do
      caps = %{@table => %{cap: div(@resolved, 4), streak: 0, patience: 2, probe: false}}
      quarter = compacted(@table, div(@resolved, 4))
      half = compacted(@table, div(@resolved, 2))

      counted = Caps.adjusted_row_caps(caps, [quarter], @resolved)

      assert counted == %{
               @table => %{cap: div(@resolved, 4), streak: 1, patience: 2, probe: false}
             }

      doubled = Caps.adjusted_row_caps(counted, [quarter], @resolved)

      assert doubled == %{
               @table => %{cap: div(@resolved, 2), streak: 0, patience: 2, probe: true}
             }

      counted = Caps.adjusted_row_caps(doubled, [half], @resolved)
      assert Caps.adjusted_row_caps(counted, [half], @resolved) == %{}
    end

    test "an OOM at a probed cap re-tightens and clears the probe (T-283)" do
      caps = %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 2, probe: true}}

      assert Caps.adjusted_row_caps(caps, [oom_failure(@table)], @resolved) ==
               %{@table => %{cap: div(@resolved, 4), streak: 0, patience: 4, probe: false}}
    end

    test "a success at a probed cap proves it and clears the probe (T-283)" do
      caps = %{@table => %{cap: div(@resolved, 2), streak: 0, patience: 4, probe: true}}
      half = compacted(@table, div(@resolved, 2))

      assert Caps.adjusted_row_caps(caps, [half], @resolved) ==
               %{@table => %{cap: div(@resolved, 2), streak: 1, patience: 4, probe: false}}
    end

    test "a small group's success is not evidence for a raise" do
      caps = %{@table => %{cap: div(@resolved, 4), streak: 1, patience: 2, probe: false}}

      assert Caps.adjusted_row_caps(caps, [compacted(@table, 100)], @resolved) == caps
    end

    test "a cap that makes every plan skip still earns its raise, so the floor unwedges" do
      caps = %{@table => %{cap: 65_536, streak: 0, patience: 2, probe: false}}

      counted = Caps.adjusted_row_caps(caps, [{:skip, @table}], @resolved)
      doubled = Caps.adjusted_row_caps(counted, [{:skip, @table}], @resolved)

      assert doubled == %{@table => %{cap: 131_072, streak: 0, patience: 2, probe: true}}
    end

    test "a success or skip without an override changes nothing" do
      assert Caps.adjusted_row_caps(%{}, [compacted(@table, 100)], @resolved) == %{}
      assert Caps.adjusted_row_caps(%{}, [{:skip, @table}], @resolved) == %{}
    end

    test "a failure that is not a merge OOM changes nothing" do
      timeout =
        {:failed,
         %{
           table: @table,
           reason:
             {:put_failed, "analytics/events/x.parquet",
              {:merge_failed, %Smolquery.Engine.CallExited{reason: :timeout}}}
         }}

      sizing = {:failed, %{table: @table, reason: {:sizing_failed, %Adbc.Error{message: "x"}}}}

      assert Caps.adjusted_row_caps(%{}, [timeout, sizing, :skip], @resolved) == %{}
    end
  end
end
