defmodule Smolquery.StorageService.Scheduler.JobTest do
  @moduledoc """
  The failure report every other part of the scheduler reads. A merge and a
  swap against a real lake are `Smolquery.StorageService.SchedulerTest`'s.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Job

  @table {"analytics", "events"}

  test "failed/5 reports the table, the reason and what the caller knew, as one swap event" do
    runtime = Runtime.new(name: __MODULE__.Failed)
    ref = :telemetry_test.attach_event_handlers(self(), [[:smolquery, :compact, :swap]])

    log =
      capture_log(fn ->
        assert Job.failed(runtime, @table, :boom, System.monotonic_time(:microsecond),
                 rows: 10,
                 paths: ["a"],
                 level: :span,
                 span_cap: 1_024,
                 ignored: true
               ) ==
                 {:failed,
                  %{
                    table: @table,
                    reason: :boom,
                    paths: ["a"],
                    rows: 10,
                    level: :span,
                    span_cap: 1_024
                  }}
      end)

    assert log =~ "compaction of #{inspect(@table)} failed: :boom"

    assert_received {[:smolquery, :compact, :swap], ^ref, %{replaced: 0},
                     %{result: :error, table_ref: @table}}
  end

  test "run/4 fails, before any merge, a group whose paths are not segments" do
    runtime = Runtime.new(name: __MODULE__.Run)
    group = %{paths: ["not-a-ulid.parquet"], row_count: 1, bytes: 1, level: :hour, span_cap: 1}

    capture_log(fn ->
      assert {:failed, %{reason: {:not_a_segment_path, "not-a-ulid.parquet"}, level: :hour}} =
               Job.run(runtime, @table, group, System.monotonic_time(:microsecond))
    end)
  end
end
