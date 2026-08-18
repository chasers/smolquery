defmodule Smolquery.TelemetryTest do
  @moduledoc """
  The aggregator, driven by events alone — the way every service reaches it.

  Assertions are deltas, not absolutes: the counter table is one per node on
  purpose, so any concurrently running test that exercises a real service
  moves the same counters.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Telemetry

  defp value(name, labels \\ "") do
    pattern = ~r/^#{Regex.escape(name <> labels)} (\d+)$/m

    case Regex.run(pattern, Telemetry.render()) do
      [_line, count] -> String.to_integer(count)
      nil -> 0
    end
  end

  test "counts commit events with their result label" do
    before_ok = value("smolquery_buffer_commits_total", ~s({result="ok"}))
    before_rows = value("smolquery_buffer_rows_committed_total")
    before_us = value("smolquery_buffer_commit_microseconds_total")

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: 40, bytes: 1_000, duration_us: 1_500},
      %{result: :ok}
    )

    assert value("smolquery_buffer_commits_total", ~s({result="ok"})) == before_ok + 1
    assert value("smolquery_buffer_rows_committed_total") == before_rows + 40
    assert value("smolquery_buffer_commit_microseconds_total") == before_us + 1_500
  end

  test "a failed commit counts the attempt but never its rows" do
    before_error = value("smolquery_buffer_commits_total", ~s({result="error"}))
    before_rows = value("smolquery_buffer_rows_committed_total")

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: 7, bytes: 10, duration_us: 3},
      %{result: :error}
    )

    assert value("smolquery_buffer_commits_total", ~s({result="error"})) == before_error + 1
    assert value("smolquery_buffer_rows_committed_total") == before_rows
  end

  test "counts every maintenance sweep's work" do
    before_swept = value("smolquery_gc_segments_swept_total")
    before_dropped = value("smolquery_retention_segments_dropped_total")
    before_replaced = value("smolquery_compaction_segments_replaced_total")

    :telemetry.execute([:smolquery, :gc, :sweep], %{swept: 2, staged: 1}, %{})
    :telemetry.execute([:smolquery, :retention, :sweep], %{dropped: 3, expired_snapshots: 5}, %{})
    :telemetry.execute([:smolquery, :compact, :swap], %{replaced: 4}, %{result: :ok})

    assert value("smolquery_gc_segments_swept_total") == before_swept + 2
    assert value("smolquery_retention_segments_dropped_total") == before_dropped + 3
    assert value("smolquery_compaction_segments_replaced_total") == before_replaced + 4
  end

  test "counts a seal attempt's duration and batch size by result" do
    before_attempts = value("smolquery_seal_attempts_total", ~s({result="crashed"}))
    before_us = value("smolquery_seal_microseconds_total", ~s({result="crashed"}))
    before_segments = value("smolquery_seal_segments_total", ~s({result="crashed"}))

    :telemetry.execute(
      [:smolquery, :seal, :attempt],
      %{duration_us: 30_000_000, segments: 36},
      %{result: :crashed}
    )

    assert value("smolquery_seal_attempts_total", ~s({result="crashed"})) == before_attempts + 1

    assert value("smolquery_seal_microseconds_total", ~s({result="crashed"})) ==
             before_us + 30_000_000

    assert value("smolquery_seal_segments_total", ~s({result="crashed"})) == before_segments + 36
  end

  test "counts a compaction's duration by result" do
    before_us = value("smolquery_compaction_microseconds_total", ~s({result="ok"}))

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 2, duration_us: 4_200},
      %{result: :ok}
    )

    assert value("smolquery_compaction_microseconds_total", ~s({result="ok"})) ==
             before_us + 4_200
  end

  test "counts terminal query jobs by state" do
    before_done = value("smolquery_query_jobs_total", ~s({state="done"}))

    :telemetry.execute([:smolquery, :query, :job], %{duration_ms: 12}, %{state: :done})

    assert value("smolquery_query_jobs_total", ~s({state="done"})) == before_done + 1
  end

  test "counts stuck seal attempts and failed oversized releases" do
    before_stuck = value("smolquery_seal_stuck_attempts_total")
    before_releases = value("smolquery_seal_release_failures_total")

    :telemetry.execute(
      [:smolquery, :seal, :stuck],
      %{consecutive: 5},
      %{table_ref: {"analytics", "events"}}
    )

    :telemetry.execute(
      [:smolquery, :buffer, :release_failure],
      %{consecutive: 1},
      %{table_ref: {"analytics", "events"}}
    )

    assert value("smolquery_seal_stuck_attempts_total") == before_stuck + 1
    assert value("smolquery_seal_release_failures_total") == before_releases + 1
  end

  test "renders HELP and TYPE lines for every series it holds" do
    :telemetry.execute([:smolquery, :seal, :attempt], %{}, %{result: :ok})

    rendered = Telemetry.render()

    assert rendered =~ "# HELP smolquery_seal_attempts_total"
    assert rendered =~ "# TYPE smolquery_seal_attempts_total counter"
    assert rendered =~ ~r/^smolquery_seal_attempts_total\{result="ok"\} \d+$/m
  end

  test "a malformed event moves nothing and detaches nothing" do
    before_rows = value("smolquery_ingest_rows_accepted_total")

    :telemetry.execute([:smolquery, :ingest, :insert], %{unexpected: "shape"}, %{})
    :telemetry.execute([:smolquery, :ingest, :insert], %{accepted: 2, rejected: 0}, %{})

    assert value("smolquery_ingest_rows_accepted_total") == before_rows + 2
  end
end
