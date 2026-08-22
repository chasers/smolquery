defmodule Smolquery.TelemetryTest do
  @moduledoc """
  The aggregator, driven by events alone — the way every service reaches it.

  Assertions are deltas, not absolutes: the counter table is one per node on
  purpose, so any concurrently running test that exercises a real service
  moves the same counters.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Telemetry

  defp le(bound), do: ~s({le="#{bound}"})

  defp stable_buckets(bounds) do
    snapshot = buckets(bounds)

    if snapshot == buckets(bounds), do: snapshot, else: stable_buckets(bounds)
  end

  defp buckets(bounds),
    do: Map.new(bounds, &{&1, value("smolquery_buffer_commit_rows_bucket", le(&1))})

  defp value(name, labels \\ "") do
    pattern = ~r/^#{Regex.escape(name <> labels)} (\d+)$/m

    case Regex.run(pattern, Telemetry.render()) do
      [_line, count] -> String.to_integer(count)
      nil -> 0
    end
  end

  test "tracks whether the manifest index is in steady state or growing (T-320)" do
    added = ~s({change="added"})
    reaped = ~s({change="reaped"})
    before_added = value("smolquery_hot_manifest_index_entries_total", added)
    before_reaped = value("smolquery_hot_manifest_index_entries_total", reaped)

    for {change, entries} <- [added: 64, retired: 64, reaped: 40, recovered: 12] do
      :telemetry.execute(
        [:smolquery, :hot_manifest, :change],
        %{entries: entries},
        %{change: change}
      )
    end

    assert value("smolquery_hot_manifest_index_entries_total", added) == before_added + 64
    assert value("smolquery_hot_manifest_index_entries_total", reaped) == before_reaped + 40

    assert value("smolquery_hot_manifest_index_entries_total", ~s({change="retired"})) > 0
    assert value("smolquery_hot_manifest_index_entries_total", ~s({change="recovered"})) > 0
  end

  test "prices a hot-tier read by route, in requests, time, bytes and entries (T-315)" do
    labels = ~s({route="manifest",method="get"})
    counted = ~s({route="manifest",method="get",class="2xx"})
    before_requests = value("smolquery_hot_server_requests_total", counted)
    before_us = value("smolquery_hot_server_microseconds_total", labels)
    before_bytes = value("smolquery_hot_server_response_bytes_total", labels)
    before_entries = value("smolquery_hot_manifest_entries_total", labels)

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{duration_us: 900, response_bytes: 45_000, entries: 6_738},
      %{route: :manifest, method: "GET", status: 200}
    )

    assert value("smolquery_hot_server_requests_total", counted) == before_requests + 1
    assert value("smolquery_hot_server_microseconds_total", labels) == before_us + 900
    assert value("smolquery_hot_server_response_bytes_total", labels) == before_bytes + 45_000
    assert value("smolquery_hot_manifest_entries_total", labels) == before_entries + 6_738
  end

  test "a HEAD counts its cost but none of its bytes, and stays its own series" do
    head = ~s({route="manifest",method="head"})
    get = ~s({route="manifest",method="get"})
    before_us = value("smolquery_hot_server_microseconds_total", head)
    before_bytes = value("smolquery_hot_server_response_bytes_total", head)
    before_entries = value("smolquery_hot_manifest_entries_total", head)
    before_get_us = value("smolquery_hot_server_microseconds_total", get)

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{duration_us: 900, response_bytes: 0, entries: 6_738},
      %{route: :manifest, method: "HEAD", status: 200}
    )

    assert value("smolquery_hot_server_microseconds_total", head) == before_us + 900
    assert value("smolquery_hot_manifest_entries_total", head) == before_entries + 6_738
    assert value("smolquery_hot_server_response_bytes_total", head) == before_bytes
    assert value("smolquery_hot_server_microseconds_total", get) == before_get_us
  end

  test "narrows a method it does not serve, rather than labelling with it" do
    other = ~s({route="unknown",method="other",class="4xx"})
    before_other = value("smolquery_hot_server_requests_total", other)

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{duration_us: 5, response_bytes: 9, entries: 0},
      %{route: :unknown, method: "FROBNICATE", status: 404}
    )

    assert value("smolquery_hot_server_requests_total", other) == before_other + 1
  end

  test "counts a scoped manifest read apart from a whole one" do
    scoped = ~s({route="manifest_scoped",method="post"})
    whole = ~s({route="manifest",method="get"})
    before_scoped = value("smolquery_hot_manifest_entries_total", scoped)
    before_whole = value("smolquery_hot_manifest_entries_total", whole)

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{duration_us: 30, response_bytes: 400, entries: 8},
      %{route: :manifest_scoped, method: "POST", status: 200}
    )

    assert value("smolquery_hot_manifest_entries_total", scoped) == before_scoped + 8
    assert value("smolquery_hot_manifest_entries_total", whole) == before_whole
  end

  test "counts a 206 as its own series, because one input is several of them" do
    before_ranged = value("smolquery_hot_server_range_responses_total")

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{duration_us: 10, response_bytes: 8_192, entries: 0},
      %{route: :segment, method: "GET", status: 206}
    )

    :telemetry.execute(
      [:smolquery, :hot_server, :request],
      %{duration_us: 10, response_bytes: 8_192, entries: 0},
      %{route: :segment, method: "GET", status: 200}
    )

    assert value("smolquery_hot_server_range_responses_total") == before_ranged + 1
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

  test "counts a commit's wire bytes, which is what flush_max_bytes gates on (T-333)" do
    before_bytes = value("smolquery_buffer_commit_bytes_total")

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: 40, bytes: 1_000, duration_us: 1_500},
      %{result: :ok}
    )

    assert value("smolquery_buffer_commit_bytes_total") >= before_bytes + 1_000
  end

  test "buckets a commit's row count cumulatively, so the mean cannot hide a split" do
    bounds = ~w(1000 4000 16000 64000 +Inf)
    before = stable_buckets(bounds)

    for rows <- [800, 10_000] do
      :telemetry.execute(
        [:smolquery, :buffer, :commit],
        %{rows: rows, bytes: 1, duration_us: 1},
        %{result: :ok}
      )
    end

    # 800 lands in every bucket; 10,000 only from 16000 up. A mean of 5,400
    # would have looked identical to two commits of 5,400. The assertions are
    # differences between buckets, not absolute deltas: the counter table is
    # node-wide and the suite is async, so concurrent buffer tests land their
    # own small commits in every bucket — which cancels in a difference. The
    # cancellation only holds inside an internally consistent snapshot, so
    # both sides read through stable_buckets/1: a commit landing between two
    # bucket reads broke the difference by one on CI.
    deltas =
      Map.new(stable_buckets(bounds), fn {bound, count} ->
        {bound, count - Map.fetch!(before, bound)}
      end)

    assert deltas["16000"] - deltas["4000"] == 1
    assert deltas["4000"] - deltas["1000"] == 0
    assert deltas["+Inf"] - deltas["16000"] == 0
    assert deltas["1000"] >= 1
    assert deltas["+Inf"] >= 2
  end

  test "a failed commit is neither bucketed nor counted in bytes" do
    before_bytes = value("smolquery_buffer_commit_bytes_total")
    before_inf = value("smolquery_buffer_commit_rows_bucket", le("+Inf"))

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: 7, bytes: 10, duration_us: 3},
      %{result: :error}
    )

    assert value("smolquery_buffer_commit_bytes_total") == before_bytes
    assert value("smolquery_buffer_commit_rows_bucket", le("+Inf")) == before_inf
  end

  test "names which threshold closed a group-commit window (T-333)" do
    before_bytes = value("smolquery_buffer_flush_trigger_total", ~s({reason="bytes"}))
    before_idle = value("smolquery_buffer_flush_trigger_total", ~s({reason="idle"}))

    for reason <- [:bytes, :bytes, :idle] do
      :telemetry.execute(
        [:smolquery, :buffer, :flush_trigger],
        %{rows: 10, bytes: 20},
        %{reason: reason}
      )
    end

    assert value("smolquery_buffer_flush_trigger_total", ~s({reason="bytes"})) ==
             before_bytes + 2

    assert value("smolquery_buffer_flush_trigger_total", ~s({reason="idle"})) == before_idle + 1
  end

  test "an unrecognised close reason cannot widen the label set" do
    before_unknown = value("smolquery_buffer_flush_trigger_total", ~s({reason="unknown"}))

    :telemetry.execute(
      [:smolquery, :buffer, :flush_trigger],
      %{rows: 1, bytes: 1},
      %{reason: :something_new}
    )

    assert value("smolquery_buffer_flush_trigger_total", ~s({reason="unknown"})) ==
             before_unknown + 1

    refute Telemetry.render() =~ ~s(reason="something_new")
  end

  test "counts sealed segment bytes and rows as written (T-333)" do
    before_bytes = value("smolquery_seal_segment_bytes_total")
    before_rows = value("smolquery_seal_segment_rows_total")

    :telemetry.execute(
      [:smolquery, :seal, :segment],
      %{bytes: 4_194_304, rows: 250_000},
      %{table_ref: {"analytics", "events"}}
    )

    assert value("smolquery_seal_segment_bytes_total") == before_bytes + 4_194_304
    assert value("smolquery_seal_segment_rows_total") == before_rows + 250_000
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

  test "counts every quarantined path (T-310)" do
    before_quarantined = value("smolquery_compaction_quarantined_segments_total")

    :telemetry.execute(
      [:smolquery, :compact, :quarantine],
      %{},
      %{table_ref: {"analytics", "events"}, paths: ["a.parquet", "b.parquet"]}
    )

    assert value("smolquery_compaction_quarantined_segments_total") == before_quarantined + 2
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
