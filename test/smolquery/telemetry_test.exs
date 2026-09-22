defmodule Smolquery.TelemetryTest do
  @moduledoc """
  The aggregator, driven by events alone — the way every service reaches it.

  Assertions are deltas, not absolutes: the counter table is one per node on
  purpose. The module runs sync so those deltas hold — ExUnit runs every
  async module first and sync modules one at a time, so no other test's
  commit or manifest change lands between a `before` read and its assert
  (T-224, T-305, and the T-320 test on CI run 32856347788).
  """

  use ExUnit.Case, async: false

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

  describe "span/3 (T-380)" do
    @span_event [:smolquery, :test, :span]

    setup do
      handler = "span-test-#{:erlang.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        @span_event,
        fn _event, measurements, meta, _config -> send(test, {:span, measurements, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    test "static meta: emits duration_us and returns the result" do
      assert Telemetry.span(@span_event, %{kind: :static}, fn -> {:ok, 1} end) == {:ok, 1}

      assert_receive {:span, %{duration_us: duration}, %{kind: :static}}
      assert is_integer(duration) and duration >= 0
    end

    test "describe: merges measurements and meta derived from the result" do
      describe = fn {:ok, bytes} -> {%{bytes: bytes}, %{result: :ok}} end

      assert Telemetry.span(@span_event, describe, fn -> {:ok, 42} end) == {:ok, 42}
      assert_receive {:span, %{duration_us: _duration, bytes: 42}, %{result: :ok}}
    end

    test "every emit carries start_us for a trace to order by" do
      Telemetry.span(@span_event, %{kind: :clock}, fn -> :ok end)

      assert_receive {:span, %{start_us: start, duration_us: _duration}, %{kind: :clock}}
      assert is_integer(start)
    end

    test "a raise reaches describe as {:raised, kind, reason}, emits, then continues" do
      describe = fn {:raised, :error, %RuntimeError{message: message}} ->
        {%{}, %{result: :error, message: message}}
      end

      assert_raise RuntimeError, "boom", fn ->
        Telemetry.span(@span_event, describe, fn -> raise "boom" end)
      end

      assert_receive {:span, %{duration_us: _duration}, %{result: :error, message: "boom"}}
    end

    test "a throw and an exit emit under static meta and continue" do
      assert catch_throw(Telemetry.span(@span_event, %{kind: :thrown}, fn -> throw(:ball) end)) ==
               :ball

      assert_receive {:span, _measurements, %{kind: :thrown}}

      assert catch_exit(Telemetry.span(@span_event, %{kind: :exited}, fn -> exit(:bye) end)) ==
               :bye

      assert_receive {:span, _measurements, %{kind: :exited}}
    end
  end

  test "prices object-store requests by op, class, bytes, and latency bucket (T-379)" do
    put = ~s({op="put"})
    counted = ~s({op="put",class="2xx"})
    fast = ~s({op="put",le="10000"})
    mid = ~s({op="put",le="50000"})
    inf = ~s({op="put",le="+Inf"})
    failed_delete = ~s({op="delete",class="error"})

    before_requests = value("smolquery_s3_requests_total", counted)
    before_us = value("smolquery_s3_request_microseconds_total", put)
    before_bytes = value("smolquery_s3_request_bytes_total", put)
    before_fast = value("smolquery_s3_request_microseconds_bucket", fast)
    before_mid = value("smolquery_s3_request_microseconds_bucket", mid)
    before_inf = value("smolquery_s3_request_microseconds_bucket", inf)
    before_failed = value("smolquery_s3_requests_total", failed_delete)

    :telemetry.execute(
      [:smolquery, :s3, :request],
      %{duration_us: 30_000, bytes: 4_096},
      %{op: :put, status: 200, result: :ok}
    )

    assert value("smolquery_s3_requests_total", counted) == before_requests + 1
    assert value("smolquery_s3_request_microseconds_total", put) == before_us + 30_000
    assert value("smolquery_s3_request_bytes_total", put) == before_bytes + 4_096
    assert value("smolquery_s3_request_microseconds_bucket", fast) == before_fast
    assert value("smolquery_s3_request_microseconds_bucket", mid) == before_mid + 1
    assert value("smolquery_s3_request_microseconds_bucket", inf) == before_inf + 1

    :telemetry.execute(
      [:smolquery, :s3, :request],
      %{duration_us: 1_000, bytes: 0},
      %{op: :delete, status: nil, result: :error}
    )

    assert value("smolquery_s3_requests_total", failed_delete) == before_failed + 1
  end

  describe "the HTTP edges' latency (T-546)" do
    defp stopped(event, private, status, duration_us) do
      :telemetry.execute(
        event,
        %{duration: System.convert_time_unit(duration_us, :microsecond, :native)},
        %{conn: %Plug.Conn{status: status, private: private}}
      )
    end

    test "an API request is timed by route, cumulatively in le, and +Inf is the route's count" do
      family = "smolquery_api_request_microseconds_bucket"
      series = fn le -> ~s({route="insert",le="#{le}"}) end
      bounds = [5_000, 25_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 10_000_000]
      before = Map.new(bounds ++ ["+Inf"], &{&1, value(family, series.(&1))})
      before_us = value("smolquery_api_request_microseconds_total", ~s({route="insert"}))
      before_class = value("smolquery_api_requests_total", ~s({class="2xx"}))

      private = %{phoenix_controller: SmolqueryApi.InsertController, phoenix_action: :create}

      for duration_us <- [3_000, 588_000, 12_000_000],
          do: stopped([:smolquery, :api, :stop], private, 200, duration_us)

      moved = Map.new(before, fn {le, was} -> {le, value(family, series.(le)) - was} end)

      assert moved == %{
               5_000 => 1,
               25_000 => 1,
               100_000 => 1,
               250_000 => 1,
               500_000 => 1,
               1_000_000 => 2,
               2_500_000 => 2,
               10_000_000 => 2,
               "+Inf" => 3
             }

      assert value("smolquery_api_request_microseconds_total", ~s({route="insert"})) ==
               before_us + 12_591_000

      assert value("smolquery_api_requests_total", ~s({class="2xx"})) == before_class + 3
    end

    test "a route is its controller's, a closed set: a path is never a label" do
      inf = fn route -> ~s({route="#{route}",le="+Inf"}) end
      family = "smolquery_api_request_microseconds_bucket"

      for {controller, route} <- [
            {SmolqueryApi.QueryController, :query},
            {SmolqueryApi.JobController, :job},
            {SmolqueryApi.TableController, :catalog},
            {SmolqueryApi.ConnectionController, :connection},
            {SmolqueryApi.HealthController, :ops},
            {SomeoneElses.InsertedLaterController, :other}
          ] do
        was = value(family, inf.(route))
        stopped([:smolquery, :api, :stop], %{phoenix_controller: controller}, 200, 1_000)

        assert value(family, inf.(route)) == was + 1, inspect(controller)
      end

      was = value(family, inf.(:other))
      stopped([:smolquery, :api, :stop], %{}, 404, 1_000)

      assert value(family, inf.(:other)) == was + 1
      refute Telemetry.render() =~ "InsertedLater"
    end

    test "a ClickHouse edge request is timed by kind, and a kind it does not know is other" do
      family = "smolquery_clickhouse_request_microseconds_bucket"
      inf = fn kind -> ~s({kind="#{kind}",le="+Inf"}) end
      fast = ~s({kind="query",le="250000"})
      before_fast = value(family, fast)
      before_us = value("smolquery_clickhouse_request_microseconds_total", ~s({kind="query"}))

      for {kind, label} <- [insert: :insert, query: :query, ping: :ping, surprise: :other] do
        was = value(family, inf.(label))

        stopped(
          [:smolquery, :clickhouse, :stop],
          %{smolquery_clickhouse_kind: kind},
          200,
          230_000
        )

        assert value(family, inf.(label)) == was + 1, inspect(kind)
      end

      assert value(family, fast) == before_fast + 1

      assert value("smolquery_clickhouse_request_microseconds_total", ~s({kind="query"})) ==
               before_us + 230_000
    end

    test "a VictoriaMetrics edge request is counted by class and timed by kind" do
      family = "smolquery_victoriametrics_request_microseconds_bucket"
      inf = fn kind -> ~s({kind="#{kind}",le="+Inf"}) end
      fast = ~s({kind="write",le="250000"})
      before_fast = value(family, fast)

      before_us =
        value("smolquery_victoriametrics_request_microseconds_total", ~s({kind="write"}))

      before_class = value("smolquery_victoriametrics_requests_total", ~s({class="2xx"}))

      for {kind, label} <- [write: :write, health: :health, query: :query, nil: :other] do
        was = value(family, inf.(label))

        stopped(
          [:smolquery, :victoriametrics, :stop],
          %{smolquery_victoriametrics_kind: kind},
          204,
          230_000
        )

        assert value(family, inf.(label)) == was + 1, inspect(kind)
      end

      assert value(family, fast) == before_fast + 1

      assert value("smolquery_victoriametrics_request_microseconds_total", ~s({kind="write"})) ==
               before_us + 230_000

      assert value("smolquery_victoriametrics_requests_total", ~s({class="2xx"})) ==
               before_class + 4

      rendered = Telemetry.render()

      for name <- [
            "smolquery_victoriametrics_requests_total",
            "smolquery_victoriametrics_request_microseconds_total",
            "smolquery_victoriametrics_request_microseconds_bucket"
          ] do
        assert rendered =~ ~r/^# HELP #{name} .*VictoriaMetrics edge requests/m
        assert rendered =~ "# TYPE #{name} counter"
      end
    end
  end

  test "counts remote-write samples by result, a closed set" do
    results = ~w(written nan histogram exemplar refused)

    before =
      Map.new(
        results,
        &{&1, value("smolquery_victoriametrics_samples_total", ~s({result="#{&1}"}))}
      )

    for result <- results ++ ["surprise"] do
      :telemetry.execute([:smolquery, :victoriametrics, :samples], %{count: 7}, %{result: result})
    end

    for result <- results do
      assert value("smolquery_victoriametrics_samples_total", ~s({result="#{result}"})) ==
               before[result] + 7
    end

    rendered = Telemetry.render()
    refute rendered =~ ~s(result="surprise")
    assert rendered =~ "# HELP smolquery_victoriametrics_samples_total Remote-write samples"
  end

  test "counts the series and samples the VictoriaMetrics edge's queries read (T-564)" do
    before_series = value("smolquery_victoriametrics_query_series_total", "")
    before_samples = value("smolquery_victoriametrics_query_samples_total", "")

    :telemetry.execute(
      [:smolquery, :victoriametrics, :query],
      %{series: 3, samples: 1_200, duration_us: 900},
      %{}
    )

    assert value("smolquery_victoriametrics_query_series_total", "") == before_series + 3
    assert value("smolquery_victoriametrics_query_samples_total", "") == before_samples + 1_200

    rendered = Telemetry.render()
    assert rendered =~ "# HELP smolquery_victoriametrics_query_series_total Series"
    assert rendered =~ "# HELP smolquery_victoriametrics_query_samples_total Raw samples"
  end

  test "prices the ClickHouse edge's catalog checks apart from its rebuilds (T-529)" do
    unchanged = ~s({result="unchanged"})
    rebuilt = ~s({result="rebuilt"})
    before_checks = value("smolquery_clickhouse_catalog_refreshes_total", unchanged)
    before_rebuilds = value("smolquery_clickhouse_catalog_refreshes_total", rebuilt)
    before_us = value("smolquery_clickhouse_catalog_refresh_microseconds_total", rebuilt)

    for {result, duration_us} <- [unchanged: 900, unchanged: 1_100, rebuilt: 2_300_000] do
      :telemetry.execute(
        [:smolquery, :clickhouse, :catalog_refresh],
        %{duration_us: duration_us},
        %{result: result, name: SmolqueryClickHouse}
      )
    end

    assert value("smolquery_clickhouse_catalog_refreshes_total", unchanged) == before_checks + 2
    assert value("smolquery_clickhouse_catalog_refreshes_total", rebuilt) == before_rebuilds + 1

    assert value("smolquery_clickhouse_catalog_refresh_microseconds_total", rebuilt) ==
             before_us + 2_300_000
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

  test "counts rows the backlog valve refused apart from admission's (T-457)" do
    before_backlog = value("smolquery_buffer_backlog_refused_rows_total")
    before_admission = value("smolquery_buffer_admission_refused_rows_total")

    :telemetry.execute([:smolquery, :buffer, :admission], %{rows: 7}, %{outcome: :backlog})

    assert value("smolquery_buffer_backlog_refused_rows_total") == before_backlog + 7
    assert value("smolquery_buffer_admission_refused_rows_total") == before_admission
  end

  test "counts every compaction deferral (T-458)" do
    before_backoffs = value("smolquery_compaction_backoffs_total")

    :telemetry.execute(
      [:smolquery, :compact, :backoff],
      %{consecutive: 1, wait_ms: 600_000},
      %{table_ref: {"analytics", "events"}}
    )

    assert value("smolquery_compaction_backoffs_total") == before_backoffs + 1
  end

  test "counts job engines and their acquire time by source" do
    before_warm = value("smolquery_query_engines_total", ~s({source="warm"}))
    before_warm_us = value("smolquery_query_engine_microseconds_total", ~s({source="warm"}))
    before_cold_us = value("smolquery_query_engine_microseconds_total", ~s({source="cold"}))

    :telemetry.execute([:smolquery, :query, :engine], %{duration_us: 300_000}, %{source: :warm})
    :telemetry.execute([:smolquery, :query, :engine], %{duration_us: 650_000}, %{source: :cold})

    assert value("smolquery_query_engines_total", ~s({source="warm"})) == before_warm + 1

    assert value("smolquery_query_engine_microseconds_total", ~s({source="warm"})) ==
             before_warm_us + 300_000

    assert value("smolquery_query_engine_microseconds_total", ~s({source="cold"})) ==
             before_cold_us + 650_000
  end

  test "counts the pool's warm engine probes by outcome" do
    before_stale = value("smolquery_query_engine_probes_total", ~s({outcome="stale"}))
    before_ok_us = value("smolquery_query_engine_probe_microseconds_total", ~s({outcome="ok"}))

    :telemetry.execute([:smolquery, :query, :engine_probe], %{duration_us: 250_000}, %{
      outcome: :ok
    })

    :telemetry.execute([:smolquery, :query, :engine_probe], %{duration_us: 5_000_000}, %{
      outcome: :stale
    })

    assert value("smolquery_query_engine_probes_total", ~s({outcome="stale"})) == before_stale + 1

    assert value("smolquery_query_engine_probe_microseconds_total", ~s({outcome="ok"})) ==
             before_ok_us + 250_000
  end

  test "counts catalog ops and statements by result, with latency buckets (T-549)" do
    op = ~s({op="current_snapshot",result="ok"})
    op_fast = ~s({op="current_snapshot",result="ok",le="1000"})
    op_slow = ~s({op="current_snapshot",result="ok",le="1000000"})
    statement = ~s({kind="query",result="error"})
    statement_inf = ~s({kind="query",result="error",le="+Inf"})
    before_ops = value("smolquery_catalog_ops_total", op)
    before_op_us = value("smolquery_catalog_op_microseconds_total", op)
    before_op_fast = value("smolquery_catalog_op_microseconds_bucket", op_fast)
    before_op_slow = value("smolquery_catalog_op_microseconds_bucket", op_slow)
    before_statements = value("smolquery_catalog_statements_total", statement)
    before_statement_us = value("smolquery_catalog_statement_microseconds_total", statement)
    before_statement_inf = value("smolquery_catalog_statement_microseconds_bucket", statement_inf)

    :telemetry.execute(
      [:smolquery, :catalog, :op],
      %{duration_us: 300_000},
      %{op: :current_snapshot, result: :ok}
    )

    :telemetry.execute(
      [:smolquery, :catalog, :statement],
      %{duration_us: 5_000_000},
      %{kind: :query, result: :error}
    )

    assert value("smolquery_catalog_ops_total", op) == before_ops + 1
    assert value("smolquery_catalog_op_microseconds_total", op) == before_op_us + 300_000
    assert value("smolquery_catalog_op_microseconds_bucket", op_fast) == before_op_fast
    assert value("smolquery_catalog_op_microseconds_bucket", op_slow) == before_op_slow + 1
    assert value("smolquery_catalog_statements_total", statement) == before_statements + 1

    assert value("smolquery_catalog_statement_microseconds_total", statement) ==
             before_statement_us + 5_000_000

    assert value("smolquery_catalog_statement_microseconds_bucket", statement_inf) ==
             before_statement_inf + 1
  end

  test "outcome/1 folds a call's answer to ok or error" do
    assert Telemetry.outcome(:ok) == :ok
    assert Telemetry.outcome({:ok, 7}) == :ok
    assert Telemetry.outcome({:error, :gone}) == :error
    assert Telemetry.outcome({:raised, :exit, :timeout}) == :error
  end

  test "counts ring configuration store calls by op and result, with latency buckets (T-552)" do
    fetch = ~s({op="fetch",result="ok"})
    fetch_sub_ms = ~s({op="fetch",result="ok",le="500"})
    fetch_1ms = ~s({op="fetch",result="ok",le="1000"})
    conflict = ~s({op="advance",result="conflict"})
    before_fetches = value("smolquery_config_store_ops_total", fetch)
    before_fetch_us = value("smolquery_config_store_op_microseconds_total", fetch)
    before_sub_ms = value("smolquery_config_store_op_microseconds_bucket", fetch_sub_ms)
    before_1ms = value("smolquery_config_store_op_microseconds_bucket", fetch_1ms)
    before_conflicts = value("smolquery_config_store_ops_total", conflict)

    :telemetry.execute([:smolquery, :config_store, :op], %{duration_us: 320}, %{
      op: :fetch,
      result: :ok
    })

    :telemetry.execute([:smolquery, :config_store, :op], %{duration_us: 900}, %{
      op: :advance,
      result: :conflict
    })

    assert value("smolquery_config_store_ops_total", fetch) == before_fetches + 1
    assert value("smolquery_config_store_op_microseconds_total", fetch) == before_fetch_us + 320

    assert value("smolquery_config_store_op_microseconds_bucket", fetch_sub_ms) ==
             before_sub_ms + 1

    assert value("smolquery_config_store_op_microseconds_bucket", fetch_1ms) == before_1ms + 1
    assert value("smolquery_config_store_ops_total", conflict) == before_conflicts + 1
  end

  test "counts failed Postgrex pool checkouts by reason (T-552)" do
    before_queue =
      value("smolquery_catalog_database_checkout_errors_total", ~s({reason="queue_timeout"}))

    before_other = value("smolquery_catalog_database_checkout_errors_total", ~s({reason="other"}))

    :telemetry.execute([:db_connection, :connection_error], %{count: 1}, %{
      error: %DBConnection.ConnectionError{message: "queue", reason: :queue_timeout},
      opts: []
    })

    :telemetry.execute([:db_connection, :connection_error], %{count: 1}, %{error: :unshaped})

    assert value("smolquery_catalog_database_checkout_errors_total", ~s({reason="queue_timeout"})) ==
             before_queue + 1

    assert value("smolquery_catalog_database_checkout_errors_total", ~s({reason="other"})) ==
             before_other + 1
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
