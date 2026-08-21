defmodule Smolquery.Telemetry do
  @moduledoc """
  The node's metrics: `:telemetry` events in, Prometheus text out (PL-10 D8).

  Every service emits plain `:telemetry` events at its seams and knows nothing
  about metrics; this module attaches to those events, keeps counters in a
  public ETS table, and renders them for `GET /metrics`. Events are the seam
  on purpose — a StatsD or OpenTelemetry exporter can attach to the same
  events later without touching a call site.

  Counters, with one deliberate exception: `put_info/2` records a service's
  resolved shape as an `_info` gauge pinned at 1, because "which path did this
  node come up on" is the question a throughput regression usually turns out to
  be, and no counter answers it. See `Smolquery.DeployedShape`.

  Counters otherwise, deliberately. A counter answers rates and ratios under
  `rate()` in whatever scrapes this, and paired totals answer averages
  (`_microseconds_total / _total` is a mean duration) — the shapes operators
  actually alert on — without this module growing histogram buckets to keep
  honest. `smolquery_buffer_commit_rows_bucket` bends that rule as far as it
  goes: it borrows the `le` convention for cumulative buckets, but every series
  is still a plain counter, with no `_sum`/`_count` pair and no
  `# TYPE histogram`. A mean commit size cannot distinguish a steady 5,292 rows
  from half at 800 and half at 10,000, and the seal path's cost is not linear
  in segment size, so the distribution is the measurement (T-333). Labels are closed sets (a result atom, a status class), never a
  table or job id, so cardinality is bounded by this file rather than by
  traffic.

  Handlers run in the emitting process, so they are a handful of ETS
  `update_counter`s at most and can never crash the caller: `:telemetry`
  detaches a handler that raises, which would silently stop counting — the
  one failure mode a metrics pipe must not have. Unknown shapes are counted
  as nothing rather than guessed at.

  ## The event catalog

      [:smolquery, :api, :stop]           Plug.Telemetry — measurements.duration, conn status
      [:smolquery, :ingest, :insert]      %{accepted, rejected, parse_us, write_us}
      [:smolquery, :buffer, :wire]        %{duration_us}, meta %{transport: :local | :remote}
      [:smolquery, :buffer, :commit]      %{rows, bytes, duration_us, accumulate_us,
                                            queue_us, encode_us, manifest_us,
                                            replicate_us},
                                          meta %{result: :ok | :error, table_ref: ref}
      [:smolquery, :buffer, :flush_trigger] %{rows, bytes},
                                          meta %{reason: :rows | :bytes | :interval | :idle |
                                          :schema | :kind | :flush | :drain | :shutdown,
                                          table_ref: ref}
                                          — why a group-commit window closed; the one
                                          fact that says which knob is in control (T-333)
      [:smolquery, :buffer, :admission]   %{rows}, meta %{outcome: :refused}
      [:smolquery, :buffer, :dedup]       %{rows}
      [:smolquery, :seal, :attempt]       %{duration_us, segments},
                                          meta %{result: :ok | :error | :crashed, table_ref: ref}
      [:smolquery, :seal, :segment]       %{bytes, rows}, meta %{table_ref: ref}
                                          — one sealed segment as written, after the
                                          merge and before registration (T-333)
      [:smolquery, :seal, :stuck]         %{consecutive}, meta %{table_ref: ref}
                                          — a failed attempt at or past the sealer's
                                          stuck threshold; alert on rate > 0 (T-293)
      [:smolquery, :buffer, :release_failure] %{consecutive}, meta %{table_ref: ref}
                                          — an oversized-claim release that could not
                                          replicate (T-294, T-297)
      [:smolquery, :compact, :swap]       %{replaced, duration_us},
                                          meta %{result: :ok | :error, table_ref: ref}
      [:smolquery, :compact, :quarantine] %{count}, meta %{table_ref: ref, paths: [String.t()]}
                                          — a compaction group that failed identically
                                          5 times (a Compactor module constant, not a
                                          runtime setting) and stopped being planned on
                                          this node; alert on rate > 0 (T-310)
      [:smolquery, :hot_manifest, :change] %{entries},
                                          meta %{change: :added | :retired | :reaped |
                                          :recovered}
                                          — entries entering or leaving a node's manifest
                                          index; `added + recovered - reaped` is what it
                                          holds (T-320). Distinct from the two counters
                                          named for entries *served*
                                          (`..._entries_total{route}`) and entries *read*
                                          (`..._read_entries_total{op}`)
      [:smolquery, :hot_manifest, :read]  %{duration_us, entries},
                                          meta %{op: :entries | :pending | :claimable |
                                          :live_claim | :retired_before}
                                          — one scanning read of a node's manifest index,
                                          whoever made it (T-318)
      [:smolquery, :hot_server, :request] %{duration_us, response_bytes, entries},
                                          meta %{route: :manifest | :manifest_scoped |
                                          :segment | :unknown, method: String.t(),
                                          status: integer, table_ref: ref | nil}
                                          — one hot-tier read served to a sealer or a
                                          query planner (T-315). The method is the raw
                                          request method; this module narrows it to a
                                          closed set, the way it narrows the status

      [:smolquery, :lifecycle, :broadcast] %{count},
                                          meta %{kind: :commit | :seal | :compaction}

  The per-table events carry `table_ref` in metadata for
  `Smolquery.Lifecycle`'s PubSub bridge; it is never a label here, so
  metric cardinality stays bounded by this file. The bridge's own
  broadcasts are counted back here per node, by kind — a closed set.
      [:smolquery, :retention, :sweep]    %{dropped, expired_snapshots}
      [:smolquery, :gc, :sweep]           %{swept, staged}
      [:smolquery, :query, :job]          %{duration_ms}, meta %{state: :done | :failed | :cancelled}
      [:smolquery, :query, :scatter]      %{shards, partial_bytes}, meta %{workers: [node()]}
                                          — one per query the distributed path answered (PL-49)
      [:smolquery, :query, :span]         %{start_us, duration_us}, meta %{phase: closed set}
                                          — one per query phase (Smolquery.QueryService.Trace);
                                          not aggregated here, collected per job when tracing.
                                          start_us is raw monotonic time (usually negative);
                                          only a collected trace rebases it to an offset
  """

  use GenServer

  @table __MODULE__
  @handler_id "smolquery-metrics"

  @events [
    [:smolquery, :api, :stop],
    [:smolquery, :ingest, :insert],
    [:smolquery, :buffer, :commit],
    [:smolquery, :buffer, :wire],
    [:smolquery, :buffer, :flush_trigger],
    [:smolquery, :buffer, :admission],
    [:smolquery, :buffer, :dedup],
    [:smolquery, :seal, :attempt],
    [:smolquery, :seal, :segment],
    [:smolquery, :seal, :stuck],
    [:smolquery, :buffer, :release_failure],
    [:smolquery, :compact, :swap],
    [:smolquery, :compact, :quarantine],
    [:smolquery, :hot_manifest, :change],
    [:smolquery, :hot_manifest, :read],
    [:smolquery, :hot_server, :request],
    [:smolquery, :retention, :sweep],
    [:smolquery, :gc, :sweep],
    [:smolquery, :query, :job],
    [:smolquery, :query, :scatter],
    [:smolquery, :lifecycle, :broadcast]
  ]

  @help %{
    "smolquery_api_requests_total" => "HTTP requests answered, by status class.",
    "smolquery_ingest_rows_accepted_total" => "Rows the ingest edge accepted and forwarded.",
    "smolquery_ingest_rows_rejected_total" => "Rows the ingest edge rejected in validation.",
    "smolquery_buffer_commits_total" => "Group commits, by result.",
    "smolquery_buffer_rows_committed_total" => "Rows made durable by group commits.",
    "smolquery_buffer_commit_microseconds_total" =>
      "Time spent in group commits; divide by commits for the mean.",
    "smolquery_buffer_commit_phase_microseconds_total" =>
      "Time spent in each term of a group commit; divide by commits for the mean.",
    "smolquery_ingest_phase_microseconds_total" =>
      "Time the ingest edge spent parsing bytes and awaiting the buffer; divide by inserts.",
    "smolquery_ingest_inserts_total" => "Insert calls the ingest edge answered.",
    "smolquery_buffer_wire_microseconds_total" =>
      "Time spent serializing batches for the wire, by transport; divide by inserts.",
    "smolquery_api_request_microseconds_total" =>
      "Time spent answering HTTP requests; divide by requests for the mean.",
    "smolquery_buffer_admission_refused_rows_total" =>
      "Rows refused by Little's-law admission (PL-9).",
    "smolquery_buffer_dedup_rows_total" =>
      "Rows answered from the batch-id dedup index instead of rewritten (T-41).",
    "smolquery_seal_attempts_total" => "Seal attempts, by result.",
    "smolquery_seal_microseconds_total" =>
      "Time seal attempts ran, by result; divide by attempts for the mean (T-244).",
    "smolquery_seal_segments_total" =>
      "Micro-segments in seal attempts, by result; divide by attempts for the mean batch size (T-244).",
    "smolquery_seal_stuck_attempts_total" =>
      "Seal attempts that failed at or past the stuck threshold; a nonzero rate means a claim may never seal (T-293).",
    "smolquery_seal_release_failures_total" =>
      "Oversized-claim releases that could not replicate; a nonzero rate means sealing on a table is stalled (T-297).",
    "smolquery_compactions_total" => "Compaction swaps, by result.",
    "smolquery_compaction_microseconds_total" =>
      "Time compaction attempts ran, by result; divide by compactions for the mean (T-244).",
    "smolquery_compaction_segments_replaced_total" =>
      "Sealed segments replaced by compaction merges.",
    "smolquery_compaction_quarantined_segments_total" =>
      "Sealed segments quarantined after repeated compaction failures; a nonzero rate " <>
        "means a table needs an operator to drop or replace a segment (T-310).",
    "smolquery_hot_manifest_index_entries_total" =>
      "Entries entering and leaving a node's manifest index, by change. " <>
        "`added + recovered - reaped` is the resident entry count — the index's real " <>
        "size, and nothing else reports it. `retired` below `added` means sealing is " <>
        "not keeping up, which is the one condition under which nothing is ever reaped " <>
        "(T-320).",
    "smolquery_hot_manifest_reads_total" =>
      "Scanning reads of a node's manifest index, by op (T-318).",
    "smolquery_hot_manifest_read_microseconds_total" =>
      "Time spent in scanning manifest reads, by op; divide by reads for the mean. " <>
        "`live_claim` and `retired_before` run on every maintenance tick, so their rate " <>
        "is what the buffer's write path pays to consult its own index.",
    "smolquery_hot_manifest_read_entries_total" =>
      "Entries scanning manifest reads answered with, by op; divide by that op's reads " <>
        "for the mean. At op=\"entries\" that mean is the unsealed backlog depth; at " <>
        "op=\"live_claim\" it is the mean claim size. `live_claim` and `retired_before` " <>
        "are constant-time — a rising duration there means the index changed shape, not " <>
        "that it grew.",
    "smolquery_hot_server_requests_total" =>
      "Hot-tier reads served, by route, method and status class (T-315).",
    "smolquery_hot_server_microseconds_total" =>
      "Time spent serving hot-tier reads, by route and method; divide by requests for " <>
        "the mean. Its rate is also the mean concurrency, which is why there is no " <>
        "in-flight gauge.",
    "smolquery_hot_server_response_bytes_total" =>
      "Bytes hot-tier reads produced, by route and method; a HEAD counts none, though " <>
        "it still pays the duration. The series that prices a manifest read against a " <>
        "segment read (T-315).",
    "smolquery_hot_manifest_entries_total" =>
      "Micro-segment entries hot-tier manifest reads answered with, by route and " <>
        "method; divide by those requests for the mean. On the unscoped route that " <>
        "mean is the unsealed backlog depth (T-315).",
    "smolquery_hot_server_range_responses_total" =>
      "Hot-tier segment reads answered with a 206; one DuckDB input is several (T-315).",
    "smolquery_retention_segments_dropped_total" => "Sealed segments dropped past their TTL.",
    "smolquery_snapshots_expired_total" => "Catalog snapshots expired by retention sweeps.",
    "smolquery_gc_segments_swept_total" => "Uncommitted sealed segments GC deleted.",
    "smolquery_gc_staged_files_swept_total" => "Leaked staging files GC deleted.",
    "smolquery_query_jobs_total" => "Query jobs reaching a terminal state, by state.",
    "smolquery_lifecycle_broadcasts_total" =>
      "Lifecycle events this node broadcast over PubSub, by kind (T-295).",
    "smolquery_query_job_milliseconds_total" =>
      "Time query jobs ran; divide by jobs for the mean.",
    "smolquery_query_scattered_total" =>
      "Queries answered by the distributed scatter/gather path (PL-49).",
    "smolquery_query_scatter_shards_total" =>
      "Shards executed by scattered queries; divide by scattered for the mean fan-out.",
    "smolquery_query_scatter_partial_bytes_total" =>
      "Partial-result bytes scattered queries merged; the network bill once workers are remote.",
    "smolquery_buffer_commit_bytes_total" =>
      "Wire bytes in committed group commits; what flush_max_bytes gates on (T-333).",
    "smolquery_buffer_commit_rows_bucket" =>
      "Committed group commits by row count, cumulative in le; counters, not a histogram.",
    "smolquery_buffer_flush_trigger_total" =>
      "Group-commit windows closed, by which threshold or event closed them (T-333).",
    "smolquery_seal_segment_bytes_total" =>
      "Compressed Parquet bytes the seal merge wrote; divide by segments for the mean.",
    "smolquery_seal_segment_rows_total" => "Rows the seal merge wrote into sealed segments."
  }

  # Bounds for `smolquery_buffer_commit_rows_bucket`, ascending. A closed list
  # here is what bounds the family's cardinality, the same rule every label in
  # this module follows.
  @commit_row_buckets [1_000, 4_000, 16_000, 64_000]

  # The closed set of window-close reasons `TableBuffer` names. An unrecognised
  # one counts as `:unknown` rather than creating a series, so the label can
  # never be widened by anything but this list.
  @flush_reasons ~w(rows bytes interval idle schema kind flush drain shutdown)a

  @info %{
    "smolquery_buffer_shape_info" =>
      "The buffer path this node resolved at boot; always 1, read the labels.",
    "smolquery_ingest_shape_info" =>
      "The ingest path this node resolved at boot; always 1, read the labels.",
    "smolquery_storage_shape_info" =>
      "The seal and merge budgets this node resolved at boot; always 1, read the labels."
  }

  @doc """
  Records a service's resolved configuration as an `_info` gauge.

  The one exception to counters-only, and it is the Prometheus convention for
  exactly this case: a series pinned at 1 whose *labels* are the payload. It
  answers "what shape did this node come up in", which no counter can, and
  which a throughput regression turns out to be most of the time.

  Cardinality stays bounded the same way the rest of this file bounds it —
  callers pass resolved configuration, a closed set per deployment, and
  re-registering the same name replaces rather than accumulates. Written by
  `Smolquery.DeployedShape` at boot; nothing else should call it.

  A metrics write must never take down its caller, so a missing table (no
  aggregator running, which is normal in unit tests) is `:ok`, not a raise.
  """
  @spec put_info(String.t(), keyword()) :: :ok
  def put_info(name, labels) when is_binary(name) and is_list(labels) do
    :ets.insert(@table, {{name, labels}, 1})

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Starts the aggregator: the counter table, and the handlers feeding it.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl GenServer
  def init(nil) do
    :ets.new(@table, [:ordered_set, :public, :named_table, write_concurrency: true])

    # Detach before attaching, so a restart works. `terminate/2` runs only on a
    # graceful stop — this process does not trap exits, so a supervisor
    # shutdown or a crash leaves the handler attached. `attach_many/4` then
    # answers `{:error, :already_exists}`, the `:ok =` raises, and the restart
    # fails identically forever: one crash here would end this node's metrics
    # for good, and take the application down with it once the supervisor ran
    # out of restart intensity. Detaching an absent handler is a no-op.
    :telemetry.detach(@handler_id)
    :ok = :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)

    {:ok, nil}
  end

  @impl GenServer
  def terminate(_reason, _state), do: :telemetry.detach(@handler_id)

  @doc """
  The current counters as Prometheus text exposition.
  """
  @spec render() :: String.t()
  def render do
    series = :ets.tab2list(@table)

    grouped = Enum.group_by(series, fn {{name, _labels}, _value} -> name end)

    grouped
    |> Enum.sort()
    |> Enum.map_join(fn {name, rows} ->
      help = Map.get(@help, name) || Map.get(@info, name) || name
      type = if Map.has_key?(@info, name), do: "gauge", else: "counter"
      header = "# HELP #{name} #{help}\n# TYPE #{name} #{type}\n"

      lines =
        Enum.map_join(rows, fn {{_name, labels}, value} ->
          "#{name}#{format_labels(labels)} #{value}\n"
        end)

      header <> lines
    end)
  end

  @doc false
  def handle_event([:smolquery, :api, :stop], measurements, %{conn: conn}, nil) do
    bump({"smolquery_api_requests_total", [class: status_class(conn.status)]}, 1)

    # Plug.Telemetry measures in native units; the counter is microseconds so it
    # divides against the other spans without a unit lookup at read time.
    bump(
      {"smolquery_api_request_microseconds_total", []},
      System.convert_time_unit(Map.get(measurements, :duration, 0), :native, :microsecond)
    )
  end

  def handle_event([:smolquery, :ingest, :insert], measurements, _meta, nil) do
    bump({"smolquery_ingest_rows_accepted_total", []}, Map.get(measurements, :accepted, 0))
    bump({"smolquery_ingest_rows_rejected_total", []}, Map.get(measurements, :rejected, 0))
    bump({"smolquery_ingest_inserts_total", []}, 1)

    for phase <- ~w(parse write)a do
      bump(
        {"smolquery_ingest_phase_microseconds_total", [phase: phase]},
        Map.get(measurements, :"#{phase}_us", 0)
      )
    end
  end

  def handle_event([:smolquery, :buffer, :wire], measurements, meta, nil) do
    bump(
      {"smolquery_buffer_wire_microseconds_total",
       [transport: Map.get(meta, :transport, :local)]},
      Map.get(measurements, :duration_us, 0)
    )
  end

  def handle_event([:smolquery, :buffer, :commit], measurements, meta, nil) do
    bump({"smolquery_buffer_commits_total", [result: result(meta)]}, 1)
    bump({"smolquery_buffer_rows_committed_total", []}, committed_rows(measurements, meta))
    bump({"smolquery_buffer_commit_bytes_total", []}, committed_bytes(measurements, meta))
    bucket_commit_rows(measurements, meta)

    bump(
      {"smolquery_buffer_commit_microseconds_total", []},
      Map.get(measurements, :duration_us, 0)
    )

    for phase <- ~w(accumulate queue encode manifest replicate)a do
      bump(
        {"smolquery_buffer_commit_phase_microseconds_total", [phase: phase]},
        Map.get(measurements, :"#{phase}_us", 0)
      )
    end
  end

  def handle_event([:smolquery, :buffer, :flush_trigger], _measurements, meta, nil) do
    bump({"smolquery_buffer_flush_trigger_total", [reason: flush_reason(meta)]}, 1)
  end

  def handle_event([:smolquery, :buffer, :admission], measurements, _meta, nil) do
    bump({"smolquery_buffer_admission_refused_rows_total", []}, Map.get(measurements, :rows, 0))
  end

  def handle_event([:smolquery, :buffer, :dedup], measurements, _meta, nil) do
    bump({"smolquery_buffer_dedup_rows_total", []}, Map.get(measurements, :rows, 0))
  end

  def handle_event([:smolquery, :seal, :attempt], measurements, meta, nil) do
    bump({"smolquery_seal_attempts_total", [result: result(meta)]}, 1)

    bump(
      {"smolquery_seal_microseconds_total", [result: result(meta)]},
      Map.get(measurements, :duration_us, 0)
    )

    bump(
      {"smolquery_seal_segments_total", [result: result(meta)]},
      Map.get(measurements, :segments, 0)
    )
  end

  def handle_event([:smolquery, :seal, :segment], measurements, _meta, nil) do
    bump({"smolquery_seal_segment_bytes_total", []}, Map.get(measurements, :bytes, 0))
    bump({"smolquery_seal_segment_rows_total", []}, Map.get(measurements, :rows, 0))
  end

  def handle_event([:smolquery, :seal, :stuck], _measurements, _meta, nil) do
    bump({"smolquery_seal_stuck_attempts_total", []}, 1)
  end

  def handle_event([:smolquery, :buffer, :release_failure], _measurements, _meta, nil) do
    bump({"smolquery_seal_release_failures_total", []}, 1)
  end

  def handle_event([:smolquery, :compact, :swap], measurements, meta, nil) do
    bump({"smolquery_compactions_total", [result: result(meta)]}, 1)

    bump(
      {"smolquery_compaction_microseconds_total", [result: result(meta)]},
      Map.get(measurements, :duration_us, 0)
    )

    bump(
      {"smolquery_compaction_segments_replaced_total", []},
      Map.get(measurements, :replaced, 0)
    )
  end

  def handle_event([:smolquery, :compact, :quarantine], _measurements, meta, nil) do
    bump(
      {"smolquery_compaction_quarantined_segments_total", []},
      length(Map.get(meta, :paths, []))
    )
  end

  def handle_event([:smolquery, :hot_manifest, :change], measurements, meta, nil) do
    bump(
      {"smolquery_hot_manifest_index_entries_total", [change: Map.get(meta, :change, :unknown)]},
      Map.get(measurements, :entries, 0)
    )
  end

  def handle_event([:smolquery, :hot_manifest, :read], measurements, meta, nil) do
    labels = [op: Map.get(meta, :op, :unknown)]

    bump({"smolquery_hot_manifest_reads_total", labels}, 1)

    bump(
      {"smolquery_hot_manifest_read_microseconds_total", labels},
      Map.get(measurements, :duration_us, 0)
    )

    bump(
      {"smolquery_hot_manifest_read_entries_total", labels},
      Map.get(measurements, :entries, 0)
    )
  end

  def handle_event([:smolquery, :hot_server, :request], measurements, meta, nil) do
    labels = [route: Map.get(meta, :route, :unknown), method: hot_method(meta)]

    bump(
      {"smolquery_hot_server_requests_total", labels ++ [class: hot_class(meta)]},
      1
    )

    bump(
      {"smolquery_hot_server_microseconds_total", labels},
      Map.get(measurements, :duration_us, 0)
    )

    bump(
      {"smolquery_hot_server_response_bytes_total", labels},
      Map.get(measurements, :response_bytes, 0)
    )

    bump(
      {"smolquery_hot_manifest_entries_total", labels},
      Map.get(measurements, :entries, 0)
    )

    bump({"smolquery_hot_server_range_responses_total", []}, ranged(meta))
  end

  def handle_event([:smolquery, :retention, :sweep], measurements, _meta, nil) do
    bump({"smolquery_retention_segments_dropped_total", []}, Map.get(measurements, :dropped, 0))
    bump({"smolquery_snapshots_expired_total", []}, Map.get(measurements, :expired_snapshots, 0))
  end

  def handle_event([:smolquery, :gc, :sweep], measurements, _meta, nil) do
    bump({"smolquery_gc_segments_swept_total", []}, Map.get(measurements, :swept, 0))
    bump({"smolquery_gc_staged_files_swept_total", []}, Map.get(measurements, :staged, 0))
  end

  def handle_event([:smolquery, :query, :job], measurements, meta, nil) do
    bump({"smolquery_query_jobs_total", [state: Map.get(meta, :state, :unknown)]}, 1)
    bump({"smolquery_query_job_milliseconds_total", []}, Map.get(measurements, :duration_ms, 0))
  end

  def handle_event([:smolquery, :query, :scatter], measurements, _meta, nil) do
    bump({"smolquery_query_scattered_total", []}, 1)
    bump({"smolquery_query_scatter_shards_total", []}, Map.get(measurements, :shards, 0))

    bump(
      {"smolquery_query_scatter_partial_bytes_total", []},
      Map.get(measurements, :partial_bytes, 0)
    )
  end

  def handle_event([:smolquery, :lifecycle, :broadcast], _measurements, meta, nil) do
    bump({"smolquery_lifecycle_broadcasts_total", [kind: Map.get(meta, :kind, :unknown)]}, 1)
  end

  def handle_event(_event, _measurements, _meta, nil), do: :ok

  defp bump(_key, increment) when not is_integer(increment), do: :ok
  defp bump(_key, 0), do: :ok

  defp bump(key, increment) do
    :ets.update_counter(@table, key, increment, {key, 0})

    :ok
  end

  defp committed_rows(measurements, %{result: :ok}), do: Map.get(measurements, :rows, 0)
  defp committed_rows(_measurements, _meta), do: 0

  defp committed_bytes(measurements, %{result: :ok}), do: Map.get(measurements, :bytes, 0)
  defp committed_bytes(_measurements, _meta), do: 0

  # Cumulative buckets, the Prometheus `le` convention, but each series is a
  # plain counter rather than a histogram family: there is no `_sum`/`_count`
  # pair and no `# TYPE histogram`. That keeps the module's counters-only rule
  # while answering the one question a mean cannot — a mean of 5,292 rows is
  # equally consistent with every commit being 5,292 and with half being 800
  # and half 10,000, and the seal path's cost is not linear in segment size
  # (T-333).
  defp bucket_commit_rows(measurements, %{result: :ok} = meta) do
    rows = committed_rows(measurements, meta)

    for bound <- @commit_row_buckets, rows <= bound do
      bump({"smolquery_buffer_commit_rows_bucket", [le: bound]}, 1)
    end

    bump({"smolquery_buffer_commit_rows_bucket", [le: "+Inf"]}, 1)
  end

  defp bucket_commit_rows(_measurements, _meta), do: :ok

  defp flush_reason(%{reason: reason}) when reason in @flush_reasons, do: reason
  defp flush_reason(_meta), do: :unknown

  defp result(%{result: result}) when is_atom(result), do: result
  defp result(_meta), do: :unknown

  defp status_class(status) when is_integer(status), do: "#{div(status, 100)}xx"
  defp status_class(_status), do: "unknown"

  defp hot_class(%{status: status}), do: status_class(status)
  defp hot_class(_meta), do: status_class(nil)

  defp hot_method(%{method: "GET"}), do: :get
  defp hot_method(%{method: "HEAD"}), do: :head
  defp hot_method(%{method: "POST"}), do: :post
  defp hot_method(_meta), do: :other

  defp ranged(%{status: 206}), do: 1
  defp ranged(_meta), do: 0

  defp format_labels([]), do: ""

  defp format_labels(labels) do
    inner = Enum.map_join(labels, ",", fn {key, value} -> ~s(#{key}="#{value}") end)

    "{#{inner}}"
  end
end
