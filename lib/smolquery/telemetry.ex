defmodule Smolquery.Telemetry do
  @moduledoc """
  The node's metrics: `:telemetry` events in, Prometheus text out (PL-10 D8).

  Every service emits plain `:telemetry` events at its seams and knows nothing
  about metrics; this module attaches to those events, keeps counters in a
  public ETS table, and renders them for `GET /metrics`. Events are the seam
  on purpose — a StatsD or OpenTelemetry exporter can attach to the same
  events later without touching a call site.

  Counters only, deliberately. A counter answers rates and ratios under
  `rate()` in whatever scrapes this, and paired totals answer averages
  (`_microseconds_total / _total` is a mean duration) — the shapes operators
  actually alert on — without this module growing histogram buckets to keep
  honest. Labels are closed sets (a result atom, a status class), never a
  table or job id, so cardinality is bounded by this file rather than by
  traffic.

  Handlers run in the emitting process, so they are two ETS
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
                                            replicate_us}, meta %{result: :ok | :error}
      [:smolquery, :buffer, :admission]   %{rows}, meta %{outcome: :refused}
      [:smolquery, :buffer, :dedup]       %{rows}
      [:smolquery, :seal, :attempt]       %{}, meta %{result: :ok | :error | :crashed}
      [:smolquery, :compact, :swap]       %{replaced}, meta %{result: :ok | :error}
      [:smolquery, :retention, :sweep]    %{dropped, expired_snapshots}
      [:smolquery, :gc, :sweep]           %{swept, staged}
      [:smolquery, :query, :job]          %{duration_ms}, meta %{state: :done | :failed | :cancelled}
  """

  use GenServer

  @table __MODULE__
  @handler_id "smolquery-metrics"

  @events [
    [:smolquery, :api, :stop],
    [:smolquery, :ingest, :insert],
    [:smolquery, :buffer, :commit],
    [:smolquery, :buffer, :wire],
    [:smolquery, :buffer, :admission],
    [:smolquery, :buffer, :dedup],
    [:smolquery, :seal, :attempt],
    [:smolquery, :compact, :swap],
    [:smolquery, :retention, :sweep],
    [:smolquery, :gc, :sweep],
    [:smolquery, :query, :job]
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
    "smolquery_compactions_total" => "Compaction swaps, by result.",
    "smolquery_compaction_segments_replaced_total" =>
      "Sealed segments replaced by compaction merges.",
    "smolquery_retention_segments_dropped_total" => "Sealed segments dropped past their TTL.",
    "smolquery_snapshots_expired_total" => "Catalog snapshots expired by retention sweeps.",
    "smolquery_gc_segments_swept_total" => "Uncommitted sealed segments GC deleted.",
    "smolquery_gc_staged_files_swept_total" => "Leaked staging files GC deleted.",
    "smolquery_query_jobs_total" => "Query jobs reaching a terminal state, by state.",
    "smolquery_query_job_milliseconds_total" =>
      "Time query jobs ran; divide by jobs for the mean."
  }

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
      header = "# HELP #{name} #{Map.get(@help, name, name)}\n# TYPE #{name} counter\n"

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

  def handle_event([:smolquery, :buffer, :admission], measurements, _meta, nil) do
    bump({"smolquery_buffer_admission_refused_rows_total", []}, Map.get(measurements, :rows, 0))
  end

  def handle_event([:smolquery, :buffer, :dedup], measurements, _meta, nil) do
    bump({"smolquery_buffer_dedup_rows_total", []}, Map.get(measurements, :rows, 0))
  end

  def handle_event([:smolquery, :seal, :attempt], _measurements, meta, nil) do
    bump({"smolquery_seal_attempts_total", [result: result(meta)]}, 1)
  end

  def handle_event([:smolquery, :compact, :swap], measurements, meta, nil) do
    bump({"smolquery_compactions_total", [result: result(meta)]}, 1)

    bump(
      {"smolquery_compaction_segments_replaced_total", []},
      Map.get(measurements, :replaced, 0)
    )
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

  def handle_event(_event, _measurements, _meta, nil), do: :ok

  defp bump(_key, increment) when not is_integer(increment), do: :ok
  defp bump(_key, 0), do: :ok

  defp bump(key, increment) do
    :ets.update_counter(@table, key, increment, {key, 0})

    :ok
  end

  defp committed_rows(measurements, %{result: :ok}), do: Map.get(measurements, :rows, 0)
  defp committed_rows(_measurements, _meta), do: 0

  defp result(%{result: result}) when is_atom(result), do: result
  defp result(_meta), do: :unknown

  defp status_class(status) when is_integer(status), do: "#{div(status, 100)}xx"
  defp status_class(_status), do: "unknown"

  defp format_labels([]), do: ""

  defp format_labels(labels) do
    inner = Enum.map_join(labels, ",", fn {key, value} -> ~s(#{key}="#{value}") end)

    "{#{inner}}"
  end
end
