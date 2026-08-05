Code.require_file("support.exs", __DIR__)

defmodule Bench.OtelLogs do
  @moduledoc """
  The OpenTelemetry logs workload, end to end over the HTTP API (PL-17).

  Every other script in `bench/` isolates a component and calls its client
  directly against a four-column fixture. This one is the opposite: a wide
  OTel log record (61 flattened columns, ~2.1 KB of JSON) streaming in through
  `POST /v1/datasets/:ds/tables/:t/insert` while somebody tails the last 100
  rows through `POST /v1/queries` — Phoenix, the auth plug, the JSON parser,
  the ingest edge's validator, group commit, sealing, the planner, the job
  engine, and result framing all inside the measurement.

  Four sections. One prices a single batch stage by stage; the other three are
  a ceiling, a floor, and the interference between them, which are different
  numbers:

    * **stage profile** — JSON decode, validation, and the rows → Arrow →
      Parquet write for one batch, plus what a *columnar* load would cost
      (Parquet decode, then `DataFrame.to_rows`). This is where the ceiling
      comes from, and it is why a faster wire format alone does not move it.
    * **ingest ceiling** — saturating writers, no reader. Achieved rows/s and
      MiB/s through the front door for a wide row, insert latency percentiles,
      and how much the ack budget (PL-9) shed.
    * **tail floor** — no ingest, tails back to back against the hot tier the
      first phase left. What a tail costs when nothing competes with it.
    * **sustained** — ingest paced at a target rate (default half the measured
      ceiling) with a tailer at 1 Hz. Both distributions, plus **freshness**:
      `now - max(timestamp)` over the page, the staleness a person tailing
      logs actually feels, and the end-to-end statement of the ack contract
      (a 200 means queryable, so freshness should track the flush interval and
      not drift upward across the run).

  Two tail shapes each time — unfiltered, and filtered by service and severity
  the way a real tail is — since `ORDER BY timestamp DESC LIMIT 100` may have
  to touch every surviving file regardless of the predicate.

      mix run bench/otel_logs.exs 2>/dev/null
      WRITERS=8,32 BATCH=4000 RATE=40000 mix run bench/otel_logs.exs 2>/dev/null
      TABLES=8 WRITERS=16 mix run bench/otel_logs.exs 2>/dev/null

  `WRITERS` is the phase-1 sweep (comma-separated); `SUSTAINED_WRITERS`,
  `BATCH`, `SECONDS`, `TAIL_SECONDS`, `SUSTAINED_SECONDS`, `TAIL_INTERVAL_MS`,
  `REPS` (the stage profile's), `RATE` (phase 3's offered rows/s), and
  `FLUSH_MS` are the rest. Redirecting stderr is not cosmetic: ADBC's Explorer
  callback deprecation warns once per query, which would otherwise interleave
  with every table.

  `TABLES` spreads the writers over N tables, which is how to tell a *table's*
  ceiling from the *node's*: one table is one `TableBuffer` doing its encode
  inline, so throughput that scales with `TABLES` says that process is the
  serialization point rather than the machine. The tail always reads the first
  table, so at `TABLES > 1` it sees its share of the rows, not all of them.

  ## Why this script owns the node's boot

  `SmolqueryApi.Endpoint` is a module-based Phoenix endpoint, so unlike
  `BufferService` or `QueryService` there cannot be a second instance beside
  the application's own — an e2e bench has to *be* the node rather than start
  a private stack next to it. `mix run` has already booted every role against
  `priv/data`, so `boot!/1` terminates the role subtrees, repoints the catalog,
  buffer, and sealed directories at a scratch dir, and restarts all of them
  except `SmolqueryWeb.Supervisor` — no web endpoint and no asset watchers in
  the measurement.

  `Application.stop/1` plus `ensure_all_started/1` cannot do this:
  `Smolquery.Telemetry.init/1` attaches its handler with `:ok = attach_many/4`
  and does not trap exits, so a second start crashes on `:already_exists`.
  Restarting the role subtrees skips that child entirely.

  The driver runs on the same machine as the node it measures, so the ceiling
  is a *node* ceiling with a co-resident client — the reported prep cost per
  batch is how much of the machine the driver took.
  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.IngestService.Validator
  alias Smolquery.InternalSecret
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Segments.Writer

  @dataset "logs"
  @table "otel_logs"
  @api_key "otel-logs-bench-key"
  @tail_limit 100
  @pool_size 256

  @subtrees [
    SmolqueryApi.Supervisor,
    SmolqueryWeb.Supervisor,
    Smolquery.QueryService.Supervisor,
    Smolquery.BufferService.Supervisor,
    Smolquery.StorageService.Supervisor,
    Smolquery.IngestService.Supervisor
  ]

  @columns [
    {"timestamp", "TIMESTAMP"},
    {"observed_timestamp", "TIMESTAMP"},
    {"severity_number", "INT64"},
    {"severity_text", "STRING"},
    {"body", "STRING"},
    {"trace_id", "STRING"},
    {"span_id", "STRING"},
    {"trace_flags", "INT64"},
    {"dropped_attributes_count", "INT64"},
    {"service_name", "STRING"},
    {"service_namespace", "STRING"},
    {"service_version", "STRING"},
    {"service_instance_id", "STRING"},
    {"deployment_environment", "STRING"},
    {"cloud_provider", "STRING"},
    {"cloud_region", "STRING"},
    {"cloud_availability_zone", "STRING"},
    {"cloud_account_id", "STRING"},
    {"k8s_cluster_name", "STRING"},
    {"k8s_namespace_name", "STRING"},
    {"k8s_deployment_name", "STRING"},
    {"k8s_pod_name", "STRING"},
    {"k8s_pod_uid", "STRING"},
    {"k8s_container_name", "STRING"},
    {"k8s_node_name", "STRING"},
    {"host_name", "STRING"},
    {"host_arch", "STRING"},
    {"os_type", "STRING"},
    {"os_version", "STRING"},
    {"container_id", "STRING"},
    {"container_image_tag", "STRING"},
    {"telemetry_sdk_name", "STRING"},
    {"telemetry_sdk_language", "STRING"},
    {"telemetry_sdk_version", "STRING"},
    {"scope_name", "STRING"},
    {"scope_version", "STRING"},
    {"code_namespace", "STRING"},
    {"code_function", "STRING"},
    {"code_lineno", "INT64"},
    {"http_request_method", "STRING"},
    {"http_route", "STRING"},
    {"http_response_status_code", "INT64"},
    {"http_request_body_size", "INT64"},
    {"http_response_body_size", "INT64"},
    {"url_path", "STRING"},
    {"url_scheme", "STRING"},
    {"network_protocol_version", "STRING"},
    {"user_agent_original", "STRING"},
    {"client_address", "STRING"},
    {"server_address", "STRING"},
    {"server_port", "INT64"},
    {"duration_ms", "FLOAT64"},
    {"error_type", "STRING"},
    {"exception_type", "STRING"},
    {"exception_message", "STRING"},
    {"exception_stacktrace", "STRING"},
    {"enduser_id", "STRING"},
    {"session_id", "STRING"},
    {"thread_name", "STRING"},
    {"log_file_path", "STRING"},
    {"sampled", "BOOL"}
  ]

  @services ~w(checkout-api cart-api catalog-api payments-api auth-api)
  @routes ~w(/v1/checkout /v1/cart /v1/cart/:id /v1/catalog/items /v1/auth/token)
  @methods ~w(GET POST PUT DELETE)
  @regions ~w(us-east-1 us-west-2 eu-central-1)
  @tail_service "checkout-api"
  @error_severity 17

  def main do
    Logger.configure(level: :warning)

    config = %{
      sweep: sweep_env("WRITERS", [4, 8, 16, 32]),
      writers: env("SUSTAINED_WRITERS", 8),
      batch: env("BATCH", 2_000),
      tables: env("TABLES", 1),
      seconds: env("SECONDS", 10),
      tail_seconds: env("TAIL_SECONDS", 10),
      sustained_seconds: env("SUSTAINED_SECONDS", 30),
      tail_interval_ms: env("TAIL_INTERVAL_MS", 1_000),
      reps: env("REPS", 5),
      rate: env("RATE", 0)
    }

    dir =
      Path.join(
        System.tmp_dir!(),
        "smolquery-bench-otel-#{System.unique_integer([:positive])}"
      )

    try do
      req = boot!(dir)
      create_tables!(req, config.tables)
      pool = pool()

      heading(
        "OTel logs e2e — #{length(@columns)}-column rows through #{base_url()}, " <>
          "#{config.batch} rows/batch × #{config.tables} table(s), " <>
          "flush every #{flush_interval_ms()} ms"
      )

      stage_profile(pool, config, dir)

      ceiling = ingest_ceiling(req, pool, config)
      tail_floor(req, config)
      sustained(req, pool, config, target_rate(config, ceiling))

      counters()
    after
      teardown!(dir)
    end
  end

  # ── the node ──────────────────────────────────────────────────────────

  defp boot!(dir) do
    File.mkdir_p!(dir)

    for id <- @subtrees, do: Supervisor.terminate_child(Smolquery.Supervisor, id)

    Application.put_env(:smolquery, :data_dir, dir)

    Application.put_env(:smolquery, Smolquery.Catalog.DuckLake,
      metadata: "sqlite:#{Path.join(dir, "catalog.sqlite")}",
      data_path: Path.join(dir, "ducklake")
    )

    merge_env(Smolquery.BufferService, dir: Path.join(dir, "buffer"))
    merge_env(Smolquery.StorageService, dir: Path.join(dir, "sealed"))
    merge_env(SmolqueryApi, api_key: @api_key)

    if interval = System.get_env("FLUSH_MS") do
      merge_env(Smolquery.BufferService, flush_interval_ms: String.to_integer(interval))
    end

    for id <- @subtrees -- [SmolqueryWeb.Supervisor] do
      {:ok, _pid} = Supervisor.restart_child(Smolquery.Supervisor, id)
    end

    Req.new(
      base_url: base_url(),
      auth: {:bearer, @api_key},
      retry: false,
      receive_timeout: 120_000
    )
  end

  defp merge_env(key, opts) do
    Application.put_env(
      :smolquery,
      key,
      Keyword.merge(Application.get_env(:smolquery, key, []), opts)
    )
  end

  defp base_url, do: SmolqueryApi.Endpoint.base_url()

  defp flush_interval_ms do
    :smolquery
    |> Application.get_env(Smolquery.BufferService, [])
    |> Keyword.fetch!(:flush_interval_ms)
  end

  defp sweep_env(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> value |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)
    end
  end

  defp teardown!(dir) do
    for id <- @subtrees, do: Supervisor.terminate_child(Smolquery.Supervisor, id)

    File.rm_rf!(dir)
    IO.puts("")
  end

  defp create_tables!(req, count) do
    schema = for {name, type} <- @columns, do: %{"name" => name, "type" => type}

    %{status: 200} = Req.post!(req, url: "/v1/datasets", json: %{"id" => @dataset})

    for index <- 0..(count - 1) do
      %{status: 200} =
        Req.post!(req,
          url: "/v1/datasets/#{@dataset}/tables",
          json: %{"id" => table_at(index), "schema" => schema}
        )
    end

    :ok
  end

  defp table_at(0), do: @table
  defp table_at(index), do: "#{@table}_#{index + 1}"

  # ── phase 0: where one batch's time goes ──────────────────────────────

  defp stage_profile(pool, config, dir) do
    body = body(pool, config.batch, 0)
    schema = table_schema()
    store = Local.new(dir: Path.join(dir, "profile"))
    File.mkdir_p!(Path.join(dir, "profile"))

    heading(
      "Phase 0 — stage profile: one #{config.batch}-row batch " <>
        "(#{mib(byte_size(body))} MiB of JSON), median of #{config.reps}"
    )

    IO.puts("  " <> label("stage", 34) <> pad("ms", 9) <> pad("krows/s", 10) <> pad("share", 8))

    decode = timed(fn -> JSON.decode!(body) end, config.reps)
    rows = Map.fetch!(decode.value, "rows")
    validate = timed(fn -> Validator.validate(schema, rows) end, config.reps)
    {valid, []} = validate.value
    write = timed(fn -> segment!(valid, schema, store) end, config.reps)

    total = decode.median + validate.median + write.median

    for {name, stage} <- [
          {"JSON decode (Phoenix parser)", decode},
          {"validate + coerce (per row)", validate},
          {"rows → Arrow → Parquet + fsync", write}
        ] do
      stage_row(name, stage.median, config.batch, total)
    end

    stage_row("insert path, one core", total, config.batch, total)

    columnar_profile(valid, schema, store, config, total)
  end

  defp columnar_profile(valid, schema, store, config, insert_total) do
    path = segment!(valid, schema, store).path
    read = timed(fn -> {:ok, _frame} = DataFrame.from_parquet(path) end, config.reps)
    {:ok, frame} = DataFrame.from_parquet(path)
    explode = timed(fn -> DataFrame.to_rows(frame) end, config.reps)

    IO.puts("")

    stage_row("Parquet decode → Arrow", read.median, config.batch, insert_total)
    stage_row("Arrow → rows (DataFrame.to_rows)", explode.median, config.batch, insert_total)
  end

  defp stage_row(name, us, batch, total) do
    IO.puts(
      "  " <>
        label(name, 34) <>
        pad(ms(us), 9) <>
        pad(Float.round(batch / us * 1_000, 1), 10) <>
        pad("#{round(us / total * 100)}%", 8)
    )
  end

  defp segment!(rows, schema, store) do
    {:ok, segment} = Writer.write(rows, schema, store: store)

    segment
  end

  defp table_schema do
    Schema.new!(
      for {name, api_type} <- @columns do
        {:ok, type} = Schema.type_from_api(api_type)

        {name, type}
      end
    )
  end

  # ── phase 1: what the front door sustains ─────────────────────────────

  defp ingest_ceiling(req, pool, config) do
    heading(
      "Phase 1 — ingest ceiling: writers sweep, #{config.seconds}s each, no reader " <>
        "(latencies in ms)"
    )

    IO.puts(
      "  " <>
        pad("writers", 9) <>
        pad("rows/s", 10) <>
        pad("MiB/s", 8) <>
        pad("req/s", 8) <>
        pad("p50", 9) <>
        pad("p95", 9) <>
        pad("p99", 9) <> pad("max", 9) <> pad("429s", 7) <> pad("prep", 8)
    )

    warm(req, pool, config.batch)

    config.sweep
    |> Enum.map(&saturate(req, pool, &1, config))
    |> Enum.max()
  end

  defp saturate(req, pool, writers, config) do
    {us, results} =
      :timer.tc(fn ->
        drive(writers, config.tables, fn acc ->
          deadline = System.monotonic_time(:microsecond) + config.seconds * 1_000_000

          step(deadline, fn acc -> insert(req, pool, config.batch, acc) end, acc)
        end)
      end)

    totals = totals(results)
    seconds = us / 1_000_000
    stats = percentiles(totals.latencies)
    rows = round(totals.rows / seconds)

    IO.puts(
      "  " <>
        pad(writers, 9) <>
        pad(rows, 10) <>
        pad(Float.round(totals.bytes / seconds / 1_048_576, 1), 8) <>
        pad(round(totals.requests / seconds), 8) <>
        pad(stats.p50, 9) <>
        pad(stats.p95, 9) <>
        pad(stats.p99, 9) <>
        pad(stats.max, 9) <>
        pad(totals.shed, 7) <> pad(ms(div(totals.prep, max(totals.requests, 1))), 8)
    )

    rows
  end

  # ── phase 2: what a tail costs with nothing in its way ────────────────

  defp tail_floor(req, config) do
    heading("Phase 2 — tail floor (no ingest, #{config.tail_seconds}s, last #{@tail_limit})")

    state(req, config.tables)
    tail_header()

    for shape <- [:all, :filtered] do
      deadline = System.monotonic_time(:microsecond) + div(config.tail_seconds, 2) * 1_000_000

      samples =
        collect(deadline, 0, fn acc ->
          tail(req, shape, acc)
        end)

      report_tail(shape, samples)
    end
  end

  # ── phase 3: the workload ─────────────────────────────────────────────

  defp sustained(req, pool, config, target) do
    heading(
      "Phase 3 — sustained (#{config.sustained_seconds}s, #{target} rows/s offered, " <>
        "tail every #{config.tail_interval_ms} ms)"
    )

    period = round(config.batch / (target / config.writers) * 1_000_000)
    started = System.monotonic_time(:microsecond)
    deadline = started + config.sustained_seconds * 1_000_000

    tailer =
      Task.async(fn ->
        paced(
          deadline,
          config.tail_interval_ms * 1_000,
          fn acc -> tail(req, :all, acc) end,
          acc(0, @table)
        )
      end)

    writes =
      drive(config.writers, config.tables, fn acc ->
        paced(deadline, period, fn acc -> insert(req, pool, config.batch, acc) end, acc)
      end)

    tails = Task.await(tailer, :infinity)

    elapsed = (System.monotonic_time(:microsecond) - started) / 1_000_000
    totals = totals(writes)

    IO.puts(
      "  " <>
        pad("offered", 10) <>
        pad("achieved", 10) <>
        pad("p50", 9) <>
        pad("p95", 9) <> pad("p99", 9) <> pad("max", 9) <> pad("429s", 7) <> pad("late", 7)
    )

    stats = percentiles(totals.latencies)

    IO.puts(
      "  " <>
        pad(target, 10) <>
        pad(round(totals.rows / elapsed), 10) <>
        pad(stats.p50, 9) <>
        pad(stats.p95, 9) <>
        pad(stats.p99, 9) <>
        pad(stats.max, 9) <> pad(totals.shed, 7) <> pad(totals.late, 7)
    )

    IO.puts("  inserts above, tail below — ms")

    tail_header()
    report_tail(:all, tails)

    state(req, config.tables)
    timeline(writes, tails, started, elapsed)
  end

  defp target_rate(%{rate: rate}, _ceiling) when rate > 0, do: rate
  defp target_rate(_config, ceiling), do: div(ceiling, 2)

  defp timeline(writes, tails, started, elapsed) do
    heading("Phase 3 timeline — per second")

    IO.puts(
      "  " <>
        pad("second", 8) <>
        pad("rows/s", 10) <>
        pad("insert p50", 12) <> pad("tail ms", 10) <> pad("fresh ms", 10)
    )

    inserts = Enum.group_by(Enum.flat_map(writes, & &1.samples), &bucket(&1, started))
    reads = Enum.group_by(tails.samples, &bucket(&1, started))

    for second <- 0..trunc(elapsed) do
      batches = Map.get(inserts, second, [])
      read = Map.get(reads, second, [])

      IO.puts(
        "  " <>
          pad(second, 8) <>
          pad(Enum.sum(Enum.map(batches, & &1.rows)), 10) <>
          pad(percentiles(Enum.map(batches, & &1.us)).p50 || "-", 12) <>
          pad(percentiles(Enum.map(read, & &1.us)).p50 || "-", 10) <>
          pad(percentiles(Enum.map(read, &(&1.fresh * 1_000_000))).p50 || "-", 10)
      )
    end
  end

  defp bucket(sample, started), do: div(sample.at - started, 1_000_000)

  # ── driving ───────────────────────────────────────────────────────────

  defp drive(writers, tables, fun) do
    1..writers
    |> Task.async_stream(
      fn writer -> fun.(acc(writer, table_at(rem(writer - 1, tables)))) end,
      max_concurrency: writers,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp collect(deadline, writer, fun), do: step(deadline, fun, acc(writer, @table))

  defp acc(writer, table) do
    %{
      samples: [],
      rows: 0,
      bytes: 0,
      prep: 0,
      shed: 0,
      errors: 0,
      late: 0,
      writer: writer,
      table: table
    }
  end

  defp step(deadline, fun, acc) do
    if System.monotonic_time(:microsecond) >= deadline do
      acc
    else
      step(deadline, fun, fun.(acc))
    end
  end

  defp paced(deadline, period, fun, acc),
    do: pace(deadline, period, System.monotonic_time(:microsecond), fun, acc)

  defp pace(deadline, period, tick, fun, acc) do
    now = System.monotonic_time(:microsecond)

    cond do
      now >= deadline ->
        acc

      now < tick ->
        Process.sleep(max(div(tick - now, 1_000), 1))
        pace(deadline, period, tick, fun, acc)

      true ->
        acc = fun.(acc)
        late = if System.monotonic_time(:microsecond) > tick + period, do: 1, else: 0

        pace(deadline, period, tick + period, fun, %{acc | late: acc.late + late})
    end
  end

  # ── the two requests ──────────────────────────────────────────────────

  defp insert(req, pool, batch, acc) do
    {prep, body} = :timer.tc(fn -> body(pool, batch, acc.rows + acc.writer) end)

    {us, response} =
      :timer.tc(fn ->
        Req.post!(req,
          url: "/v1/datasets/#{@dataset}/tables/#{acc.table}/insert",
          headers: [{"content-type", "application/json"}],
          body: body
        )
      end)

    acc = %{
      acc
      | prep: acc.prep + prep,
        bytes: acc.bytes + byte_size(body),
        samples: [%{at: System.monotonic_time(:microsecond), us: us, rows: batch} | acc.samples]
    }

    case response.status do
      200 -> %{acc | rows: acc.rows + response.body["insertedRows"]}
      429 -> %{acc | shed: acc.shed + 1}
      _other -> %{acc | errors: acc.errors + 1}
    end
  end

  defp tail(req, shape, acc) do
    {us, response} =
      :timer.tc(fn ->
        Req.post!(req,
          url: "/v1/queries",
          json: %{"query" => tail_sql(shape), "maxResults" => @tail_limit}
        )
      end)

    case response.status do
      200 ->
        rows = response.body["rows"]

        %{
          acc
          | rows: acc.rows + length(rows),
            samples: [
              %{
                at: System.monotonic_time(:microsecond),
                us: us,
                rows: length(rows),
                fresh: freshness(rows)
              }
              | acc.samples
            ]
        }

      _other ->
        %{acc | errors: acc.errors + 1}
    end
  end

  defp tail_sql(:all),
    do: "SELECT * FROM #{@dataset}.#{@table} ORDER BY timestamp DESC LIMIT #{@tail_limit}"

  defp tail_sql(:filtered) do
    "SELECT * FROM #{@dataset}.#{@table} " <>
      "WHERE service_name = '#{@tail_service}' AND severity_number >= #{@error_severity} " <>
      "ORDER BY timestamp DESC LIMIT #{@tail_limit}"
  end

  defp freshness([]), do: 0.0

  defp freshness([newest | _rest]) do
    at = NaiveDateTime.from_iso8601!(Map.fetch!(newest, "timestamp"))

    NaiveDateTime.diff(NaiveDateTime.utc_now(), at, :microsecond) / 1_000_000
  end

  defp warm(req, pool, batch), do: insert(req, pool, batch, acc(0, @table))

  # ── reporting ─────────────────────────────────────────────────────────

  defp tail_header do
    IO.puts(
      "  " <>
        label("shape", 10) <>
        pad("tails", 7) <>
        pad("p50", 9) <>
        pad("p95", 9) <>
        pad("p99", 9) <> pad("max", 9) <> pad("rows", 10) <> pad("fresh p50", 11)
    )
  end

  defp report_tail(shape, samples) do
    stats = percentiles(Enum.map(samples.samples, & &1.us))
    fresh = percentiles(Enum.map(samples.samples, &(&1.fresh * 1_000_000)))

    IO.puts(
      "  " <>
        label(shape, 10) <>
        pad(stats.n, 7) <>
        pad(stats.p50, 9) <>
        pad(stats.p95, 9) <>
        pad(stats.p99, 9) <>
        pad(stats.max, 9) <>
        pad(rows_returned(samples.samples), 10) <> pad(fresh.p50, 11)
    )
  end

  defp rows_returned([]), do: "-"

  defp rows_returned(samples) do
    {low, high} = samples |> Enum.map(& &1.rows) |> Enum.min_max()

    "#{low}-#{high}"
  end

  defp state(req, tables) do
    entries =
      Enum.flat_map(0..(tables - 1), fn index ->
        {:ok, entries} =
          BufferService.Client.hot_manifest(Smolquery.BufferService, {@dataset, table_at(index)})

        entries
      end)

    unsealed = Enum.count(entries, &is_nil(&1.retired_at))

    rows =
      Req.post!(req,
        url: "/v1/queries",
        json: %{"query" => "SELECT count(*) AS n FROM #{@dataset}.#{@table}"}
      ).body["rows"]

    IO.puts(
      "  #{hd(rows)["n"]} rows × #{length(@columns)} columns in #{@table}, " <>
        "#{unsealed} unsealed micro-segments a tail reads " <>
        "(#{length(entries)} in the manifest, the rest retired inside their grace period)"
    )
  end

  defp counters do
    heading("Node counters (GET /metrics)")

    body =
      Req.get!(
        base_url() <> "/metrics",
        headers: [{InternalSecret.header(), InternalSecret.value()}]
      ).body

    for name <- ~w(smolquery_buffer_commits_total smolquery_buffer_rows_committed_total
                   smolquery_buffer_admission_refused_rows_total smolquery_seal_attempts_total
                   smolquery_ingest_rows_rejected_total smolquery_query_jobs_total) do
      IO.puts(
        "  #{label(String.replace_prefix(name, "smolquery_", ""), 40)} #{series(body, name)}"
      )
    end
  end

  defp series(body, name) do
    ~r/^#{name}(\{[^}]*\})? ([\d.e+]+)$/m
    |> Regex.scan(body)
    |> Enum.map_join(", ", fn match -> match |> List.last() |> round_series() end)
    |> case do
      "" -> "-"
      text -> text
    end
  end

  defp round_series(value) do
    {number, _rest} = Float.parse(value)

    number |> round() |> to_string()
  end

  defp totals(results) do
    Enum.reduce(
      results,
      %{rows: 0, bytes: 0, prep: 0, shed: 0, errors: 0, late: 0, requests: 0, latencies: []},
      fn result, acc ->
        %{
          rows: acc.rows + result.rows,
          bytes: acc.bytes + result.bytes,
          prep: acc.prep + result.prep,
          shed: acc.shed + result.shed,
          errors: acc.errors + result.errors,
          late: acc.late + result.late,
          requests: acc.requests + length(result.samples),
          latencies: acc.latencies ++ Enum.map(result.samples, & &1.us)
        }
      end
    )
  end

  # ── the rows ──────────────────────────────────────────────────────────

  defp pool do
    templates = for i <- 0..(@pool_size - 1), do: template(i)

    case Enum.count(templates, &tailable?/1) do
      0 ->
        raise "no #{@tail_service} row at severity >= #{@error_severity} in the pool: " <>
                "the filtered tail would be measuring an empty result"

      _matches ->
        List.to_tuple(templates)
    end
  end

  defp tailable?(template) do
    template["service_name"] == @tail_service and
      template["severity_number"] >= @error_severity
  end

  defp body(pool, batch, offset) do
    base = DateTime.utc_now()

    rows =
      for i <- 0..(batch - 1) do
        at = DateTime.add(base, i, :microsecond)

        pool
        |> elem(rem(offset + i, @pool_size))
        |> Map.merge(%{
          "timestamp" => DateTime.to_iso8601(at),
          "observed_timestamp" => at |> DateTime.add(2, :millisecond) |> DateTime.to_iso8601()
        })
      end

    JSON.encode!(%{"rows" => rows})
  end

  defp template(i) do
    {severity, text} = severity(rem(i * 7, 100))
    service = Enum.at(@services, rem(div(i, 5), length(@services)))
    route = Enum.at(@routes, rem(i, length(@routes)))
    status = if severity >= @error_severity, do: 500, else: 200
    pod = "#{service}-#{rem(i, 8)}f4c9-#{rem(i, 26)}kx2"
    region = Enum.at(@regions, rem(i, length(@regions)))

    %{
      "severity_number" => severity,
      "severity_text" => text,
      "body" => body_text(text, service, route, status, i),
      "trace_id" => hex(i, 32),
      "span_id" => hex(i * 3, 16),
      "trace_flags" => 1,
      "dropped_attributes_count" => 0,
      "service_name" => service,
      "service_namespace" => "storefront",
      "service_version" => "2.#{rem(i, 12)}.#{rem(i, 5)}",
      "service_instance_id" => pod,
      "deployment_environment" => "production",
      "cloud_provider" => "aws",
      "cloud_region" => region,
      "cloud_availability_zone" => region <> Enum.at(~w(a b c), rem(i, 3)),
      "cloud_account_id" => "9284#{rem(i, 100)}17263",
      "k8s_cluster_name" => "storefront-prod",
      "k8s_namespace_name" => "storefront",
      "k8s_deployment_name" => service,
      "k8s_pod_name" => pod,
      "k8s_pod_uid" => hex(i * 11, 32),
      "k8s_container_name" => service,
      "k8s_node_name" => "ip-10-#{rem(i, 200)}-#{rem(i, 250)}-14.ec2.internal",
      "host_name" => "ip-10-#{rem(i, 200)}-#{rem(i, 250)}-14",
      "host_arch" => "arm64",
      "os_type" => "linux",
      "os_version" => "6.1.#{rem(i, 90)}-amzn2023",
      "container_id" => hex(i * 13, 64),
      "container_image_tag" => "sha256-#{hex(i * 17, 12)}",
      "telemetry_sdk_name" => "opentelemetry",
      "telemetry_sdk_language" => "erlang",
      "telemetry_sdk_version" => "1.5.0",
      "scope_name" => "#{service}.plug.router",
      "scope_version" => "1.20.2",
      "code_namespace" =>
        "Storefront.#{Macro.camelize(String.replace(service, "-", "_"))}.Router",
      "code_function" => "call/2",
      "code_lineno" => 40 + rem(i, 600),
      "http_request_method" => Enum.at(@methods, rem(i, length(@methods))),
      "http_route" => route,
      "http_response_status_code" => status,
      "http_request_body_size" => rem(i * 97, 8_192),
      "http_response_body_size" => rem(i * 131, 65_536),
      "url_path" => String.replace(route, ":id", to_string(1_000 + rem(i, 9_000))),
      "url_scheme" => "https",
      "network_protocol_version" => "2",
      "user_agent_original" =>
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " <>
          "(KHTML, like Gecko) Chrome/1#{rem(i, 30)}.0.0.0 Safari/537.36",
      "client_address" => "203.0.#{rem(i, 250)}.#{rem(i * 3, 250)}",
      "server_address" => "#{service}.storefront.svc.cluster.local",
      "server_port" => 4000 + rem(i, 3),
      "duration_ms" => Float.round(1.5 + rem(i * 37, 4_000) / 100, 2),
      "error_type" => error_field(severity, "Storefront.PaymentError"),
      "exception_type" => error_field(severity, "Storefront.PaymentError"),
      "exception_message" => error_field(severity, "gateway declined authorization #{i}"),
      "exception_stacktrace" => error_field(severity, stacktrace(i)),
      "enduser_id" => "usr_#{hex(i * 19, 16)}",
      "session_id" => "sess_#{hex(i * 23, 24)}",
      "thread_name" => "erl_sched_#{rem(i, 10)}",
      "log_file_path" => "/var/log/pods/storefront_#{pod}/#{service}/0.log",
      "sampled" => rem(i, 4) != 0
    }
  end

  defp severity(draw) when draw < 1, do: {1, "TRACE"}
  defp severity(draw) when draw < 16, do: {5, "DEBUG"}
  defp severity(draw) when draw < 86, do: {9, "INFO"}
  defp severity(draw) when draw < 96, do: {13, "WARN"}
  defp severity(_draw), do: {@error_severity, "ERROR"}

  defp body_text("ERROR", service, route, status, i) do
    "#{service} #{route} failed with #{status} after #{rem(i * 37, 4_000) / 100}ms: " <>
      "gateway declined authorization #{i}, retry scheduled"
  end

  defp body_text(text, service, route, status, i) do
    "#{text} #{service} handled #{route} #{status} in #{rem(i * 37, 4_000) / 100}ms " <>
      "req_id=#{hex(i * 29, 16)} bytes=#{rem(i * 131, 65_536)}"
  end

  defp error_field(severity, value) when severity >= @error_severity, do: value
  defp error_field(_severity, _value), do: nil

  defp stacktrace(i) do
    Enum.map_join(1..4, "\n", fn frame ->
      "    (storefront 2.1.0) lib/storefront/payments.ex:#{frame * 40 + rem(i, 30)}: " <>
        "Storefront.Payments.authorize/#{frame}"
    end)
  end

  defp hex(seed, length) do
    digest = :crypto.hash(:sha256, <<seed::64>>)

    digest |> Base.encode16(case: :lower) |> binary_part(0, length)
  end
end

Bench.OtelLogs.main()
