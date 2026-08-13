Code.require_file("support.exs", __DIR__)

defmodule Bench.Query do
  @moduledoc """
  What a query job costs, and where the hot tier's read path spends it.

  Four questions, each pricing a PL-7 decision:

    * **Job engine startup** — D8 gave every job a private engine (isolation,
      cancellation, per-job memory limits) and named its cost the load-bearing
      unknown. This is the number that decides whether a warm pool is needed
      before Milestone 6 puts an HTTP API in front of jobs.
    * **Sync query overhead** — `Client.query("SELECT 1")` end to end: submit,
      runner, engine, plan, execute, frame. The floor under every query.
    * **Planning cost vs hot entry count** — manifest fetch over HTTP, the
      membership filter, pruning, and view SQL, as the hot tier grows.
    * **Hot-tier scan vs micro-segment count, and what pruning saves** — each
      micro-segment is an HTTP footer read before it is data; pruning exists
      to skip exactly that, so both are measured on the same fixture.

  The sealed tier is deliberately thin here: `bench/planner.exs` already
  priced scanning DuckLake and settled that the sealed side plans itself.

      mix run bench/query.exs
      ENTRIES=256 ROWS=2000 REPS=7 mix run bench/query.exs

  ## What this measured, and what it settled

  On the machine this was last run on (`bench/results/query.md` for the full
  tables):

  - **A per-job engine costs ~50 ms, and 87% of it is `LOAD httpfs`,
    `LOAD ducklake`, and the ATTACH — not the engine (5.8 ms bare).** Tens of
    milliseconds, not hundreds, so D8's warm-pool escape hatch stays unbuilt.
    If one is ever built, it should hold bootstrapped connections.
  - **`Client.query("SELECT 1")` is 56 ms end to end** — the engine is the
    whole story, everything else combined ~6 ms. That caps one synchronous
    caller at ~18 qps; jobs run concurrently, so it is a per-caller floor,
    not a node ceiling.
  - **Planning is 8-12 ms and nearly flat from 1 to 256 hot entries.** The
    manifest fetch, membership filter, pruning, and view SQL are not where
    query time goes.
  - **The hot tier costs ~0.7 ms of scan per micro-segment** — the per-file
    HTTP footer read D7 predicted. Linear and modest, but it is what grows
    when sealing falls behind: seal lag made visible to queries.
  - **Pruning works end to end**: a one-batch id range dropped 256 hot files
    to 1 and halved the scan. The saving is per-file cost × files dropped.
  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime

  @parser __MODULE__.Parser

  def run do
    entries = env("ENTRIES", 256)
    rows = env("ROWS", 1_000)
    reps = env("REPS", 7)

    with_tmp_dir("query", fn dir ->
      catalog = start_lake!(__MODULE__.Lake, dir)
      metadata = "sqlite:#{Path.join(dir, "catalog.sqlite")}"
      data_path = Path.join(dir, "data")
      attach = DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)

      buffer = start_buffer!(dir)
      query = start_query!(catalog, buffer, attach)

      {:ok, _pid} = Engine.start_link(name: @parser, extensions: [])

      engine_startup(attach, reps)
      sync_overhead(query, reps)
      hot_tier(query, buffer, entries, rows, reps)
    end)
  end

  defp start_buffer!(dir) do
    name = __MODULE__.Buffer

    {:ok, _pid} =
      BufferService.Supervisor.start_link(
        name: name,
        dir: Path.join(dir, "buffer"),
        flush_interval_ms: 5,
        hot_server_port: 0,
        seal_max_files: 1_000_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000,
        seal_consumer: {BufferService.SealLog, []}
      )

    name
  end

  defp start_query!(catalog, buffer, attach) do
    name = __MODULE__.Query

    {:ok, _pid} =
      QueryService.Supervisor.start_link(
        name: name,
        catalog: catalog,
        buffer_name: buffer,
        buffer_base_url: BufferService.HotServer.base_url(buffer),
        engine_extensions: [:httpfs],
        job_bootstrap: [attach]
      )

    name
  end

  defp engine_startup(attach, reps) do
    heading("Job engine startup (median of #{reps}, ms)")

    variants = [
      {"bare (database + connection)", [], []},
      {"+ httpfs", [:httpfs], []},
      {"+ httpfs + ducklake + ATTACH", [:httpfs, :ducklake], [attach]}
    ]

    for {label_text, extensions, statements} <- variants do
      result = timed(fn -> start_and_stop_engine(extensions, statements) end, reps)

      IO.puts("  #{label(label_text, 32)} #{pad(ms(result.median), 8)}")
    end
  end

  defp start_and_stop_engine(extensions, statements) do
    {:ok, database} = Smolquery.DuckDB.start_link()

    {:ok, connection} =
      Connection.start_link(
        database: database,
        extensions: extensions,
        settings: [memory_limit: "1GB"],
        statements: statements
      )

    {:ok, _result} = Connection.query(connection, "SELECT 1")

    Process.unlink(connection)
    Process.unlink(database)
    Process.exit(connection, :kill)
    Process.exit(database, :kill)

    :ok
  end

  defp sync_overhead(query, reps) do
    heading("Sync query end to end, no tables (median of #{reps})")

    result = timed(fn -> {:ok, _job, _frame} = Client.query(query, "SELECT 1") end, reps)

    IO.puts("  #{label("Client.query(\"SELECT 1\")", 32)} #{pad(ms(result.median), 8)} ms")

    IO.puts(
      "  #{label("implied ceiling", 32)} #{pad(Float.round(1000 / ms(result.median), 1), 8)} qps/caller"
    )
  end

  defp hot_tier(query, buffer, entries, rows, reps) do
    {:ok, runtime} = Runtime.fetch(query)
    parser = Engine.connection_name(@parser)

    count_sql = "SELECT count(*) AS n FROM analytics.events"

    heading("Hot tier: plan cost and scan latency vs micro-segment count")

    IO.puts(
      "  #{label("entries", 8)} #{pad("plan ms", 9)} #{pad("scan ms", 9)} #{pad("rows", 10)}"
    )

    written =
      Enum.reduce(levels(entries), 0, fn level, written ->
        fill(buffer, written, level, rows)

        plan = timed(fn -> {:ok, _plan} = Planner.plan(runtime, parser, count_sql) end, reps)

        scan =
          timed(
            fn ->
              {:ok, %{state: :done}, frame} = Client.query(query, count_sql)
              frame
            end,
            reps
          )

        total = scan.value |> DataFrame.to_columns() |> Map.fetch!("n") |> hd()

        IO.puts(
          "  #{label(level, 8)} #{pad(ms(plan.median), 9)} #{pad(ms(scan.median), 9)} #{pad(total, 10)}"
        )

        level
      end)

    pruning(query, runtime, parser, written, rows, reps)
  end

  defp levels(entries) do
    [1, 32, 256] |> Enum.filter(&(&1 <= entries)) |> Enum.concat([entries]) |> Enum.uniq()
  end

  defp fill(buffer, from, to, rows) when from < to do
    for batch <- (from + 1)..to do
      base = batch * 100_000

      values =
        for i <- 1..rows do
          %{
            "id" => base + i,
            "ts" => NaiveDateTime.add(~N[2026-01-01 00:00:00], batch * 86_400, :second),
            "name" => "row-#{i}",
            "amount" => Decimal.new("#{rem(i, 997)}.#{rem(i, 100)}")
          }
        end

      {:ok, _ack} =
        BufferService.Client.write_batch(buffer, table(), %{schema: schema(), rows: values})
    end

    :ok
  end

  defp fill(_buffer, _from, _to, _rows), do: :ok

  defp pruning(query, runtime, parser, entries, rows, reps) do
    heading("Pruning: one batch's id range out of #{entries} micro-segments")

    base = entries * 100_000

    selective =
      "SELECT count(*) AS n FROM analytics.events WHERE id BETWEEN #{base + 1} AND #{base + rows}"

    {:ok, full} = Planner.plan(runtime, parser, "SELECT count(*) AS n FROM analytics.events")
    {:ok, pruned} = Planner.plan(runtime, parser, selective)

    survivors = fn plan -> plan.hot |> Map.fetch!(table()) |> length() end

    IO.puts("  #{label("entries planned, no predicate", 32)} #{pad(survivors.(full), 8)}")
    IO.puts("  #{label("entries planned, id range", 32)} #{pad(survivors.(pruned), 8)}")

    scan_full =
      timed(fn -> Client.query(query, "SELECT count(*) FROM analytics.events") end, reps)

    scan_pruned = timed(fn -> Client.query(query, selective) end, reps)

    IO.puts("  #{label("scan ms, no predicate", 32)} #{pad(ms(scan_full.median), 8)}")
    IO.puts("  #{label("scan ms, id range", 32)} #{pad(ms(scan_pruned.median), 8)}")
  end
end

Bench.Query.run()
