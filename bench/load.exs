Code.require_file("support.exs", __DIR__)
Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.Load do
  @moduledoc """
  What a batch load costs, and which format to send (PL-18).

  `POST /v1/datasets/:ds/tables/:t/load` takes the file as the body — NDJSON, CSV,
  or Parquet by content type — spools it to disk, parses it, and pushes 10,000-row
  chunks through the same insert path a streaming write uses. This measures that
  path over real HTTP with `bench/otel_logs.exs`'s 61-column OTel fixture, so the
  two are comparable.

  Four questions:

    * **Which format is fastest?** `bench/results/otel_logs.md`'s stage profile
      predicts Parquet is *not*, despite decoding 38× faster than JSON: the
      controller's `indexed/1` calls `DataFrame.to_rows`, which costs more than the
      JSON parse it replaces. That prediction deserves an end-to-end test, because
      anyone picking a bulk format will assume the opposite.
    * **What does `load_max_bytes` mean in rows?** It is a byte cap, and at 61
      columns NDJSON, CSV, and Parquet carry a row in wildly different numbers of
      bytes — so the same cap admits very different row counts. Derived from the
      measured bytes/row rather than by hunting the 413 boundary.
    * **What does a load cost in memory?** `write_body/3` streams the body to a
      temp file in 8 MB chunks, but `parse/3` then materializes *every* row before
      chunking, and `Stream.chunk_every` runs over an already-complete list. So the
      spool bounds the request, not the heap. Sampled from `:erlang.memory/1`.
    * **Is a load faster than the equivalent inserts?** Same rows as one NDJSON
      file, then as N 2,000-row `POST /insert` calls from one writer. Both serial,
      so this compares paths rather than concurrency.

        mix run bench/load.exs 2>/dev/null
        ROWS=100000 SCALE=25000,100000 mix run bench/load.exs 2>/dev/null

  `ROWS` is the shootout's row count, `SCALE` the sweep's (comma-separated), `BATCH`
  the insert comparison's batch size, `POOL` the fixture's cardinality. The
  controller's own chunking is a fixed 10,000 rows. Redirecting stderr is not
  cosmetic: ADBC's Explorer callback deprecation warns once per query.

  Every load reports `cores` — whole CPUs consumed, from the OS process's CPU time,
  so it counts DuckDB's native threads as well as the BEAM. On a 10-core machine a
  load measures ~1.0, because one request is one process: that is the parallelism
  gap, not an inference about it.

  Fixture files are built through the production writer — the Parquet file comes
  from `Smolquery.Segments.Writer.write/3`, which is exactly what an export from
  smolquery looks like, and the CSV is that file re-serialized by Polars. NDJSON is
  built by hand, because that is what a client sends. Building them is driver-side
  work, reported separately and never inside a load's latency.
  """

  import Bench.Otel
  import Bench.Support, except: [table: 0, schema: 0]

  alias Explorer.DataFrame
  alias Smolquery.IngestService.Validator
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Segments.Writer

  @formats [
    {:ndjson, "application/x-ndjson", "ndjson"},
    {:csv, "text/csv", "csv"},
    {:parquet, "application/vnd.apache.parquet", "parquet"}
  ]

  @sample_ms 50

  def main do
    Logger.configure(level: :warning)

    config = %{
      rows: env("ROWS", 50_000),
      scale: sweep_env("SCALE", [10_000, 50_000]),
      batch: env("BATCH", 2_000)
    }

    dir =
      Path.join(
        System.tmp_dir!(),
        "smolquery-bench-load-#{System.unique_integer([:positive])}"
      )

    try do
      req = boot!(dir)
      create_tables!(req, 1)

      heading(
        "Batch loads — #{length(columns())}-column OTel rows through " <>
          "#{base_url()}/v1/…/load, cap #{mib(load_max_bytes())} MiB"
      )

      schedulers()
      files = build!(dir, config.rows)

      shootout(req, files, config.rows)
      cap_in_rows(files, config.rows)
      scaling(req, dir, config)
      memory(req, files)
      versus_insert(req, dir, config)

      counters()
    after
      teardown!(dir)
    end
  end

  # ── fixtures ──────────────────────────────────────────────────────────

  defp build!(dir, rows) do
    heading("Fixture files — #{rows} rows, built driver-side (not in any load below)")

    IO.puts(
      "  " <> label("format", 10) <> pad("MiB", 9) <> pad("B/row", 9) <> pad("build ms", 10)
    )

    files =
      Map.new(@formats, fn {format, _content_type, extension} ->
        path = Path.join(dir, "fixture-#{rows}.#{extension}")
        {us, :ok} = :timer.tc(fn -> write_fixture!(format, path, dir, rows) end)
        bytes = File.stat!(path).size

        IO.puts(
          "  " <>
            label(format, 10) <>
            pad(mib(bytes), 9) <>
            pad(round(bytes / rows), 9) <> pad(ms(us), 10)
        )

        {format, %{path: path, bytes: bytes}}
      end)

    files
  end

  defp write_fixture!(:ndjson, path, _dir, rows) do
    pool = pool()

    File.open!(path, [:write, :raw, :binary], fn file ->
      for chunk <- chunks(rows) do
        lines = Enum.map_join(rows(pool, chunk.count, chunk.offset), "\n", &JSON.encode!/1)

        IO.binwrite(file, [lines, "\n"])
      end
    end)

    :ok
  end

  defp write_fixture!(:parquet, path, dir, rows) do
    File.cp!(segment_path!(dir, rows), path)

    :ok
  end

  defp write_fixture!(:csv, path, dir, rows) do
    {:ok, frame} = DataFrame.from_parquet(segment_path!(dir, rows))

    :ok = DataFrame.to_csv(frame, path)
  end

  defp segment_path!(dir, rows) do
    case Process.get({:segment, rows}) do
      nil ->
        schema = table_schema()
        store = Local.new(dir: Path.join(dir, "fixtures"))
        File.mkdir_p!(Path.join(dir, "fixtures"))

        pool = pool()

        frames =
          for chunk <- chunks(rows) do
            {valid, []} = Validator.validate(schema, rows(pool, chunk.count, chunk.offset))

            valid
          end

        {:ok, segment} = Writer.write(List.flatten(frames), schema, store: store)
        Process.put({:segment, rows}, segment.path)

        segment.path

      path ->
        path
    end
  end

  defp chunks(rows, size \\ 10_000), do: chunks(rows, size, 0, [])

  defp chunks(remaining, size, offset, acc) when remaining > 0 do
    count = min(remaining, size)

    chunks(remaining - count, size, offset + count, [%{count: count, offset: offset} | acc])
  end

  defp chunks(_remaining, _size, _offset, acc), do: Enum.reverse(acc)

  # ── phase 1: which format ─────────────────────────────────────────────

  defp shootout(req, files, rows) do
    heading("Phase 1 — same #{rows} rows through each format, one request each")

    IO.puts(
      "  " <>
        label("format", 10) <>
        pad("MiB", 8) <>
        pad("load ms", 10) <>
        pad("rows/s", 10) <>
        pad("MiB/s", 8) <>
        pad("inserted", 10) <>
        pad("status", 8) <> pad("cores", 8) <> pad("sched%", 9)
    )

    for {format, content_type, _extension} <- @formats do
      file = Map.fetch!(files, format)
      cpu = cpu_snapshot()
      {us, response} = load(req, table(), content_type, file.path)

      report_load(format, file.bytes, us, response, cpu_since(cpu))
    end
  end

  defp report_load(label_text, bytes, us, response, used) do
    seconds = us / 1_000_000
    inserted = response.body["insertedRows"] || 0

    IO.puts(
      "  " <>
        label(label_text, 10) <>
        pad(mib(bytes), 8) <>
        pad(ms(us), 10) <>
        pad(round(inserted / seconds), 10) <>
        pad(Float.round(bytes / seconds / 1_048_576, 1), 8) <>
        pad(inserted, 10) <>
        pad(response.status, 8) <> pad(used.cores, 8) <> pad(used.scheduler, 9)
    )
  end

  defp load(req, table, content_type, path) do
    :timer.tc(fn ->
      Req.post!(req,
        url: "/v1/datasets/#{dataset()}/tables/#{table}/load",
        headers: [{"content-type", content_type}],
        body: File.stream!(path, 8_000_000)
      )
    end)
  end

  # ── phase 2: what the cap means ───────────────────────────────────────

  defp cap_in_rows(files, rows) do
    heading("Phase 1b — what `load_max_bytes` admits, per format (derived)")

    IO.puts("  " <> label("format", 10) <> pad("B/row", 9) <> pad("rows per load", 15))

    for {format, _content_type, _extension} <- @formats do
      per_row = Map.fetch!(files, format).bytes / rows

      IO.puts(
        "  " <>
          label(format, 10) <>
          pad(round(per_row), 9) <> pad(round(load_max_bytes() / per_row), 15)
      )
    end
  end

  defp load_max_bytes do
    {:ok, runtime} = SmolqueryApi.Runtime.fetch(SmolqueryApi)

    runtime.load_max_bytes
  end

  # ── phase 3: does it scale linearly ───────────────────────────────────

  defp scaling(req, dir, config) do
    heading("Phase 2 — rows/s against file size (does parse-then-chunk stay linear)")

    IO.puts(
      "  a load shorter than the #{flush_interval_ms()} ms flush interval is mostly " <>
        "waiting for a group commit,\n  so read only the rows large enough to swamp it"
    )

    IO.puts(
      "  " <>
        label("format", 10) <>
        pad("rows", 10) <> pad("MiB", 8) <> pad("load ms", 10) <> pad("rows/s", 10)
    )

    for {format, content_type, extension} <- @formats, rows <- config.scale do
      path = Path.join(dir, "scale-#{rows}.#{extension}")
      :ok = write_fixture!(format, path, dir, rows)
      bytes = File.stat!(path).size

      {us, response} = load(req, table(), content_type, path)
      inserted = response.body["insertedRows"] || 0

      IO.puts(
        "  " <>
          label(format, 10) <>
          pad(rows, 10) <>
          pad(mib(bytes), 8) <>
          pad(ms(us), 10) <> pad(round(inserted / (us / 1_000_000)), 10)
      )

      File.rm(path)
    end
  end

  # ── phase 4: what it costs in memory ──────────────────────────────────

  defp memory(req, files) do
    heading("Phase 3 — peak BEAM memory across one NDJSON load (#{@sample_ms} ms samples)")

    file = Map.fetch!(files, :ndjson)
    baseline = :erlang.memory()
    sampler = Task.async(fn -> sample([], self()) end)

    {us, _response} = load(req, table(), "application/x-ndjson", file.path)

    send(sampler.pid, :stop)
    samples = Task.await(sampler, 60_000)

    IO.puts(
      "  " <>
        label("kind", 10) <>
        pad("baseline", 12) <> pad("peak", 12) <> pad("delta", 12) <> pad("× file", 9)
    )

    for kind <- [:total, :binary, :processes] do
      peak = samples |> Enum.map(&Keyword.fetch!(&1, kind)) |> Enum.max(fn -> 0 end)
      base = Keyword.fetch!(baseline, kind)

      IO.puts(
        "  " <>
          label(kind, 10) <>
          pad(mib(base), 12) <>
          pad(mib(peak), 12) <>
          pad(mib(peak - base), 12) <> pad(Float.round((peak - base) / file.bytes, 2), 9)
      )
    end

    IO.puts(
      "  #{mib(file.bytes)} MiB file, #{ms(us)} ms load, #{length(samples)} samples — " <>
        "a #{@sample_ms} ms sampler can miss a spike, so peak is a floor"
    )
  end

  defp sample(acc, _parent) do
    receive do
      :stop -> Enum.reverse(acc)
    after
      @sample_ms -> sample([:erlang.memory() | acc], self())
    end
  end

  # ── phase 5: against the streaming path ───────────────────────────────

  defp versus_insert(req, dir, config) do
    rows = hd(config.scale)

    heading(
      "Phase 4 — #{rows} rows: one NDJSON load vs #{div(rows, config.batch)} × " <>
        "#{config.batch}-row inserts, both serial"
    )

    IO.puts("  " <> label("path", 24) <> pad("ms", 10) <> pad("rows/s", 10) <> pad("cores", 8))

    path = Path.join(dir, "versus.ndjson")
    :ok = write_fixture!(:ndjson, path, dir, rows)

    cpu = cpu_snapshot()
    {load_us, response} = load(req, table(), "application/x-ndjson", path)
    load_used = cpu_since(cpu)
    File.rm(path)

    IO.puts(
      "  " <>
        label("POST /load, one file", 24) <>
        pad(ms(load_us), 10) <>
        pad(round((response.body["insertedRows"] || 0) / (load_us / 1_000_000)), 10) <>
        pad(load_used.cores, 8)
    )

    pool = pool()
    insert_cpu = cpu_snapshot()

    {insert_us, inserted} =
      :timer.tc(fn ->
        Enum.reduce(chunks(rows, config.batch), 0, fn chunk, total ->
          total + insert(req, pool, chunk)
        end)
      end)

    insert_used = cpu_since(insert_cpu)

    IO.puts(
      "  " <>
        label("POST /insert × #{div(rows, config.batch)}", 24) <>
        pad(ms(insert_us), 10) <>
        pad(round(inserted / (insert_us / 1_000_000)), 10) <> pad(insert_used.cores, 8)
    )
  end

  defp insert(req, pool, chunk) do
    response =
      Req.post!(req,
        url: "/v1/datasets/#{dataset()}/tables/#{table()}/insert",
        headers: [{"content-type", "application/json"}],
        body: body(pool, chunk.count, chunk.offset)
      )

    response.body["insertedRows"] || 0
  end
end

Bench.Load.main()
