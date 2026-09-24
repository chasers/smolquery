Code.require_file("victoriametrics_support.exs", __DIR__)

defmodule Bench.VictoriaMetricsCardinality do
  @moduledoc """
  Label-value filters at cardinality (PL-70, T-583): `SERIES` series of
  `m`, each with `SAMPLES` samples spread over the last `HOURS` hours, an
  `instance` per series, a `job` of ten values and a `project` of
  `PROJECTS` values, written as zstd Parquet by DuckDB `COPY`, ten million rows a file, and
  registered as the table's sealed segments, which is what a sealed table
  looks like to the query service. Remote write at a billion series
  would take hours the scan does not need; `bench/victoriametrics.exs`
  measures the write path.

  Then, over the whole range, `count(m{...})` for each matcher shape through
  the edge's `/api/v1/query_range`: the one-series `instance`, a `project`
  equality (`SERIES / PROJECTS` series), a regular expression matching three
  projects, one matching a ninth of them, a `job` equality (a tenth of the
  series) and a `project` inequality (nearly all). A selector expected to
  match more than `MAX_SERIES` series is skipped and says so, since the edge
  would refuse it (its ceiling here is one million). With the aggregate
  pushed down (T-568) a `count(...)` is not refused, so raise `MAX_SERIES`
  to run them. `in range` is every
  sample of `m` in the range, which is what the scan reads since nothing
  prunes on a label. Then the SQL controls and the label routes
  (`Bench.VictoriaMetricsSupport`).

      SERIES=1000000 PROJECTS=10000 mix run bench/victoriametrics_cardinality.exs 2>/dev/null
      TMPDIR=/home/dev/bench-tmp SERIES=1000000000 PROJECTS=10000000 LAKE_MEMORY=16GB \\
        mix run bench/victoriametrics_cardinality.exs 2>/dev/null

  `TMPDIR` is where the lake is written. `SEGMENTS_DIR` keeps the generated
  files (about 90 MB per ten million rows) and the range they cover, so a
  rerun with the same `SERIES`, `PROJECTS`, `SAMPLES` and `HOURS` reuses
  them instead of generating again. `LAKE_MEMORY` (12 GB) bounds the engine
  generating them.
  """

  import Bench.Support, except: [table: 0, schema: 0]
  import Bench.VictoriaMetricsSupport

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Segments.Segment

  @batch 10_000_000

  def main do
    Logger.configure(level: :warning)
    Supervisor.terminate_child(Smolquery.Supervisor, Smolquery.StorageService.Supervisor)
    series = env("SERIES", 1_000_000)

    config = %{
      series: series,
      projects: env("PROJECTS", max(div(series, 100), 1)),
      samples: env("SAMPLES", 1),
      hours: env("HOURS", 1),
      reps: env("REPS", 3),
      max_series: env("MAX_SERIES", 1_000_000),
      memory: System.get_env("LAKE_MEMORY", "12GB")
    }

    with_tmp_dir("victoriametrics-cardinality", fn dir ->
      segments_dir = System.get_env("SEGMENTS_DIR") || Path.join(dir, "segments")
      File.mkdir_p!(segments_dir)
      lake_opts = [memory_limit: config.memory, threads: System.schedulers_online()]
      base = start_node!(dir, lake_opts, [segments_dir], max_series: config.max_series)
      req = Req.new(base_url: base, auth: {:bearer, password()}, retry: false)
      {from_s, now_s} = range_of(segments_dir, config)

      schedulers()
      generate(config, segments_dir, from_s, now_s)
      label_filters(req, config, from_s, now_s)
      controls(config, from_s, now_s)
      label_routes(req, config, from_s, now_s)
    end)
  end

  defp range_of(segments_dir, config) do
    path = Path.join(segments_dir, "range.txt")
    shape = "#{config.series} #{config.projects} #{config.samples} #{config.hours}"

    case File.read(path) do
      {:ok, text} ->
        case String.split(text, "\n", trim: true) do
          [^shape, range] ->
            range |> String.split() |> Enum.map(&String.to_integer/1) |> List.to_tuple()

          [other | _rest] ->
            raise ArgumentError,
                  "SEGMENTS_DIR holds a dataset of SERIES PROJECTS SAMPLES HOURS = #{other}; " <>
                    "this run asks for #{shape}"
        end

      {:error, :enoent} ->
        now_s = div(System.system_time(:second), step_s()) * step_s()
        from_s = now_s - config.hours * 3_600
        File.write!(path, "#{shape}\n#{from_s} #{now_s}\n")
        {from_s, now_s}
    end
  end

  defp generate(config, segments_dir, from_s, now_s) do
    rows = config.series * config.samples
    span_ms = (now_s - from_s) * 1_000

    heading(
      "Generate — #{config.series} series x #{config.samples} samples, " <>
        "#{config.projects} projects, over #{config.hours} h"
    )

    started = System.monotonic_time(:microsecond)

    segments =
      for lo <- 0..(config.series - 1)//@batch do
        hi = min(lo + @batch, config.series)
        path = Path.join(segments_dir, "samples-#{div(lo, @batch)}.parquet")

        unless File.exists?(path) do
          sql = copy_sql(config, lo, hi, from_s, span_ms, path <> ".part")
          {:ok, _result} = Engine.query(lake(), sql, [], 3_600_000)
          File.rename!(path <> ".part", path)
        end

        IO.write(".")
        segment(path, (hi - lo) * config.samples)
      end

    {:ok, _snapshot} =
      Catalog.register_segments(DuckLake.new(engine: lake()), {"metrics", "samples"}, segments)

    elapsed = System.monotonic_time(:microsecond) - started
    IO.puts("")
    file_count = length(segments)

    IO.puts(
      "  #{rows} rows in #{Float.round(elapsed / 1_000_000, 1)} s " <>
        "(#{round(rows * 1_000_000 / elapsed)} rows/s), #{file_count} files, " <>
        "#{mib(Enum.sum_by(segments, & &1.byte_size))} MiB on disk"
    )
  end

  defp segment(path, rows) do
    %Segment{
      id: Path.basename(path, ".parquet"),
      key: Path.basename(path),
      path: path,
      row_count: rows,
      byte_size: File.stat!(path).size
    }
  end

  defp copy_sql(config, lo, hi, from_s, span_ms, path) do
    rows = config.series * config.samples

    "COPY (SELECT 'm' AS name, " <>
      "((hash(i)::HUGEINT) - 9223372036854775808::HUGEINT)::BIGINT AS series, " <>
      "MAP {'instance': 'host-' || i, 'job': 'job-' || (i % 10), " <>
      "'project': 'project-' || (i % #{config.projects})} AS labels, " <>
      "make_timestamp((#{from_s * 1_000} + ((i * #{config.samples} + s) * #{span_ms}) // #{rows}) * 1000) AS ts, " <>
      "((i % 7) + s)::DOUBLE AS value " <>
      "FROM range(#{lo}, #{hi}) t(i), range(#{config.samples}) u(s)) " <>
      "TO '#{path}' (FORMAT parquet, COMPRESSION zstd)"
  end

  defp label_filters(req, config, from_s, now_s) do
    per_project = div(config.series, config.projects)
    in_range = config.series * config.samples

    heading(
      "Label filters — #{config.series} series, #{config.projects} projects, " <>
        "#{config.hours} h; #{in_range} samples of m in range; median of #{config.reps}"
    )

    read_header(34)

    for {selector, expected} <- [
          {~s|m{instance="host-1"}|, 1},
          {~s|m{project="project-1"}|, per_project},
          {~s|m{project=~"project-(1\|22\|333)"}|, 3 * per_project},
          {~s|m{project=~"project-1.*"}|, prefixed(config.projects, "1") * per_project},
          {~s|m{job="job-1"}|, div(config.series, 10)},
          {~s|m{project!="project-1"}|, config.series - per_project},
          {~s|rate(m{job="job-1"}[1m])|, div(config.series, 10)},
          {~s|increase(m{project="project-1"}[5m])|, per_project},
          {~s|changes(m{job="job-1"}[1m])|, div(config.series, 10)},
          {~s|quantile_over_time(0.9, m{job="job-1"}[1m])|, div(config.series, 10)},
          {~s|stddev_over_time(m{job="job-1"}[1m])|, div(config.series, 10)}
        ] do
      if expected > config.max_series do
        IO.puts(label(selector, 34) <> "  skipped: #{expected} series expected")
      else
        {path, params} = range("count(#{selector})", from_s, now_s)
        read_row(req, selector, path, params, config.reps, 34)
      end
    end
  end

  defp prefixed(projects, prefix),
    do: Enum.count(0..(projects - 1), &String.starts_with?(Integer.to_string(&1), prefix))

  defp controls(config, from_s, now_s) do
    {:ok, host} =
      Engine.query(
        lake(),
        "SELECT series FROM lake.metrics.samples WHERE labels['instance'] = 'host-1' LIMIT 1",
        [],
        600_000
      )

    {:ok, group} =
      Engine.query(
        lake(),
        "SELECT DISTINCT series FROM lake.metrics.samples WHERE labels['project'] = 'project-1'",
        [],
        600_000
      )

    [[host_series]] = host.rows

    sql_controls(
      from_s,
      now_s,
      {{"instance", "host-1"}, host_series},
      {{"project", "project-1"}, List.flatten(group.rows)},
      {"job", "job-1"},
      config.reps
    )
  end

  defp label_routes(req, config, from_s, now_s) do
    IO.puts("")
    read_header(34)

    for path <- ["/api/v1/labels", "/api/v1/label/job/values", "/api/v1/label/project/values"] do
      read_row(
        req,
        "#{path} #{config.hours}h",
        path,
        %{"start" => from_s, "end" => now_s},
        config.reps,
        34
      )
    end
  end
end

Bench.VictoriaMetricsCardinality.main()
