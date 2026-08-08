Code.require_file("support.exs", __DIR__)
Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.CompareSupport do
  @moduledoc """
  Shared vocabulary for the smolquery-vs-ClickHouse comparison bench.

  The workload knobs, the ClickHouse DDL generator (derived from
  `Bench.Otel.columns/0`, never hand-written), the frozen ten-query set,
  the OS-process resource sampler, and the `Backend` behaviour live here. Backend
  adapters and the driver that drives them live elsewhere — this module holds the
  things both arms must agree on so a difference in the report is a difference
  in the systems, not in the harness.

  ## Why the sampler is here

  `Bench.Support.cpu_since/1` measures the BEAM process the driver itself runs
  in. On a cross-system comparison that would count the driver's CPU on one arm
  and not the other. The sampler below reads a single OS process by pid from
  outside, identically on both arms, which is the only figure that can sit next
  to ClickHouse's.
  """

  import Bench.Support, only: [env: 2, sweep_env: 2, mib: 1]

  @type query :: %{
          id: String.t(),
          label: String.t(),
          hypothesis: String.t(),
          needs_project: boolean(),
          sql: %{smolquery: String.t(), clickhouse: String.t()}
        }

  @type sample :: %{
          rss_peak_mib: float(),
          rss_p95_mib: float(),
          rss_mean_mib: float(),
          rss_end_mib: float(),
          cores_mean: float(),
          cores_peak: float(),
          cpu_seconds: float(),
          wall_seconds: float(),
          samples: non_neg_integer()
        }

  @low_cardinality_strings ~w(
    severity_text
    service_name
    service_namespace
    deployment_environment
    cloud_provider
    cloud_region
    cloud_availability_zone
    k8s_cluster_name
    k8s_namespace_name
    k8s_deployment_name
    k8s_container_name
    host_arch
    os_type
    telemetry_sdk_name
    telemetry_sdk_language
    http_request_method
    http_route
    url_scheme
    network_protocol_version
  )

  @doc """
  Scale knobs for a comparison run.

  `projects` is the axis the whole comparison exists to exercise — tenant
  cardinality is what the clustering key is supposed to make cheap, and the
  interesting runs push it to 100_000. Headline runs use `ROWS=100_000_000`;
  the default here is smaller so a dry run finishes overnight rather than over a
  weekend.
  """
  @spec workload() :: %{
          rows: pos_integer(),
          projects: pos_integer(),
          batch: pos_integer(),
          writers: [pos_integer()],
          reps: pos_integer(),
          query_reps: pos_integer()
        }
  def workload do
    %{
      rows: env("ROWS", 10_000_000),
      projects: env("PROJECTS", 1_000),
      batch: env("BATCH", 2_000),
      writers: sweep_env("WRITERS", [4, 8, 16, 32]),
      reps: env("REPS", 5),
      query_reps: env("QUERY_REPS", 3)
    }
  end

  @doc """
  The sort key both arms use.

  Keeping this list in one place is what makes the claim "same sort key on both
  sides" checkable rather than a promise: the smolquery arm PATCHes it as the
  table's clustering key, the ClickHouse arm renders it into `ORDER BY`, and a
  reader can grep for the single definition.
  """
  @spec clustering_key() :: [String.t()]
  def clustering_key, do: ["project_id", "timestamp"]

  @doc """
  Maps a smolquery `api_type` to its ClickHouse counterpart.

  An unknown type raises naming the offender: silently defaulting would let a
  schema change produce a ClickHouse table that quietly differs from the
  smolquery one, which is the exact failure this benchmark cannot survive.
  """
  @spec clickhouse_type(String.t()) :: String.t()
  def clickhouse_type("STRING"), do: "String"
  def clickhouse_type("INT64"), do: "Int64"
  def clickhouse_type("FLOAT64"), do: "Float64"
  def clickhouse_type("BOOL"), do: "Bool"
  def clickhouse_type("TIMESTAMP"), do: "DateTime64(6)"

  def clickhouse_type(other) do
    raise "unknown api_type for ClickHouse mapping: #{inspect(other)}"
  end

  @doc """
  Full `CREATE TABLE` DDL for the ClickHouse arm, generated from
  `Bench.Otel.columns/0`.

  Every column is `Nullable(T)` except the clustering-key columns, which are
  bare `T`: the fixture leaves exception/error columns null on non-error rows,
  and a non-nullable ClickHouse column would coerce those to empty string or
  zero and silently change what is being compared — but a MergeTree sort key
  column cannot be Nullable.

  `low_cardinality: true` wraps only the allowlisted low-cardinality STRING
  columns in `LowCardinality(...)`. That produces a table that is **not** the
  identical model: it exists to show ClickHouse as it is really deployed, and
  both variants get reported side by side rather than one replacing the other.
  Nesting when a column is both nullable and low-cardinality is
  `LowCardinality(Nullable(String))`. `project_id` is deliberately absent from
  the allowlist — at 100k tenants LowCardinality is the wrong structure and
  would flatter the ClickHouse arm dishonestly.

  `:codec :zstd` appends `CODEC(ZSTD(1))` to every column; `:lz4` appends
  nothing because LZ4 is ClickHouse's stock default. The smolquery arm writes
  Parquet with zstd, so `:zstd` is the like-for-like setting and `:lz4` is the
  out-of-the-box one — both are worth reporting.
  """
  @spec clickhouse_ddl(keyword()) :: String.t()
  def clickhouse_ddl(opts \\ []) do
    table = Keyword.get(opts, :table, "otel_logs")
    database = Keyword.get(opts, :database, "smolbench")
    low_cardinality = Keyword.get(opts, :low_cardinality, false)
    codec = Keyword.get(opts, :codec, :lz4)
    clustering = MapSet.new(clustering_key())

    columns =
      Enum.map_join(Bench.Otel.columns(), ",\n", fn {name, api_type} ->
        type = render_clickhouse_type(name, api_type, low_cardinality, clustering)
        "  `#{name}` #{type}#{codec_suffix(codec)}"
      end)

    order =
      clustering_key()
      |> Enum.map_join(", ", fn col -> "`#{col}`" end)

    """
    CREATE TABLE #{database}.#{table} (
    #{columns}
    )
    ENGINE = MergeTree
    ORDER BY (#{order})
    SETTINGS index_granularity = 8192
    """
  end

  @doc """
  `CREATE DATABASE IF NOT EXISTS` for the ClickHouse arm — kept next to the DDL
  so the driver does not re-derive the default database name.
  """
  @spec clickhouse_create_database(keyword()) :: String.t()
  def clickhouse_create_database(opts \\ []) do
    database = Keyword.get(opts, :database, "smolbench")
    "CREATE DATABASE IF NOT EXISTS `#{database}`"
  end

  @doc """
  `DROP TABLE IF EXISTS` for the ClickHouse arm — same reason as
  `clickhouse_create_database/1`.
  """
  @spec clickhouse_drop_table(keyword()) :: String.t()
  def clickhouse_drop_table(opts \\ []) do
    table = Keyword.get(opts, :table, "otel_logs")
    database = Keyword.get(opts, :database, "smolbench")
    "DROP TABLE IF EXISTS `#{database}`.`#{table}`"
  end

  @doc """
  The frozen ten-query set, Q1 through Q10.

  Dialect divergences (approx distinct, p95, minute bucket) follow the contract
  table exactly so neither arm is handed a friendlier function by accident.

  Time-window predicates go through `recent_window/1` so the two arms cannot
  drift on "now". Fixture timestamps are UTC wall-clock stored as naive
  `TIMESTAMP` / `DateTime64(6)`; DuckDB's `now()` is `TIMESTAMPTZ` in the
  session zone, and comparing it to that column silently matches nothing on
  off-UTC hosts. Both arms therefore take an explicit-UTC "now"
  (`timezone('UTC', now())` / `now64(6, 'UTC')`).
  """
  @spec queries() :: [query()]
  def queries do
    sq = "#{Bench.Otel.dataset()}.#{Bench.Otel.table()}"
    ch = "smolbench.otel_logs"
    window = recent_window("1 HOUR")

    [
      %{
        id: "Q1",
        label: "count(*) whole table",
        hypothesis: "metadata-only in ClickHouse; we must pay I/O — expect to lose outright",
        needs_project: false,
        sql: %{
          smolquery: "SELECT count(*) FROM #{sq}",
          clickhouse: "SELECT count(*) FROM #{ch}"
        }
      },
      %{
        id: "Q2",
        label: "count() for one project",
        hypothesis: "the tenant point lookup; sort key should make both cheap",
        needs_project: true,
        sql: %{
          smolquery: "SELECT count(*) FROM #{sq} WHERE project_id = '{{PROJECT}}'",
          clickhouse: "SELECT count(*) FROM #{ch} WHERE project_id = '{{PROJECT}}'"
        }
      },
      %{
        id: "Q3",
        label: "last 100 rows of one project by timestamp desc",
        hypothesis: "the real product query (log tail UI)",
        needs_project: true,
        sql: %{
          smolquery:
            "SELECT * FROM #{sq} WHERE project_id = '{{PROJECT}}' ORDER BY timestamp DESC LIMIT 100",
          clickhouse:
            "SELECT * FROM #{ch} WHERE project_id = '{{PROJECT}}' ORDER BY timestamp DESC LIMIT 100"
        }
      },
      %{
        id: "Q4",
        label: "count() grouped by project_id, all projects",
        hypothesis: "the tenant fan-out; the whole point of the exercise",
        needs_project: false,
        sql: %{
          smolquery: "SELECT project_id, count(*) FROM #{sq} GROUP BY project_id",
          clickhouse: "SELECT project_id, count(*) FROM #{ch} GROUP BY project_id"
        }
      },
      %{
        id: "Q5",
        label: "count grouped by service_name, one project, 1h window",
        hypothesis: "narrow group-by inside one tenant",
        needs_project: true,
        sql: %{
          smolquery:
            "SELECT service_name, count(*) FROM #{sq} WHERE project_id = '{{PROJECT}}' " <>
              "AND #{window.smolquery} GROUP BY service_name",
          clickhouse:
            "SELECT service_name, count(*) FROM #{ch} WHERE project_id = '{{PROJECT}}' " <>
              "AND #{window.clickhouse} GROUP BY service_name"
        }
      },
      %{
        id: "Q6",
        label: "p95 duration_ms grouped by http_route, one project",
        hypothesis: "heavier aggregate function",
        needs_project: true,
        sql: %{
          smolquery:
            "SELECT http_route, quantile_cont(duration_ms, 0.95) FROM #{sq} " <>
              "WHERE project_id = '{{PROJECT}}' GROUP BY http_route",
          clickhouse:
            "SELECT http_route, quantile(0.95)(duration_ms) FROM #{ch} " <>
              "WHERE project_id = '{{PROJECT}}' GROUP BY http_route"
        }
      },
      %{
        id: "Q7",
        label: "approx distinct trace_id, one project",
        hypothesis: "approx-distinct: uniq vs approx_count_distinct",
        needs_project: true,
        sql: %{
          smolquery:
            "SELECT approx_count_distinct(trace_id) FROM #{sq} WHERE project_id = '{{PROJECT}}'",
          clickhouse: "SELECT uniq(trace_id) FROM #{ch} WHERE project_id = '{{PROJECT}}'"
        }
      },
      %{
        id: "Q8",
        label: "per-minute error count, top 10 projects",
        hypothesis: "time bucket + top-N across tenants",
        needs_project: false,
        sql: %{
          smolquery:
            "WITH top_projects AS (" <>
              "SELECT project_id FROM #{sq} WHERE severity_number >= 17 " <>
              "GROUP BY project_id ORDER BY count(*) DESC LIMIT 10" <>
              ") " <>
              "SELECT t.project_id, date_trunc('minute', t.timestamp) AS minute, " <>
              "count(*) AS errors FROM #{sq} t " <>
              "INNER JOIN top_projects tp ON t.project_id = tp.project_id " <>
              "WHERE t.severity_number >= 17 " <>
              "GROUP BY t.project_id, minute ORDER BY t.project_id, minute",
          clickhouse:
            "WITH top_projects AS (" <>
              "SELECT project_id FROM #{ch} WHERE severity_number >= 17 " <>
              "GROUP BY project_id ORDER BY count(*) DESC LIMIT 10" <>
              ") " <>
              "SELECT t.project_id, toStartOfMinute(t.timestamp) AS minute, " <>
              "count(*) AS errors FROM #{ch} t " <>
              "INNER JOIN top_projects tp ON t.project_id = tp.project_id " <>
              "WHERE t.severity_number >= 17 " <>
              "GROUP BY t.project_id, minute ORDER BY t.project_id, minute"
        }
      },
      %{
        id: "Q9",
        label: "substring match in body, one project",
        hypothesis: "needle in haystack; forces a scan inside the tenant range",
        needs_project: true,
        sql: %{
          smolquery:
            "SELECT count(*) FROM #{sq} WHERE project_id = '{{PROJECT}}' " <>
              "AND body LIKE '%gateway declined%'",
          clickhouse:
            "SELECT count(*) FROM #{ch} WHERE project_id = '{{PROJECT}}' " <>
              "AND body LIKE '%gateway declined%'"
        }
      },
      %{
        id: "Q10",
        label: "SELECT * limit 100, one project",
        hypothesis: "cost of the wide 62-column projection",
        needs_project: true,
        sql: %{
          smolquery: "SELECT * FROM #{sq} WHERE project_id = '{{PROJECT}}' LIMIT 100",
          clickhouse: "SELECT * FROM #{ch} WHERE project_id = '{{PROJECT}}' LIMIT 100"
        }
      }
    ]
  end

  defp recent_window(interval) when is_binary(interval) do
    %{
      smolquery: "timestamp >= timezone('UTC', now()) - INTERVAL #{interval}",
      clickhouse: "timestamp >= now64(6, 'UTC') - INTERVAL #{interval}"
    }
  end

  @doc """
  Substitutes `{{PROJECT}}` in a query's SQL.

  Raises if any `{{` placeholder remains afterwards: an unbound placeholder
  reaching a database would either error or, worse, run against a literal string
  and return zero rows fast, which reads as a fantastic benchmark result.
  """
  @spec bind_project(String.t(), String.t()) :: String.t()
  def bind_project(sql, project) do
    bound = String.replace(sql, "{{PROJECT}}", project)

    if String.contains?(bound, "{{") do
      raise "unbound placeholder remains after bind_project/2: #{inspect(bound)}"
    end

    bound
  end

  @doc """
  RSS of OS process `pid` in KiB, or `nil` if the process is gone.

  Uses portable `ps` rather than `/proc` so one code path serves both darwin and
  Linux — the compare driver has to run on whichever machine holds the servers.
  """
  @spec rss_kib(pos_integer()) :: non_neg_integer() | nil
  def rss_kib(pid) when is_integer(pid) and pid > 0 do
    case System.cmd("ps", ["-o", "rss=", "-p", Integer.to_string(pid)], stderr_to_stdout: true) do
      {output, 0} ->
        case Integer.parse(String.trim(output)) do
          {kib, _rest} when kib >= 0 -> kib
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @doc """
  Cumulative CPU seconds of OS process `pid`, or `nil` if unreadable.

  Same portable `ps` path as `rss_kib/1`. `ps` reports cumulative CPU time of
  the process including all its threads, which is what makes a `cores` figure
  comparable between a multi-threaded ClickHouse and a BEAM plus DuckDB thread
  pool. Unparseable or vanished-process input returns `nil` rather than raising
  so a vanishing server skips a sample.
  """
  @spec cpu_seconds(pos_integer()) :: float() | nil
  def cpu_seconds(pid) when is_integer(pid) and pid > 0 do
    case System.cmd("ps", ["-o", "time=", "-p", Integer.to_string(pid)], stderr_to_stdout: true) do
      {output, 0} -> parse_ps_time(output)
      _ -> nil
    end
  end

  @doc """
  Starts a linked sampler that polls `os_pid` every `:interval_ms` (default 200).

  Collects `{monotonic_us, rss_kib, cpu_seconds}` triples until `sample_stop/1`
  sends `:stop`. Survives a vanishing target by skipping unreadable samples
  rather than crashing the benchmark.
  """
  @spec sample_start(pos_integer(), keyword()) :: pid()
  def sample_start(os_pid, opts \\ []) when is_integer(os_pid) and os_pid > 0 do
    interval_ms = Keyword.get(opts, :interval_ms, 200)

    spawn_link(fn -> sample_loop(os_pid, interval_ms, []) end)
  end

  @doc """
  Stops a sampler and returns the summarised resource sample.

  Waits at most 5_000 ms; on timeout returns a zeroed sample with `samples: 0`
  rather than hanging the benchmark.

  `cores_mean` is cpu_seconds / wall_seconds — the number to trust. `cores_peak`
  is the maximum instantaneous ratio over consecutive sample pairs; with a
  200 ms interval and `ps`'s coarse CPU-time resolution it is noisy, and a
  reader must not be misled by a spiky peak.
  """
  @spec sample_stop(pid()) :: sample()
  def sample_stop(sampler) when is_pid(sampler) do
    send(sampler, {:stop, self()})

    receive do
      {:sample_done, points} -> summarise_samples(points)
    after
      5_000 -> empty_sample()
    end
  end

  defp sample_loop(os_pid, interval_ms, acc) do
    acc = maybe_collect(os_pid, acc)

    receive do
      {:stop, from} ->
        send(from, {:sample_done, Enum.reverse(acc)})
    after
      interval_ms ->
        sample_loop(os_pid, interval_ms, acc)
    end
  end

  defp maybe_collect(os_pid, acc) do
    case {rss_kib(os_pid), cpu_seconds(os_pid)} do
      {rss, cpu} when is_integer(rss) and is_float(cpu) ->
        [{System.monotonic_time(:microsecond), rss, cpu} | acc]

      _ ->
        acc
    end
  end

  defp summarise_samples([]), do: empty_sample()
  defp summarise_samples([_single]), do: empty_sample()

  defp summarise_samples(points) do
    [{first_at, _first_rss, first_cpu} | _] = points
    {last_at, _last_rss, last_cpu} = List.last(points)
    rss_mibs = Enum.map(points, fn {_at, kib, _cpu} -> mib(kib * 1024) end)
    wall_seconds = (last_at - first_at) / 1_000_000
    cpu_delta = last_cpu - first_cpu

    %{
      rss_peak_mib: Enum.max(rss_mibs),
      rss_p95_mib: rss_percentile(rss_mibs, 0.95),
      rss_mean_mib: Float.round(Enum.sum(rss_mibs) / length(rss_mibs), 1),
      rss_end_mib: List.last(rss_mibs),
      cores_mean: cores_ratio(cpu_delta, wall_seconds),
      cores_peak: cores_peak(points),
      cpu_seconds: Float.round(cpu_delta, 3),
      wall_seconds: Float.round(wall_seconds, 3),
      samples: length(points)
    }
  end

  defp empty_sample do
    %{
      rss_peak_mib: 0.0,
      rss_p95_mib: 0.0,
      rss_mean_mib: 0.0,
      rss_end_mib: 0.0,
      cores_mean: 0.0,
      cores_peak: 0.0,
      cpu_seconds: 0.0,
      wall_seconds: 0.0,
      samples: 0
    }
  end

  defp cores_peak(points) do
    points
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [{at0, _rss0, cpu0}, {at1, _rss1, cpu1}] ->
      cores_ratio(cpu1 - cpu0, (at1 - at0) / 1_000_000)
    end)
    |> Enum.max()
  end

  defp cores_ratio(_cpu, wall) when wall <= 0.0, do: 0.0
  defp cores_ratio(cpu, wall), do: Float.round(cpu / wall, 2)

  defp rss_percentile(values, fraction) do
    sorted = Enum.sort(values)
    count = length(sorted)
    Enum.at(sorted, min(count - 1, trunc(fraction * count)))
  end

  defp parse_ps_time(raw) do
    text = String.trim(raw)

    {days, clock} =
      case String.split(text, "-", parts: 2) do
        [day_part, rest] ->
          case Integer.parse(day_part) do
            {days, ""} when days >= 0 -> {days, rest}
            _ -> {0, text}
          end

        [_only] ->
          {0, text}
      end

    case String.split(clock, ":") do
      [mm, ss] ->
        combine_ps_time(days, 0, mm, ss)

      [hh, mm, ss] ->
        combine_ps_time(days, hh, mm, ss)

      _ ->
        nil
    end
  end

  defp combine_ps_time(days, hours, minutes, seconds) do
    with {h, ""} <- parse_int_or_zero(hours),
         {m, ""} <- Integer.parse(minutes),
         {s, ""} <- Float.parse(seconds) do
      days * 86_400 + h * 3_600 + m * 60 + s
    else
      _ -> nil
    end
  end

  defp parse_int_or_zero(0), do: {0, ""}
  defp parse_int_or_zero(value) when is_binary(value), do: Integer.parse(value)

  defp render_clickhouse_type(name, api_type, low_cardinality, clustering) do
    base = clickhouse_type(api_type)

    inner =
      if MapSet.member?(clustering, name) do
        base
      else
        "Nullable(#{base})"
      end

    if low_cardinality and api_type == "STRING" and name in @low_cardinality_strings do
      "LowCardinality(#{inner})"
    else
      inner
    end
  end

  defp codec_suffix(:zstd), do: " CODEC(ZSTD(1))"
  defp codec_suffix(:lz4), do: ""

  defp codec_suffix(other) do
    raise "unknown ClickHouse codec: #{inspect(other)}"
  end
end

defmodule Bench.CompareSupport.Backend do
  @moduledoc """
  Behaviour both comparison arms implement.

  The benchmark's honesty depends on the driver being unable to tell the two
  arms apart: everything arm-specific — how a table is created, how rows are
  inserted, what "settled" means, how caches are dropped — lives behind these
  callbacks, so the measured code path around each call is byte-identical.
  """

  @type state :: term()

  @type query_result :: %{
          rows: non_neg_integer(),
          ms: float(),
          rows_read: non_neg_integer() | nil,
          bytes_read: non_neg_integer() | nil
        }

  @doc "Human-readable arm name for the report."
  @callback name() :: String.t()

  @doc "Opens a connection / takes over a node; returns opaque state."
  @callback setup(keyword()) :: {:ok, state()} | {:error, term()}

  @doc """
  Creates the table from `columns` with `clustering` as the sort key.

  Columns are `{name, api_type}` pairs from `Bench.Otel.columns/0`.
  """
  @callback create_table(state(), [{String.t(), String.t()}], [String.t()]) ::
              :ok | {:error, term()}

  @doc """
  Inserts one batch of rows.

  `:mode` is one of `:default | :durable | :async`. `:default` is ClickHouse as
  people actually run it (`fsync_after_insert=0`); `:durable` turns on
  `fsync_after_insert=1, fsync_part_directory=1` so the promise matches
  smolquery's fsynced 200; `:async` is `async_insert=1, wait_for_async_insert=1`,
  the architectural analog of group commit. The smolquery arm has one mode and
  is compared against `:durable` as the like-for-like number, with `:default`
  and `:async` shown beside it.
  """
  @callback insert(state(), iodata(), keyword()) ::
              {:ok, %{rows: non_neg_integer()}} | {:error, term()}

  @doc """
  Blocks until all background work is finished — seal and compaction on
  smolquery, `OPTIMIZE TABLE … FINAL` on ClickHouse.

  `disk_bytes/1` measured before settling is meaningless: the bytes that matter
  are the ones left after every merge has completed.
  """
  @callback settle(state()) :: :ok | {:error, term()}

  @doc "Runs one SQL statement and returns row count, latency, and scan stats."
  @callback query(state(), String.t()) :: {:ok, query_result()} | {:error, term()}

  @doc "On-disk bytes occupied by the table, measured only after `settle/1`."
  @callback disk_bytes(state()) :: {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  OS pid of the server process being measured, or `nil`.

  When `nil`, the driver must degrade to reporting no resource figures rather
  than failing the run.
  """
  @callback os_pid(state()) :: pos_integer() | nil

  @doc """
  Drops both the system's own caches and the OS page cache.

  Must not fail the run when the OS page cache cannot be dropped without root —
  cold numbers simply become less cold, and that is recorded rather than
  aborting.
  """
  @callback drop_caches(state()) :: :ok | {:error, term()}

  @doc "Releases resources; always succeeds from the driver's point of view."
  @callback teardown(state()) :: :ok
end
