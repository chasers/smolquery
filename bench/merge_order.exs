defmodule Bench.MergeOrder do
  @moduledoc """
  Does a compaction merge need to re-sort inputs that are already sorted (T-605)?

  Builds `FILES` sealed files shaped like `metrics.samples` (name, series,
  labels map, ts, value), each sorted by the clustering key the way a seal
  writes it. Seals overlap in time the way several buffer nodes' seals do:
  each file spans `OVERLAP` seal intervals, so every instant is covered by
  `OVERLAP` files. It then merges them into one file three ways:

    * **sorted**: `COPY (SELECT ... ORDER BY <key>)`, what `Merge.compact/5`
      does today;
    * **concatenated**: the same `COPY` with no `ORDER BY`, inputs read in
      file order with insertion order preserved;
    * **k-way**: a merge of the sorted inputs built from DuckDB, since DuckDB
      has no operator that merges pre-sorted inputs. The leading key column is
      split at sampled quantiles into windows of about `WINDOW_ROWS` rows; each
      window is sorted in memory from only the inputs whose key range overlaps
      it, written as a part, and the parts are concatenated in key order. A
      streaming k-way merge reads each input once; this one reads an input
      once per window it overlaps, and reports that count.

  For each output it reports the merge's time and peak spill under
  `MEMORY_LIMIT`, and, for a `name = 'x' AND ts BETWEEN ...` query, the row
  groups whose statistics admit the predicate and the query's time. It runs
  once clustered by `(name, ts)` and once by `(ts, name)`, and deletes each
  case's files before starting the next.

      mix run bench/merge_order.exs
      FILES=500 OVERLAP=4 WINDOW_ROWS=500000 MEMORY_LIMIT=512MB mix run bench/merge_order.exs
  """

  @files String.to_integer(System.get_env("FILES", "300"))
  @rows String.to_integer(System.get_env("ROWS", "9200"))
  @names String.to_integer(System.get_env("NAMES", "400"))
  @overlap String.to_integer(System.get_env("OVERLAP", "4"))
  @window_rows String.to_integer(System.get_env("WINDOW_ROWS", "500000"))
  @memory System.get_env("MEMORY_LIMIT", "256MB")
  @threads String.to_integer(System.get_env("THREADS", "4"))
  @row_group 100_000
  @seal_ms 70_000

  @cases [
    {"(name, ts)", "name, ts", "name", "VARCHAR"},
    {"(ts, name)", "ts, name", "ts", "TIMESTAMP"}
  ]

  def run do
    dir = Path.join(System.tmp_dir!(), "merge_order_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    IO.puts(
      "#{@files} files x #{@rows} rows, #{@overlap} files over every instant, #{@names} names, " <>
        "memory_limit #{@memory}, #{@threads} threads, k-way windows of #{@window_rows} rows\n"
    )

    for {label, key, lead, type} <- @cases do
      case_dir = Path.join(dir, String.replace(key, ", ", "_"))
      File.mkdir_p!(case_dir)
      inputs = seal(case_dir, key)
      input_bytes = Enum.sum_by(inputs, &File.stat!(&1).size)

      IO.puts("## clustered by #{label}: inputs #{mib(input_bytes)}")

      IO.puts(
        "| merge | time | peak spill | output | row groups | admitting the query | query time |"
      )

      IO.puts("|---|---:|---:|---:|---:|---:|---:|")

      notes =
        for variant <- [:sorted, :concatenated, :kway] do
          out = Path.join(case_dir, "#{variant}.parquet")
          {ms, spill, note} = merge(variant, case_dir, inputs, out, {key, lead, type})
          if variant != :concatenated, do: ordered!(out, key)
          {groups, admitting} = pruning(out)
          query_ms = query(out)

          IO.puts(
            "| #{variant_label(variant)} | #{ms} ms | #{mib(spill)} | #{mib(File.stat!(out).size)} | " <>
              "#{groups} | #{admitting} | #{query_ms} ms |"
          )

          note
        end

      notes |> Enum.reject(&is_nil/1) |> Enum.each(&IO.puts("\n" <> &1))
      IO.puts("")
      File.rm_rf!(case_dir)
    end

    File.rm_rf!(dir)
  end

  defp variant_label(:kway), do: "k-way"
  defp variant_label(variant), do: Atom.to_string(variant)

  defp with_engine(settings, fun) do
    {:ok, db} = Adbc.Database.start_link(driver: :duckdb, version: "1.5.3")
    {:ok, conn} = Adbc.Connection.start_link(database: db)
    for setting <- settings, do: {:ok, _} = Adbc.Connection.query(conn, setting)

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
      GenServer.stop(db)
    end
  end

  defp q(conn, sql) do
    case Adbc.Connection.query(conn, sql) do
      {:ok, result} -> Adbc.Result.to_map(result)
      {:error, error} -> raise Exception.message(error)
    end
  end

  defp seal(dir, key) do
    with_engine(["SET threads = #{@threads}"], fn conn ->
      for f <- 1..@files do
        path = Path.join(dir, "seal_#{pad(f)}.parquet")
        start = "TIMESTAMP '2026-09-01' + INTERVAL #{f * @seal_ms} MILLISECOND"

        q(conn, """
        COPY (
          SELECT 'metric_' || lpad(CAST(i % #{@names} AS VARCHAR), 4, '0') AS name,
                 CAST(i % (#{@names} * 20) AS BIGINT) AS series,
                 MAP(['job', 'instance', 'region'],
                     ['node-exporter', 'host-' || CAST(i % 50 AS VARCHAR), 'eu-west-' || CAST(i % 3 AS VARCHAR)]) AS labels,
                 #{start} + INTERVAL (CAST(i * #{@seal_ms * @overlap} / #{@rows} AS BIGINT)) MILLISECOND AS ts,
                 CAST(i AS DOUBLE) AS value
          FROM range(#{@rows}) r(i)
          ORDER BY #{key}
        ) TO '#{path}' (FORMAT PARQUET, ROW_GROUP_SIZE #{@row_group})
        """)

        path
      end
    end)
  end

  defp merge(:sorted, dir, inputs, out, {key, _lead, _type}) do
    measure(dir, false, fn conn -> copy(conn, inputs, " ORDER BY #{key}", out) && nil end)
  end

  defp merge(:concatenated, dir, inputs, out, _key) do
    measure(dir, true, fn conn -> copy(conn, inputs, "", out) && nil end)
  end

  defp merge(:kway, dir, inputs, out, {key, lead, type}) do
    measure(dir, true, &kway(&1, dir, inputs, out, key, lead, type))
  end

  defp measure(dir, preserve_order, fun) do
    spill = Path.join(dir, "spill_#{System.unique_integer([:positive])}")

    settings = [
      "SET memory_limit = '#{@memory}'",
      "SET threads = #{@threads}",
      "SET temp_directory = '#{spill}'",
      "SET preserve_insertion_order = #{preserve_order}"
    ]

    parent = self()
    poller = spawn(fn -> poll(spill, parent, 0) end)
    {us, note} = :timer.tc(fn -> with_engine(settings, fun) end)
    send(poller, :stop)
    peak = receive do: ({:peak, peak} -> peak)
    File.rm_rf!(spill)
    {div(us, 1000), peak, note}
  end

  defp copy(conn, inputs, clause, out) do
    q(
      conn,
      "COPY (SELECT * FROM read_parquet([#{list(inputs)}])#{clause}) TO '#{out}' " <>
        "(FORMAT PARQUET, ROW_GROUP_SIZE #{@row_group})"
    )
  end

  defp kway(conn, dir, inputs, out, key, lead, type) do
    windows = max(1, div(length(inputs) * @rows + @window_rows - 1, @window_rows))
    bounds = boundaries(conn, inputs, lead, windows)

    q(conn, """
    CREATE TEMP TABLE ranges AS
    SELECT file_name,
           min(CAST(stats_min_value AS #{type})) AS lo,
           max(CAST(stats_max_value AS #{type})) AS hi
    FROM parquet_metadata([#{list(inputs)}])
    WHERE path_in_schema = '#{lead}'
    GROUP BY file_name
    """)

    {parts, reads} =
      [nil | bounds]
      |> Enum.zip(bounds ++ [nil])
      |> Enum.with_index()
      |> Enum.flat_map_reduce(0, fn {{lo, hi}, i}, reads ->
        case overlapping(conn, lo, hi) do
          [] ->
            {[], reads}

          files ->
            part = Path.join(dir, "part_#{pad(i)}.parquet")
            copy(conn, files, " WHERE #{within(lead, lead, lo, hi)} ORDER BY #{key}", part)
            {[part], reads + length(files)}
        end
      end)

    {stitch_us, _} = :timer.tc(fn -> copy(conn, parts, "", out) end)
    Enum.each(parts, &File.rm!/1)

    "k-way: #{length(parts)} windows, #{reads} file reads for #{length(inputs)} inputs " <>
      "(#{Float.round(reads / length(inputs), 1)}x), final concatenation #{div(stitch_us, 1000)} ms"
  end

  defp boundaries(_conn, _inputs, _lead, 1), do: []

  defp boundaries(conn, inputs, lead, windows) do
    fractions = Enum.map_join(1..(windows - 1), ", ", &"#{&1 / windows}")

    %{"b" => bounds} =
      q(conn, """
      WITH sample AS (
        SELECT #{lead} AS v FROM read_parquet([#{list(inputs)}]) USING SAMPLE 100000 ROWS
      ),
      q AS (SELECT quantile_disc(v, [#{fractions}]) AS qs FROM sample)
      SELECT DISTINCT CAST(b AS VARCHAR) AS b, b AS ord
      FROM (SELECT unnest(qs) AS b FROM q)
      ORDER BY ord
      """)

    bounds
  end

  defp overlapping(conn, lo, hi) do
    %{"file_name" => files} =
      q(
        conn,
        "SELECT file_name FROM ranges WHERE #{within("lo", "hi", lo, hi)} ORDER BY file_name"
      )

    files
  end

  defp within(low_col, high_col, lo, hi) do
    [lo && "#{high_col} > '#{lo}'", hi && "#{low_col} <= '#{hi}'"]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> "true"
      conditions -> Enum.join(conditions, " AND ")
    end
  end

  defp poll(dir, parent, peak) do
    size =
      case File.ls(dir) do
        {:ok, files} ->
          Enum.sum_by(files, fn f ->
            case File.stat(Path.join(dir, f)) do
              {:ok, stat} -> stat.size
              _gone -> 0
            end
          end)

        _none ->
          0
      end

    receive do
      :stop -> send(parent, {:peak, max(peak, size)})
    after
      10 -> poll(dir, parent, max(peak, size))
    end
  end

  @target "metric_0123"
  @middle_ms div(@files, 2) * @seal_ms
  @from "TIMESTAMP '2026-09-01' + INTERVAL #{@middle_ms} MILLISECOND"
  @to "TIMESTAMP '2026-09-01' + INTERVAL #{@middle_ms + 600_000} MILLISECOND"

  defp pruning(out) do
    %{"groups" => [groups], "admitting" => [admitting]} =
      with_engine([], fn conn ->
        q(conn, """
        WITH stats AS (
          SELECT row_group_id,
                 max(CASE WHEN path_in_schema = 'name' THEN stats_min_value END) AS name_min,
                 max(CASE WHEN path_in_schema = 'name' THEN stats_max_value END) AS name_max,
                 max(CASE WHEN path_in_schema = 'ts' THEN stats_min_value END) AS ts_min,
                 max(CASE WHEN path_in_schema = 'ts' THEN stats_max_value END) AS ts_max
          FROM parquet_metadata('#{out}') GROUP BY row_group_id
        )
        SELECT count(*) AS groups,
               count(*) FILTER (WHERE name_min <= '#{@target}' AND name_max >= '#{@target}'
                                AND CAST(ts_min AS TIMESTAMP) <= #{@to}
                                AND CAST(ts_max AS TIMESTAMP) >= #{@from}) AS admitting
        FROM stats
        """)
      end)

    {groups, admitting}
  end

  defp ordered!(out, key) do
    [first, second] = String.split(key, ", ")

    %{"rows" => [rows], "inversions" => [inversions]} =
      with_engine([], fn conn ->
        q(conn, """
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE prev_first IS NOT NULL AND (#{first}, #{second}) < (prev_first, prev_second)) AS inversions
        FROM (
          SELECT #{first}, #{second},
                 lag(#{first}) OVER (ORDER BY file_row_number) AS prev_first,
                 lag(#{second}) OVER (ORDER BY file_row_number) AS prev_second
          FROM read_parquet('#{out}', file_row_number = true)
        )
        """)
      end)

    if rows != @files * @rows or inversions > 0,
      do: raise("#{out}: #{rows} rows of #{@files * @rows}, #{inversions} out of order")
  end

  defp query(out) do
    sql =
      "SELECT count(*), sum(value) FROM read_parquet('#{out}') " <>
        "WHERE name = '#{@target}' AND ts BETWEEN #{@from} AND #{@to}"

    times =
      for _ <- 1..5 do
        with_engine(
          ["SET enable_external_file_cache = false", "SET parquet_metadata_cache = false"],
          fn conn ->
            {us, _} = :timer.tc(fn -> q(conn, sql) end)
            div(us, 1000)
          end
        )
      end

    times |> Enum.sort() |> Enum.at(2)
  end

  defp list(paths), do: Enum.map_join(paths, ", ", &"'#{&1}'")
  defp pad(n), do: String.pad_leading("#{n}", 5, "0")
  defp mib(bytes), do: "#{Float.round(bytes / 1_048_576, 1)} MiB"
end

Bench.MergeOrder.run()
