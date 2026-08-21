Code.require_file("support.exs", __DIR__)

defmodule Bench.DistributedQuery do
  @moduledoc """
  Can K DuckDB instances run one query faster than one instance? (PL-48, T-348)

  DuckDB parallelizes a query across threads inside one instance, and no
  further. Both storage tiers hand the planner a flat list of parquet
  segments, so the scan is shardable: give each of K instances 1/K of the
  file list, run a *partial* query per shard, and merge the partials with a
  *final* query in a coordinator instance.

  This is a single-machine simulation of K machines. The baseline gets all
  T threads; each sharded worker is capped at T/K threads, so total thread
  budget is constant. If `sharded(K, T/K) ≈ baseline(T)`, the scatter/gather
  overhead is small and K real machines would scale the scan near-linearly.
  The partial-result bytes per shard are the price a network would pay.

  Four query shapes, each with a partial/final decomposition, each checked
  for equality against the single-instance answer:

    * global aggregates — count/sum/min/max/avg
    * group-by, low cardinality (3 groups)
    * group-by, high cardinality (~500k groups; big partials)
    * top-k by aggregate (full partial group-by, final order + limit)

      mix run bench/distributed_query.exs
      FILES=64 ROWS=2000000 THREADS=8 SHARDS=2,4,8 mix run bench/distributed_query.exs
  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Smolquery.Engine

  @user_cardinality 500_000

  def run do
    schedulers()

    files = env("FILES", 32)
    rows = env("ROWS", 1_000_000)
    threads = env("THREADS", 8)
    shards = sweep_env("SHARDS", [2, 4, 8])
    reps = env("REPS", 3)

    with_tmp_dir("distributed-query", fn dir ->
      paths = generate!(dir, files, rows, threads)
      baseline = start_engine!(:baseline, threads)

      for shape <- shapes() do
        heading("#{shape.name}")

        IO.puts(
          "  #{label("config", 18)}#{pad("wall ms", 9)}#{pad("x base", 8)}" <>
            "#{pad("partials MiB", 14)}#{pad("merge ms", 10)}   check"
        )

        base = timed(fn -> Engine.frame!(baseline, shape.base.(scan(paths))) end, reps)
        IO.puts("  #{label("1 x #{threads} threads", 18)}#{pad(ms(base.min), 9)}#{pad("1.0", 8)}")

        for k <- shards do
          worker_threads = max(div(threads, k), 1)
          workers = start_workers!(k, worker_threads)

          run = fn -> scatter_gather(workers, baseline, paths, shape, dir, k) end
          sharded = timed(run, reps)

          for {_name, pid} <- workers, do: Supervisor.stop(pid)

          speedup = Float.round(base.min / sharded.min, 2)
          check = verify(base.value, sharded.value.frame, shape.keys)

          IO.puts(
            "  #{label("#{k} x #{worker_threads} threads", 18)}#{pad(ms(sharded.min), 9)}" <>
              "#{pad(speedup, 8)}#{pad(mib(sharded.value.bytes), 14)}" <>
              "#{pad(ms(sharded.value.merge_us), 10)}   #{check}"
          )
        end
      end
    end)
  end

  defp generate!(dir, files, rows, threads) do
    heading("fixture: #{files} parquet files x #{count(rows)} rows")
    data_dir = Path.join(dir, "data")
    File.mkdir_p!(data_dir)
    {:ok, gen} = Engine.start_link(name: __MODULE__.Gen, extensions: [], threads: threads)

    {us, paths} =
      :timer.tc(fn ->
        for i <- 0..(files - 1) do
          path = Path.join(data_dir, "segment-#{i}.parquet")

          Engine.query!(
            __MODULE__.Gen,
            "COPY (#{generator(i, rows)}) TO '#{path}' (FORMAT parquet)"
          )

          path
        end
      end)

    Supervisor.stop(gen)
    bytes = paths |> Enum.map(&File.stat!(&1).size) |> Enum.sum()
    IO.puts("  wrote #{count(files * rows)} rows, #{mib(bytes)} MiB, in #{ms(us)} ms")
    paths
  end

  defp generator(i, rows) do
    offset = i * rows

    """
    SELECT
      #{offset} + n AS id,
      TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (#{i}) DAY + INTERVAL (n % 86400) SECOND AS ts,
      CAST(hash(#{offset} + n) % #{@user_cardinality} AS INTEGER) AS user_id,
      CASE WHEN n % 10 < 7 THEN 'ok' WHEN n % 10 < 9 THEN 'warn' ELSE 'error' END AS status,
      CAST((#{offset} + n) * 1009 % 99991 AS DOUBLE) / 7 AS value
    FROM range(#{rows}) t(n)
    """
  end

  defp shapes do
    [
      %{
        name: "global aggregates (count / sum / min / max / avg)",
        keys: [],
        base: fn from ->
          "SELECT count(*) AS c, sum(value) AS s, min(ts) AS tmin, max(ts) AS tmax, " <>
            "avg(value) AS a FROM #{from}"
        end,
        partial: fn from ->
          "SELECT count(*) AS c, sum(value) AS s, min(ts) AS tmin, max(ts) AS tmax FROM #{from}"
        end,
        final: fn from ->
          "SELECT sum(c)::BIGINT AS c, sum(s) AS s, min(tmin) AS tmin, max(tmax) AS tmax, " <>
            "sum(s) / sum(c) AS a FROM #{from}"
        end
      },
      %{
        name: "group-by, low cardinality (3 groups)",
        keys: ["status"],
        base: fn from ->
          "SELECT status, count(*) AS c, sum(value) AS s FROM #{from} GROUP BY status"
        end,
        partial: fn from ->
          "SELECT status, count(*) AS c, sum(value) AS s FROM #{from} GROUP BY status"
        end,
        final: fn from ->
          "SELECT status, sum(c)::BIGINT AS c, sum(s) AS s FROM #{from} GROUP BY status"
        end
      },
      %{
        name: "group-by, high cardinality (~#{count(@user_cardinality)} groups)",
        keys: ["user_id"],
        base: fn from ->
          "SELECT user_id, count(*) AS c, sum(value) AS s FROM #{from} GROUP BY user_id"
        end,
        partial: fn from ->
          "SELECT user_id, count(*) AS c, sum(value) AS s FROM #{from} GROUP BY user_id"
        end,
        final: fn from ->
          "SELECT user_id, sum(c)::BIGINT AS c, sum(s) AS s FROM #{from} GROUP BY user_id"
        end
      },
      %{
        name: "top-k by aggregate (LIMIT 10)",
        keys: ["user_id"],
        base: fn from ->
          "SELECT user_id, sum(value) AS v FROM #{from} GROUP BY user_id ORDER BY v DESC, user_id LIMIT 10"
        end,
        partial: fn from ->
          "SELECT user_id, sum(value) AS v FROM #{from} GROUP BY user_id"
        end,
        final: fn from ->
          "SELECT user_id, sum(v) AS v FROM #{from} GROUP BY user_id ORDER BY v DESC, user_id LIMIT 10"
        end
      }
    ]
  end

  defp scatter_gather(workers, coordinator, paths, shape, dir, k) do
    partials_dir = Path.join(dir, "partials-#{k}")
    File.mkdir_p!(partials_dir)

    shard_files =
      paths
      |> Enum.with_index()
      |> Enum.group_by(fn {_path, index} -> rem(index, k) end, fn {path, _index} -> path end)

    partial_paths =
      workers
      |> Enum.with_index()
      |> Task.async_stream(
        fn {{name, _pid}, index} ->
          path = Path.join(partials_dir, "partial-#{index}.parquet")
          sql = shape.partial.(scan(shard_files[index]))
          Engine.query!(name, "COPY (#{sql}) TO '#{path}' (FORMAT parquet)")
          path
        end,
        max_concurrency: k,
        timeout: :infinity,
        ordered: true
      )
      |> Enum.map(fn {:ok, path} -> path end)

    bytes = partial_paths |> Enum.map(&File.stat!(&1).size) |> Enum.sum()

    {merge_us, frame} =
      :timer.tc(fn -> Engine.frame!(coordinator, shape.final.(scan(partial_paths))) end)

    %{frame: frame, merge_us: merge_us, bytes: bytes}
  end

  defp scan(paths) do
    "read_parquet([" <> Enum.map_join(paths, ", ", &"'#{&1}'") <> "])"
  end

  defp start_engine!(suffix, threads) do
    name = Module.concat(__MODULE__, "Engine#{suffix}")
    {:ok, _pid} = Engine.start_link(name: name, extensions: [], threads: threads)
    name
  end

  defp start_workers!(k, threads) do
    for index <- 1..k do
      name = Module.concat(__MODULE__, "Worker#{k}_#{index}")
      {:ok, pid} = Engine.start_link(name: name, extensions: [], threads: threads)
      {name, pid}
    end
  end

  defp verify(base_frame, sharded_frame, keys) do
    cond do
      DataFrame.n_rows(base_frame) != DataFrame.n_rows(sharded_frame) ->
        "MISMATCH: #{DataFrame.n_rows(base_frame)} vs #{DataFrame.n_rows(sharded_frame)} rows"

      DataFrame.n_rows(base_frame) > 10_000 ->
        checksum_verify(base_frame, sharded_frame)

      true ->
        row_verify(base_frame, sharded_frame, keys)
    end
  end

  defp checksum_verify(base_frame, sharded_frame) do
    base = DataFrame.to_columns(base_frame)
    sharded = DataFrame.to_columns(sharded_frame)

    mismatch =
      Enum.find(Map.keys(base), fn column ->
        not equivalent?(Enum.sum(base[column]), Enum.sum(sharded[column]))
      end)

    case mismatch do
      nil -> "ok (checksum over #{DataFrame.n_rows(base_frame)} rows)"
      column -> "MISMATCH: sum(#{column}) differs"
    end
  end

  defp row_verify(base_frame, sharded_frame, keys) do
    base = sorted_rows(base_frame, keys)
    sharded = sorted_rows(sharded_frame, keys)

    mismatch =
      Enum.zip(base, sharded)
      |> Enum.find(fn {base_row, sharded_row} ->
        Enum.any?(base_row, fn {column, value} ->
          not equivalent?(value, sharded_row[column])
        end)
      end)

    case mismatch do
      nil -> "ok (#{length(base)} rows)"
      {base_row, sharded_row} -> "MISMATCH: #{inspect(base_row)} vs #{inspect(sharded_row)}"
    end
  end

  defp sorted_rows(frame, keys) do
    frame
    |> DataFrame.to_rows()
    |> Enum.sort_by(fn row -> Enum.map(keys, &row[&1]) end)
  end

  defp equivalent?(a, b) when is_float(a) and is_float(b) do
    abs(a - b) <= 1.0e-6 * max(1.0, max(abs(a), abs(b)))
  end

  defp equivalent?(a, b), do: a == b

  defp count(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp count(n), do: "#{div(n, 1000)}k"
end

Bench.DistributedQuery.run()
