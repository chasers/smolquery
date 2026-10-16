defmodule Smolquery.Burst.Spike do
  @moduledoc """
  The measurements behind T-673: can a Lambda host a FLAME runner, and what
  does it read.

  Run from the parent node with `bin/smolquery rpc`:

      Smolquery.Burst.Spike.boot(4)
      Smolquery.Burst.Spike.scan(["s3://bucket/bench/onebrc_v6/<id>.parquet"])
      Smolquery.Burst.Spike.lifetime()

  Every function returns plain terms, so the numbers paste into the plan.
  """

  alias Smolquery.Burst
  alias Smolquery.Engine
  alias Smolquery.Segments.Store.S3

  @scan_extensions [:httpfs, :aws]

  @doc """
  Leases `count` runners at once and reports, per runner, the wall time of
  its first call. A cold pool pays the boot inside that call; a warm one
  answers in milliseconds.
  """
  @spec boot(pos_integer()) :: [map()]
  def boot(count) do
    1..count
    |> Task.async_stream(
      fn index ->
        {micro, result} =
          :timer.tc(fn -> FLAME.call(Burst.pool(), &probe/0, timeout: 300_000) end)

        Map.merge(result, %{index: index, wall_ms: div(micro, 1000)})
      end,
      max_concurrency: count,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  @doc """
  Scans `urls` on one runner with a private DuckDB engine and reports rows,
  wall time, and the bytes the runner's process read (`/proc/self/io`), so
  MB/s per runner falls out.
  """
  @spec scan([String.t()], keyword()) :: map()
  def scan(urls, opts \\ []) do
    bucket = Keyword.get(opts, :bucket, System.get_env("SMOLQUERY_S3_BUCKET"))
    region = Keyword.get(opts, :region, System.get_env("SMOLQUERY_S3_REGION", "eu-central-1"))
    threads = Keyword.get(opts, :threads, 6)
    memory_limit = Keyword.get(opts, :memory_limit, "7GB")

    FLAME.call(
      Burst.pool(),
      fn -> scan_on_runner(urls, bucket, region, threads, memory_limit) end,
      timeout: 600_000
    )
  end

  @doc """
  Holds one runner for longer than Lambda's 15-minute cap and reports how
  the parent sees the end: the exit reason and when it arrived.
  """
  @spec lifetime(pos_integer()) :: map()
  def lifetime(minutes \\ 16) do
    started = System.monotonic_time(:millisecond)

    outcome =
      try do
        FLAME.call(
          Burst.pool(),
          fn ->
            Process.sleep(minutes * 60_000)
            :survived
          end,
          timeout: (minutes + 1) * 60_000
        )
      catch
        :exit, reason -> {:exit, reason}
      end

    %{outcome: outcome, after_ms: System.monotonic_time(:millisecond) - started}
  end

  defp probe do
    %{
      node: node(),
      schedulers: System.schedulers_online(),
      memory_mb: div(:erlang.memory(:total), 1_048_576),
      hidden: node() not in Node.list(:visible)
    }
  end

  defp scan_on_runner(urls, bucket, region, threads, memory_limit) do
    %{config: store} = S3.new(bucket: bucket, region: region)
    name = Module.concat(__MODULE__, "Engine#{System.unique_integer([:positive])}")
    rchar_before = rchar()

    {:ok, pid} =
      Engine.start_link(
        name: name,
        extensions: @scan_extensions ++ S3.required_extensions(store),
        statements: [S3.create_secret_statement(store)],
        memory_limit: memory_limit,
        threads: threads,
        temp_directory: Path.join(System.tmp_dir!(), "smolquery-spike")
      )

    try do
      list = Enum.map_join(urls, ", ", &"'#{&1}'")

      {micro, result} =
        :timer.tc(fn ->
          Engine.query(name, "SELECT count(*) AS n FROM read_parquet([#{list}])", [], 600_000)
        end)

      rows =
        case result do
          {:ok, %Engine.Result{rows: [[n]]}} -> n
          other -> other
        end

      bytes = rchar() - rchar_before

      %{
        node: node(),
        files: length(urls),
        rows: rows,
        wall_ms: div(micro, 1000),
        bytes_read: bytes,
        mb_per_s: if(micro > 0, do: Float.round(bytes / 1_048_576 / (micro / 1_000_000), 1))
      }
    after
      Process.unlink(pid)
      Process.exit(pid, :shutdown)
    end
  end

  defp rchar do
    case File.read("/proc/self/io") do
      {:ok, io} ->
        io
        |> String.split("\n")
        |> Enum.find_value(0, fn
          "rchar: " <> n -> String.to_integer(String.trim(n))
          _line -> nil
        end)

      _unreadable ->
        0
    end
  end
end
