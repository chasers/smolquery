Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.Writers do
  @moduledoc """
  How both wire shapes scale with concurrent writers on one table.

  Every other number in `bench/results/pathprof.md` is one writer posting and
  waiting, which makes its rows/s a latency measurement wearing a throughput
  costume: with nothing overlapping, the rate is just batch ÷ latency. It says
  what one sequential client gets, not what the node holds.

  This sweeps writers against both bodies. The pair at each writer count runs
  back to back on the same booted node, so whatever drifts over a run — seal
  work, page cache, accumulated segments — drifts across both arms of a pair,
  and the ratio inside a pair stays honest even if the absolute numbers slide
  between pairs.

      mix run bench/writers.exs

  `WRITERS` (default 1,3,5,7,10), `SECONDS` per arm (default 10), `ROWS` per
  batch (default 3062).
  """

  @dir Path.join(System.tmp_dir!(), "smolquery-writers")

  def main do
    seconds = env_int("SECONDS", 10)
    rows = env_int("ROWS", 3_062)
    counts = list("WRITERS", "1,3,5,7,10")
    pool = Bench.Otel.pool()

    Logger.configure(level: :warning)

    req = Bench.Otel.boot!(@dir)
    Bench.Otel.create_tables!(req, 1)

    bodies = bodies(pool, rows)

    IO.puts("""

    #{IO.ANSI.bright()}writers × wire shape, one table, #{rows}-row batches, #{seconds}s per arm#{IO.ANSI.reset()}

      #{cells(["writers", "row-major", "columnar", "columnar gain"])}\
    """)

    try do
      for count <- counts, do: pair(req, bodies, count, seconds, rows)
    after
      Bench.Otel.teardown!(@dir)
    end
  end

  # Both bodies carry the same rows, so a difference between the arms is the
  # shape and nothing else.
  defp bodies(pool, rows) do
    batch = Bench.Otel.rows(pool, rows, 0)
    names = Enum.map(Bench.Otel.columns(), &elem(&1, 0))

    %{
      row: JSON.encode!(%{"rows" => batch}),
      columns:
        JSON.encode!(%{
          "rowCount" => rows,
          "columns" => Map.new(names, fn name -> {name, Enum.map(batch, &Map.get(&1, name))} end)
        })
    }
  end

  defp pair(req, bodies, writers, seconds, rows) do
    row_rate = arm(req, bodies.row, writers, seconds, rows)
    column_rate = arm(req, bodies.columns, writers, seconds, rows)

    IO.puts(
      "      " <>
        cells([
          Integer.to_string(writers),
          "#{row_rate} rows/s",
          "#{column_rate} rows/s",
          "#{:erlang.float_to_binary((column_rate / row_rate - 1) * 100, decimals: 1)}%"
        ])
    )
  end

  defp arm(req, body, writers, seconds, rows) do
    deadline = System.monotonic_time(:millisecond) + seconds * 1_000
    started = System.monotonic_time(:millisecond)

    acked =
      1..writers
      |> Task.async_stream(fn _writer -> post_until(req, body, deadline, rows, 0) end,
        max_concurrency: writers,
        timeout: :infinity
      )
      |> Enum.reduce(0, fn {:ok, count}, total -> total + count end)

    round(acked / ((System.monotonic_time(:millisecond) - started) / 1_000))
  end

  defp post_until(req, body, deadline, rows, acked) do
    if System.monotonic_time(:millisecond) >= deadline do
      acked
    else
      # Anything but a 200 counts zero rather than retrying, so a run that starts
      # being refused shows up as a falling rate instead of quietly becoming a
      # benchmark of the retry loop.
      case Req.post(req,
             url: "/v1/datasets/#{Bench.Otel.dataset()}/tables/#{Bench.Otel.table()}/insert",
             headers: [{"content-type", "application/json"}],
             body: body,
             decode_body: false
           ) do
        {:ok, %{status: 200}} -> post_until(req, body, deadline, rows, acked + rows)
        _refused_or_failed -> post_until(req, body, deadline, rows, acked)
      end
    end
  end

  defp cells(values) do
    values
    |> Enum.zip([9, 16, 16, 15])
    |> Enum.map_join("  ", fn {value, width} -> String.pad_leading(value, width) end)
  end

  defp list(name, default),
    do:
      name
      |> System.get_env(default)
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_integer/1)

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Bench.Writers.main()
