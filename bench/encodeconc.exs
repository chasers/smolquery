Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.EncodeConc do
  @moduledoc """
  Whether overlapping a table's Parquet encodes buys anything, A/B.

  `:encode_concurrency` has been implemented and defaulted to 1 without ever
  being measured. The case for raising it is that one `TableBuffer` is a table's
  serialization point by design, so the only way a single table goes faster is
  for the work behind that point to overlap: at 1 the committer encodes a commit,
  fsyncs it, appends the manifest, and only then starts the next.

  The case against is that the encode is a dirty-IO NIF and the manifest append
  is a single held fd — neither parallelises by being asked to — so raising the
  knob may buy nothing while costing memory, since every in-flight commit holds
  its own batch.

  ## What this measures

  Concurrent writers against **one** table, which is the only shape where the
  question is live: a single sequential writer cannot fill a second encode slot,
  so it would answer 1-vs-N with noise no matter what the truth is. Writers post
  the flush-unit batch in a loop for `SECONDS`, and the arm's number is acked
  rows per second.

  `ack_budget_ms` is `:infinity` throughout. `Smolquery.BufferService.Load`'s
  Little's-law estimate is deliberately conservative and it refuses batches it
  predicts will exceed the budget; above concurrency 1 that prediction is wrong
  in the direction that refuses work the buffer would in fact have absorbed, so
  leaving the budget finite would measure the admission controller rather than
  the knob.

      mix run bench/encodeconc.exs

  `WRITERS` (default 4), `LEVELS` (default 1,2,4), `SECONDS` (default 20).
  """

  alias Smolquery.BufferService

  @dir Path.join(System.tmp_dir!(), "smolquery-encodeconc")

  def main do
    writers = env_int("WRITERS", 4)
    seconds = env_int("SECONDS", 20)
    levels = levels()
    pool = Bench.Otel.pool()

    Logger.configure(level: :warning)

    IO.puts("""

    #{IO.ANSI.bright()}encode_concurrency A/B — #{writers} writers, one table, #{seconds}s per arm#{IO.ANSI.reset()}

      #{cells(["concurrency", "acked rows", "rows/s", "vs 1", "RSS"])}\
    """)

    baseline =
      Enum.reduce(levels, nil, fn level, baseline ->
        run(level, writers, seconds, pool, baseline)
      end)

    IO.puts("""

      Threshold set before running: concurrency 2 must beat 1 by at least 30%,
      or the knob is not worth its memory. #{if baseline, do: "", else: ""}
    """)
  end

  defp run(level, writers, seconds, pool, baseline) do
    File.rm_rf!(@dir)

    Application.put_env(
      :smolquery,
      BufferService,
      Keyword.merge(Application.get_env(:smolquery, BufferService, []),
        encode_concurrency: level,
        ack_budget_ms: :infinity
      )
    )

    req = Bench.Otel.boot!(@dir)
    Bench.Otel.create_tables!(req, 1)

    try do
      {acked, elapsed_ms} = drive(req, pool, writers, seconds)
      rate = round(acked / (elapsed_ms / 1_000))

      IO.puts(
        "      " <>
          cells([
            Integer.to_string(level),
            Integer.to_string(acked),
            "#{rate} rows/s",
            versus(rate, baseline),
            "#{rss_mib()} MiB"
          ])
      )

      baseline || rate
    after
      Bench.Otel.teardown!(@dir)
    end
  end

  # Every writer owns a disjoint slice of the fixture's offsets, so no two post
  # identical batches and the tenant mix stays what the fixture intends.
  defp drive(req, pool, writers, seconds) do
    deadline = System.monotonic_time(:millisecond) + seconds * 1_000
    started = System.monotonic_time(:millisecond)

    acked =
      1..writers
      |> Task.async_stream(
        fn writer -> post_until(req, pool, deadline, writer * 1_000_000, 0) end,
        max_concurrency: writers,
        timeout: :infinity
      )
      |> Enum.reduce(0, fn {:ok, rows}, total -> total + rows end)

    {acked, System.monotonic_time(:millisecond) - started}
  end

  defp post_until(req, pool, deadline, offset, acked) do
    if System.monotonic_time(:millisecond) >= deadline do
      acked
    else
      rows = Bench.Otel.rows(pool, batch(), offset)
      body = JSON.encode!(%{"rows" => rows})

      case Req.post(req,
             url: "/v1/datasets/#{Bench.Otel.dataset()}/tables/#{Bench.Otel.table()}/insert",
             headers: [{"content-type", "application/json"}],
             body: body,
             decode_body: false
           ) do
        {:ok, %{status: 200}} ->
          post_until(req, pool, deadline, offset + batch(), acked + batch())

        # A 429 is the admission controller refusing, which `ack_budget_ms:
        # :infinity` should make impossible. It is counted as zero rows rather
        # than retried, so if it ever happens the arm's number falls and the
        # run does not quietly turn into a retry benchmark.
        {:ok, %{status: _other}} ->
          post_until(req, pool, deadline, offset + batch(), acked)

        {:error, _reason} ->
          post_until(req, pool, deadline, offset + batch(), acked)
      end
    end
  end

  # Below the flush trigger the interval timer sets the pace and the encode is
  # never the constraint, so the batch is the same flush unit the profile uses.
  defp batch, do: env_int("BATCH", 3_062)

  defp levels do
    "LEVELS"
    |> System.get_env("1,2,4")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_integer/1)
  end

  defp versus(_rate, nil), do: "—"

  defp versus(rate, baseline),
    do: "#{:erlang.float_to_binary((rate / baseline - 1) * 100, decimals: 1)}%"

  defp cells(values) do
    values
    |> Enum.zip([13, 12, 14, 10, 10])
    |> Enum.map_join("  ", fn {value, width} -> String.pad_leading(value, width) end)
  end

  defp rss_mib do
    {out, 0} = System.cmd("ps", ["-o", "rss=", "-p", System.pid()])

    out |> String.trim() |> String.to_integer() |> Kernel./(1024) |> round()
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Bench.EncodeConc.main()
