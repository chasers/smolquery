Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.BatchSweep do
  @moduledoc """
  What one POST may carry, and whether carrying more of it is worth anything.

  `bench/pathprof.exs` profiles a batch sized to the flush trigger, which is the
  granularity a client sending small batches ends up flushing at. It is not a
  cap on the request. Two different bounds are in play and they are easy to
  confuse:

    * `flush_max_bytes` decides when the accumulator is *handed off*. It is
      checked after a batch is appended, so it never splits one — a batch that
      arrives already larger than the trigger is flushed whole.
    * `max_buffered_bytes` decides what is *admitted*. A batch that would push
      the accumulator past it is refused outright with a 429, not queued.

  So the real per-request ceiling is `max_buffered_bytes`, and this sweep finds
  it empirically rather than dividing two config numbers and hoping the row size
  is what the estimate says. It also answers the question the ceiling is only
  interesting for: does a bigger request buy throughput?

  Sizes come from `SIZES` (comma-separated row counts); each is posted `REPS`
  times and the median is reported, with the segment the flush produced.

      mix run bench/batchsweep.exs
  """

  alias Smolquery.BufferService
  alias Smolquery.IngestService

  @dir Path.join(System.tmp_dir!(), "smolquery-batchsweep")

  def main do
    reps = env_int("REPS", 5)
    sizes = sizes()
    schema = %{Bench.Otel.table_schema() | clustering: ["project_id", "timestamp"]}
    pool = Bench.Otel.pool()

    Logger.configure(level: :warning)

    raise_ceilings()

    limits = limits()
    req = Bench.Otel.boot!(@dir)
    Bench.Otel.create_tables!(req, 1)

    banner(limits, schema, pool)

    try do
      for size <- sizes, do: run(req, schema, pool, size, reps)
    after
      Bench.Otel.teardown!(@dir)
    end
  end

  defp run(req, schema, pool, size, reps) do
    rows = Bench.Otel.rows(pool, size, 0)
    body = JSON.encode!(%{"rows" => rows})
    {measured, []} = IngestService.Validator.validate(schema, rows)
    bytes = measured.byte_size

    case post(req, body) do
      {:ok, _ms} ->
        median =
          1..reps
          |> Enum.map(fn _rep -> elem(post(req, body), 1) end)
          |> Enum.sort()
          |> Enum.at(div(reps, 2))

        line(size, body, bytes, "#{fmt(median)} ms", "#{round(size / (median / 1_000))} rows/s")

      {:error, reason} ->
        line(size, body, bytes, reason, "refused")
    end
  end

  defp cells(values) do
    values
    |> Enum.zip([8, 12, 12, 18, 14, 10])
    |> Enum.map_join("  ", fn {value, width} -> String.pad_leading(value, width) end)
  end

  defp post(req, body) do
    {us, response} =
      :timer.tc(fn ->
        Req.post!(req,
          url: "/v1/datasets/#{Bench.Otel.dataset()}/tables/#{Bench.Otel.table()}/insert",
          headers: [{"content-type", "application/json"}],
          body: body,
          decode_body: false
        )
      end)

    case response.status do
      200 -> {:ok, us / 1_000}
      status -> {:error, "HTTP #{status}"}
    end
  rescue
    # Past some size Bandit drops the connection rather than answering, which is
    # a different refusal from a 413 and worth telling apart in the table.
    error in [Req.TransportError] -> {:error, "closed (#{error.reason})"}
  end

  # Raising the body limit alone only moves the refusal one layer down, into the
  # buffer's `max_buffered_bytes`, which measures the term rather than the wire
  # and so trips first. To find out what a bigger request actually buys, both
  # have to move together — `BODY_MIB` sets the wire cap and `BUFFER_MIB` the
  # term cap, and leaving them unset measures the shipped defaults.
  defp raise_ceilings do
    if mib = env_int("BODY_MIB", nil) do
      merge_env(SmolqueryApi, max_body_bytes: mib * 1_048_576)
    end

    if mib = env_int("BUFFER_MIB", nil) do
      merge_env(BufferService, max_buffered_bytes: mib * 1_048_576, max_buffered_rows: 5_000_000)
    end
  end

  defp merge_env(key, opts) do
    Application.put_env(
      :smolquery,
      key,
      Keyword.merge(Application.get_env(:smolquery, key, []), opts)
    )
  end

  # Peak RSS of this OS process across the sweep is what a bigger request costs
  # in the only currency that decides whether it is safe: a node's memory.
  defp rss_mib do
    {out, 0} = System.cmd("ps", ["-o", "rss=", "-p", System.pid()])

    out |> String.trim() |> String.to_integer() |> Kernel./(1024) |> round()
  end

  defp limits do
    config = Application.get_env(:smolquery, BufferService, [])

    Map.new(
      [:flush_max_rows, :flush_max_bytes, :max_buffered_rows, :max_buffered_bytes],
      fn key ->
        {key, Keyword.fetch!(config, key)}
      end
    )
  end

  defp banner(limits, schema, pool) do
    {measured, []} = IngestService.Validator.validate(schema, Bench.Otel.rows(pool, 1_000, 0))
    per_row = measured.byte_size / 1_000

    IO.puts("""

    #{IO.ANSI.bright()}one POST: how big may it be, and does bigger pay?#{IO.ANSI.reset()}

      per row          #{round(per_row)} bytes, as the buffer's bounds measure it
      hands off at     #{fmt_bytes(limits.flush_max_bytes)} or #{limits.flush_max_rows} rows \
    (~#{round(limits.flush_max_bytes / per_row)} rows here)
      refuses past     #{fmt_bytes(limits.max_buffered_bytes)} or #{limits.max_buffered_rows} rows \
    (~#{round(limits.max_buffered_bytes / per_row)} rows here)

      #{head()}
    """)
  end

  defp head do
    cells(["rows", "wire bytes", "term bytes", "latency", "throughput", "RSS"])
  end

  defp line(size, body, bytes, latency, throughput) do
    IO.puts(
      "      " <>
        cells([
          Integer.to_string(size),
          fmt_bytes(byte_size(body)),
          fmt_bytes(bytes),
          latency,
          throughput,
          "#{rss_mib()} MiB"
        ])
    )
  end

  defp sizes do
    "SIZES"
    |> System.get_env("1000,3062,6000,12000,24000,26000,30000")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_integer/1)
  end

  defp fmt(ms), do: :erlang.float_to_binary(ms, decimals: 1)

  defp fmt_bytes(bytes) when bytes >= 1_048_576,
    do: "#{:erlang.float_to_binary(bytes / 1_048_576, decimals: 1)} MiB"

  defp fmt_bytes(bytes), do: "#{:erlang.float_to_binary(bytes / 1024, decimals: 1)} KiB"

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Bench.BatchSweep.main()
