Code.require_file("support.exs", __DIR__)

defmodule Bench.RowBinary do
  @moduledoc """
  The PL-64 measurement: what decoding ClickHouse RowBinary on the ingest node
  costs, against the NDJSON path it feeds.

  DuckDB has no RowBinary reader, so `Smolquery.RowBinary` transcodes a body
  into the NDJSON the flush reads (PL-64 D1). That is per-row CPU the NDJSON
  route does not pay: an NDJSON body is forwarded after a line count (T-180).
  This script prices it, on two table shapes — a narrow log line and a wide
  row with a map, a decimal and a date.

    * **Wire size** — the RowBinary body against the NDJSON it becomes. The
      buffer, the replicas and the in-flight valve see the NDJSON.
    * **Transcode** — `RowBinary.decode/3` over one body, plain and with a
      typed header, and the binary the forward-batch carries. Two references
      run on the same rows: the NDJSON route's line count, and a
      `JSON.decode!/1` of every line (the parse this node did before T-180).
    * **Per request** — one body's transcode at the sizes an insert carries.
    * **Concurrent** — N bodies at once, one process each, the way concurrent
      inserts would run.

  End-to-end ack latency waits for the route (T-470); T-471 adds it here.

      mix run bench/rowbinary.exs
      ROWS=500000 REPS=7 WRITERS=1,4,8,16 mix run bench/rowbinary.exs
  """

  import Bench.Support
  import Bitwise

  alias Smolquery.RowBinary
  alias Smolquery.Schema

  @request_rows [1_000, 10_000, 50_000]
  @max_ndjson_bytes 8_000_000

  def main do
    rows = env("ROWS", 200_000)
    reps = env("REPS", 5)
    writers = sweep_env("WRITERS", [1, 2, 4, 8])

    schedulers()

    for shape <- shapes() do
      bodies = bodies(shape, rows)

      wire_size(shape, bodies, rows)
      transcode(shape, bodies, rows, reps)
      per_request(shape, reps)
      concurrent(shape, bodies, rows, writers, reps)
    end

    IO.puts("")
  end

  # ── shapes ────────────────────────────────────────────────────────────

  defp shapes do
    [
      %{name: "logs (6 cols)", columns: log_columns()},
      %{name: "wide (24 cols)", columns: log_columns() ++ wide_columns()}
    ]
  end

  defp log_columns do
    [
      {"id", "Int64", {:int64, nullable: false}, & &1},
      {"ts", "DateTime64(6)", {:timestamp, nullable: false},
       &(1_757_800_000_000_000 + &1 * 1_000)},
      {"level", "LowCardinality(String)", {:string, nullable: false},
       &Enum.at(~w(INFO WARN ERROR DEBUG), rem(&1, 4))},
      {"msg", "String", {:string, nullable: false},
       &"request handled path=/v1/items/#{&1} status=200 took #{rem(&1, 900)}ms"},
      {"host", "Nullable(String)", {:string, []},
       &if(rem(&1, 7) == 0, do: nil, else: "host-#{rem(&1, 40)}.internal")},
      {"dur", "Float64", {:float64, nullable: false}, &(rem(&1, 900) / 3.0)}
    ]
  end

  defp wide_columns do
    strings =
      for n <- 1..8,
          do: {"s#{n}", "String", {:string, nullable: false}, &"value-#{n}-#{rem(&1, 1000)}"}

    floats = for n <- 1..6, do: {"f#{n}", "Float64", {:float64, nullable: false}, &(&1 * n / 7)}

    strings ++
      floats ++
      [
        {"amount", "Decimal(18, 2)", {{:numeric, 18, 2}, nullable: false}, &(&1 * 37)},
        {"day", "Date32", {:date, nullable: false}, &(20_000 + rem(&1, 365))},
        {"ok", "Bool", {:bool, nullable: false}, &(rem(&1, 2) == 0)},
        {"attrs", "Map(String, String)", {{:map, :string, :string}, []},
         &%{"service" => "api", "region" => "r#{rem(&1, 5)}", "pod" => "pod-#{rem(&1, 50)}"}}
      ]
  end

  defp bodies(%{columns: columns}, rows) do
    schema =
      Schema.new!(
        Enum.map(columns, fn {name, _type, {type, opts}, _value} -> {name, type, opts} end)
      )

    encoders =
      Enum.map(columns, fn {_name, type, _field, value} ->
        {:ok, wire} = RowBinary.parse_type(type)
        {wire, value}
      end)

    plain =
      IO.iodata_to_binary(
        for i <- 1..rows, do: Enum.map(encoders, fn {wire, value} -> encode(wire, value.(i)) end)
      )

    header =
      IO.iodata_to_binary([
        leb(length(columns)),
        Enum.map(columns, fn {name, _type, _field, _value} -> string(name) end),
        Enum.map(columns, fn {_name, type, _field, _value} -> string(type) end)
      ])

    {:ok, %{ndjson: ndjson}} = RowBinary.decode(schema, plain, :row_binary)

    %{schema: schema, plain: plain, typed: header <> plain, ndjson: IO.iodata_to_binary(ndjson)}
  end

  defp encode({:nullable, _inner}, nil), do: <<1>>
  defp encode({:nullable, inner}, value), do: [0, encode(inner, value)]
  defp encode({:int, 64, :signed}, value), do: <<value::little-signed-64>>
  defp encode({:float, 64}, value), do: <<value::little-float-64>>
  defp encode(:string, value), do: string(value)
  defp encode(:bool, value), do: if(value, do: <<1>>, else: <<0>>)
  defp encode({:datetime64, 6}, value), do: <<value::little-signed-64>>
  defp encode(:date32, value), do: <<value::little-signed-32>>
  defp encode({:decimal, 18, _scale}, value), do: <<value::little-signed-64>>

  defp encode({:map, key, entry}, map),
    do: [leb(map_size(map)), Enum.map(map, fn {k, v} -> [encode(key, k), encode(entry, v)] end)]

  defp string(bytes), do: [leb(byte_size(bytes)), bytes]

  defp leb(n) when n < 128, do: <<n>>
  defp leb(n), do: <<1::1, n &&& 127::7, leb(n >>> 7)::binary>>

  # ── sections ──────────────────────────────────────────────────────────

  defp wire_size(shape, bodies, rows) do
    heading("#{shape.name}: wire size, #{rows} rows")

    rb = byte_size(bodies.plain)
    nd = byte_size(bodies.ndjson)

    IO.puts("  #{label("RowBinary", 24)} #{pad(mib(rb), 8)} MiB  #{pad(div(rb, rows), 5)} B/row")

    IO.puts(
      "  #{label("NDJSON it becomes", 24)} #{pad(mib(nd), 8)} MiB  #{pad(div(nd, rows), 5)} B/row"
    )

    IO.puts("  #{label("expansion", 24)} #{pad(Float.round(nd / rb, 2), 8)}x")

    IO.puts(
      "  #{label("rows per 8 MB body", 24)} #{pad(div(@max_ndjson_bytes * rows, nd), 8)} " <>
        "(SMOLQUERY_INSERT_MAX_NDJSON_BYTES, measured as NDJSON)"
    )
  end

  defp transcode(shape, bodies, rows, reps) do
    heading("#{shape.name}: transcode one #{rows}-row body (#{reps} reps, median)")

    IO.puts(
      "  #{label("", 44)} #{pad("min ms", 9)} #{pad("med ms", 9)} #{pad("rows/s", 10)} #{pad("µs/row", 7)} #{pad("MiB/s in", 9)}"
    )

    lines = fn -> String.split(bodies.ndjson, "\n", trim: true) end

    [
      {"decode, RowBinary", fn -> RowBinary.decode(bodies.schema, bodies.plain, :row_binary) end,
       bodies.plain},
      {"decode, RowBinaryWithNamesAndTypes",
       fn -> RowBinary.decode(bodies.schema, bodies.typed, :with_names_and_types) end,
       bodies.typed},
      {"decode typed + iodata_to_binary (the batch)",
       fn ->
         {:ok, decoded} = RowBinary.decode(bodies.schema, bodies.typed, :with_names_and_types)
         IO.iodata_to_binary(decoded.ndjson)
       end, bodies.typed},
      {"ref: NDJSON route's line count", fn -> Enum.count(lines.(), &(String.trim(&1) != "")) end,
       bodies.ndjson},
      {"ref: JSON.decode! every NDJSON line", fn -> Enum.map(lines.(), &JSON.decode!/1) end,
       bodies.ndjson}
    ]
    |> Enum.each(fn {name, fun, input} ->
      %{min: min, median: median} = timed(fun, reps)

      IO.puts(
        "  #{label(name, 44)} #{pad(ms(min), 9)} #{pad(ms(median), 9)} " <>
          "#{pad(round(rows / (median / 1_000_000)), 10)} #{pad(Float.round(median / rows, 2), 7)} " <>
          "#{pad(Float.round(byte_size(input) / 1_048_576 / (median / 1_000_000), 1), 9)}"
      )
    end)
  end

  defp per_request(shape, reps) do
    heading("#{shape.name}: one request's transcode, typed header + binary (median)")

    IO.puts(
      "  #{label("rows", 10)} #{pad("RowBinary", 12)} #{pad("NDJSON", 12)} #{pad("ms", 9)} #{pad("µs/row", 7)}"
    )

    for rows <- @request_rows do
      bodies = bodies(shape, rows)

      %{median: median} =
        timed(
          fn ->
            {:ok, decoded} = RowBinary.decode(bodies.schema, bodies.typed, :with_names_and_types)
            IO.iodata_to_binary(decoded.ndjson)
          end,
          reps
        )

      IO.puts(
        "  #{label(rows, 10)} #{pad("#{mib(byte_size(bodies.typed))} MiB", 12)} " <>
          "#{pad("#{mib(byte_size(bodies.ndjson))} MiB", 12)} #{pad(ms(median), 9)} " <>
          "#{pad(Float.round(median / rows, 2), 7)}"
      )
    end
  end

  defp concurrent(shape, bodies, rows, writers, reps) do
    heading("#{shape.name}: concurrent bodies, #{rows} rows each (median)")

    IO.puts(
      "  #{label("writers", 10)} #{pad("wall ms", 10)} #{pad("rows/s", 11)} #{pad("scaling", 8)}"
    )

    Enum.reduce(writers, nil, fn count, single ->
      %{median: median} =
        timed(
          fn ->
            1..count
            |> Task.async_stream(
              fn _writer ->
                {:ok, decoded} =
                  RowBinary.decode(bodies.schema, bodies.typed, :with_names_and_types)

                IO.iodata_to_binary(decoded.ndjson)
                :ok
              end,
              max_concurrency: count,
              timeout: :infinity
            )
            |> Stream.run()
          end,
          reps
        )

      throughput = count * rows / (median / 1_000_000)
      single = single || throughput

      IO.puts(
        "  #{label(count, 10)} #{pad(ms(median), 10)} #{pad(round(throughput), 11)} " <>
          "#{pad(Float.round(throughput / single, 2), 7)}x"
      )

      single
    end)
  end
end

Bench.RowBinary.main()
