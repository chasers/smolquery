Code.require_file("support.exs", __DIR__)
Code.require_file("ingest_transport_peer.exs", __DIR__)

defmodule Bench.IngestTransport do
  @moduledoc """
  The PL-8 D4 measurement: for a forward-batch crossing ingest → buffer node,
  is gen_rpc term transfer or Arrow IPC over HTTP the better transport?

  A real peer BEAM runs the buffer role; both transports land on it and fully
  materialize the payload, so neither gets to stop at "bytes arrived".

    * **Wire size** — `term_to_binary` of the batch vs the Arrow IPC binary
      (which the ingest side must first encode into a DataFrame).
    * **Round trip, serial** — one writer, per batch: gen_rpc `Sink.sink/1`
      vs HTTP POST of IPC bytes decoded to a frame (`/sink/frame`) and to
      rows (`/sink/rows`, the drop-in for today's row-based `TableBuffer`).
    * **Round trip, concurrent** — N writers at once. gen_rpc on the single
      `:bulk` channel (today's shape, T-29's head-of-line concern), gen_rpc
      on one channel per writer (T-29's fix, previewed), and HTTP on a
      connection pool.
    * **End to end** — the real write: `Client.write_batch/3` over gen_rpc vs
      IPC into `Endpoint.write_batch/3`, group commit and fsync included, to
      show how much of a write the transport even is.

      mix run bench/ingest_transport.exs
      ROWS=5000 WRITERS=8 REPS=15 mix run bench/ingest_transport.exs
  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Routing
  alias Smolquery.Schema

  @table {"analytics", "events"}
  @peer_gen_rpc_port 5_370
  @peer_http_port 43_119

  def main do
    Logger.configure(level: :warning)

    reps = env("REPS", 15)
    writers = env("WRITERS", 8)
    concurrent_rows = env("ROWS", 5_000)
    batches = env("BATCHES", 10)

    {node, base_url} = start_peer!()

    wire_size()
    serial(node, base_url, reps)
    concurrent(node, base_url, writers, batches, concurrent_rows)
    end_to_end(node, base_url, reps)

    IO.puts("")
  end

  # ── fixtures ──────────────────────────────────────────────────────────

  defp shapes, do: [{"narrow (4 cols)", narrow_schema()}, {"wide (20 cols)", wide_schema()}]

  defp narrow_schema, do: schema()

  defp wide_schema do
    extra =
      for i <- 1..16 do
        if rem(i, 2) == 0, do: {"f#{i}", :float64}, else: {"s#{i}", :string}
      end

    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"name", :string},
      {"amount", {:numeric, 38, 2}} | extra
    ])
  end

  defp rows_for(%Schema{} = schema, count) do
    base = ~N[2026-01-01 00:00:00]

    for i <- 1..count do
      Map.new(schema.fields, fn field ->
        {field.name, value_for(field.name, field.type, base, i)}
      end)
    end
  end

  defp value_for(_name, :int64, _base, i), do: i
  defp value_for(_name, :timestamp, base, i), do: NaiveDateTime.add(base, i, :second)
  defp value_for(name, :string, _base, i), do: "#{name}-value-#{i}"
  defp value_for(_name, :float64, _base, i), do: i * 1.5

  defp value_for(_name, {:numeric, _p, _s}, _base, i),
    do: Decimal.new("#{rem(i, 997)}.#{rem(i, 100)}")

  defp batch_for(schema, count), do: %{schema: schema, rows: rows_for(schema, count)}

  defp encode_ipc(schema, rows) do
    {:ok, dtypes} = Schema.explorer_dtypes(schema)

    columns =
      Enum.map(dtypes, fn {name, dtype} ->
        {name, Series.from_list(Enum.map(rows, &Map.get(&1, name)), dtype: dtype)}
      end)

    columns |> DataFrame.new() |> DataFrame.dump_ipc!()
  end

  # ── sections ──────────────────────────────────────────────────────────

  defp wire_size do
    heading("Wire size — term_to_binary vs Arrow IPC (bytes/row)")

    IO.puts(
      label("shape", 18) <>
        pad("rows", 8) <> pad("term KiB", 12) <> pad("ipc KiB", 12) <> pad("ipc/term", 10)
    )

    for {name, schema} <- shapes(), count <- [100, 1_000, 10_000] do
      batch = batch_for(schema, count)
      term = byte_size(:erlang.term_to_binary(batch))
      ipc = byte_size(encode_ipc(schema, batch.rows))

      IO.puts(
        label(name, 18) <>
          pad(count, 8) <>
          pad(kib(term), 12) <> pad(kib(ipc), 12) <> pad(Float.round(ipc / term, 2), 10)
      )
    end
  end

  defp serial(node, base_url, reps) do
    heading("Round trip, serial — one writer, ms/batch (min/median of #{reps})")

    IO.puts(
      label("shape", 18) <>
        pad("rows", 8) <>
        pad("gen_rpc", 14) <> pad("ipc→frame", 14) <> pad("ipc→rows", 14)
    )

    for {name, schema} <- shapes(), count <- [100, 1_000, 10_000] do
      batch = batch_for(schema, count)

      gen_rpc = timed(fn -> {:ok, _n} = rpc_sink(node, :bulk, batch) end, reps)

      frame =
        timed(fn -> http_sink(base_url, "/sink/frame", encode_ipc(schema, batch.rows)) end, reps)

      rows =
        timed(fn -> http_sink(base_url, "/sink/rows", encode_ipc(schema, batch.rows)) end, reps)

      IO.puts(
        label(name, 18) <>
          pad(count, 8) <>
          pad(min_median(gen_rpc), 14) <> pad(min_median(frame), 14) <> pad(min_median(rows), 14)
      )
    end
  end

  defp concurrent(node, base_url, writers, batches, count) do
    heading(
      "Round trip, concurrent — #{writers} writers × #{batches} batches × #{count} rows, krows/s"
    )

    IO.puts(
      label("shape", 18) <>
        pad("gen_rpc :bulk", 16) <> pad("gen_rpc/writer", 16) <> pad("ipc→rows", 14)
    )

    for {name, schema} <- shapes() do
      batch = batch_for(schema, count)
      total = writers * batches * count

      one_channel =
        throughput(total, writers, batches, fn _writer -> rpc_sink(node, :bulk, batch) end)

      per_writer =
        throughput(total, writers, batches, fn writer ->
          rpc_sink(node, {:bulk, writer}, batch)
        end)

      http =
        throughput(total, writers, batches, fn _writer ->
          http_sink(base_url, "/sink/rows", encode_ipc(schema, batch.rows))
        end)

      IO.puts(label(name, 18) <> pad(one_channel, 16) <> pad(per_writer, 16) <> pad(http, 14))
    end
  end

  defp end_to_end(node, base_url, reps) do
    heading("End to end — the real write, group commit included, ms/batch (min/median)")

    IO.puts(
      label("shape", 18) <> pad("rows", 8) <> pad("gen_rpc write", 16) <> pad("ipc write", 14)
    )

    for {name, schema} <- shapes(), count <- [1_000] do
      batch = batch_for(schema, count)

      gen_rpc =
        timed(fn -> {:ok, _ack} = Client.write_batch(buffer_name(), @table, batch) end, reps)

      ipc =
        timed(fn -> http_sink(base_url, "/write", encode_ipc(schema, batch.rows)) end, reps)

      IO.puts(
        label(name, 18) <>
          pad(count, 8) <> pad(min_median(gen_rpc), 16) <> pad(min_median(ipc), 14)
      )
    end
  end

  # ── transports ────────────────────────────────────────────────────────

  defp rpc_sink(node, channel, batch) do
    :gen_rpc.call({node, channel}, Bench.IngestTransport.Sink, :sink, [batch], 30_000)
  end

  defp http_sink(base_url, path, body) do
    %{status: 200} =
      Req.post!(base_url <> path, body: body, retry: false, receive_timeout: 30_000)
  end

  defp throughput(total_rows, writers, batches, fun) do
    {us, _results} =
      :timer.tc(fn ->
        1..writers
        |> Task.async_stream(
          fn writer -> for _batch <- 1..batches, do: fun.(writer) end,
          max_concurrency: writers,
          timeout: 120_000,
          ordered: false
        )
        |> Stream.run()
      end)

    Float.round(total_rows / (us / 1_000_000) / 1_000, 1)
  end

  defp min_median(%{min: min, median: median}), do: "#{ms(min)}/#{ms(median)}"

  defp kib(bytes), do: Float.round(bytes / 1024, 1)

  # ── peer bootstrap ────────────────────────────────────────────────────

  defp buffer_name, do: :ingest_transport_bench_buffer

  defp start_peer! do
    ensure_distributed!()

    {:ok, _peer, node} =
      :peer.start(%{
        name: :"ingest_transport_peer_#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"smolquery_bench_cookie"]
      })

    dir =
      Path.join(
        System.tmp_dir!(),
        "smolquery-bench-ingest-transport-#{System.unique_integer([:positive])}"
      )

    buffer = [
      name: buffer_name(),
      dir: dir,
      flush_max_rows: 1,
      flush_interval_ms: 25,
      hot_server_port: 0,
      ring: [node]
    ]

    :erpc.call(node, :code, :add_paths, [:code.get_path()])
    :erpc.call(node, Application, :put_env, [:smolquery, :roles, [:buffer]])
    :erpc.call(node, Application, :put_env, [:smolquery, Smolquery.BufferService, buffer])

    peer_gen_rpc = [
      tcp_server_port: @peer_gen_rpc_port,
      tcp_client_port: @peer_gen_rpc_port,
      rpc_module_control: :whitelist,
      rpc_module_list: [Smolquery.BufferService.Endpoint, Bench.IngestTransport.Sink],
      client_config_per_node: {:internal, %{node() => 5_369}}
    ]

    for {key, value} <- peer_gen_rpc do
      :erpc.call(node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:smolquery])

    :erpc.call(node, Code, :require_file, [Path.expand("ingest_transport_peer.exs", __DIR__)])

    :erpc.call(node, Bench.IngestTransport.Peer, :start_http, [
      @peer_http_port,
      %{buffer: buffer_name(), table: @table}
    ])

    :application.set_env(
      :gen_rpc,
      :client_config_per_node,
      {:internal, %{node => @peer_gen_rpc_port}},
      persistent: true
    )

    Application.put_env(:smolquery, Smolquery.BufferService, buffer)
    Routing.forget(buffer_name())

    {node, "http://127.0.0.1:#{@peer_http_port}"}
  end

  defp ensure_distributed! do
    case Node.start(:"smolquery_bench@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_bench_cookie)
  end
end

Bench.IngestTransport.main()
