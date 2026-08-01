defmodule Bench.IngestTransport.Sink do
  @moduledoc """
  The gen_rpc side's landing pad: receives a forward-batch as Erlang terms and
  fully materializes it, so the measurement includes everything a real
  endpoint would have paid for the payload — and nothing it would not.

  Loaded on the peer with `Code.require_file/1` and added to gen_rpc's module
  allowlist there; it exists only while the bench runs.
  """

  def sink(%{rows: rows}) when is_list(rows), do: {:ok, length(rows)}
end

defmodule Bench.IngestTransport.HttpSink do
  @moduledoc """
  The Arrow IPC side's landing pad: a Bandit plug on the peer that reads the
  body and decodes it to the level the path name asks for.

    * `/sink/frame` — decode to an Explorer DataFrame and stop, the cost if a
      future `TableBuffer` accepted frames
    * `/sink/rows`  — decode and convert to row maps, the drop-in cost for
      today's row-based `TableBuffer`
    * `/write`      — decode to rows and run the real
      `BufferService.Endpoint.write_batch/3`, the end-to-end comparison
  """

  @behaviour Plug

  import Plug.Conn

  alias Explorer.DataFrame

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    {:ok, body, conn} = read_body(conn, length: 1_000_000_000)

    respond(conn, conn.path_info, body, opts)
  end

  defp respond(conn, ["sink", "frame"], body, _opts) do
    frame = DataFrame.load_ipc!(body)

    send_resp(conn, 200, Integer.to_string(DataFrame.n_rows(frame)))
  end

  defp respond(conn, ["sink", "rows"], body, _opts) do
    rows = body |> DataFrame.load_ipc!() |> DataFrame.to_rows()

    send_resp(conn, 200, Integer.to_string(length(rows)))
  end

  defp respond(conn, ["write"], body, opts) do
    frame = DataFrame.load_ipc!(body)
    batch = %{schema: schema_from(frame), rows: DataFrame.to_rows(frame)}

    {:ok, _ack} = Smolquery.BufferService.Endpoint.write_batch(opts.buffer, opts.table, batch)

    send_resp(conn, 200, "ok")
  end

  defp schema_from(frame) do
    dtypes = DataFrame.dtypes(frame)

    frame
    |> DataFrame.names()
    |> Enum.map(fn name ->
      {:ok, logical} = Smolquery.Schema.logical_from_explorer(Map.fetch!(dtypes, name))
      {name, logical}
    end)
    |> Smolquery.Schema.new!()
  end
end

defmodule Bench.IngestTransport.Peer do
  @moduledoc """
  Peer-side bootstrap, invoked over `:erpc`: starts the HTTP sink listener,
  unlinked so it outlives the erpc worker that started it.
  """

  def start_http(port, opts) do
    {:ok, pid} =
      Bandit.start_link(
        plug: {Bench.IngestTransport.HttpSink, opts},
        ip: {127, 0, 0, 1},
        port: port,
        startup_log: false
      )

    Process.unlink(pid)

    :ok
  end
end
