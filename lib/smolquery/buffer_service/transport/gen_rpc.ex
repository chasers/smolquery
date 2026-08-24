defmodule Smolquery.BufferService.Transport.GenRpc do
  @moduledoc """
  `Smolquery.BufferService.Transport` over `:gen_rpc`.

  Carries buffer-service traffic on its own TCP or TLS sockets rather than the
  Erlang distribution connection, so a steady stream of forward-batches cannot
  stall the cluster's heartbeats and monitors.

  ## Channels are load-bearing

  The destination is `{node, key}`, not `node`. gen_rpc keys one connection
  per destination, and its client process sends each packet synchronously, so
  the key is what actually separates traffic. `:control` is one connection.
  `{:bulk, key}` is a pool of `bulk_channels/0` connections: the key hashes to
  a slot, so one table keeps one socket and its calls stay in order, while
  different tables spread over the pool instead of queueing behind each other
  (T-29). Collapsing everything onto one key would restore exactly the
  head-of-line blocking this transport exists to avoid.

  ## Membership is checked before the socket

  A node outside `Node.list/0` is answered `{:error, {:badrpc, :nodedown}}`
  without a connect attempt (T-374). Cluster membership already comes from
  distribution, so a node the ring names but distribution has lost is one
  gen_rpc would only wait `connect_timeout` on, then leave a dead client
  process behind for.

  ## Errors

  gen_rpc reports transport failures in-band, and returns a bare value on
  success — including for a remote `throw`, which is indistinguishable from a
  normal return. `Endpoint` therefore never throws; it returns tagged tuples like
  everything else, and this module only has to translate the two failure shapes:

    * `{:badrpc, reason}` — unreachable node, timeout, or a remote exception
    * `{:badtcp, reason}` — the connection itself failed

  Both become `{:error, {:badrpc | :badtcp, reason}}`, so a caller sees the same
  shape it would from any other failure and never has to know a transport exists.

  ## The rpc deadline outlives the operation's

  The remote endpoint runs the operation under the caller-supplied timeout; the
  rpc waits that long *plus a margin* for connect, serialization, and the reply's
  trip back. With the two deadlines equal, a write finishing just under its
  budget commits durably on the owner while the caller is already holding
  `{:badrpc, :timeout}` — and gen_rpc does not cancel the remote worker, so a
  retry would write the rows twice.

  ## Configuration

      config :gen_rpc,
        tcp_server_port: 5369,
        tcp_client_port: 5369,
        rpc_module_control: :whitelist,
        rpc_module_list: [Smolquery.BufferService.Endpoint],
        extra_process_flags: [fullsweep_after: 20]

      config :smolquery, Smolquery.BufferService.Transport.GenRpc, bulk_channels: 4

  The allowlist is not optional. gen_rpc's default is `:disabled`, which lets any
  peer holding the cookie execute arbitrary MFAs — a second remote-execution
  surface on a second port. Naming the one module this transport dispatches to
  closes it. TLS with per-node certificates is the other half (`GEN_RPC_TLS`).

  `extra_process_flags` applies to every gen_rpc client and acceptor process.
  Those carry each forward-batch binary, and a full sweep every 20 minor
  collections keeps their heaps from pinning those binaries (T-373).

  `bulk_channels` is the pool size per node pair (`GEN_RPC_BULK_CHANNELS`).
  The bench in `bench/results/ingest_transport.md` measured 2.5–2.6× concurrent
  throughput for per-writer channels with 8 writers on one node pair.
  """

  @behaviour Smolquery.BufferService.Transport

  alias Smolquery.BufferService.Transport

  @rpc_margin_ms 5_000
  @default_bulk_channels 4

  @impl Transport
  def invoke(node, channel, function, args, timeout) do
    if node in Node.list() do
      call(destination(node, channel), function, args, timeout)
    else
      {:error, {:badrpc, :nodedown}}
    end
  end

  @doc """
  The gen_rpc destination a channel maps to on `node`.

  `:control` is its own connection. `{:bulk, key}` hashes `key` into one of
  `bulk_channels/0` slots, so the same key always lands on the same socket.
  """
  @spec destination(node(), Transport.channel()) :: {node(), :control | {:bulk, pos_integer()}}
  def destination(node, :control), do: {node, :control}

  def destination(node, {:bulk, key}),
    do: {node, {:bulk, :erlang.phash2(key, bulk_channels()) + 1}}

  @doc """
  How many `:bulk` connections this node opens to each peer.
  """
  @spec bulk_channels() :: pos_integer()
  def bulk_channels do
    :smolquery
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:bulk_channels, @default_bulk_channels)
  end

  defp call(destination, function, args, timeout) do
    case :gen_rpc.call(destination, Transport.endpoint(), function, args, deadline(timeout)) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      {:badtcp, reason} -> {:error, {:badtcp, reason}}
      result -> result
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: timeout + @rpc_margin_ms
end
