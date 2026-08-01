defmodule Smolquery.BufferService.Transport.GenRpc do
  @moduledoc """
  `Smolquery.BufferService.Transport` over `:gen_rpc`.

  Carries buffer-service traffic on its own TCP or TLS sockets rather than the
  Erlang distribution connection, so a steady stream of forward-batches cannot
  stall the cluster's heartbeats and monitors.

  ## Channels are load-bearing

  The destination is `{node, channel}`, not `node`. gen_rpc keys one connection
  per destination, so the tag is what actually separates bulk writes from
  latency-sensitive manifest reads. Collapsing them onto one key would restore
  exactly the head-of-line blocking this transport exists to avoid.

  ## Errors

  gen_rpc reports transport failures in-band, and returns a bare value on
  success — including for a remote `throw`, which is indistinguishable from a
  normal return. `Endpoint` therefore never throws; it returns tagged tuples like
  everything else, and this module only has to translate the two failure shapes:

    * `{:badrpc, reason}` — unreachable node, timeout, or a remote exception
    * `{:badtcp, reason}` — the connection itself failed

  Both become `{:error, {:badrpc | :badtcp, reason}}`, so a caller sees the same
  shape it would from any other failure and never has to know a transport exists.

  ## Configuration

      config :gen_rpc,
        tcp_server_port: 5369,
        tcp_client_port: 5369,
        rpc_module_control: :whitelist,
        rpc_module_list: [Smolquery.BufferService.Endpoint]

  The allowlist is not optional. gen_rpc's default is `:disabled`, which lets any
  peer holding the cookie execute arbitrary MFAs — a second remote-execution
  surface on a second port. Naming the one module this transport dispatches to
  closes it. TLS with per-node certificates is the other half, and belongs to the
  cluster milestone.
  """

  @behaviour Smolquery.BufferService.Transport

  alias Smolquery.BufferService.Transport

  @impl Transport
  def invoke(node, channel, function, args, timeout) do
    case :gen_rpc.call({node, channel}, Transport.endpoint(), function, args, timeout) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      {:badtcp, reason} -> {:error, {:badtcp, reason}}
      result -> result
    end
  end
end
