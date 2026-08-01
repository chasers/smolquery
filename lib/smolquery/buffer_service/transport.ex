defmodule Smolquery.BufferService.Transport do
  @moduledoc """
  How a call reaches the buffer node that owns a table.

  The buffer service is the one stateful thing in smolquery, so every other
  service talks to it constantly: the ingest edge forwards batches, the planner
  asks for hot manifests once per query per table, the sealer retires segments.
  In a cluster most of that crosses a node boundary, and the transport it crosses
  on is a deployment decision rather than a property of the caller.

  So `Client` never picks a transport. It resolves the owning node and hands the
  call here, which is what makes splitting the buffer service into its own
  deployment a config change: local calls and remote ones go through the same
  seam.

  ## Not Erlang distribution

  The obvious implementation — `:erpc` — puts this traffic on the single TCP
  connection the cluster uses for everything else. Forward-batches are large and
  constant, so they would sit in front of cluster chatter and monitors, and a
  `busy_dist_port` stall looks to the rest of OTP like nodes going down. The
  default remote transport is therefore `Transport.GenRpc`, which carries this
  traffic on its own sockets and leaves distribution alone.

  ## Channels, because gen_rpc has the same problem one level down

  gen_rpc opens one connection per *destination key*, not per call, so routing
  everything at a node through one key just moves the head-of-line blocking onto
  that socket. Traffic is therefore split by weight: `:bulk` carries
  forward-batches, `:control` carries manifest reads and retirements. A manifest
  read waits behind a queue of batches on a shared connection and does not on a
  separate one — which is the whole reason the channel is part of this API rather
  than an implementation detail.

  ## Implementations

    * `Transport.Local` — a direct call, used whenever this node is the owner
    * `Transport.GenRpc` — `:gen_rpc` over its own TCP or TLS sockets

  """

  alias Smolquery.BufferService.Endpoint

  @typedoc """
  Which connection a call travels on — `:bulk` for row-carrying writes,
  `:control` for small, latency-sensitive metadata calls.
  """
  @type channel :: :bulk | :control

  @callback invoke(node(), channel(), atom(), [term()], timeout()) :: term()

  @doc """
  Invokes `function` on `node`'s `Smolquery.BufferService.Endpoint`.
  """
  @spec invoke(module(), node(), channel(), atom(), [term()], timeout()) :: term()
  def invoke(impl, node, channel, function, args, timeout),
    do: impl.invoke(node, channel, function, args, timeout)

  @doc """
  The module every transport dispatches to on the far side.

  One entry point, and the only module named in `:gen_rpc`'s allowlist — a peer
  that has authenticated can otherwise run any MFA it likes.
  """
  @spec endpoint() :: module()
  def endpoint, do: Endpoint
end
