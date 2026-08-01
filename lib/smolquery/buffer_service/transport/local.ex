defmodule Smolquery.BufferService.Transport.Local do
  @moduledoc """
  `Smolquery.BufferService.Transport` for a table this node owns.

  A direct call, so an owned table costs nothing to reach — no serialization, no
  socket, no channel. The single-node deployment runs entirely on this, and in a
  cluster it is still the path every buffer node takes to its own tables.

  An exit comes back in the same tagged shapes `Transport.GenRpc` translates —
  gen_rpc reports a remote timeout as `{:badrpc, :timeout}` and a remote crash
  as an in-band error, so the identical failure on an owned table must not exit
  the caller instead. That symmetry is the Client contract: a caller cannot tell
  from the return value where the work ran.
  """

  @behaviour Smolquery.BufferService.Transport

  alias Smolquery.BufferService.Transport

  @impl Transport
  def invoke(_node, _channel, function, args, _timeout) do
    apply(Transport.endpoint(), function, args)
  catch
    :exit, {:timeout, _call} -> {:error, {:badrpc, :timeout}}
    :exit, reason -> {:error, {:badrpc, {:EXIT, reason}}}
  end
end
