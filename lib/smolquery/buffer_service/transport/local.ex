defmodule Smolquery.BufferService.Transport.Local do
  @moduledoc """
  `Smolquery.BufferService.Transport` for a table this node owns.

  A direct call, so an owned table costs nothing to reach — no serialization, no
  socket, no channel. The single-node deployment runs entirely on this, and in a
  cluster it is still the path every buffer node takes to its own tables.
  """

  @behaviour Smolquery.BufferService.Transport

  alias Smolquery.BufferService.Transport

  @impl Transport
  def invoke(_node, _channel, function, args, _timeout),
    do: apply(Transport.endpoint(), function, args)
end
