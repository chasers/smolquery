defmodule Smolquery.Cluster.RpcProbe do
  @moduledoc """
  The one function `Smolquery.Cluster.RpcClients` calls over a gen_rpc
  channel to learn whether its socket still carries replies.

  It is on gen_rpc's `rpc_module_list` beside the service endpoints, and it
  does nothing a peer could abuse: it answers `:pong`.
  """

  @doc """
  Answers `:pong`.
  """
  @spec pong() :: :pong
  def pong, do: :pong
end
