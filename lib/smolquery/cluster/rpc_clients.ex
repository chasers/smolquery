defmodule Smolquery.Cluster.RpcClients do
  @moduledoc """
  Drops a peer's `:gen_rpc` clients whenever distribution sees it leave or
  rejoin (T-613).

  gen_rpc keeps one client process per `{node, key}` destination, and each
  holds its socket until that socket fails. Nothing in gen_rpc watches
  distribution, so a pod that dies without closing its sockets leaves every
  client pointed at its old IP. The replacement pod has the same node name
  and a new IP, joins `Node.list/0` within seconds, and every call routed to
  it is sent on the dead socket: kernel keepalive is set only on accepted
  sockets, gen_rpc's own keepalive ping queues in the kernel without
  failing, and `send_timeout` fires only once the send buffer fills. The
  channel stays dead until Linux gives up retransmitting, about 15 minutes
  later (`tcp_retries2`).

  This process subscribes to `:net_kernel.monitor_nodes/2` and, on every
  `:nodedown` and `:nodeup`, stops every gen_rpc client whose destination is
  that node: the buffer transport's `:control` and `{:bulk, _}` channels and
  the query service's `{:scatter, _}` channels alike. The next call starts a
  fresh client, which dials the address the node name resolves to now.

  ## Why not `Smolquery.Cluster.Membership`

  Membership broadcasts the settled member list after a debounce. A pod
  replaced inside that window leaves the list unchanged, which is exactly the
  case this must catch, so the raw per-node events are watched here instead.

  ## Cost

  A call in flight on a dropped client fails at once rather than waiting out
  its deadline, which is the point for a dead peer. A client to a healthy
  peer dropped on a spurious `:nodeup` costs one reconnect. Each event reads
  gen_rpc's client registry once; it holds one entry per peer and channel.
  """

  use GenServer

  require Logger

  @doc """
  Starts the watcher.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Stops every gen_rpc client whose destination is `node` and returns how many
  were stopped.
  """
  @spec drop(node()) :: non_neg_integer()
  def drop(node) do
    node
    |> clients()
    |> Enum.map(&:gen_rpc_client_sup.stop_child/1)
    |> length()
  end

  @doc """
  The pids of every gen_rpc client whose destination is `node`.
  """
  @spec clients(node()) :: [pid()]
  def clients(node) do
    for {destination, pid} <- :gen_rpc_registry.all_processes(:client),
        destination_node(destination) == node,
        do: pid
  end

  @impl GenServer
  def init(_opts) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)
    {:ok, %{}}
  end

  @impl GenServer
  def handle_info({event, node, _info}, state) when event in [:nodeup, :nodedown] do
    log_dropped(event, node, drop(node))
    {:noreply, state}
  end

  defp destination_node({node, _key}), do: node
  defp destination_node(node), do: node

  defp log_dropped(_event, _node, 0), do: :ok

  defp log_dropped(event, node, count) do
    Logger.info("dropped #{count} gen_rpc client(s) to #{node} on #{event}")
  end
end
