defmodule Smolquery.Cluster.RpcClients do
  @moduledoc """
  Drops `:gen_rpc` clients whose socket no longer reaches their peer (T-613).

  gen_rpc keeps one client process per `{node, key}` destination, and each
  holds its socket until that socket fails. Nothing in gen_rpc watches
  distribution, so a pod that dies without closing its sockets leaves every
  client pointed at its old IP. The replacement pod has the same node name
  and a new IP, joins `Node.list/0` within seconds, and every call routed to
  it is sent on the dead socket. Kernel keepalive probes only an idle
  connection; one with unacknowledged data waits out retransmission instead.
  gen_rpc's own keepalive ping queues in the kernel without failing, and
  `send_timeout` fires only once the send buffer fills, so a quiet channel
  stays dead until Linux gives up retransmitting, about 15 minutes later
  (`tcp_retries2`).

  ## On `:nodedown`

  This process subscribes to `:net_kernel.monitor_nodes/2` and, on every
  `:nodedown`, kills every gen_rpc client whose destination is that node:
  the buffer transport's `:control` and `{:bulk, _}` channels and the query
  service's `{:scatter, _}` channels alike. The next call starts a fresh
  client, which dials the address the node name resolves to then.

  Distribution delivers `:nodedown` for a connection before `:nodeup` for a
  same-named node that replaces it, and both transports refuse a node outside
  `Node.list/0`. So once `:nodeup` arrives, every client to that node was
  started after the node came back and dialed its new address. Dropping them
  then would only fail calls on healthy sockets, so `:nodeup` is ignored.

  Membership broadcasts the settled member list after a debounce. A pod
  replaced inside that window leaves the list unchanged, which is exactly the
  case this must catch, so the raw per-node events are watched here instead
  of subscribing to `Smolquery.Cluster.Membership`.

  ## On a call timeout

  A call can also time out to a node distribution still lists, for a
  partition distribution has not noticed yet. A timeout alone does not say
  the socket is dead: the remote operation may just be slow. So a transport
  that sees `{:badrpc, :timeout}` hands the destination to `check/2`, which
  takes the destination's current client and calls
  `Smolquery.Cluster.RpcProbe.pong/0` on it. gen_rpc runs every call in its
  own process on the remote side, so a slow operation does not hold the
  probe up. A probe that gets no answer within `:probe_timeout_ms` means
  nothing is coming back on that socket, and that client, the one probed, is
  killed. Any answer, an error included, keeps it. A destination with no
  client has nothing to probe, so a check never dials. One probe runs per
  destination at a time and the caller never waits for it.

  The probe queues behind the sends already on the client, and a send blocks
  for up to gen_rpc's `send_timeout` (5 s) when the peer reads slowly. A
  client stuck that way fails its own send and stops, so the probe waits
  longer than that, 10 s by default, and only a socket that accepts
  bytes but returns nothing is left for it to catch.

  ## Cost

  Clients are killed rather than shut down. A client traps exits, and one
  blocked in a send to a dead socket would hold an orderly shutdown for its
  full `send_timeout`, with this process and every later event waiting
  behind it. A call in flight on a killed client fails at once rather than
  waiting out its deadline, which is the point for a dead peer. Each
  `:nodedown` reads gen_rpc's client registry once; it holds one entry per
  peer and channel.
  """

  use GenServer

  alias Smolquery.Cluster.RpcProbe

  require Logger

  @default_probe_timeout_ms 10_000

  @type destination :: {node(), term()}

  @doc """
  Starts the watcher. `:probe_timeout_ms` defaults to
  #{@default_probe_timeout_ms}.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Probes `destination`'s current client after a call on it timed out, and
  kills that client if the probe goes unanswered. Returns at once.
  """
  @spec check(destination(), GenServer.server()) :: :ok
  def check(destination, server \\ __MODULE__),
    do: GenServer.cast(server, {:check, destination})

  @doc """
  Kills every gen_rpc client whose destination is `node` and returns how many
  it killed.
  """
  @spec drop(node()) :: non_neg_integer()
  def drop(node) do
    node
    |> clients()
    |> Enum.count(&Process.exit(&1, :kill))
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
  def init(opts) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)

    {:ok,
     %{
       probe_timeout_ms: Keyword.get(opts, :probe_timeout_ms, @default_probe_timeout_ms),
       probes: %{}
     }}
  end

  @impl GenServer
  def handle_cast({:check, destination}, state) do
    if Map.has_key?(state.probes, destination) do
      {:noreply, state}
    else
      {_pid, ref} = spawn_monitor(fn -> probe(destination, state.probe_timeout_ms) end)
      {:noreply, put_in(state.probes[destination], ref)}
    end
  end

  @impl GenServer
  def handle_info({:nodedown, node, _info}, state) do
    log_dropped(node, drop(node))
    {:noreply, state}
  end

  def handle_info({:nodeup, _node, _info}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | probes: Map.reject(state.probes, fn {_dest, probe} -> probe == ref end)}}
  end

  defp probe(destination, timeout_ms) do
    case :gen_rpc_registry.whereis_name({:client, destination}) do
      pid when is_pid(pid) -> probe(pid, destination, timeout_ms)
      :undefined -> :ok
    end
  end

  defp probe(pid, destination, timeout_ms) do
    case :gen_rpc.call(destination, RpcProbe, :pong, [], timeout_ms) do
      {:badrpc, :timeout} -> kill_silent(pid, destination)
      _answered -> :ok
    end
  end

  defp kill_silent(pid, destination) do
    if Process.alive?(pid) do
      Process.exit(pid, :kill)
      Logger.warning("dropped gen_rpc client to #{inspect(destination)}: probe unanswered")
    end
  end

  defp destination_node({node, _key}), do: node
  defp destination_node(node), do: node

  defp log_dropped(_node, 0), do: :ok

  defp log_dropped(node, count) do
    Logger.info("dropped #{count} gen_rpc client(s) to #{node} on nodedown")
  end
end
