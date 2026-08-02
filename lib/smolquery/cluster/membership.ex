defmodule Smolquery.Cluster.Membership do
  @moduledoc """
  Debounced node-up/node-down events, broadcast to subscribers.

  Nothing in the ring path consumes these events today: ownership reads live
  `:pg` membership per call (`Smolquery.Cluster.PgGroup`,
  `Smolquery.Cluster.RingCache`), which needs no debounce. This exists for
  the things that should react to the *fleet* changing shape rather than to
  a group — reactive maintenance like force-sealing tables whose ownership
  moved away is the intended consumer (PL-11), and anything growing such a
  need should subscribe here rather than run its own
  `:net_kernel.monitor_nodes/2` and coalescing.

  ## Debouncing

  `libcluster_postgres` (and any other `libcluster` strategy) reports nodes
  joining a fleet one at a time — a fleet of N nodes coming up together fires
  N-1 individual `:nodeup` events at any node already running. Recomputing a
  ring on every one of those would mean every layer built on this
  (`Smolquery.BufferService.Ring`, table ownership, force-seal-on-drain) does
  work against N-1 transient, already-stale memberships before the real one.
  A subscriber only ever sees the member list once it has been stable for
  `debounce_ms` (default 2s) — cheap, since nothing here depends on reacting
  to a *single* node's arrival, only on the eventual settled list.

  This is not a correctness requirement: a write against a stale ring fails
  the same honest way any other unreachable-owner call does today
  (`{:error, {:badrpc, _}}`), and is retried once routing catches up. The
  debounce exists to avoid needless churn — a force-seal signal fired against
  every intermediate membership, for instance — not to hide a race.

  ## Usage

      Smolquery.Cluster.Membership.subscribe()
      #=> :ok, and this process immediately receives
      #   {:cluster_membership, members}

      receive do
        {:cluster_membership, members} -> ...
      end
  """

  use GenServer

  @default_debounce_ms 2_000

  @type members :: [node()]

  @doc """
  Starts the broadcaster. `:debounce_ms` defaults to #{@default_debounce_ms}.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Subscribes the caller to membership changes.

  The caller receives `{:cluster_membership, members}` immediately with the
  current list, then again after the debounce window following any
  subsequent `:nodeup`/`:nodedown`. A subscriber that exits is dropped
  without needing to unsubscribe.
  """
  @spec subscribe(GenServer.server()) :: :ok
  def subscribe(server \\ __MODULE__), do: GenServer.call(server, {:subscribe, self()})

  @doc """
  The current member list — this node plus every node it is connected to.
  """
  @spec members(GenServer.server()) :: members()
  def members(server \\ __MODULE__), do: GenServer.call(server, :current_members)

  @impl GenServer
  def init(opts) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)

    {:ok,
     %{
       subscribers: MapSet.new(),
       debounce_ms: Keyword.get(opts, :debounce_ms, @default_debounce_ms),
       timer: nil
     }}
  end

  @impl GenServer
  def handle_call({:subscribe, pid}, _from, state) do
    Process.monitor(pid)
    send(pid, {:cluster_membership, current_members()})
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call(:current_members, _from, state), do: {:reply, current_members(), state}

  @impl GenServer
  def handle_info({:nodeup, _node, _info}, state), do: {:noreply, debounce(state)}
  def handle_info({:nodedown, _node, _info}, state), do: {:noreply, debounce(state)}

  def handle_info({:broadcast, ref}, %{timer: {_timer, ref}} = state) do
    current = current_members()
    Enum.each(state.subscribers, &send(&1, {:cluster_membership, current}))
    {:noreply, %{state | timer: nil}}
  end

  def handle_info({:broadcast, _stale}, state), do: {:noreply, state}

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
  end

  defp debounce(%{timer: nil} = state) do
    ref = make_ref()

    %{state | timer: {Process.send_after(self(), {:broadcast, ref}, state.debounce_ms), ref}}
  end

  defp debounce(%{timer: {timer, _ref}} = state) do
    Process.cancel_timer(timer)
    debounce(%{state | timer: nil})
  end

  defp current_members, do: Enum.sort([node() | Node.list()])
end
