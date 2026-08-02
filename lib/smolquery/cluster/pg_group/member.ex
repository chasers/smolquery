defmodule Smolquery.Cluster.PgGroup.Member do
  @moduledoc """
  The process whose liveness is this node's membership in one service's ring
  group.

  `Smolquery.Cluster.PgGroup.join/3` needs a pid that lives exactly as long
  as the node should be in the ring. The hosting supervisor's own pid does
  that — until the `:pg` scope process restarts: a restarted scope comes back
  with empty state, remote scopes that monitored the dead one drop this
  node's members, and a supervisor that joined once in `init/1` never
  re-asserts. The node would silently vanish from every ring while perfectly
  healthy, with nothing logged anywhere.

  So membership gets its own process. It joins with its own pid on start,
  monitors the scope process, and re-joins (retrying until the application
  supervisor has restarted the scope) whenever the scope dies. Started as the
  first child of the service's `rest_for_one` subtree, it still dies with the
  subtree — an ungraceful crash removes the node from the ring exactly as
  before.

  `leave/1` is the graceful exit (`Smolquery.BufferService.Drain`): it leaves
  the group *and* stops re-joining, so a scope restart after a completed
  drain does not resurrect the node's arc.

  When clustering is off the join is a no-op and there is no scope process to
  watch; the process idles.
  """

  use GenServer

  require Logger

  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup

  @rejoin_retry_ms 100

  @doc """
  A child spec joining `scope`'s `name` group for as long as this process lives.
  """
  @spec child_spec({module(), atom()}) :: Supervisor.child_spec()
  def child_spec({scope, name}) do
    %{
      id: process_name(scope, name),
      start: {__MODULE__, :start_link, [{scope, name}]}
    }
  end

  @doc """
  Starts the member for `scope`'s `name` group.
  """
  @spec start_link({module(), atom()}) :: GenServer.on_start()
  def start_link({scope, name}) do
    GenServer.start_link(__MODULE__, {scope, name}, name: process_name(scope, name))
  end

  @doc """
  The registered name of `scope`'s member for instance `name`.
  """
  @spec process_name(module(), atom()) :: atom()
  def process_name(scope, name) do
    Module.concat(name, "#{scope |> Module.split() |> List.last()}RingMember")
  end

  @doc """
  Leaves the group and stops re-joining — the graceful, permanent exit a
  drain performs. A `server` that is not running is `:ok`; there is no
  membership left to remove.
  """
  @spec leave(GenServer.server()) :: :ok
  def leave(server) do
    GenServer.call(server, :leave)
  catch
    :exit, {:noproc, _call} -> :ok
  end

  @impl GenServer
  def init({scope, name}) do
    {:ok, join(%{scope: scope, name: name, monitor: nil, left: false})}
  end

  @impl GenServer
  def handle_call(:leave, _from, state) do
    PgGroup.leave(state.scope, state.name, self())

    {:reply, :ok, %{state | left: true}}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor: ref} = state) do
    {:noreply, rejoin(%{state | monitor: nil})}
  end

  def handle_info(:rejoin, state), do: {:noreply, rejoin(state)}

  def handle_info(_message, state), do: {:noreply, state}

  defp rejoin(%{left: true} = state), do: state

  defp rejoin(state) do
    case Process.whereis(Cluster.pg_scope()) do
      nil ->
        Process.send_after(self(), :rejoin, @rejoin_retry_ms)

        state

      _scope ->
        Logger.info(fn ->
          "re-joined #{inspect(state.scope)} ring group #{inspect(state.name)} " <>
            "after a :pg scope restart"
        end)

        join(state)
    end
  end

  defp join(state) do
    PgGroup.join(state.scope, state.name, self())

    %{state | monitor: monitor_scope()}
  end

  defp monitor_scope do
    case Cluster.enabled?() && Process.whereis(Cluster.pg_scope()) do
      pid when is_pid(pid) -> Process.monitor(pid)
      _disabled_or_absent -> nil
    end
  end
end
