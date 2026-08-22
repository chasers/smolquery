defmodule Smolquery.QueryService.EnginePool do
  @moduledoc """
  A few job engines built ahead of the jobs that will use them (PL-50).

  A job engine's bootstrap — `LOAD` of three extensions and an `ATTACH` to
  the catalog — measured ~650 ms per query on a production node, most of
  a short query's latency. The engines themselves stay private to their
  jobs (PL-7 D8): this pool only moves *when* one is built. It keeps
  `runtime.warm_engines` engines bootstrapped with
  `Smolquery.QueryService.JobEngine.options/1`, builds them in background
  tasks, and hands one over on `checkout/1`. The caller owns it from then
  on, exactly as if it had started the engine itself, and the pool starts a
  replacement. An empty pool answers `:empty` at once — a job never waits
  on a build.

  Ownership transfers by link: the pool unlinks the engine's two processes
  before it replies, and the caller links them (`JobEngine.link/1`). An
  engine that dies while pooled is dropped, its sibling killed, and a
  replacement built. An engine older than `warm_engine_max_age_ms` is
  recycled on a timer, so an idle catalog connection never ages into a
  failed job; the checkout's probe (`JobEngine.acquire/1`) is the backstop
  for the rest.

  The pool is a `rest_for_one` child after the runners: its crash restarts
  nothing else, and a restart simply rebuilds.
  """

  use GenServer

  require Logger

  alias Smolquery.QueryService.JobEngine
  alias Smolquery.QueryService.Runtime

  @recycle_every_ms 30_000
  @retry_after_ms 1_000

  @doc """
  A child spec for `runtime`'s pool.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.engine_pool(runtime.name))
  end

  @doc """
  One warm engine, unlinked from the pool and owned by the caller, or
  `:empty`. The caller links it; see `JobEngine.acquire/1`.
  """
  @spec checkout(atom()) :: {:ok, JobEngine.t()} | :empty
  def checkout(name), do: GenServer.call(Runtime.engine_pool(name), :checkout)

  @doc """
  How many engines are warm right now.
  """
  @spec size(atom()) :: non_neg_integer()
  def size(name), do: GenServer.call(Runtime.engine_pool(name), :size)

  @impl GenServer
  def init(%Runtime{} = runtime) do
    Process.flag(:trap_exit, true)
    Process.send_after(self(), :recycle, @recycle_every_ms)

    {:ok, %{runtime: runtime, warm: [], building: %{}}, {:continue, :fill}}
  end

  @impl GenServer
  def handle_continue(:fill, state), do: {:noreply, fill(state)}

  @impl GenServer
  def handle_call(:checkout, _from, %{warm: []} = state), do: {:reply, :empty, state}

  def handle_call(:checkout, _from, %{warm: [{engine, _built_at} | rest]} = state) do
    Process.unlink(engine.connection)
    Process.unlink(engine.database)

    {:reply, {:ok, engine}, %{state | warm: rest}, {:continue, :fill}}
  end

  def handle_call(:size, _from, state), do: {:reply, length(state.warm), state}

  @impl GenServer
  def handle_info({ref, outcome}, %{building: building} = state) when is_map_key(building, ref) do
    Process.demonitor(ref, [:flush])
    state = %{state | building: Map.delete(building, ref)}

    case outcome do
      {:ok, engine} ->
        JobEngine.link(engine)

        {:noreply, %{state | warm: state.warm ++ [{engine, now()}]}, {:continue, :fill}}

      {:error, reason} ->
        Logger.warning("warm engine build failed: #{inspect(reason)}")
        Process.send_after(self(), :fill, @retry_after_ms)

        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{building: building} = state)
      when is_map_key(building, ref) do
    Logger.warning("warm engine build crashed: #{inspect(reason)}")
    Process.send_after(self(), :fill, @retry_after_ms)

    {:noreply, %{state | building: Map.delete(building, ref)}}
  end

  def handle_info({:EXIT, pid, _reason}, state) do
    {dead, alive} =
      Enum.split_with(state.warm, fn {engine, _built_at} ->
        pid in [engine.connection, engine.database]
      end)

    Enum.each(dead, fn {engine, _built_at} -> JobEngine.stop(engine) end)

    {:noreply, %{state | warm: alive}, {:continue, :fill}}
  end

  def handle_info(:fill, state), do: {:noreply, fill(state)}

  def handle_info(:recycle, state) do
    Process.send_after(self(), :recycle, @recycle_every_ms)
    oldest = now() - state.runtime.warm_engine_max_age_ms

    {stale, fresh} = Enum.split_with(state.warm, fn {_engine, built_at} -> built_at < oldest end)
    Enum.each(stale, fn {engine, _built_at} -> JobEngine.stop(engine) end)

    {:noreply, %{state | warm: fresh}, {:continue, :fill}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.warm, fn {engine, _built_at} -> JobEngine.stop(engine) end)
  end

  defp fill(state) do
    missing = state.runtime.warm_engines - length(state.warm) - map_size(state.building)
    options = JobEngine.options(state.runtime)

    Enum.reduce(1..max(missing, 0)//1, state, fn _index, acc ->
      task = Task.async(fn -> build(options) end)

      %{acc | building: Map.put(acc.building, task.ref, true)}
    end)
  end

  defp build(options) do
    with {:ok, engine} <- JobEngine.start(options) do
      Process.unlink(engine.connection)
      Process.unlink(engine.database)

      {:ok, engine}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
