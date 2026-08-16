defmodule Smolquery.StorageService.Sweeper do
  @moduledoc """
  The shared shell of an interval sweeper.

  The compactor and the retention sweeper differ in what a sweep *does* and
  never in how one is scheduled: init schedules the first tick, `:sweep` as a
  call runs one now and replies with the report, the tick runs one and
  reschedules, an error is logged and costs one interval. That shape lives
  here once, so a third sweeper cannot drift from the first two.

      use Smolquery.StorageService.Sweeper, interval: :compact_interval_ms

  The using module supplies `run/1` — state in, `{:ok, report}` or
  `{:error, reason}` out — and defines its struct (with a `runtime` field)
  before the `use`. A sweeper that carries state across sweeps returns
  `{:ok, report, state}` or `{:error, reason, state}` instead; the two-tuple
  forms keep the state unchanged.
  """

  @doc false
  @spec split_result(term(), state) :: {term(), state} when state: var
  def split_result({:ok, report, state}, _state), do: {{:ok, report}, state}
  def split_result({:error, reason, state}, _state), do: {{:error, reason}, state}
  def split_result(result, state), do: {result, state}

  defmacro __using__(opts) do
    interval = Keyword.fetch!(opts, :interval)

    quote do
      use GenServer

      require Logger

      alias Smolquery.StorageService.Sweeper

      @impl GenServer
      def init(%Smolquery.StorageService.Runtime{} = runtime) do
        {:ok, schedule(%__MODULE__{runtime: runtime})}
      end

      @impl GenServer
      def handle_call(:sweep, _from, state) do
        {reply, state} = Sweeper.split_result(run(state), state)
        {:reply, reply, state}
      end

      @impl GenServer
      def handle_info(:sweep, state) do
        {result, state} = Sweeper.split_result(run(state), state)

        case result do
          {:ok, _report} ->
            :ok

          {:error, reason} ->
            Logger.warning("#{inspect(__MODULE__)} sweep failed: #{inspect(reason)}")
        end

        {:noreply, schedule(state)}
      end

      @impl GenServer
      def handle_info(_message, state), do: {:noreply, state}

      defp schedule(state) do
        Process.send_after(self(), :sweep, Map.fetch!(state.runtime, unquote(interval)))

        state
      end
    end
  end
end
