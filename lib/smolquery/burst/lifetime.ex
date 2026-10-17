defmodule Smolquery.Burst.Lifetime do
  @moduledoc """
  A burst runner's own clock (PL-77 D8): the VM stops itself
  `max_lifetime_ms` after it started, so a runner on a host with a hard cap
  (Lambda's 15 minutes) leaves on its own terms.

  FLAME has no per-runner lifetime. At Lambda's cap the host kills the
  runner mid-shard with no shutdown: the shard in flight is lost, the
  invocation ends as a timeout Lambda reports as an error, and nothing
  on the runner gets to clean up. A stop from here is an ordinary
  shutdown: the Terminator leaves, the shard in flight fails as an
  unreachable runner, which the scatter retries on a peer, and the pool
  boots a fresh runner on the next checkout. The default, 14 minutes, sits
  one minute under the cap.

  Started by `Smolquery.Application` in runner mode when
  `SMOLQUERY_BURST_MAX_LIFETIME_MS` is in the runner's assignment
  (`Smolquery.Burst.runner_children/0`). The `stop` option is for tests.
  The value is bounded at boot to what a timer takes (`config/runtime.exs`).
  """

  use GenServer

  require Logger

  @spec start_link(pos_integer() | keyword()) :: GenServer.on_start()
  def start_link(max_lifetime_ms) when is_integer(max_lifetime_ms),
    do: start_link(max_lifetime_ms: max_lifetime_ms)

  def start_link(opts) when is_list(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    max_lifetime_ms = Keyword.fetch!(opts, :max_lifetime_ms)
    Process.send_after(self(), :expire, max_lifetime_ms)

    {:ok, %{max_lifetime_ms: max_lifetime_ms, stop: Keyword.get(opts, :stop, &System.stop/0)}}
  end

  @impl true
  def handle_info(:expire, state) do
    Logger.info("burst runner reached its lifetime of #{state.max_lifetime_ms}ms; stopping")
    state.stop.()

    {:noreply, state}
  end
end
