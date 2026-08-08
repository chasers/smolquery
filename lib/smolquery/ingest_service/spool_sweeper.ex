defmodule Smolquery.IngestService.SpoolSweeper do
  @moduledoc """
  Deletes request bodies the spool directory was left holding.

  `Smolquery.IngestService.Client.insert_file/5` spools an `application/x-ndjson`
  body to `<data_dir>/tmp` and the request handler deletes it on every outcome it
  lives to see. Two things it cannot live to see:

    * its own process being killed — a shutdown mid-request, or the web server
      reaping a connection;
    * `Smolquery.BufferService.TableBuffer.Committer.terminate/2`, which kills
      in-flight encode tasks with `Process.exit(pid, :kill)`. A killed process
      runs no `after`, by definition, so no amount of care in the committer can
      cover that case.

  Nothing else names these files. They never become segments, so no manifest,
  catalog, or garbage collector will ever find them, and they sit on the same
  volume as the manifest logs and the catalog — what they fill up is the write
  path itself. This is the only thing that reclaims them.

  ## The age guard is the whole design

  A spooled body is written incrementally and is at its final size only when the
  request finishes, so a sweep with no age guard would delete a body that is
  merely still uploading. `spool_sweep_age_ms` must therefore stay comfortably
  above the slowest insert this node serves, which is bounded by the client's
  upload rate rather than by any timeout the server sets. The default is
  deliberately far above `write_timeout_ms`: reclaiming a leaked body an hour
  late costs disk, and deleting a live one costs the request.
  """

  use GenServer

  require Logger

  alias Smolquery.IngestService.Client
  alias Smolquery.IngestService.Runtime

  defstruct [:runtime]

  @doc """
  A child of the ingest subtree, named after its instance.
  """
  @spec child_spec(Runtime.t()) :: Supervisor.child_spec()
  def child_spec(%Runtime{} = runtime) do
    %{
      id: Runtime.spool_sweeper(runtime.name),
      start:
        {GenServer, :start_link,
         [__MODULE__, runtime, [name: Runtime.spool_sweeper(runtime.name)]]}
    }
  end

  @doc """
  Runs a sweep now and replies with what it deleted.

  For tests and for an operator with a full disk; the interval is what runs in
  a deployment.
  """
  @spec sweep(atom()) :: {:ok, [Path.t()]} | {:error, term()}
  def sweep(name \\ Runtime.spool_sweeper(Smolquery.IngestService)) do
    GenServer.call(name, :sweep)
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    {:ok, schedule(%__MODULE__{runtime: runtime})}
  end

  @impl GenServer
  def handle_call(:sweep, _from, state) do
    {:reply, run(state), state}
  end

  @impl GenServer
  def handle_info(:sweep, state) do
    case run(state) do
      {:ok, []} ->
        :ok

      {:ok, swept} ->
        Logger.info(fn -> "swept #{length(swept)} leaked request body/bodies from the spool" end)

      {:error, reason} ->
        # One failed sweep costs one interval and nothing else. The directory is
        # on the write path's own volume, so a transient failure here is far more
        # likely than a permanent one.
        Logger.warning("spool sweep failed: #{inspect(reason)}")
    end

    {:noreply, schedule(state)}
  end

  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  defp run(state), do: Client.sweep_spool(state.runtime.spool_sweep_age_ms)

  defp schedule(state) do
    Process.send_after(self(), :sweep, state.runtime.spool_sweep_interval_ms)

    state
  end
end
