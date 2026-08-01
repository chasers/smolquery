defmodule Smolquery.BufferService.Adopter do
  @moduledoc """
  Starts buffers on boot for the tables this node already holds data for.

  Without this, a table's buffer only exists once a write arrives — and a buffer
  is what runs the seal check and the grace-period sweep. So a node that restarts
  holding an unsealed tail for a table nobody writes to again would keep that tail
  forever: never signalled, never sealed, never reaped. The data is safe and
  queryable, and permanently stuck.

  So adoption is the boot half of ownership: the manifest logs on disk say which
  tables this node was accumulating, and every one it still owns gets its buffer
  back. Starting a buffer recovers that table's manifest, which is also what
  re-adopts its segments and clears any that were never acked.

  Boot is also the one moment the store's staging directory can be swept with no
  age guard at all: this runs before any buffer starts, so nothing can be
  mid-write, and whatever a killed encoder left staged is deleted rather than
  leaked forever (`Smolquery.Segments.Store.sweep_staging/2`).

  Tables this node no longer owns are left alone. Handing an unsealed tail to a
  new owner is a ring-change problem, and ring changes are Milestone 8.

  ## Adoption blocks the boot, on purpose

  This runs in `init/1` and the subtree is not started until it finishes, so a
  node never serves a table whose tail it has not adopted yet. Doing it
  asynchronously would open exactly the window the hot tier exists to close: a
  query arriving in between would read an empty manifest for an unadopted table
  and quietly return results missing its unsealed rows. The cost is a boot
  proportional to the tail on disk, which is bounded by how often sealing runs.

  Having adopted, there is nothing left to supervise, so `init/1` returns
  `:ignore` and no process lingers.
  """

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Segments.Store

  @behaviour GenServer

  @doc """
  A child that adopts while starting, then leaves nothing behind.
  """
  @spec child_spec(Runtime.t()) :: Supervisor.child_spec()
  def child_spec(%Runtime{} = runtime) do
    %{
      id: Module.concat(runtime.name, "Adopter"),
      start: {GenServer, :start_link, [__MODULE__, runtime]},
      restart: :transient
    }
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    sweep_staging(runtime)
    adopt(runtime)

    :ignore
  end

  @doc """
  Starts a buffer for every owned table with a manifest log, returning their refs.
  """
  @spec adopt(Runtime.t()) :: [Smolquery.Segments.Store.table_ref()]
  def adopt(%Runtime{} = runtime) do
    adopted =
      runtime.manifest
      |> HotManifest.tables()
      |> Enum.filter(&(Ring.own?(runtime.ring, &1) and start_buffer(runtime, &1)))

    log(adopted)

    adopted
  end

  defp start_buffer(runtime, table_ref) do
    supervisor = {:via, PartitionSupervisor, {Runtime.buffers(runtime.name), table_ref}}
    spec = {Smolquery.BufferService.TableBuffer, runtime: runtime, table_ref: table_ref}

    case DynamicSupervisor.start_child(supervisor, spec) do
      {:ok, _pid} ->
        true

      {:error, {:already_started, _pid}} ->
        true

      {:error, reason} ->
        Logger.error(fn ->
          "buffer adoption failed for #{inspect(table_ref)}: #{inspect(reason)}"
        end)

        false
    end
  end

  defp sweep_staging(runtime) do
    case Store.sweep_staging(runtime.store, 0) do
      {:ok, []} ->
        :ok

      {:ok, swept} ->
        Logger.info(fn -> "swept #{length(swept)} staged file(s) a previous run left behind" end)

      {:error, reason} ->
        Logger.warning("boot staging sweep failed: #{inspect(reason)}")
    end
  end

  defp log([]), do: :ok

  defp log(adopted),
    do: Logger.info(fn -> "adopted #{length(adopted)} table(s) into the hot tier" end)
end
