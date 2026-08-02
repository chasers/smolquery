defmodule Smolquery.BufferService.Adopter do
  @moduledoc """
  Starts buffers on boot for the tables this node already holds data for.

  Without this, a table's buffer only exists once a write arrives — and a buffer
  is what runs the seal check and the grace-period sweep. So a node that restarts
  holding an unsealed tail for a table nobody writes to again would keep that tail
  forever: never signalled, never sealed, never reaped. The data is safe and
  queryable, and permanently stuck.

  So adoption is the boot half of holding data: the manifest logs on disk say
  which tables this node was accumulating, and every one of them gets its
  buffer back — owned or not. The hot tier is single-copy and the bytes are
  *here*, so this is the only node that can ever seal them; filtering on a
  ring (static or live) strands acked rows exactly when a restart and a ring
  change coincide, because the ring's new owner starts with an empty manifest
  and nothing hands the tail over. A recovered table this node no longer owns
  takes no new writes — routing sends those to the ring's owner — but its
  seal check still runs, its tail still seals, and until it does the query
  planner's member-wide manifest fan-out (Milestone 8 L5) keeps its rows
  visible.

  Boot is also the one moment the store's staging directory can be swept with no
  age guard at all: this runs before any buffer starts, so nothing can be
  mid-write, and whatever a killed encoder left staged is deleted rather than
  leaked forever (`Smolquery.Segments.Store.sweep_staging/2`).

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
  Starts a buffer for every table with a manifest log, returning their refs.
  """
  @spec adopt(Runtime.t()) :: [Smolquery.Segments.Store.table_ref()]
  def adopt(%Runtime{} = runtime) do
    adopted =
      runtime.manifest
      |> HotManifest.tables()
      |> Enum.filter(&start_buffer(runtime, &1))

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
