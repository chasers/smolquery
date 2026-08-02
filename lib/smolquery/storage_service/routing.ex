defmodule Smolquery.StorageService.Routing do
  @moduledoc """
  Which storage node owns a table's seal work (Milestone 8 L6, PL-11 D6).

  The storage-side twin of `Smolquery.BufferService.Routing`: a second `Ring`
  instance, keyed by the StorageService node subset of cluster membership
  (`Smolquery.Cluster.PgGroup`, scoped to `Smolquery.StorageService` rather
  than the buffer's). Reusing `Ring` — already table-agnostic per its own
  moduledoc — rather than a Postgres advisory lock means naming an owner
  costs no round trip, and a
  transiently wrong owner during a ring change is safe by construction:
  `Smolquery.Catalog.replace_segments/4` is atomic and idempotent-by-key, so
  two nodes racing a stale view produces at worst a duplicate merge attempt,
  not corruption.

  Two sources, in the same order `BufferService.Routing` uses and for the
  same reason — resolvable from a node that runs no storage subtree at all,
  which is what lets `Smolquery.StorageService.Client` name a *remote* owner
  for a seal signal instead of only ever reaching a sealer on its own node:

    * a published `Smolquery.StorageService.Runtime`, on nodes running the
      `:storage` role
    * otherwise application configuration — cached in `:persistent_term` when
      clustering is off, since the ring is then genuinely immutable

  `Smolquery.StorageService.Sealer` and `Smolquery.StorageService.Compactor`
  both call `own?/2` before acting on a signal or a sweep's table — the gate
  D6 describes. Routing to the right node in the first place (this module's
  `owner/2`) makes that gate meaningful rather than a permanent no-op: without
  it, a signal only ever reached whichever node happened to receive it.
  """

  alias Smolquery.BufferService.Ring
  alias Smolquery.Cluster.PgGroup
  alias Smolquery.Cluster.RingCache
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:name, :ring]
  defstruct [:name, :ring]

  @type t :: %__MODULE__{name: atom(), ring: Ring.t()}

  @doc """
  The routing for an instance.
  """
  @spec resolve(atom()) :: t()
  def resolve(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> from_runtime(runtime)
      :error -> cached(name)
    end
  end

  @doc """
  The node owning `table_ref`'s seal work.
  """
  @spec owner(t(), Store.table_ref()) :: node()
  def owner(%__MODULE__{ring: ring}, table_ref), do: Ring.owner(ring, table_ref)

  @doc """
  Whether this node owns `table_ref`'s seal work right now.
  """
  @spec own?(t(), Store.table_ref()) :: boolean()
  def own?(%__MODULE__{} = routing, table_ref), do: owner(routing, table_ref) == node()

  @doc """
  Drops a cached routing, so the next resolve rebuilds it from configuration.
  """
  @spec forget(atom()) :: boolean()
  def forget(name), do: RingCache.forget(key(name))

  defp from_runtime(%Runtime{} = runtime) do
    %__MODULE__{
      name: runtime.name,
      ring:
        Ring.new!(PgGroup.nodes(Smolquery.StorageService, runtime.name, Ring.nodes(runtime.ring)))
    }
  end

  defp cached(name), do: RingCache.resolve(key(name), fn -> build(name) end)

  defp build(name) do
    config = Application.get_env(:smolquery, Smolquery.StorageService, [])
    static = Keyword.get(config, :ring, [node()])

    %__MODULE__{
      name: name,
      ring: Ring.new!(PgGroup.nodes(Smolquery.StorageService, name, static))
    }
  end

  defp key(name), do: {__MODULE__, name}
end
