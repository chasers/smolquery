defmodule Smolquery.StorageService.Routing do
  @moduledoc """
  Which storage node owns a table's seal work (Milestone 8 L6, PL-11 D6).

  The storage-side twin of `Smolquery.BufferService.Routing`: a second `Ring`
  instance, keyed by the StorageService node subset of cluster membership
  (`Smolquery.Cluster.PgGroup`, scoped to `Smolquery.StorageService` rather
  than the buffer's). Reusing `Ring` — already table-agnostic per its own
  moduledoc — rather than a Postgres advisory lock means naming an owner
  costs no round trip. The price is that ownership is advisory, not mutual
  exclusion: `:pg` propagates per node, so during a ring change two nodes can
  each pass their own `own?/2` for the same table and run the same merge.
  What keeps that from corrupting the catalog is
  `Smolquery.Catalog.DuckLake` re-deriving its registration diff inside every
  commit retry — the loser of a commit conflict re-reads what the winner
  registered instead of replaying a stale statement. The residual window —
  both nodes reading before either commits, and the metadata store accepting
  both appends without a conflict — is accepted and narrow (one merge each,
  same derived key, sub-second commit against a minute-scale merge), but it
  is a window, not a proof; closing it outright needs a per-table catalog
  lock, which PL-11 D6 declined for now.

  Two sources, in the same order `BufferService.Routing` uses and for the
  same reason — resolvable from a node that runs no storage subtree at all,
  which is what lets `Smolquery.StorageService.Client` name a *remote* owner
  for a seal signal instead of only ever reaching a sealer on its own node:

    * a published `Smolquery.StorageService.Runtime`, on nodes running the
      `:storage` role
    * otherwise application configuration — either way the built ring is
      cached against the member list it came from (`Smolquery.Cluster.RingCache`)

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
    members = PgGroup.nodes(Smolquery.StorageService, runtime.name, Ring.nodes(runtime.ring))

    RingCache.resolve(key(runtime.name), {:runtime, members}, fn ->
      %__MODULE__{name: runtime.name, ring: Ring.new!(members)}
    end)
  end

  defp cached(name) do
    config = Application.get_env(:smolquery, Smolquery.StorageService, [])
    static = Keyword.get(config, :ring, [node()])
    members = PgGroup.nodes(Smolquery.StorageService, name, static)

    RingCache.resolve(key(name), {:config, members}, fn ->
      %__MODULE__{name: name, ring: Ring.new!(members)}
    end)
  end

  defp key(name), do: {__MODULE__, name}
end
