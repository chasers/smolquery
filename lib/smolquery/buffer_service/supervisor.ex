defmodule Smolquery.BufferService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:buffer` role.

  Started only on nodes whose roles include `:buffer` (see `Smolquery.Roles`).
  Holds the hot tier: the manifest index, the table-to-buffer registry, and the
  partitioned supervisor that `TableBuffer` processes start under on demand.

  The strategy is `rest_for_one`, in that order, because the dependency runs one
  way. The manifest owns the ETS index every buffer writes into, so if it dies the
  registry and the buffers that hold stale handles must go with it — and each
  buffer rebuilds its table's entries from the log when it restarts. A single
  buffer crashing, by contrast, disturbs nothing else.

  `Adopter` comes next, once the pieces it needs are up: it starts a buffer for
  every owned table that already has a manifest log, so an unsealed tail is not
  stranded waiting for a write that may never come.

  `HotServer` is last — it only reads the manifest and the store, so nothing
  beneath it depends on it being up.

  Ring membership is the first child, a
  `Smolquery.Cluster.PgGroup.Member` (Milestone 8 L4) — a no-op when
  clustering is off, and otherwise what puts this node in the ring at all.
  Its pid dies with this subtree, so an ungraceful crash removes the node
  from the ring the same way `Smolquery.BufferService.Drain.drain/2`'s
  explicit `leave` does — and unlike a one-shot join from `init/1`, it
  re-asserts membership after a `:pg` scope restart. It is omitted entirely
  when the drain flag is already up: a subtree restarted after a completed
  drain must not silently rejoin a ring whose writes it refuses.

  `RingEpoch` follows it when epoch fencing is configured (T-92) —
  `:epoch_fencing` is set by `config/runtime.exs` wherever
  `CATALOG_DATABASE_URL` enables clustering, and `:epoch_store` opts a test
  in explicitly; a test that merely flips `Smolquery.Cluster` on gets no
  keeper and no gate, because the keeper without a reachable store fails
  every write closed, which is right in production and wrong in a test that
  never promised a Postgres. Membership asserts
  what `:pg` sees, the epoch keeper fences what this node may act on. It
  sits above the manifest and the buffers because they consume its gate; if
  it cannot keep its lease verified the writes it would have permitted must
  fail closed, which its published-but-stale state already guarantees even
  across its own restarts.

  `ExpectedNodes` starts under the same condition (T-109) but *last*: the
  durable expected-fleet row lives in the same config store the epoch keeper
  CASes against, but unlike the epoch keeper nothing in this subtree
  consumes it — readers go through its ETS table — so under
  `rest_for_one` it must sit where its crash restarts nothing else. Query
  nodes start their own from `Smolquery.QueryService.Supervisor`; on a node
  running both roles the first one wins and the other start is `:ignore`d.
  """

  use Supervisor

  alias Smolquery.BufferService.Adopter
  alias Smolquery.BufferService.Drain
  alias Smolquery.BufferService.ExpectedNodes
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Cluster.PgGroup

  @doc """
  Starts the buffer service.

  Takes any `Smolquery.BufferService.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children = [
      membership(runtime),
      ring_epoch(runtime),
      {HotManifest, name: Runtime.manifest(runtime.name)},
      {Registry, keys: :unique, name: Runtime.registry(runtime.name)},
      {PartitionSupervisor, child_spec: DynamicSupervisor, name: Runtime.buffers(runtime.name)},
      {Adopter, runtime},
      Supervisor.child_spec(
        {Bandit,
         plug: {HotServer, runtime.name},
         ip: runtime.hot_server_ip,
         port: runtime.hot_server_port,
         startup_log: false},
        id: Runtime.hot_server(runtime.name)
      ),
      expected_nodes(runtime)
    ]

    Supervisor.init(Enum.reject(children, &is_nil/1), strategy: :rest_for_one)
  end

  defp membership(runtime) do
    unless Drain.draining?(runtime.name) do
      {PgGroup.Member, {Smolquery.BufferService, runtime.name}}
    end
  end

  defp ring_epoch(runtime) do
    if ExpectedNodes.configured?() do
      {RingEpoch, name: runtime.name, static: Ring.nodes(runtime.ring)}
    end
  end

  defp expected_nodes(runtime) do
    if ExpectedNodes.configured?() do
      {ExpectedNodes, name: runtime.name}
    end
  end
end
