defmodule Smolquery.Application do
  @moduledoc """
  Boots the service subtrees this node's roles enable.

  Each role contributes an independent top-level subtree, so a service's crash
  domain is its own and a node runs only what it was given. `SMOLQUERY_ROLES`
  selects them; see `Smolquery.Roles`.

  Order within a node does not encode a dependency: services reach each other
  through client modules that tolerate a peer being absent, so a node starting
  `:storage` before `:buffer` is not a problem for either.

  `Smolquery.PubSub` and `Smolquery.Lifecycle` run on every node regardless
  of role: lifecycle events originate wherever the work runs (a buffer's
  commit, a storage node's seal or compaction), and Phoenix.PubSub's pg
  adapter carries a broadcast across the cluster only between nodes that
  run the same-named pubsub. A web-only pubsub would hear nothing (T-295).
  `Smolquery.MetricsServer` is role-independent for the same reason: the
  counters live where the work runs, so every node must be scrapable (T-302).

  `Smolquery.Catalog.Migrator` prepares a Postgres metadata database before
  the cluster and every service start, and the one ordering this module
  does encode: they read the tables it makes (T-609).

  A VM that a FLAME backend booted is a burst runner (`Smolquery.Burst`,
  PL-77). It starts telemetry, the extension loader and the engine log
  supervisor, and nothing else: no cluster, no role, no catalog, no
  listener, no pool (`FLAME.Application` starts the Terminator that takes
  the parent's work). `Smolquery.InternalSecret.ensure/0` is not called
  either; a runner must not hold the secret, and a hot-tier read never
  reaches it.
  """

  use Application

  alias Smolquery.Roles

  @impl true
  def start(_type, _args) do
    runner? = Smolquery.Burst.runner?()
    unless runner?, do: Smolquery.InternalSecret.ensure()

    Supervisor.start_link(children(runner?), strategy: :one_for_one, name: Smolquery.Supervisor)
  end

  @doc """
  What this VM starts: a burst runner's three children when a FLAME backend
  booted it (`Smolquery.Burst.runner?/0`), the node's full tree otherwise.
  """
  @spec children(boolean()) :: [Supervisor.child_spec() | {module(), term()} | module()]
  def children(true) do
    [
      Smolquery.Telemetry,
      Smolquery.Engine.ExtensionLoader,
      {DynamicSupervisor, name: Smolquery.Engine.LogSupervisor, strategy: :one_for_one}
    ]
  end

  def children(false) do
    [
      Smolquery.Telemetry,
      Smolquery.Engine.ExtensionLoader,
      Smolquery.Tracing,
      Smolquery.MetricsServer,
      Smolquery.MemoryMetrics,
      Smolquery.Allocator,
      {DynamicSupervisor, name: Smolquery.Engine.LogSupervisor, strategy: :one_for_one},
      {Phoenix.PubSub, name: Smolquery.PubSub},
      Smolquery.Lifecycle,
      Smolquery.Catalog.Migrator,
      Smolquery.Cluster.RingCache
    ] ++
      Smolquery.Burst.children() ++
      Smolquery.Cluster.children() ++ Enum.flat_map(Roles.enabled(), &subtree/1)
  end

  defp subtree(:api), do: [SmolqueryApi.Supervisor]
  defp subtree(:web), do: [SmolqueryWeb.Supervisor]
  defp subtree(:pg), do: [SmolqueryPg.Supervisor]
  defp subtree(:clickhouse), do: [SmolqueryClickHouse.Supervisor]
  defp subtree(:victoriametrics), do: [SmolqueryVictoriaMetrics.Supervisor]
  defp subtree(:query), do: [Smolquery.QueryService.Supervisor]
  defp subtree(:buffer), do: [Smolquery.BufferService.Supervisor]
  defp subtree(:storage), do: [Smolquery.StorageService.Supervisor]
  defp subtree(:ingest), do: [Smolquery.IngestService.Supervisor]
end
