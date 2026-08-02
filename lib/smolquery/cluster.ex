defmodule Smolquery.Cluster do
  @moduledoc """
  Node discovery, over the same Postgres the catalog runs on.

  A single-node dev deployment runs no distribution at all — there is nothing
  to discover, and paying for a Postgres connection just to watch an empty
  node list would be pure overhead. Discovery only starts when the deployment
  gives it something to discover through: `CATALOG_DATABASE_URL`, the
  connection also used to build the catalog's own Postgres metadata
  connection string (`Smolquery.Catalog.DuckLake`). One URL, split two ways
  at boot — there is no separately-configured discovery channel.

  ## Why Postgres, not a k8s API strategy

  `libcluster` ships a Kubernetes strategy too, but it means a `ClusterRole`
  and API server credentials the rest of this system does not otherwise
  need. `libcluster_postgres` reuses a connection this system already
  requires wherever the catalog is Postgres-backed (Milestone 8 territory by
  definition), so clustering costs nothing new to operate: the same
  connection string, the same failure domain as the catalog itself.

  ## What this does not do

  This starts Erlang distribution membership only — `Node.list/0` becomes
  real. It says nothing by itself about which node owns which table; that is
  `Smolquery.Cluster.PgGroup`, scoped per service (Milestone 8 L4), built
  on the `:pg` server this module starts. `Smolquery.Cluster.Membership`'s
  debounced events are for things that react to the fleet changing shape —
  the pg-backed ring lookups themselves need no debounce, since `:pg` is
  already a live, per-node-replicated membership table, not something to
  poll.
  """

  alias Smolquery.Cluster.Membership

  @pg_scope :smolquery

  @doc """
  This node's cluster subtree, or `[]` when clustering is not configured.
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()} | module()]
  def children do
    if enabled?() do
      [
        {Cluster.Supervisor, [topologies(), [name: __MODULE__.Supervisor]]},
        Membership,
        %{id: :pg, start: {:pg, :start_link, [@pg_scope]}}
      ]
    else
      []
    end
  end

  @doc """
  Whether this node was configured to cluster at all.
  """
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:smolquery, __MODULE__, [])[:enabled] || false

  @doc """
  The `:pg` scope every cluster-membership-derived group (per-service
  ownership rings) joins and reads from.
  """
  @spec pg_scope() :: atom()
  def pg_scope, do: @pg_scope

  @doc """
  The host part of a node's name — the address peers dial its HTTP
  services on.

  This is the contract `rel/env.sh.eex` sets up: `RELEASE_NODE` is the pod's
  headless-service DNS name, so `smolquery@pod-1.smolquery.ns.svc` answers
  `"pod-1.smolquery.ns.svc"`. `Smolquery.QueryService.Planner` (Milestone 8
  L5) and `Smolquery.StorageService.HotTier` derive `HotServer` URLs from it,
  so a clustered deployment needs no separate service-discovery call.
  """
  @spec node_host(node()) :: String.t()
  def node_host(node) do
    node |> Atom.to_string() |> String.split("@", parts: 2) |> List.last()
  end

  defp topologies do
    Application.get_env(:libcluster, :topologies) || default_topologies()
  end

  defp default_topologies do
    config = Application.fetch_env!(:smolquery, __MODULE__)

    postgres_config =
      Keyword.get(config, :postgres) ||
        raise ArgumentError,
              "Smolquery.Cluster is enabled but has no :postgres config — set CATALOG_DATABASE_URL"

    [
      postgres: [
        strategy: LibclusterPostgres.Strategy,
        config: Keyword.merge(postgres_config, channel_name: "smolquery_cluster")
      ]
    ]
  end
end
