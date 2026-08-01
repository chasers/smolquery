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
  real. It says nothing about which node owns which table
  (`Smolquery.BufferService.Ring`) or which StorageService worker claims a
  seal; those consume `Smolquery.Cluster.Membership`'s events but are wired
  in later Milestone 8 layers.
  """

  alias Smolquery.Cluster.Membership

  @doc """
  This node's cluster subtree, or `[]` when clustering is not configured.
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()} | module()]
  def children do
    if enabled?() do
      [{Cluster.Supervisor, [topologies(), [name: __MODULE__.Supervisor]]}, Membership]
    else
      []
    end
  end

  @doc """
  Whether this node was configured to cluster at all.
  """
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:smolquery, __MODULE__, [])[:enabled] || false

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
