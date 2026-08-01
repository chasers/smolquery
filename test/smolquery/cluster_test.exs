defmodule Smolquery.ClusterTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster

  setup do
    previous = Application.fetch_env(:smolquery, Cluster)
    on_exit(fn -> restore(previous) end)
  end

  test "clustering is disabled by default" do
    refute Cluster.enabled?()
    assert Cluster.children() == []
  end

  test "enabling clustering without a Postgres config raises" do
    Application.put_env(:smolquery, Cluster, enabled: true)

    assert_raise ArgumentError, fn -> Cluster.children() end
  end

  test "an enabled config with a Postgres connection returns the cluster subtree" do
    Application.put_env(:smolquery, Cluster,
      enabled: true,
      postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "smolquery"]
    )

    assert Cluster.enabled?()

    assert [{Elixir.Cluster.Supervisor, [topologies, opts]}, Smolquery.Cluster.Membership] =
             Cluster.children()

    assert opts[:name] == Smolquery.Cluster.Supervisor
    assert [postgres: config] = topologies
    assert config[:strategy] == LibclusterPostgres.Strategy
    assert config[:config][:channel_name] == "smolquery_cluster"
    assert config[:config][:hostname] == "localhost"
  end

  defp restore(:error), do: Application.delete_env(:smolquery, Cluster)
  defp restore({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
end
