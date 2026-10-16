defmodule Smolquery.ApplicationTest do
  use ExUnit.Case, async: false

  alias Smolquery.Application, as: App

  setup do
    on_exit(fn -> System.delete_env("FLAME_PARENT") end)
  end

  describe "children/0" do
    test "a node starts the cluster, the lifecycle and its role subtrees" do
      children = App.children()

      assert Smolquery.Lifecycle in children
      assert Smolquery.Catalog.Migrator in children
      assert {Phoenix.PubSub, name: Smolquery.PubSub} in children
    end

    test "a burst runner starts the pool and nothing a node does" do
      parent =
        make_ref()
        |> FLAME.Parent.new(self(), FLAME.LocalBackend, "runner", "SMOLQUERY_BURST_HOST")
        |> FLAME.Parent.encode()

      System.put_env("FLAME_PARENT", parent)

      children = App.children()

      assert [{FLAME.Pool, pool}] = Enum.filter(children, &match?({FLAME.Pool, _}, &1))
      assert pool[:name] == Smolquery.Burst.pool()
      assert Smolquery.Engine.ExtensionLoader in children
      refute Smolquery.Lifecycle in children
      refute Smolquery.Catalog.Migrator in children
      refute Smolquery.Cluster.RingCache in children
      refute Enum.any?(children, &match?({Phoenix.PubSub, _}, &1))
      refute SmolqueryApi.Supervisor in children
    end
  end
end
