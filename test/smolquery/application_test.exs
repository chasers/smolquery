defmodule Smolquery.ApplicationTest do
  use ExUnit.Case, async: true

  alias Smolquery.Application, as: App

  describe "children/1" do
    test "a node starts the cluster, the lifecycle and its role subtrees" do
      children = App.children(false)

      assert Smolquery.Lifecycle in children
      assert Smolquery.Catalog.Migrator in children
      assert {Phoenix.PubSub, name: Smolquery.PubSub} in children
    end

    test "a burst runner starts an engine's prerequisites and nothing a node does" do
      children = App.children(true)

      assert Smolquery.Engine.ExtensionLoader in children
      assert Smolquery.Telemetry in children
      refute Smolquery.Lifecycle in children
      refute Smolquery.Catalog.Migrator in children
      refute Smolquery.Cluster.RingCache in children
      refute Enum.any?(children, &match?({Phoenix.PubSub, _}, &1))
      refute Enum.any?(children, &match?({FLAME.Pool, _}, &1))
      refute SmolqueryApi.Supervisor in children
    end
  end
end
