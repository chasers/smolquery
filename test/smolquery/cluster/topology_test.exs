defmodule Smolquery.Cluster.TopologyTest do
  use ExUnit.Case, async: true

  alias Smolquery.Cluster.Pods
  alias Smolquery.Cluster.Topology

  describe "fleet/0 without clustering" do
    test "answers a single row for this node, unclaimed and not draining" do
      assert [row] = Topology.fleet()

      assert row.node == node()
      assert row.alive
      refute row.buffer_member
      refute row.storage_member
      refute row.expected_buffer
      assert row.buffer_epoch == nil
      refute row.draining
    end

    test "the pod is derived from the node name" do
      assert [row] = Topology.fleet()

      assert row.pod == Pods.pod_of_node(node())
    end
  end
end
