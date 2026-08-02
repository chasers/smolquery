defmodule Smolquery.StorageService.RoutingTest do
  use ExUnit.Case, async: true

  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp unique_name, do: :"storage_routing_#{:erlang.unique_integer([:positive])}"

  describe "resolve/1" do
    test "prefers the published runtime's ring", %{tmp_dir: dir} do
      name = unique_name()

      runtime = Runtime.new(name: name, dir: dir, ring: [:"storage1@elsewhere.invalid"])

      :ok = Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(name) end)

      routing = Routing.resolve(name)

      assert routing.ring == runtime.ring
      assert Routing.owner(routing, @table) == :"storage1@elsewhere.invalid"
    end

    test "builds from configuration on a node with no runtime, and caches it" do
      name = unique_name()
      on_exit(fn -> Routing.forget(name) end)

      routing = Routing.resolve(name)

      assert Routing.owner(routing, @table) == node()
      assert Routing.own?(routing, @table)
      assert Routing.resolve(name) == routing
    end
  end

  describe "own?/2" do
    test "false for a table this node's ring hands to someone else", %{tmp_dir: dir} do
      name = unique_name()

      runtime =
        Runtime.new(
          name: name,
          dir: dir,
          ring: [node(), :"storage1@elsewhere.invalid"]
        )

      :ok = Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(name) end)

      routing = Routing.resolve(name)
      foreign_table = find_table_owned_by(routing, :"storage1@elsewhere.invalid")

      refute Routing.own?(routing, foreign_table)
    end
  end

  describe "forget/1" do
    test "drops the cached routing so the next resolve rebuilds" do
      name = unique_name()

      _routing = Routing.resolve(name)

      assert Routing.forget(name)
      refute Routing.forget(name)
    end
  end

  defp find_table_owned_by(routing, target_node) do
    Enum.find(1..1_000, fn i ->
      Routing.owner(routing, {"analytics", "t#{i}"}) == target_node
    end)
    |> then(&{"analytics", "t#{&1}"})
  end
end
