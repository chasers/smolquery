defmodule Smolquery.BufferService.RoutingTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Routing
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.Transport

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp unique_name, do: :"routing_#{:erlang.unique_integer([:positive])}"

  describe "resolve/1" do
    test "prefers the published runtime and honors its timeouts", %{tmp_dir: dir} do
      name = unique_name()

      runtime =
        Runtime.new(name: name, dir: dir, write_timeout_ms: 456, control_timeout_ms: 123)

      :ok = Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(name) end)

      routing = Routing.resolve(name)

      assert routing.write_timeout_ms == 456
      assert routing.control_timeout_ms == 123
      assert routing.ring == runtime.ring
    end

    test "builds from configuration on a node with no runtime, and caches it" do
      name = unique_name()
      on_exit(fn -> Routing.forget(name) end)

      routing = Routing.resolve(name)

      assert Routing.owner(routing, @table) == node()
      assert Routing.resolve(name) == routing
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

  describe "transport/2" do
    test "an owned table is local, everything else goes remote" do
      name = unique_name()
      on_exit(fn -> Routing.forget(name) end)

      routing = Routing.resolve(name)

      assert Routing.transport(routing, node()) == Transport.Local
      assert Routing.transport(routing, :"buffer1@elsewhere.invalid") == Transport.GenRpc
    end
  end
end
