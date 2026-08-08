defmodule Smolquery.BufferService.RuntimeTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Segments.Store

  @moduletag :tmp_dir

  defp unique_name, do: :"runtime_#{:erlang.unique_integer([:positive])}"

  describe "new/1" do
    test "resolves options into handles rooted under :dir", %{tmp_dir: dir} do
      name = unique_name()
      runtime = Runtime.new(name: name, dir: dir)

      assert runtime.name == name
      assert %Store{impl: Store.Local} = runtime.store
      assert runtime.store.config.dir == Path.join(dir, "segments")
      assert runtime.manifest.log_dir == Path.join(dir, "manifests")
      assert Ring.owner(runtime.ring, {"analytics", "events"}) == node()
    end

    test "opts override the limits, and the defaults hold otherwise", %{tmp_dir: dir} do
      runtime =
        Runtime.new(
          name: unique_name(),
          dir: dir,
          write_timeout_ms: 456,
          control_timeout_ms: 123
        )

      assert runtime.write_timeout_ms == 456
      assert runtime.control_timeout_ms == 123
      assert runtime.flush_interval_ms == 1_000
      assert runtime.compression == :zstd
    end

    test "refuses an unsupported compression codec at boot, not per group commit", %{
      tmp_dir: dir
    } do
      assert_raise ArgumentError, ~r/unsupported hot-tier compression/, fn ->
        Runtime.new(name: unique_name(), dir: dir, compression: :lz4)
      end
    end
  end

  describe "put/1, fetch/1, delete/1" do
    test "publishes, reads back, and withdraws", %{tmp_dir: dir} do
      name = unique_name()
      runtime = Runtime.new(name: name, dir: dir)

      assert Runtime.fetch(name) == :error
      assert Runtime.put(runtime) == :ok
      assert Runtime.fetch(name) == {:ok, runtime}
      assert Runtime.delete(name)
      assert Runtime.fetch(name) == :error
    end
  end

  describe "naming" do
    test "derives every process name from the instance name" do
      assert Runtime.supervisor(MyBuffer) == MyBuffer.Supervisor
      assert Runtime.registry(MyBuffer) == MyBuffer.Registry
      assert Runtime.manifest(MyBuffer) == MyBuffer.HotManifest
      assert Runtime.buffers(MyBuffer) == MyBuffer.Buffers
      assert Runtime.hot_server(MyBuffer) == MyBuffer.HotServer
    end

    test "via/2 registers a buffer under its table", %{tmp_dir: dir} do
      name = unique_name()
      runtime = Runtime.new(name: name, dir: dir)

      assert Runtime.via(runtime, {"analytics", "events"}) ==
               {:via, Registry, {Runtime.registry(name), {"analytics", "events"}}}
    end
  end
end
