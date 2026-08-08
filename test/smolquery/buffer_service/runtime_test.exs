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
    end

    test "defaults to fsync on the store and the manifest", %{tmp_dir: dir} do
      runtime = Runtime.new(name: unique_name(), dir: dir)

      assert runtime.manifest.fsync == true
      assert runtime.store.config.fsync == true
    end

    test "passes :fsync through to the default store and the manifest", %{tmp_dir: dir} do
      runtime = Runtime.new(name: unique_name(), dir: dir, fsync: false)

      assert runtime.manifest.fsync == false
      assert runtime.store.config.fsync == false
    end

    test "defaults :encode_concurrency to one encode at a time", %{tmp_dir: dir} do
      assert Runtime.new(name: unique_name(), dir: dir).encode_concurrency == 1
    end

    test "takes :encode_concurrency from the config", %{tmp_dir: dir} do
      runtime = Runtime.new(name: unique_name(), dir: dir, encode_concurrency: 4)

      assert runtime.encode_concurrency == 4
    end

    test "defaults the heap flags to full sweeps and no heap floor", %{tmp_dir: dir} do
      runtime = Runtime.new(name: unique_name(), dir: dir)

      assert runtime.buffer_fullsweep_after == 10
      assert runtime.committer_fullsweep_after == 0
      assert runtime.buffer_min_heap_size == nil
      assert runtime.committer_min_heap_size == nil
    end

    test "takes the heap flags from the config", %{tmp_dir: dir} do
      runtime =
        Runtime.new(
          name: unique_name(),
          dir: dir,
          buffer_fullsweep_after: 5,
          buffer_min_heap_size: 8_192,
          committer_fullsweep_after: nil,
          committer_min_heap_size: 16_384
        )

      assert runtime.buffer_fullsweep_after == 5
      assert runtime.buffer_min_heap_size == 8_192
      assert runtime.committer_fullsweep_after == nil
      assert runtime.committer_min_heap_size == 16_384
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
