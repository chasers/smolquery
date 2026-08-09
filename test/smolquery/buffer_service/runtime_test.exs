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

    test "refuses an unusable write pool size at boot, not at first flush", %{tmp_dir: dir} do
      for size <- [0, -1, 33, "4"] do
        assert_raise ArgumentError, ~r/unusable write pool size/, fn ->
          Runtime.new(name: unique_name(), dir: dir, write_pool_size: size)
        end
      end

      assert Runtime.new(name: unique_name(), dir: dir, write_pool_size: 32).write_pool_size ==
               32
    end
  end

  describe "engines/1" do
    test "a pool of one names one engine, not a descending range", %{tmp_dir: dir} do
      name = unique_name()
      runtime = Runtime.new(name: name, dir: dir, write_pool_size: 1)

      assert Runtime.engines(runtime) == [Runtime.engine(name, 0)]
    end

    test "names every member in index order", %{tmp_dir: dir} do
      name = unique_name()
      runtime = Runtime.new(name: name, dir: dir, write_pool_size: 3)

      assert Runtime.engines(runtime) == Enum.map(0..2, &Runtime.engine(name, &1))
    end
  end

  describe "write_engine_budget/1" do
    test "divides the engine's configured thread count by the pool size", %{tmp_dir: dir} do
      configured =
        :smolquery
        |> Application.get_env(Smolquery.Engine, [])
        |> Keyword.get(:threads, System.schedulers_online())

      runtime = Runtime.new(name: unique_name(), dir: dir, write_pool_size: 2)

      assert Runtime.write_engine_budget(runtime) == [threads: max(div(configured, 2), 1)]
    end

    test "floors at one thread once the pool outgrows the thread count", %{tmp_dir: dir} do
      runtime = Runtime.new(name: unique_name(), dir: dir, write_pool_size: 32)

      assert Runtime.write_engine_budget(runtime)[:threads] == 1
    end

    test "an explicit thread count replaces the division", %{tmp_dir: dir} do
      runtime =
        Runtime.new(
          name: unique_name(),
          dir: dir,
          write_pool_size: 16,
          write_engine_threads: 4
        )

      assert Runtime.write_engine_budget(runtime) == [threads: 4]
    end

    test "a memory limit joins the budget only when configured", %{tmp_dir: dir} do
      unlimited = Runtime.new(name: unique_name(), dir: dir, write_pool_size: 2)
      refute Keyword.has_key?(Runtime.write_engine_budget(unlimited), :memory_limit)

      limited =
        Runtime.new(
          name: unique_name(),
          dir: dir,
          write_pool_size: 2,
          write_engine_memory_limit: "512MB"
        )

      assert Runtime.write_engine_budget(limited)[:memory_limit] == "512MB"
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
