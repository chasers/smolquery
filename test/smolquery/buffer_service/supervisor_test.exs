defmodule Smolquery.BufferService.SupervisorTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.MemoryStore

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp start_buffer_service(context, opts \\ []) do
    opts =
      Keyword.merge(
        [
          name: :"buffer_#{:erlang.unique_integer([:positive])}",
          dir: Path.join(context.tmp_dir, "buffer"),
          flush_interval_ms: 25
        ],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp write(name, table_ref \\ @table) do
    Client.write_batch(name, table_ref, %{
      schema: Schema.new!([{"id", :int64}]),
      rows: [%{"id" => 1}]
    })
  end

  test "starts the manifest, the registry, and the buffer partitions", context do
    name = start_buffer_service(context)

    assert is_pid(Process.whereis(Runtime.manifest(name)))
    assert is_pid(Process.whereis(Runtime.registry(name)))
    assert is_pid(Process.whereis(Runtime.buffers(name)))
  end

  test "publishes a runtime the client can find", context do
    name = start_buffer_service(context)

    assert {:ok, runtime} = Runtime.fetch(name)
    assert runtime.name == name
    assert runtime.flush_interval_ms == 25
  end

  test "resolves the segment store and the log directory from one root", context do
    name = start_buffer_service(context)
    {:ok, runtime} = Runtime.fetch(name)

    root = Path.join(context.tmp_dir, "buffer")

    assert runtime.store.config.dir == Path.join(root, "segments")
    assert runtime.manifest.log_dir == Path.join(root, "manifests")
  end

  test "takes a store other than the default local one", context do
    store = MemoryStore.new()
    name = start_buffer_service(context, store: store)

    {:ok, runtime} = Runtime.fetch(name)

    assert runtime.store == store
    assert {:ok, _ack} = write(name)
  end

  test "rebuilds the registry and buffers when the manifest dies", context do
    name = start_buffer_service(context)

    {:ok, _ack} = write(name)

    [{buffer, _value}] = Registry.lookup(Runtime.registry(name), @table)
    registry = Process.whereis(Runtime.registry(name))

    ref = Process.monitor(buffer)
    Process.exit(Process.whereis(Runtime.manifest(name)), :kill)

    assert_receive {:DOWN, ^ref, :process, ^buffer, _reason}

    assert Eventually.until(fn ->
             Process.whereis(Runtime.registry(name)) not in [nil, registry]
           end)

    assert {:ok, _ack} = write(name)
  end

  test "a buffer crashing leaves the rest of the subtree alone", context do
    name = start_buffer_service(context)

    {:ok, _ack} = write(name)

    manifest = Process.whereis(Runtime.manifest(name))
    registry = Process.whereis(Runtime.registry(name))

    [{buffer, _value}] = Registry.lookup(Runtime.registry(name), @table)
    ref = Process.monitor(buffer)
    Process.exit(buffer, :kill)

    assert_receive {:DOWN, ^ref, :process, ^buffer, _reason}

    assert Process.whereis(Runtime.manifest(name)) == manifest
    assert Process.whereis(Runtime.registry(name)) == registry
    assert {:ok, _ack} = write(name)
  end

  test "keeps entries across a buffer crash, because the manifest outlives it", context do
    name = start_buffer_service(context)

    {:ok, ack} = write(name)
    {:ok, runtime} = Runtime.fetch(name)

    [{buffer, _value}] = Registry.lookup(Runtime.registry(name), @table)
    Process.exit(buffer, :kill)

    assert Eventually.until(fn ->
             Enum.map(HotManifest.entries(runtime.manifest, @table), & &1.id) == [ack.segment_id]
           end)
  end

  test "runs beside another instance without collision", context do
    one = start_buffer_service(context, dir: Path.join(context.tmp_dir, "one"))
    two = start_buffer_service(context, dir: Path.join(context.tmp_dir, "two"))

    {:ok, first} = write(one)
    {:ok, second} = write(two)

    {:ok, first_entries} = Client.hot_manifest(one, @table)
    {:ok, second_entries} = Client.hot_manifest(two, @table)

    assert Enum.map(first_entries, & &1.id) == [first.segment_id]
    assert Enum.map(second_entries, & &1.id) == [second.segment_id]
  end
end
