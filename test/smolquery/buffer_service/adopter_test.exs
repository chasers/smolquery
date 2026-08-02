defmodule Smolquery.BufferService.AdopterTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Adopter
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Test.SealCollector

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @other {"analytics", "clicks"}

  defp batch do
    %{schema: Schema.new!([{"id", :int64}]), rows: [%{"id" => 1}]}
  end

  defp options(context, opts) do
    Keyword.merge(
      [
        name: :"buffer_#{:erlang.unique_integer([:positive])}",
        dir: Path.join(context.tmp_dir, "buffer"),
        flush_interval_ms: 25,
        maintenance_interval_ms: 20,
        seal_consumer: {SealCollector, self()},
        seal_max_files: 1_000_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 60_000
      ],
      opts
    )
  end

  defp start_buffer_service(context, opts \\ []) do
    opts = options(context, opts)
    name = Keyword.fetch!(opts, :name)

    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  test "starts buffers on boot for tables that already have a manifest log", context do
    name = start_buffer_service(context)

    {:ok, _one} = Client.write_batch(name, @table, batch())
    {:ok, _two} = Client.write_batch(name, @other, batch())

    stop_supervised!(name)

    restarted = start_buffer_service(context, name: name)

    assert Registry.lookup(Runtime.registry(restarted), @table) != []
    assert Registry.lookup(Runtime.registry(restarted), @other) != []
  end

  test "an adopted table seals its stranded tail without any further write", context do
    name = start_buffer_service(context)

    {:ok, ack} = Client.write_batch(name, @table, batch())

    stop_supervised!(name)

    _restarted = start_buffer_service(context, name: name, seal_max_age_ms: 1)

    assert_receive {:seal_ready, @table, %{ids: [id]}}, 1_000
    assert id == ack.segment_id
  end

  test "adopts nothing on a node with no logs", context do
    name = start_buffer_service(context)

    assert Registry.lookup(Runtime.registry(name), @table) == []
  end

  test "adopts a table the ring no longer assigns here — the bytes are local", context do
    name = start_buffer_service(context)

    {:ok, _ack} = Client.write_batch(name, @table, batch())

    stop_supervised!(name)

    restarted = start_buffer_service(context, name: name, ring: [:buffer1@host])

    assert Registry.lookup(Runtime.registry(restarted), @table) != []
  end

  test "reports the tables it adopted", context do
    name = start_buffer_service(context)

    {:ok, _ack} = Client.write_batch(name, @table, batch())

    {:ok, runtime} = Runtime.fetch(name)

    assert Adopter.adopt(runtime) == [@table]
  end

  test "boot sweeps staged files a killed encoder left behind", context do
    staging = Path.join([context.tmp_dir, "buffer", "segments", ".tmp"])
    File.mkdir_p!(staging)
    File.write!(Path.join(staging, "leaked.parquet.42"), "half-encoded")

    start_buffer_service(context)

    refute File.exists?(Path.join(staging, "leaked.parquet.42"))
  end
end
