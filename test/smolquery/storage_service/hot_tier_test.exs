defmodule Smolquery.StorageService.HotTierTest do
  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Schema
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Runtime

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  test "reads the owning node's manifest with the storage runtime's answers", context do
    name = :"hot_tier_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: name, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: name
    )

    on_exit(fn -> BufferService.Runtime.delete(name) end)

    {:ok, ack} =
      Client.write_batch(name, @table, %{
        schema: Schema.new!([{"id", :int64}]),
        rows: [%{"id" => 1}]
      })

    runtime =
      Runtime.new(
        name: :"hot_tier_storage_#{:erlang.unique_integer([:positive])}",
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_base_url: HotServer.base_url(name)
      )

    assert {:ok, [entry]} = HotTier.manifest(runtime, @table)
    assert entry["id"] == ack.segment_id
  end
end
