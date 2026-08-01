defmodule Smolquery.BufferService.ClientTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @elsewhere [:"buffer1@nonexistent.invalid", :"buffer2@nonexistent.invalid"]

  defp batch(rows \\ [%{"id" => 1}]) do
    %{schema: Schema.new!([{"id", :int64}]), rows: rows}
  end

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

  describe "write_batch/3" do
    test "returns the segment the rows landed in", context do
      name = start_buffer_service(context)

      assert {:ok, ack} = Client.write_batch(name, @table, batch([%{"id" => 1}, %{"id" => 2}]))
      assert ack.row_count == 2
      assert is_binary(ack.segment_id)
    end

    test "routes a table this node does not own to its owner", context do
      name = start_buffer_service(context, ring: @elsewhere)

      assert {:error, {kind, _reason}} = Client.write_batch(name, @table, batch())
      assert kind in [:badrpc, :badtcp]
      assert Client.owner(name, @table) in @elsewhere
    end

    test "reports a node that does not run the buffer role" do
      assert Client.write_batch(:never_started, @table, batch()) ==
               {:error, :buffer_service_unavailable}
    end

    test "reports a buffer service whose manifest has stopped", context do
      name = start_buffer_service(context)

      stop_supervised!(name)

      assert Client.write_batch(name, @table, batch()) ==
               {:error, :buffer_service_unavailable}
    end

    test "refuses a table name that is not an identifier", context do
      name = start_buffer_service(context)

      assert {:error, {:invalid_identifier, _name}} =
               Client.write_batch(name, {"../etc", "events"}, batch())
    end
  end

  describe "hot_manifest/2" do
    test "is empty for a table nothing has been written to", context do
      name = start_buffer_service(context)

      assert Client.hot_manifest(name, @table) == {:ok, []}
    end

    test "reports the entries a write produced, with stats", context do
      name = start_buffer_service(context)

      {:ok, ack} = Client.write_batch(name, @table, batch([%{"id" => 1}, %{"id" => 7}]))

      assert {:ok, [entry]} = Client.hot_manifest(name, @table)
      assert entry.id == ack.segment_id
      assert entry.stats["id"] == %{min: 1, max: 7, null_count: 0}
      assert entry.sealed_at == nil
    end

    test "reports a node that does not run the buffer role" do
      assert Client.hot_manifest(:never_started, @table) ==
               {:error, :buffer_service_unavailable}
    end
  end

  describe "owner/2" do
    test "is this node in a single-node ring", context do
      name = start_buffer_service(context)

      assert Client.owner(name, @table) == node()
    end

    test "is a remote node when the ring says so", context do
      name = start_buffer_service(context, ring: @elsewhere)

      assert Client.owner(name, @table) in @elsewhere
    end
  end

  describe "flush/2" do
    test "makes the tail durable without waiting out the interval", context do
      name = start_buffer_service(context, flush_interval_ms: 60_000)

      writer = Task.async(fn -> Client.write_batch(name, @table, batch()) end)
      Process.sleep(50)

      assert Client.flush(name, @table) == :ok
      assert {:ok, _ack} = Task.await(writer)
    end

    test "is harmless on a table with nothing buffered", context do
      name = start_buffer_service(context)

      assert Client.flush(name, @table) == :ok
    end

    test "does not start a buffer just to flush nothing", context do
      name = start_buffer_service(context)

      assert Client.flush(name, @table) == :ok
      assert Registry.lookup(Runtime.registry(name), @table) == []
    end
  end
end
