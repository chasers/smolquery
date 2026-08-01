defmodule Smolquery.BufferService.ClientTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Load
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually

  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @elsewhere [:"buffer1@nonexistent.invalid", :"buffer2@nonexistent.invalid"]

  defp published_load(name) do
    [{_pid, load}] = Registry.lookup(Runtime.registry(name), @table)

    load
  end

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

    test "sheds a write whose predicted wait exceeds the ack budget, then recovers", context do
      name = start_buffer_service(context, flush_max_rows: 1, ack_budget_ms: 100)

      rows = for i <- 1..1_000, do: %{"id" => i}
      assert {:ok, _ack} = Client.write_batch(name, @table, batch(rows))

      load = published_load(name)

      for _crush <- 1..80,
          do: Load.sample_rate(load, 1_000, 1_000_000_000)

      assert {:error, {:overloaded, predicted}} =
               Client.write_batch(name, @table, batch([%{"id" => 1}]))

      assert predicted > 100

      for _recover <- 1..80, do: Load.sample_rate(load, 1_000, 1_000)

      assert {:ok, _ack} = Client.write_batch(name, @table, batch([%{"id" => 2}]))
    end

    test "ack_budget_ms: :infinity never sheds", context do
      name = start_buffer_service(context, flush_max_rows: 1, ack_budget_ms: :infinity)

      rows = for i <- 1..1_000, do: %{"id" => i}
      assert {:ok, _ack} = Client.write_batch(name, @table, batch(rows))

      load = published_load(name)

      for _crush <- 1..80,
          do: Load.sample_rate(load, 1_000, 1_000_000_000)

      assert {:ok, _ack} = Client.write_batch(name, @table, batch([%{"id" => 1}]))
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

      assert Eventually.until(fn ->
               assert Client.flush(name, @table) == :ok

               match?({:ok, {:ok, _ack}}, Task.yield(writer, 10))
             end)
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

  describe "write_batch/3 with a batch id (T-41)" do
    defp batch_with_id(rows, id) do
      %{schema: Schema.new!([{"id", :int64}]), rows: rows, batch_id: id}
    end

    defp hot_rows(name) do
      {:ok, entries} = Client.hot_manifest(name, @table)

      Enum.sum_by(entries, & &1.row_count)
    end

    test "a retry after the commit re-acks instead of writing twice", context do
      name = start_buffer_service(context)
      batch = batch_with_id([%{"id" => 1}, %{"id" => 2}], "retry-1")

      {:ok, ack} = Client.write_batch(name, @table, batch)

      assert {:ok, ^ack} = Client.write_batch(name, @table, batch)
      assert hot_rows(name) == 2
    end

    test "a duplicate racing the same group commit lands once", context do
      name = start_buffer_service(context, flush_interval_ms: 150)
      batch = batch_with_id([%{"id" => 1}], "race-1")

      first = Task.async(fn -> Client.write_batch(name, @table, batch) end)
      second = Task.async(fn -> Client.write_batch(name, @table, batch) end)

      assert {:ok, ack} = Task.await(first)
      assert {:ok, ^ack} = Task.await(second)
      assert hot_rows(name) == 1
    end

    test "re-acked retries do not skew the outstanding-load estimate", context do
      name = start_buffer_service(context)
      batch = batch_with_id([%{"id" => 1}], "load-1")

      {:ok, _first} = Client.write_batch(name, @table, batch)
      {:ok, _second} = Client.write_batch(name, @table, batch)
      {:ok, _third} = Client.write_batch(name, @table, batch)

      load = published_load(name)

      assert Eventually.until(fn ->
               Load.predicted_wait_ms(load, 0) in [0, :unknown]
             end)
    end

    test "distinct ids still write separately", context do
      name = start_buffer_service(context)

      {:ok, _a} = Client.write_batch(name, @table, batch_with_id([%{"id" => 1}], "a"))
      {:ok, _b} = Client.write_batch(name, @table, batch_with_id([%{"id" => 2}], "b"))

      assert hot_rows(name) == 2
    end

    test "the dedup window survives a buffer restart", context do
      name = start_buffer_service(context)
      batch = batch_with_id([%{"id" => 1}], "reborn-1")
      {:ok, ack} = Client.write_batch(name, @table, batch)

      stop_supervised!(name)
      restarted = start_buffer_service(context, name: name)

      assert {:ok, ^ack} = Client.write_batch(restarted, @table, batch)
      assert hot_rows(restarted) == 1
    end
  end
end
