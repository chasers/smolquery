defmodule Smolquery.IngestService.ClientTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.IngestService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Test.MapCatalog

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
  end

  defp start_stack(context, opts \\ []) do
    buffer = :"ingest_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       Keyword.merge(
         [name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1],
         Keyword.get(opts, :buffer, [])
       )},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    name = :"ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor,
       Keyword.merge(
         [name: name, catalog: catalog, buffer_name: buffer],
         Keyword.get(opts, :ingest, [])
       )},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, buffer: buffer}
  end

  test "acked rows are durable and queryable in the hot tier", context do
    %{name: name, buffer: buffer} = start_stack(context)

    assert {:ok, %{inserted: 2, errors: []}} =
             IngestService.Client.insert(name, @table, [
               %{"id" => 1, "ts" => "2026-08-01T10:00:00"},
               %{"id" => 2}
             ])

    {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)

    assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
  end

  test "valid rows land even when neighbors are rejected", context do
    %{name: name, buffer: buffer} = start_stack(context)

    assert {:ok, %{inserted: 1, errors: [%{index: 1}]}} =
             IngestService.Client.insert(name, @table, [
               %{"id" => 1},
               %{"id" => "junk"}
             ])

    {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)

    assert Enum.sum(Enum.map(entries, & &1.row_count)) == 1
  end

  test "a batch with no valid rows reports without touching the buffer", context do
    %{name: name, buffer: buffer} = start_stack(context)

    assert {:ok, %{inserted: 0, errors: [%{index: 0}]}} =
             IngestService.Client.insert(name, @table, [%{"id" => nil}])

    {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)

    assert entries == []
  end

  test "an unknown table is the catalog's error, uncached", context do
    %{name: name} = start_stack(context)

    assert IngestService.Client.insert(name, {"analytics", "nope"}, [%{"id" => 1}]) ==
             {:error, {:unknown_table, {"analytics", "nope"}}}
  end

  test "a full buffer refuses the whole batch", context do
    %{name: name} =
      start_stack(context,
        buffer: [flush_max_rows: 100_000, flush_interval_ms: 60_000, max_buffered_rows: 2]
      )

    blocked =
      Task.async(fn ->
        IngestService.Client.insert(name, @table, [%{"id" => 1}, %{"id" => 2}])
      end)

    Process.sleep(50)

    assert IngestService.Client.insert(name, @table, [%{"id" => 3}]) == {:error, :buffer_full}

    Task.shutdown(blocked, :brutal_kill)
  end

  test "an ingest service that is not running says so" do
    assert IngestService.Client.insert(:never_started, @table, [%{"id" => 1}]) ==
             {:error, :ingest_service_unavailable}
  end
end
