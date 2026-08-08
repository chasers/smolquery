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
        buffer: [flush_max_rows: 100_000, flush_interval_ms: 60_000, max_buffered_rows: 1]
      )

    assert IngestService.Client.insert(name, @table, [%{"id" => 1}, %{"id" => 2}]) ==
             {:error, :buffer_full}
  end

  test "an ingest service that is not running says so" do
    assert IngestService.Client.insert(:never_started, @table, [%{"id" => 1}]) ==
             {:error, :ingest_service_unavailable}
  end

  describe "write partitions (T-170)" do
    test "id-less batches spread over the table's partition refs", context do
      %{name: name, buffer: buffer} = start_stack(context, ingest: [write_partitions: 2])

      for i <- 1..8 do
        assert {:ok, %{inserted: 1, errors: []}} =
                 IngestService.Client.insert(name, @table, [%{"id" => i}])
      end

      counts =
        for ref <- Smolquery.Partitions.refs(@table, 2) do
          {:ok, entries} = BufferService.Client.hot_manifest(buffer, ref)
          Enum.sum(Enum.map(entries, & &1.row_count))
        end

      assert Enum.sum(counts) == 8
      assert Enum.all?(counts, &(&1 > 0))
    end

    test "a retried insertId dedups against the partition that holds it", context do
      %{name: name, buffer: buffer} = start_stack(context, ingest: [write_partitions: 4])

      rows = [%{"id" => 1}, %{"id" => 2}]

      assert {:ok, %{inserted: 2}} =
               IngestService.Client.insert(name, @table, rows, batch_id: "retry-1")

      assert {:ok, %{inserted: 2}} =
               IngestService.Client.insert(name, @table, rows, batch_id: "retry-1")

      total =
        Smolquery.Partitions.refs(@table, 4)
        |> Enum.flat_map(fn ref ->
          {:ok, entries} = BufferService.Client.hot_manifest(buffer, ref)
          entries
        end)
        |> Enum.sum_by(& &1.row_count)

      assert total == 2
    end
  end

  describe "batch_id (T-41)" do
    test "an insert retried with the same batch id counts its rows once", context do
      %{name: name, buffer: buffer} = start_stack(context)
      rows = [%{"id" => 1}, %{"id" => 2}]

      assert {:ok, %{inserted: 2, errors: []}} =
               IngestService.Client.insert(name, @table, rows, batch_id: "insert-1")

      assert {:ok, %{inserted: 2, errors: []}} =
               IngestService.Client.insert(name, @table, rows, batch_id: "insert-1")

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.sum_by(entries, & &1.row_count) == 2
    end
  end
end
