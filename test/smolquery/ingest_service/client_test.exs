defmodule Smolquery.IngestService.ClientTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.IngestService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
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

    catalog = Keyword.get_lazy(opts, :catalog, &MapCatalog.new/0)
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

    %{name: name, buffer: buffer, catalog: catalog}
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

  describe "catalog partition count (T-304)" do
    test "a table's catalog count spreads writes past the deployment default", context do
      %{name: name, buffer: buffer, catalog: catalog} = start_stack(context)

      :ok = Catalog.put_partitions(catalog, @table, 2)

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
  end

  describe "a row the validator takes and the flush refuses" do
    test "is reported at its index in the caller's body, beside the validator's own errors",
         context do
      %{name: name, buffer: buffer} = start_stack(context, buffer: [write_pool_size: 1])

      rows = [
        %{"id" => "not-an-int"},
        %{"id" => 1},
        %{"id" => 9_223_372_036_854_775_808},
        %{"id" => 2}
      ]

      assert {:ok, %{inserted: 2, errors: [first, third]}} =
               IngestService.Client.insert(name, @table, rows)

      assert first.index == 0
      assert third.index == 2
      assert hd(third.errors).message =~ "the flush refused the row"

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
    end
  end

  describe "a schema cache that missed a DROP and ADD of the same name (T-439)" do
    test "the buffer refuses the stale ids, the cache is dropped, and the write lands under the new column",
         context do
      catalog = MapCatalog.new()
      :ok = Catalog.create_dataset(catalog, "analytics")
      :ok = Catalog.create_table(catalog, @table, schema())

      %{name: name, buffer: buffer} =
        start_stack(context,
          buffer: [catalog: catalog, write_pool_size: 1],
          ingest: [catalog: catalog, schema_cache_ttl_ms: 600_000],
          catalog: catalog
        )

      assert {:ok, %{inserted: 1}} =
               IngestService.Client.insert(name, @table, [
                 %{"id" => 1, "ts" => "2026-08-01T10:00:00"}
               ])

      :ok = MapCatalog.alter_table(catalog.config, @table, {:drop_column, "ts"})

      :ok =
        MapCatalog.alter_table(catalog.config, @table, {:add_column, Field.new!("ts", :string)})

      assert {:ok, %{inserted: 1, errors: []}} =
               IngestService.Client.insert(name, @table, [
                 %{"id" => 2, "ts" => "2026-08-01T10:00:00"}
               ])

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.map(entries, & &1.field_ids["ts"]) |> Enum.sort() == [2, 3]

      assert {:ok, %{inserted: 0, errors: [%{errors: [%{message: message}]}]}} =
               IngestService.Client.insert(name, @table, [
                 %{"id" => 3, "ts" => 7}
               ])

      assert message =~ "cannot accept"
    end
  end

  describe "NDJSON passthrough (T-180)" do
    test "blank lines are not counted as inserted rows", context do
      %{name: name, buffer: buffer} =
        start_stack(context,
          buffer: [write_pool_size: 1]
        )

      body = ~s({"id": 1}\n\n{"id": 2}\n)

      assert {:ok, %{inserted: 2, errors: []}} =
               IngestService.Client.insert_ndjson(name, @table, body)

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)

      assert Enum.sum_by(entries, & &1.row_count) == 2
    end

    # `String.split(trim: true)` only drops the empty string, so a line of
    # spaces — or the "\r" a CRLF body leaves on a blank line — used to count
    # as a row the flush then never landed.
    test "whitespace-only lines are not counted as inserted rows", context do
      %{name: name, buffer: buffer} =
        start_stack(context,
          buffer: [write_pool_size: 1]
        )

      body = ~s({"id": 1}\n \r\n{"id": 2}\n)

      assert {:ok, %{inserted: 2, errors: []}} =
               IngestService.Client.insert_ndjson(name, @table, body)

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)

      assert Enum.sum_by(entries, & &1.row_count) == 2
    end

    test "a body of only whitespace inserts nothing and lands nothing", context do
      %{name: name, buffer: buffer} =
        start_stack(context,
          buffer: [write_pool_size: 1]
        )

      assert {:ok, %{inserted: 0, errors: []}} =
               IngestService.Client.insert_ndjson(name, @table, " \n \n")

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)

      assert entries == []
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

  describe "skip_invalid_rows: false (T-474)" do
    test "a row the validator refuses writes none of the request", context do
      %{name: name, buffer: buffer} = start_stack(context)

      assert {:ok, %{inserted: 0, errors: [%{index: 1}]}} =
               IngestService.Client.insert(name, @table, [%{"id" => 1}, %{"id" => "x"}],
                 skip_invalid_rows: false
               )

      assert {:ok, []} = BufferService.Client.hot_manifest(buffer, @table)
    end

    test "a row the flush refuses writes none of the request", context do
      %{name: name, buffer: buffer} = start_stack(context, buffer: [write_pool_size: 1])

      rows = [%{"id" => 1}, %{"id" => 9_223_372_036_854_775_808}, %{"id" => 2}]

      assert {:ok, %{inserted: 0, errors: [%{index: 1, errors: [%{message: message}]}]}} =
               IngestService.Client.insert(name, @table, rows, skip_invalid_rows: false)

      assert message =~ "the flush refused the row"
      assert {:ok, []} = BufferService.Client.hot_manifest(buffer, @table)
    end

    test "an NDJSON body with a refused line writes none of it", context do
      %{name: name, buffer: buffer} = start_stack(context, buffer: [write_pool_size: 1])

      body = ~s({"id":1}\n{"id":"junk"}\n{"id":3}\n)

      assert {:ok, %{inserted: 0, errors: [%{index: 1}]}} =
               IngestService.Client.insert_ndjson(name, @table, body, skip_invalid_rows: false)

      assert {:ok, []} = BufferService.Client.hot_manifest(buffer, @table)
    end

    test "a clean body still writes every row", context do
      %{name: name, buffer: buffer} = start_stack(context)

      assert {:ok, %{inserted: 2, errors: []}} =
               IngestService.Client.insert_ndjson(name, @table, ~s({"id":1}\n{"id":2}\n),
                 skip_invalid_rows: false
               )

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
    end
  end

  describe "insert_rowbinary/5 (T-476)" do
    defp leb(n) when n < 128, do: <<n>>
    defp leb(n), do: <<1::1, Bitwise.band(n, 127)::7, leb(Bitwise.bsr(n, 7))::binary>>
    defp str(bytes), do: [leb(byte_size(bytes)), bytes]

    defp typed(rows) do
      IO.iodata_to_binary([
        leb(2),
        str("id"),
        str("ts"),
        str("Nullable(Int64)"),
        str("DateTime64(6)"),
        Enum.map(rows, fn {id, us} ->
          [if(id, do: [0, <<id::little-signed-64>>], else: <<1>>), <<us::little-signed-64>>]
        end)
      ])
    end

    test "decodes on this node and writes the rows", context do
      %{name: name, buffer: buffer} = start_stack(context)
      body = typed([{1, 1_789_380_000_000_000}, {2, 1_789_380_000_000_001}])

      assert {:ok, %{inserted: 2, errors: []}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types)

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
    end

    test "a row the decoder refuses is reported at its index, and the rest land", context do
      %{name: name, buffer: buffer} = start_stack(context)
      body = typed([{1, 0}, {nil, 0}, {3, 0}])

      assert {:ok, %{inserted: 2, errors: [%{index: 1, errors: [%{message: message}]}]}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types)

      assert message == "column id must not be null"

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
    end

    test "skip_invalid_rows: false writes nothing when the decoder refuses a row", context do
      %{name: name, buffer: buffer} = start_stack(context)
      body = typed([{1, 0}, {nil, 0}])

      assert {:ok, %{inserted: 0, errors: [%{index: 1}]}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types,
                 skip_invalid_rows: false
               )

      assert {:ok, []} = BufferService.Client.hot_manifest(buffer, @table)
    end

    test "a body the decoder cannot read writes nothing", context do
      %{name: name} = start_stack(context)
      body = binary_part(typed([{1, 0}]), 0, 20)

      assert {:error, {:invalid_rowbinary, _message}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types)
    end

    test "a body naming a column added since the schema was cached decodes against the fresh one",
         context do
      %{name: name, buffer: buffer, catalog: catalog} = start_stack(context)

      assert {:ok, %{inserted: 1}} =
               IngestService.Client.insert_rowbinary(
                 name,
                 @table,
                 typed([{1, 0}]),
                 :with_names_and_types
               )

      [added] = Schema.new!([{"msg", :string}]).fields
      :ok = catalog.impl.alter_table(catalog.config, @table, {:add_column, added})

      body =
        IO.iodata_to_binary([
          leb(2),
          [str("id"), str("msg")],
          [str("Int64"), str("String")],
          [<<2::little-signed-64>>, str("hi")]
        ])

      assert {:ok, %{inserted: 1, errors: []}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types)

      {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
      assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
    end

    test "a body the fresh schema cannot read either keeps its refusal", context do
      %{name: name} = start_stack(context)

      body = IO.iodata_to_binary([leb(1), str("nope"), str("Int64"), <<1::little-signed-64>>])

      assert {:error, {:invalid_rowbinary, message}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types)

      assert message =~ "nope"
    end

    test "rows that decode past max_ndjson_bytes are refused before forwarding", context do
      %{name: name, buffer: buffer} = start_stack(context)
      body = typed([{1, 0}, {2, 0}])

      assert {:error, {:decoded_too_large, _bytes, 10}} =
               IngestService.Client.insert_rowbinary(name, @table, body, :with_names_and_types,
                 max_ndjson_bytes: 10
               )

      assert {:ok, []} = BufferService.Client.hot_manifest(buffer, @table)
    end
  end
end
