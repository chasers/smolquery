defmodule Smolquery.StorageService.MergeTest.ManifestStub do
  @moduledoc """
  Serves a fixed manifest, for entries a real buffer node would never produce.
  """

  @behaviour Plug

  import Plug.Conn

  @impl Plug
  def init(entries), do: entries

  @impl Plug
  def call(conn, entries) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(entries))
  end
end

defmodule Smolquery.StorageService.MergeTest do
  @moduledoc """
  The merge, against a real buffer node.

  Tagged `:integration` because the merge is DuckDB reading Parquet over HTTP:
  loading `httpfs` downloads the extension, and the inputs are served by a live
  `HotServer` rather than faked. Faking either half would test the wiring and skip
  the thing that can actually be wrong.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.MergeTest.ManifestStub
  alias Smolquery.StorageService.Runtime
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.SegmentServer

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @keys ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SV.parquet"]

  defp schema, do: Schema.new!([{"id", :int64}])
  defp batch(range), do: %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}

  setup context do
    buffer = :"merge_buffer_#{:erlang.unique_integer([:positive])}"
    storage = :"merge_storage_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    start_supervised!(
      {DuckLake,
       name: Runtime.catalog_engine(storage),
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "ducklake")},
      id: Runtime.catalog_engine(storage)
    )

    catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, Map.get(context, :declares, schema()))

    runtime =
      Runtime.new(
        name: storage,
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_base_url: HotServer.base_url(buffer),
        catalog: catalog
      )

    start_supervised!(
      {Engine,
       name: Runtime.engine(storage),
       extensions: [:httpfs],
       statements: [Smolquery.InternalSecret.create_secret_statement("http://")]}
    )

    %{buffer: buffer, runtime: runtime}
  end

  defp claim(ids, keys \\ @keys), do: %{ids: ids, keys: keys}

  defp merge(runtime, table_ref, claim) do
    with {:ok, entries} <-
           HotTier.manifest(runtime, table_ref, nil, ids: claim.ids, stats: false) do
      Merge.run(runtime, table_ref, claim, entries)
    end
  end

  defp columns_in(runtime, segment, projection) do
    Runtime.engine(runtime.name)
    |> Engine.query!("SELECT #{projection} FROM read_parquet($1) ORDER BY id", [
      Store.location(runtime.store, segment.key)
    ])
    |> Map.fetch!(:rows)
  end

  defp rows_in(runtime, segment) do
    Runtime.engine(runtime.name)
    |> Engine.query!("SELECT id FROM read_parquet($1) ORDER BY id", [
      Store.location(runtime.store, segment.key)
    ])
    |> Map.fetch!(:rows)
    |> List.flatten()
  end

  defp clustered_rows_in(runtime, segment) do
    Runtime.engine(runtime.name)
    |> Engine.query!("SELECT project_id, ts FROM read_parquet($1)", [
      Store.location(runtime.store, segment.key)
    ])
    |> Map.fetch!(:rows)
  end

  defp row_group_count(runtime, segment) do
    Runtime.engine(runtime.name)
    |> Engine.query!(
      "SELECT count(DISTINCT row_group_id) FROM parquet_metadata($1)",
      [Store.location(runtime.store, segment.key)]
    )
    |> Map.fetch!(:rows)
    |> hd()
    |> hd()
  end

  defp seal_many(buffer, table, batches) do
    Enum.map(batches, fn range ->
      {:ok, ack} = Client.write_batch(buffer, table, batch(range))
      ack.segment_id
    end)
  end

  test "sets row group size on unclustered tables too (T-280)", %{
    buffer: buffer,
    runtime: runtime
  } do
    runtime = %{runtime | seal_row_group_size: 500}

    ids = seal_many(buffer, @table, [1..1000, 1001..2000, 2001..3000, 3001..4000, 4001..5000])

    assert {:ok, segment} = merge(runtime, @table, claim(ids))
    assert segment.row_count == 5000
    assert row_group_count(runtime, segment) > 1
  end

  test "sets row group size when clustering columns are non-empty", %{
    buffer: buffer,
    runtime: runtime
  } do
    :ok = Catalog.put_clustering(runtime.catalog, @table, ["id"])
    runtime = %{runtime | seal_row_group_size: 500}

    ids = seal_many(buffer, @table, [1..1000, 1001..2000, 2001..3000, 3001..4000, 4001..5000])

    assert {:ok, segment} = merge(runtime, @table, claim(ids))
    assert segment.row_count == 5000
    assert row_group_count(runtime, segment) > 1
  end

  test "orders sealed output by the clustering columns the schema still has", context do
    schema = Schema.new!([{"project_id", :int64}, {"ts", :int64}])

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema)
    :ok = Catalog.put_clustering(catalog, @table, ["project_id", "dropped", "ts"])

    buffer = :"merge_cluster_buffer_#{:erlang.unique_integer([:positive])}"
    storage = :"merge_cluster_storage_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    runtime =
      Runtime.new(
        name: storage,
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_base_url: HotServer.base_url(buffer),
        catalog: catalog
      )

    start_supervised!(
      {Engine,
       name: Runtime.engine(storage),
       extensions: [:httpfs],
       statements: [Smolquery.InternalSecret.create_secret_statement("http://")]},
      id: Runtime.engine(storage)
    )

    first_batch = %{
      schema: schema,
      rows: [%{"project_id" => 2, "ts" => 100}, %{"project_id" => 1, "ts" => 200}]
    }

    second_batch = %{
      schema: schema,
      rows: [%{"project_id" => 1, "ts" => 50}, %{"project_id" => 2, "ts" => 50}]
    }

    {:ok, first} = Client.write_batch(buffer, @table, first_batch)
    {:ok, second} = Client.write_batch(buffer, @table, second_batch)

    assert {:ok, segment} =
             merge(runtime, @table, claim([first.segment_id, second.segment_id]))

    assert clustered_rows_in(runtime, segment) == [
             [1, 50],
             [1, 200],
             [2, 50],
             [2, 100]
           ]
  end

  test "merges a claim's micro-segments into one sealed segment", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, first} = Client.write_batch(buffer, @table, batch(1..2))
    {:ok, second} = Client.write_batch(buffer, @table, batch(3..4))

    assert {:ok, %Segment{} = segment} =
             merge(runtime, @table, claim([first.segment_id, second.segment_id]))

    assert segment.key == hd(@keys)
    assert segment.id == "01KYWPEEGAM8FQVQS5S2QF26SV"
    assert segment.row_count == 4
    assert segment.byte_size > 0
    assert rows_in(runtime, segment) == [1, 2, 3, 4]
  end

  test "writes to the key the claim already named, so a retry overwrites", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(1..2))

    assert {:ok, first} = merge(runtime, @table, claim([ack.segment_id]))
    assert {:ok, again} = merge(runtime, @table, claim([ack.segment_id]))

    assert again.key == first.key
    assert again.row_count == first.row_count
    assert rows_in(runtime, again) == [1, 2]
    assert {:ok, [_only_one]} = Store.list(runtime.store, "analytics/events")
  end

  test "merges only the claim's inputs, leaving later writes alone", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, claimed} = Client.write_batch(buffer, @table, batch(1..2))
    {:ok, _later} = Client.write_batch(buffer, @table, batch(90..91))

    assert {:ok, segment} = merge(runtime, @table, claim([claimed.segment_id]))
    assert rows_in(runtime, segment) == [1, 2]
  end

  @tag declares: Schema.new!([{"id", :int64}, {"name", :string}])
  test "unions differing-but-compatible schemas", %{buffer: buffer, runtime: runtime} do
    narrow = %{schema: Schema.new!([{"id", :int64}]), rows: [%{"id" => 1}]}

    wide = %{
      schema: Schema.new!([{"id", :int64}, {"name", :string}]),
      rows: [%{"id" => 2, "name" => "two"}]
    }

    {:ok, first} = Client.write_batch(buffer, @table, narrow)
    {:ok, second} = Client.write_batch(buffer, @table, wide)

    assert {:ok, segment} =
             merge(runtime, @table, claim([first.segment_id, second.segment_id]))

    assert columns_in(runtime, segment, "id, name") == [[1, nil], [2, "two"]]
  end

  @tag declares: Schema.new!([{"id", :int64}, {"name", :string}])
  test "seals the catalog's columns even when no input carries one", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, first} = Client.write_batch(buffer, @table, batch(1..1))
    {:ok, second} = Client.write_batch(buffer, @table, batch(2..2))

    assert {:ok, segment} =
             merge(runtime, @table, claim([first.segment_id, second.segment_id]))

    assert columns_in(runtime, segment, "id, name") == [[1, nil], [2, nil]]
  end

  @tag declares: Schema.new!([{"name", :string}, {"id", :int64}])
  test "seals in the catalog's column order, not the inputs'", %{
    buffer: buffer,
    runtime: runtime
  } do
    batch = %{
      schema: Schema.new!([{"id", :int64}, {"name", :string}]),
      rows: [%{"id" => 1, "name" => "one"}]
    }

    {:ok, ack} = Client.write_batch(buffer, @table, batch)

    assert {:ok, segment} = merge(runtime, @table, claim([ack.segment_id]))
    assert columns_in(runtime, segment, "*") == [["one", 1]]
  end

  test "refuses an input column the catalog does not declare", %{
    buffer: buffer,
    runtime: runtime
  } do
    undeclared = %{
      schema: Schema.new!([{"id", :int64}, {"name", :string}]),
      rows: [%{"id" => 1, "name" => "one"}]
    }

    {:ok, ack} = Client.write_batch(buffer, @table, undeclared)

    assert merge(runtime, @table, claim([ack.segment_id])) ==
             {:error, {:undeclared_columns, ["name"]}}

    assert {:ok, []} = Store.list(runtime.store, "analytics/events")
  end

  test "reports a table the catalog does not hold", %{buffer: buffer, runtime: runtime} do
    {:ok, ack} = Client.write_batch(buffer, {"analytics", "absent"}, batch(1..1))

    assert {:error, _reason} =
             merge(
               runtime,
               {"analytics", "absent"},
               claim([ack.segment_id], [
                 "analytics/absent/01KYWPEEGAM8FQVQS5S2QF26SV.parquet"
               ])
             )
  end

  test "skips an input the manifest no longer lists", %{buffer: buffer, runtime: runtime} do
    {:ok, present} = Client.write_batch(buffer, @table, batch(1..2))

    assert {:ok, segment} =
             merge(
               runtime,
               @table,
               claim([present.segment_id, "01KYWPEEGAM8FQVQS5S2QF26SV"])
             )

    assert segment.row_count == 2
    assert rows_in(runtime, segment) == [1, 2]
  end

  test "seals smaller than it read, because the codec matches the writer's", %{
    buffer: buffer,
    runtime: runtime
  } do
    ids =
      for i <- 1..8 do
        {:ok, ack} = Client.write_batch(buffer, @table, batch((i * 500)..(i * 500 + 499)))

        ack.segment_id
      end

    {:ok, entries} = Client.hot_manifest(buffer, @table)
    input_bytes = Enum.sum_by(entries, & &1.byte_size)

    assert {:ok, segment} = merge(runtime, @table, claim(ids))
    assert segment.byte_size < input_bytes
  end

  test "refuses an unsupported codec rather than interpolating it", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(1..1))

    assert_raise FunctionClauseError, fn ->
      merge(%{runtime | compression: :"zstd) --"}, @table, claim([ack.segment_id]))
    end
  end

  test "refuses a claim key that names no segment, before any merge runs", %{runtime: runtime} do
    key = "analytics/events/not-a-ulid.parquet"

    assert merge(runtime, @table, claim(["01KYWPEEGAM8FQVQS5S2QF26SV"], [key])) ==
             {:error, {:invalid_claim_key, key}}
  end

  test "refuses a claim with none of its inputs left", %{runtime: runtime} do
    assert merge(runtime, @table, claim(["01KYWPEEGAM8FQVQS5S2QF26SV"])) ==
             {:error, :no_inputs}
  end

  test "leaves nothing behind when an input cannot be read", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(1..1))
    {:ok, [entry]} = Client.hot_manifest(buffer, @table)

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)
    :ok = File.rm!(Store.location(buffer_runtime.store, entry.key))

    assert {:error, {:merge_failed, error}} =
             merge(runtime, @table, claim([ack.segment_id]))

    assert Exception.message(error) =~ "404"
    assert {:ok, []} = Store.list(runtime.store, "analytics/events")
  end

  test "refuses a multi-key claim rather than sealing part of it", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(1..1))
    keys = @keys ++ ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SW.parquet"]

    assert {:error, {:unsupported_claim_keys, ^keys}} =
             merge(runtime, @table, claim([ack.segment_id], keys))
  end

  test "refuses a manifest entry without a url", context do
    entry = %{"id" => "01KYWPEEGAM8FQVQS5S2QF26SW", "row_count" => 3}
    runtime = stubbed_runtime(context, [entry])

    assert merge(runtime, @table, claim([entry["id"]])) ==
             {:error, {:invalid_manifest_entry, entry}}
  end

  test "refuses a manifest entry whose row count is not a count", context do
    entry = %{
      "id" => "01KYWPEEGAM8FQVQS5S2QF26SW",
      "url" => "http://127.0.0.1:1/x.parquet",
      "row_count" => nil
    }

    runtime = stubbed_runtime(context, [entry])

    assert merge(runtime, @table, claim([entry["id"]])) ==
             {:error, {:invalid_manifest_entry, entry}}
  end

  defp stubbed_runtime(context, entries) do
    server =
      start_supervised!({Bandit, plug: {ManifestStub, entries}, port: 0, startup_log: false})

    Runtime.new(
      name: :"merge_stubbed_#{:erlang.unique_integer([:positive])}",
      dir: Path.join(context.tmp_dir, "sealed"),
      buffer_base_url: SegmentServer.base_url(server)
    )
  end

  test "merges a claim the buffer actually froze", %{buffer: buffer, runtime: runtime} do
    {:ok, first} = Client.write_batch(buffer, @table, batch(1..2))
    {:ok, second} = Client.write_batch(buffer, @table, batch(3..3))

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)
    ids = [first.segment_id, second.segment_id]
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
    {:ok, frozen} = HotManifest.claim(buffer_runtime.manifest, @table, ids, [key])

    assert {:ok, segment} = merge(runtime, @table, frozen)
    assert segment.key == key
    assert rows_in(runtime, segment) == [1, 2, 3]
  end

  describe "a claim over merge_inputs_per_call merges in chunks" do
    test "seals every input, in bounded engine calls", %{buffer: buffer, runtime: runtime} do
      runtime = %{runtime | merge_inputs_per_call: 2}
      ids = seal_many(buffer, @table, [1..2, 3..4, 5..6, 7..8, 9..10])

      assert {:ok, segment} = merge(runtime, @table, claim(ids))
      assert segment.row_count == 10
      assert rows_in(runtime, segment) == Enum.to_list(1..10)
    end

    test "a retry overwrites the same key, and the staging table with it", %{
      buffer: buffer,
      runtime: runtime
    } do
      runtime = %{runtime | merge_inputs_per_call: 1}
      ids = seal_many(buffer, @table, [1..1, 2..2, 3..3])

      assert {:ok, first} = merge(runtime, @table, claim(ids))
      assert {:ok, again} = merge(runtime, @table, claim(ids))

      assert again.key == first.key
      assert rows_in(runtime, again) == [1, 2, 3]
      assert {:ok, [_only_one]} = Store.list(runtime.store, "analytics/events")
    end

    @tag declares: Schema.new!([{"id", :int64}, {"name", :string}])
    test "every chunk projects onto the catalog's schema, so chunks with differing columns align",
         %{buffer: buffer, runtime: runtime} do
      runtime = %{runtime | merge_inputs_per_call: 1}
      narrow = %{schema: Schema.new!([{"id", :int64}]), rows: [%{"id" => 1}]}

      wide = %{
        schema: Schema.new!([{"id", :int64}, {"name", :string}]),
        rows: [%{"id" => 2, "name" => "two"}]
      }

      {:ok, first} = Client.write_batch(buffer, @table, narrow)
      {:ok, second} = Client.write_batch(buffer, @table, wide)

      assert {:ok, segment} =
               merge(runtime, @table, claim([first.segment_id, second.segment_id]))

      assert columns_in(runtime, segment, "id, name") == [[1, nil], [2, "two"]]
    end

    test "an undeclared column in a later chunk is refused", %{buffer: buffer, runtime: runtime} do
      runtime = %{runtime | merge_inputs_per_call: 1}
      declared = %{schema: Schema.new!([{"id", :int64}]), rows: [%{"id" => 1}]}

      undeclared = %{
        schema: Schema.new!([{"id", :int64}, {"name", :string}]),
        rows: [%{"id" => 2, "name" => "two"}]
      }

      {:ok, first} = Client.write_batch(buffer, @table, declared)
      {:ok, second} = Client.write_batch(buffer, @table, undeclared)

      assert merge(runtime, @table, claim([first.segment_id, second.segment_id])) ==
               {:error, {:undeclared_columns, ["name"]}}

      assert {:ok, []} = Store.list(runtime.store, "analytics/events")
    end

    test "compaction over the cap chunks the same way", %{buffer: buffer, runtime: runtime} do
      {:ok, prefix} = Store.prefix(@table)

      sealed =
        for {range, id} <- [
              {1..2, "01KYWPEEGAM8FQVQS5S2QF26S1"},
              {3..4, "01KYWPEEGAM8FQVQS5S2QF26S2"},
              {5..6, "01KYWPEEGAM8FQVQS5S2QF26S3"}
            ] do
          ids = seal_many(buffer, @table, [range])
          {:ok, key} = Store.key(prefix, id)
          {:ok, segment} = merge(runtime, @table, claim(ids, [key]))
          segment
        end

      runtime = %{runtime | merge_inputs_per_call: 1}
      {:ok, key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26S4")

      assert {:ok, merged} =
               Merge.compact(runtime, @table, key, Enum.map(sealed, & &1.path))

      assert merged.row_count == 6
      assert rows_in(runtime, merged) == Enum.to_list(1..6)
    end
  end
end
