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
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.MergeTest.ManifestStub
  alias Smolquery.StorageService.Runtime
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

    start_supervised!({Engine, name: Runtime.engine(storage), extensions: [:httpfs]})

    %{buffer: buffer, runtime: runtime}
  end

  defp claim(ids, keys \\ @keys), do: %{ids: ids, keys: keys}

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

  test "merges a claim's micro-segments into one sealed segment", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, first} = Client.write_batch(buffer, @table, batch(1..2))
    {:ok, second} = Client.write_batch(buffer, @table, batch(3..4))

    assert {:ok, %Segment{} = segment} =
             Merge.run(runtime, @table, claim([first.segment_id, second.segment_id]))

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

    assert {:ok, first} = Merge.run(runtime, @table, claim([ack.segment_id]))
    assert {:ok, again} = Merge.run(runtime, @table, claim([ack.segment_id]))

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

    assert {:ok, segment} = Merge.run(runtime, @table, claim([claimed.segment_id]))
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
             Merge.run(runtime, @table, claim([first.segment_id, second.segment_id]))

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
             Merge.run(runtime, @table, claim([first.segment_id, second.segment_id]))

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

    assert {:ok, segment} = Merge.run(runtime, @table, claim([ack.segment_id]))
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

    assert Merge.run(runtime, @table, claim([ack.segment_id])) ==
             {:error, {:undeclared_columns, ["name"]}}

    assert {:ok, []} = Store.list(runtime.store, "analytics/events")
  end

  test "reports a table the catalog does not hold", %{buffer: buffer, runtime: runtime} do
    {:ok, ack} = Client.write_batch(buffer, {"analytics", "absent"}, batch(1..1))

    assert {:error, _reason} =
             Merge.run(
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
             Merge.run(
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

    assert {:ok, segment} = Merge.run(runtime, @table, claim(ids))
    assert segment.byte_size < input_bytes
  end

  test "refuses an unsupported codec rather than interpolating it", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(1..1))

    assert_raise FunctionClauseError, fn ->
      Merge.run(%{runtime | compression: :"zstd) --"}, @table, claim([ack.segment_id]))
    end
  end

  test "refuses a claim key that names no segment, before any merge runs", %{runtime: runtime} do
    key = "analytics/events/not-a-ulid.parquet"

    assert Merge.run(runtime, @table, claim(["01KYWPEEGAM8FQVQS5S2QF26SV"], [key])) ==
             {:error, {:invalid_claim_key, key}}
  end

  test "refuses a claim with none of its inputs left", %{runtime: runtime} do
    assert Merge.run(runtime, @table, claim(["01KYWPEEGAM8FQVQS5S2QF26SV"])) ==
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

    assert {:error, {:merge_failed, message}} =
             Merge.run(runtime, @table, claim([ack.segment_id]))

    assert message =~ "404"
    assert {:ok, []} = Store.list(runtime.store, "analytics/events")
  end

  test "refuses a multi-key claim rather than sealing part of it", %{
    buffer: buffer,
    runtime: runtime
  } do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(1..1))
    keys = @keys ++ ["analytics/events/01KYWPEEGAM8FQVQS5S2QF26SW.parquet"]

    assert {:error, {:unsupported_claim_keys, ^keys}} =
             Merge.run(runtime, @table, claim([ack.segment_id], keys))
  end

  test "refuses a manifest entry without a url", context do
    entry = %{"id" => "01KYWPEEGAM8FQVQS5S2QF26SW", "row_count" => 3}
    runtime = stubbed_runtime(context, [entry])

    assert Merge.run(runtime, @table, claim([entry["id"]])) ==
             {:error, {:invalid_manifest_entry, entry}}
  end

  test "refuses a manifest entry whose row count is not a count", context do
    entry = %{
      "id" => "01KYWPEEGAM8FQVQS5S2QF26SW",
      "url" => "http://127.0.0.1:1/x.parquet",
      "row_count" => nil
    }

    runtime = stubbed_runtime(context, [entry])

    assert Merge.run(runtime, @table, claim([entry["id"]])) ==
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

  test "reports an unreachable buffer node", context do
    runtime =
      Runtime.new(
        name: :"merge_unreachable_#{:erlang.unique_integer([:positive])}",
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_base_url: "http://127.0.0.1:1"
      )

    assert {:error, {:manifest_unreachable, _reason}} =
             Merge.run(runtime, @table, claim(["01KYWPEEGAM8FQVQS5S2QF26SV"]))
  end

  test "merges a claim the buffer actually froze", %{buffer: buffer, runtime: runtime} do
    {:ok, first} = Client.write_batch(buffer, @table, batch(1..2))
    {:ok, second} = Client.write_batch(buffer, @table, batch(3..3))

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)
    ids = [first.segment_id, second.segment_id]
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
    {:ok, frozen} = HotManifest.claim(buffer_runtime.manifest, @table, ids, [key])

    assert {:ok, segment} = Merge.run(runtime, @table, frozen)
    assert segment.key == key
    assert rows_in(runtime, segment) == [1, 2, 3]
  end
end
