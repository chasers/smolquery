defmodule Smolquery.QueryService.ConsistencyTest do
  @moduledoc """
  The reader-side crash matrix — Milestone 5's counterpart to M4's centerpiece.

  M4's matrix (`Smolquery.StorageService.Handoff.SealTest`) proved that the
  union of the sealed tier and the membership-filtered hot tier counts every
  row exactly once at every partial state of the seal handoff — with the
  membership rule hand-written in test SQL, standing in for a planner that did
  not exist. These tests rerun that walk with the real thing: every read goes
  through `QueryService.Client.query/3`, a job, a private engine, and the
  planner's views.

  Partial states are staged the same way M4 stages them — by running the seal
  steps directly (claim → merge → register → retire) rather than killing a
  process mid-flight, so each named state is exact rather than whatever the
  scheduler left behind.

  Tagged `:integration`: a live `HotServer`, a real DuckLake catalog, and
  `httpfs` reads in every job engine.
  """

  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Planner
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime, as: StorageRuntime

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @sealed_id "01KYWPEEGAM8FQVQS5S2QF26SV"

  setup context do
    unique = :erlang.unique_integer([:positive])
    buffer = :"consistency_buffer_#{unique}"
    storage = :"consistency_storage_#{unique}"
    query = :"consistency_query_#{unique}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer,
       dir: Path.join(context.tmp_dir, "buffer"),
       flush_interval_ms: 25,
       seal_max_files: 1_000_000,
       seal_max_bytes: 1_000_000_000,
       seal_max_age_ms: 600_000,
       retire_grace_ms: 600_000},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "ducklake")

    start_supervised!(
      {DuckLake,
       name: StorageRuntime.catalog_engine(storage), metadata: metadata, data_path: data_path},
      id: StorageRuntime.catalog_engine(storage)
    )

    catalog = DuckLake.new(engine: StorageRuntime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    storage_runtime =
      StorageRuntime.new(
        name: storage,
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_name: buffer,
        buffer_base_url: HotServer.base_url(buffer),
        catalog: catalog
      )

    start_supervised!(
      {Engine,
       name: StorageRuntime.engine(storage),
       extensions: [:httpfs],
       statements: [Smolquery.InternalSecret.create_secret_statement("http://")]}
    )

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_name: buffer,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)

    %{
      buffer: buffer,
      buffer_runtime: buffer_runtime,
      storage_runtime: storage_runtime,
      query: query,
      catalog: catalog,
      metadata: metadata,
      data_path: data_path
    }
  end

  defp schema, do: Schema.new!([{"id", :int64}])

  defp write(buffer, range) do
    batch = %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}
    {:ok, ack} = BufferService.Client.write_batch(buffer, @table, batch)

    ack.segment_id
  end

  defp freeze_claim(context, ids) do
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, @sealed_id)
    {:ok, claim} = HotManifest.claim(context.buffer_runtime.manifest, @table, ids, [key])

    claim
  end

  defp visible_ids(context) do
    {:ok, job, frame} =
      Client.query(context.query, "SELECT id FROM analytics.events ORDER BY id")

    assert job.state == :done, "query failed: #{inspect(job.error)}"

    DataFrame.to_columns(frame)["id"]
  end

  test "every partial state of the seal handoff counts every row exactly once", context do
    first = write(context.buffer, 1..2)
    second = write(context.buffer, 3..3)
    expected = [1, 2, 3]

    assert visible_ids(context) == expected

    claim = freeze_claim(context, [first, second])
    assert visible_ids(context) == expected

    {:ok, segment} = Merge.run(context.storage_runtime, @table, claim)
    assert visible_ids(context) == expected

    {:ok, snapshot} = Catalog.register_segments(context.catalog, @table, [segment])
    assert visible_ids(context) == expected

    :ok = BufferService.Client.retire(context.buffer, @table, claim.ids, snapshot)
    assert visible_ids(context) == expected
  end

  test "rows written after the claim froze wait for the next one, and still count once",
       context do
    first = write(context.buffer, 1..2)
    claim = freeze_claim(context, [first])
    _late = write(context.buffer, 3..4)

    {:ok, segment} = Merge.run(context.storage_runtime, @table, claim)
    {:ok, snapshot} = Catalog.register_segments(context.catalog, @table, [segment])

    assert visible_ids(context) == [1, 2, 3, 4]

    :ok = BufferService.Client.retire(context.buffer, @table, claim.ids, snapshot)

    assert visible_ids(context) == [1, 2, 3, 4]
  end

  test "a plan pinned before the commit still reads the micro-segments after it", context do
    first = write(context.buffer, 1..2)
    claim = freeze_claim(context, [first])
    {:ok, segment} = Merge.run(context.storage_runtime, @table, claim)

    reader = __MODULE__.PinnedReader

    start_supervised!(
      {Engine,
       name: reader,
       extensions: [:ducklake, :httpfs],
       statements: [
         Smolquery.InternalSecret.create_secret_statement("http://"),
         DuckLake.attach_statement(
           DuckLake.default_catalog(),
           context.metadata,
           context.data_path
         )
       ]},
      id: reader
    )

    {:ok, runtime} = QueryService.Runtime.fetch(context.query)

    {:ok, pinned} =
      Planner.plan(
        runtime,
        Engine.connection_name(reader),
        "SELECT id FROM analytics.events ORDER BY id"
      )

    {:ok, _snapshot} = Catalog.register_segments(context.catalog, @table, [segment])

    Enum.each(pinned.statements, &Engine.query!(reader, &1))
    {:ok, frame} = Connection.frame(Engine.connection_name(reader), pinned.sql)

    assert DataFrame.to_columns(frame)["id"] == [1, 2]

    assert visible_ids(context) == [1, 2]
  end

  test "an unreachable buffer owner fails the query and names the table", context do
    write(context.buffer, 1..2)

    unreachable = :"consistency_unreachable_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: unreachable,
       catalog: context.catalog,
       buffer_name: context.buffer,
       buffer_base_url: "http://127.0.0.1:1",
       buffer_timeout_ms: 500,
       job_bootstrap: [
         DuckLake.attach_statement(
           DuckLake.default_catalog(),
           context.metadata,
           context.data_path
         )
       ]},
      id: unreachable
    )

    on_exit(fn -> QueryService.Runtime.delete(unreachable) end)

    assert {:ok, job, nil} =
             Client.query(unreachable, "SELECT id FROM analytics.events")

    assert job.state == :error
    assert {:hot_tier_unavailable, @table, _reason} = job.error
  end
end
