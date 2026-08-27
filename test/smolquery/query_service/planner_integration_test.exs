defmodule Smolquery.QueryService.PlannerIntegrationTest do
  @moduledoc """
  The plan, executed: views shadow the lake, `AT (VERSION)` pins the sealed
  side, and one query reads both tiers through a job-shaped engine.

  Tagged `:integration` for the same reasons as the DuckLake tests — extension
  downloads and a real catalog on disk — plus `httpfs` reads of the hot tier
  over a live `HotServer`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.SegmentFixture

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @job __MODULE__.Job
  @table {"analytics", "events"}

  setup context do
    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "data")

    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: data_path})

    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    buffer = :"planner_int_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    start_supervised!(
      {Engine,
       name: @job,
       extensions: [:ducklake, :httpfs],
       statements: [
         Smolquery.InternalSecret.create_secret_statement("http://"),
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]}
    )

    runtime =
      Runtime.new(
        name: :"planner_int_#{:erlang.unique_integer([:positive])}",
        catalog: catalog,
        buffer_base_url: HotServer.base_url(buffer)
      )

    %{catalog: catalog, buffer: buffer, runtime: runtime, tmp_dir: context.tmp_dir}
  end

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])
  end

  defp seal_rows(catalog, dir, range) do
    rows = for i <- range, do: %{"id" => i, "name" => "sealed-#{i}"}
    {:ok, segment} = SegmentFixture.write(rows, schema(), store: Local.new(dir: dir))
    {:ok, snapshot} = Catalog.register_segments(catalog, @table, [segment])

    snapshot
  end

  defp run(plan) do
    Enum.each(plan.statements, &Engine.query!(@job, &1))

    Engine.query!(@job, plan.sql)
  end

  test "one query counts both tiers, once", %{
    catalog: catalog,
    buffer: buffer,
    runtime: runtime,
    tmp_dir: tmp
  } do
    seal_rows(catalog, Path.join(tmp, "segments"), 1..2)

    rows = for i <- 3..5, do: %{"id" => i, "name" => "hot-#{i}"}
    {:ok, _ack} = Client.write_batch(buffer, @table, %{schema: schema(), rows: rows})

    {:ok, plan} =
      Planner.plan(runtime, Engine.connection_name(@job), "SELECT count(*) FROM analytics.events")

    assert run(plan).rows == [[5]]
  end

  test "a partitioned table's hot tier reads as one (T-170)", %{
    catalog: catalog,
    buffer: buffer,
    runtime: runtime,
    tmp_dir: tmp
  } do
    seal_rows(catalog, Path.join(tmp, "segments"), 1..2)

    partitioned =
      Runtime.new(
        name: :"planner_part_#{:erlang.unique_integer([:positive])}",
        catalog: catalog,
        buffer_base_url: HotServer.base_url(buffer),
        write_partitions: 3
      )

    for {ref, base} <- Enum.zip(Smolquery.Partitions.refs(@table, 3), [10, 20, 30]) do
      rows = for i <- base..(base + 1), do: %{"id" => i, "name" => "hot-#{i}"}
      {:ok, _ack} = Client.write_batch(buffer, ref, %{schema: schema(), rows: rows})
    end

    {:ok, plan} =
      Planner.plan(
        partitioned,
        Engine.connection_name(@job),
        "SELECT count(*) FROM analytics.events"
      )

    assert run(plan).rows == [[8]]

    on_exit(fn -> Runtime.delete(partitioned.name) end)
  end

  test "a table's catalog count expands the hot tier without a config change (T-304)", %{
    catalog: catalog,
    buffer: buffer,
    runtime: runtime,
    tmp_dir: tmp
  } do
    seal_rows(catalog, Path.join(tmp, "segments"), 1..2)

    :ok = Catalog.put_partitions(catalog, @table, 3)

    for {ref, base} <- Enum.zip(Smolquery.Partitions.refs(@table, 3), [10, 20, 30]) do
      rows = for i <- base..(base + 1), do: %{"id" => i, "name" => "hot-#{i}"}
      {:ok, _ack} = Client.write_batch(buffer, ref, %{schema: schema(), rows: rows})
    end

    {:ok, plan} =
      Planner.plan(runtime, Engine.connection_name(@job), "SELECT count(*) FROM analytics.events")

    assert run(plan).rows == [[8]]
  end

  test "the view shadows the lake rather than replacing it", %{
    catalog: catalog,
    runtime: runtime,
    tmp_dir: tmp
  } do
    seal_rows(catalog, Path.join(tmp, "segments"), 1..2)

    {:ok, plan} =
      Planner.plan(
        runtime,
        Engine.connection_name(@job),
        "SELECT name FROM analytics.events ORDER BY id"
      )

    assert run(plan).rows == [["sealed-1"], ["sealed-2"]]
  end

  test "a plan is pinned: sealing after planning changes nothing it reads", %{
    catalog: catalog,
    runtime: runtime,
    tmp_dir: tmp
  } do
    seal_rows(catalog, Path.join(tmp, "segments"), 1..2)

    {:ok, plan} =
      Planner.plan(runtime, Engine.connection_name(@job), "SELECT count(*) FROM analytics.events")

    seal_rows(catalog, Path.join(tmp, "segments"), 100..109)

    assert run(plan).rows == [[2]]
  end
end
