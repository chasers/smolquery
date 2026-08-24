defmodule Smolquery.QueryService.DatasetLakeIntegrationTest do
  @moduledoc """
  A query over a dataset that owns its catalog (PL-51 L3): the planner pins
  that dataset's own snapshot, qualifies its view with `ds_<name>`, and hands
  the job engine the statements that attach the lake before lockdown.

  Needs the suite's Postgres — the dataset's lake is a real one — and, like
  `Smolquery.QueryService.PlannerIntegrationTest`, drives the plan by hand on
  a job engine rather than through a full query service.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.Dataset
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Segments.Writer
  alias Smolquery.Test.Postgres

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @job __MODULE__.Job
  @database "smolquery_test"
  @owned {"owned", "events"}
  @plain {"plain", "events"}

  setup context do
    previous = Application.get_env(:smolquery, :credential_key)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    on_exit(fn ->
      if previous do
        Application.put_env(:smolquery, :credential_key, previous)
      else
        Application.delete_env(:smolquery, :credential_key)
      end
    end)

    Postgres.ensure_database!(@database)
    Postgres.reset_lake!(@database)

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "data")

    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: data_path})

    catalog = DuckLake.new(engine: @lake)

    {:ok, owned} =
      Dataset.new(%{"name" => "owned", "catalog" => Postgres.catalog_params()})

    :ok = Catalog.create_dataset(catalog, owned)
    :ok = Catalog.create_table(catalog, @owned, schema())
    :ok = Catalog.create_dataset(catalog, "plain")
    :ok = Catalog.create_table(catalog, @plain, schema())

    buffer = :"dataset_lake_buffer_#{:erlang.unique_integer([:positive])}"

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
        name: :"dataset_lake_#{:erlang.unique_integer([:positive])}",
        catalog: catalog,
        data_path: data_path,
        buffer_base_url: HotServer.base_url(buffer)
      )

    %{catalog: catalog, buffer: buffer, runtime: runtime, tmp_dir: context.tmp_dir}
  end

  defp schema, do: Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])

  defp seal_rows(catalog, dir, table, range) do
    rows = for i <- range, do: %{"id" => i, "name" => "sealed-#{i}"}
    {:ok, segment} = Writer.write(rows, schema(), store: Local.new(dir: dir))
    {:ok, snapshot} = Catalog.register_segments(catalog, table, [segment])

    snapshot
  end

  defp run(plan) do
    Enum.each(plan.statements, &Engine.query!(@job, &1))

    Engine.query!(@job, plan.sql)
  end

  test "a query over an owned dataset reads its own lake, both tiers", %{
    catalog: catalog,
    buffer: buffer,
    runtime: runtime,
    tmp_dir: tmp
  } do
    sealed_at = seal_rows(catalog, Path.join(tmp, "segments"), @owned, 1..2)

    rows = for i <- 3..5, do: %{"id" => i, "name" => "hot-#{i}"}
    {:ok, _ack} = Client.write_batch(buffer, @owned, %{schema: schema(), rows: rows})

    {:ok, plan} =
      Planner.plan(runtime, Engine.connection_name(@job), "SELECT count(*) FROM owned.events")

    assert plan.snapshots == %{"owned" => sealed_at}
    assert %{"owned" => %Dataset{}} = plan.datasets
    assert plan.prefixes == []
    assert Enum.any?(plan.statements, &String.starts_with?(&1, "ATTACH IF NOT EXISTS"))
    assert Enum.any?(plan.statements, &(&1 =~ ~s|FROM "ds_owned"."owned"."events" AT (VERSION|))

    assert run(plan).rows == [[5]]
  end

  test "a query across an owned and a default dataset pins one snapshot each", %{
    catalog: catalog,
    runtime: runtime,
    tmp_dir: tmp
  } do
    owned_at = seal_rows(catalog, Path.join(tmp, "owned"), @owned, 1..3)
    plain_at = seal_rows(catalog, Path.join(tmp, "plain"), @plain, 1..2)

    {:ok, plan} =
      Planner.plan(
        runtime,
        Engine.connection_name(@job),
        "SELECT (SELECT count(*) FROM owned.events) + (SELECT count(*) FROM plain.events)"
      )

    assert plan.snapshots == %{"owned" => owned_at, "plain" => plain_at}
    assert plan.snapshot == plain_at
    assert Enum.any?(plan.statements, &(&1 =~ ~s|FROM "lake"."plain"."events"|))
    assert Enum.any?(plan.statements, &(&1 =~ ~s|FROM "ds_owned"."owned"."events"|))

    assert run(plan).rows == [[5]]
  end

  test "current_snapshot/2 answers the dataset's own lake", %{catalog: catalog, tmp_dir: tmp} do
    {:ok, before_owned} = Catalog.current_snapshot(catalog, "owned")
    {:ok, before_plain} = Catalog.current_snapshot(catalog, "plain")

    owned_at = seal_rows(catalog, Path.join(tmp, "owned"), @owned, 1..3)

    assert {:ok, ^owned_at} = Catalog.current_snapshot(catalog, "owned")
    assert owned_at > before_owned
    assert {:ok, ^before_plain} = Catalog.current_snapshot(catalog, "plain")
    assert {:ok, ^before_plain} = Catalog.current_snapshot(catalog)
  end

  test "expire_snapshots/2 reaches every lake (PL-51 L4)", %{catalog: catalog, tmp_dir: tmp} do
    first = seal_rows(catalog, Path.join(tmp, "owned"), @owned, 1..2)
    second = seal_rows(catalog, Path.join(tmp, "owned"), @owned, 3..4)
    assert second > first

    Process.sleep(5)
    assert {:ok, expired} = Catalog.expire_snapshots(catalog, 1)
    assert expired >= 1
    assert {:ok, ^second} = Catalog.current_snapshot(catalog, "owned")
  end

  test "known_segments/1 spans every lake, so GC cannot mistake an owned segment for an orphan",
       %{catalog: catalog, tmp_dir: tmp} do
    seal_rows(catalog, Path.join(tmp, "owned"), @owned, 1..3)
    seal_rows(catalog, Path.join(tmp, "plain"), @plain, 1..2)

    {:ok, known} = Catalog.known_segments(catalog)

    assert Enum.count(known, &String.contains?(&1, "/owned/")) == 1
    assert Enum.count(known, &String.contains?(&1, "/plain/")) == 1
  end
end
