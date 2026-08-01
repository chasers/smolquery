defmodule Smolquery.QueryService.ClientIntegrationTest do
  @moduledoc """
  The whole read path through the public surface: write rows into the buffer,
  seal some into the lake, and ask `Client.query/3` — a job, a private engine,
  the planner's views, `httpfs` reads of the hot tier, and one answer that
  counts both tiers exactly once.
  """

  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.Schema
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Segments.Writer
  alias Smolquery.Test.Eventually

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @table {"analytics", "events"}

  setup context do
    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "data")

    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: data_path})

    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    buffer = :"client_int_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    query = :"client_int_query_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    %{catalog: catalog, buffer: buffer, query: query, tmp_dir: context.tmp_dir}
  end

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])
  end

  test "a sync query answers from both tiers, once", %{
    catalog: catalog,
    buffer: buffer,
    query: query,
    tmp_dir: tmp
  } do
    sealed = for i <- 1..2, do: %{"id" => i, "name" => "sealed-#{i}"}
    {:ok, segment} = Writer.write(sealed, schema(), store: Local.new(dir: Path.join(tmp, "seg")))
    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

    hot = for i <- 3..5, do: %{"id" => i, "name" => "hot-#{i}"}

    {:ok, _ack} =
      BufferService.Client.write_batch(buffer, @table, %{schema: schema(), rows: hot})

    assert {:ok, job, frame} =
             Client.query(query, "SELECT count(*) AS n FROM analytics.events")

    assert job.state == :done
    assert is_integer(job.snapshot)
    assert DataFrame.to_columns(frame)["n"] == [5]
  end

  test "an async job survives its caller and hands over its frame", %{
    buffer: buffer,
    query: query
  } do
    rows = for i <- 1..3, do: %{"id" => i, "name" => "hot-#{i}"}

    {:ok, _ack} =
      BufferService.Client.write_batch(buffer, @table, %{schema: schema(), rows: rows})

    {:ok, job} = Client.submit(query, "SELECT id, name FROM analytics.events ORDER BY id")

    assert Eventually.until(fn ->
             match?({:ok, %{state: :done}, _frame}, Client.fetch(query, job.id))
           end)

    {:ok, done, frame} = Client.fetch(query, job.id)

    assert done.row_count == 3
    assert DataFrame.to_columns(frame)["name"] == ["hot-1", "hot-2", "hot-3"]
  end

  test "two concurrent jobs against the same table do not collide", %{
    buffer: buffer,
    query: query
  } do
    rows = for i <- 1..4, do: %{"id" => i, "name" => "hot-#{i}"}

    {:ok, _ack} =
      BufferService.Client.write_batch(buffer, @table, %{schema: schema(), rows: rows})

    tasks =
      for sql <- [
            "SELECT count(*) AS n FROM analytics.events",
            "SELECT count(*) AS n FROM analytics.events WHERE id > 2"
          ] do
        Task.async(fn -> Client.query(query, sql) end)
      end

    assert [{:ok, %{state: :done}, first}, {:ok, %{state: :done}, second}] =
             Task.await_many(tasks, 30_000)

    assert DataFrame.to_columns(first)["n"] == [4]
    assert DataFrame.to_columns(second)["n"] == [2]
  end
end
