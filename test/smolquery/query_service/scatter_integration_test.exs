defmodule Smolquery.QueryService.ScatterIntegrationTest do
  @moduledoc """
  The distributed path through the public surface (PL-49): two query
  services over the same catalog and buffer, one with `distributed`
  enabled, and every answer compared between them. The scatter telemetry
  event is the proof the distributed instance actually scattered rather
  than silently falling back — a fallback would pass the equality
  assertions too, and prove nothing.

  Workers here are `local_workers` instances dispatched through
  `Smolquery.QueryService.WorkerTransport` to this node, which is a direct
  call; the gen_rpc path to a peer is `worker_transport_peer_test`.
  Genuine cross-host scatter is the kind cluster's to prove, like hot-tier
  fan-out before it (T-77).

  The table is seeded before either query service starts: each one warms an
  engine pool that attaches the same sqlite lake, and under CI load those
  attaches held the file lock long enough to exhaust the catalog's commit
  retries (`{:error, :commit_conflict}` out of `seed/3`).
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

    buffer = :"scatter_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    seed(catalog, buffer, context.tmp_dir)

    shared = [
      catalog: catalog,
      buffer_base_url: HotServer.base_url(buffer),
      engine_extensions: [:httpfs],
      allowed_directories: [context.tmp_dir],
      job_bootstrap: [
        DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
      ]
    ]

    control = :"scatter_control_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       [name: control, distributed: [enabled: false, min_files: 4, local_workers: 3]] ++ shared},
      id: control
    )

    distributed = :"scatter_distributed_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       [name: distributed, distributed: [enabled: true, min_files: 4, local_workers: 3]] ++
         shared},
      id: distributed
    )

    on_exit(fn ->
      QueryService.Runtime.delete(control)
      QueryService.Runtime.delete(distributed)
    end)

    attach_telemetry()

    %{control: control, distributed: distributed}
  end

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])
  end

  defp seed(catalog, buffer, tmp_dir) do
    store = Local.new(dir: Path.join(tmp_dir, "seg"))

    segments =
      for batch <- 0..5 do
        rows = for i <- 1..2, do: %{"id" => batch * 2 + i, "name" => "g-#{rem(batch, 3)}"}
        {:ok, segment} = Writer.write(rows, schema(), store: store)

        segment
      end

    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, segments)

    hot = for i <- 13..15, do: %{"id" => i, "name" => "g-#{rem(i, 3)}"}

    {:ok, _ack} =
      BufferService.Client.write_batch(buffer, @table, %{schema: schema(), rows: hot})
  end

  defp attach_telemetry do
    parent = self()
    handler = "scatter-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:smolquery, :query, :scatter],
      fn _event, measurements, meta, _config -> send(parent, {:scatter, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp both(control, distributed, sql) do
    assert {:ok, %{state: :done}, control_frame} = Client.query(control, sql)
    assert {:ok, %{state: :done}, distributed_frame} = Client.query(distributed, sql)

    assert DataFrame.names(control_frame) == DataFrame.names(distributed_frame)

    assert_columns(
      DataFrame.to_columns(control_frame),
      DataFrame.to_columns(distributed_frame)
    )
  end

  defp assert_columns(control, distributed) do
    assert Map.keys(control) == Map.keys(distributed)

    Enum.each(control, fn {name, values} ->
      Enum.zip(values, distributed[name])
      |> Enum.each(fn
        {left, right} when is_float(left) and is_float(right) ->
          assert abs(left - right) <= 1.0e-9 * max(1.0, max(abs(left), abs(right)))

        {left, right} ->
          assert left == right
      end)
    end)
  end

  test "global aggregates scatter and answer exactly", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT count(*) AS n, sum(id) AS s, avg(id) AS a, min(id) AS lo, max(id) AS hi " <>
        "FROM analytics.events"
    )

    assert_received {:scatter, %{shards: shards, partial_bytes: bytes}, %{workers: _workers}}
    assert shards == 3
    assert bytes > 0
  end

  test "a distributed answer lands on the job as scatter", %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT count(*) AS n FROM analytics.events")

    assert %{shards: 3, partial_bytes: bytes} = job.scatter
    assert bytes > 0
  end

  test "the per-job option turns scatter on against the deployment default", %{
    control: control
  } do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(control, "SELECT count(*) AS n FROM analytics.events",
               distributed: true
             )

    assert %{shards: 3} = job.scatter
    assert_received {:scatter, _measurements, _meta}
  end

  test "the per-job option turns scatter off against an enabled deployment", %{
    distributed: distributed
  } do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT count(*) AS n FROM analytics.events",
               distributed: false
             )

    assert job.scatter == nil
    refute_received {:scatter, _measurements, _meta}
  end

  test "a grouped top-k scatters and answers exactly", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT name, count(*) AS n, sum(id) AS s FROM analytics.events " <>
        "GROUP BY name ORDER BY s DESC, name LIMIT 2"
    )

    assert_received {:scatter, _measurements, _meta}
  end

  test "a filtered aggregate scatters and answers exactly", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT count(*) AS n FROM analytics.events WHERE id > 6"
    )

    assert_received {:scatter, _measurements, _meta}
  end

  test "the control service never scatters", %{control: control} do
    assert {:ok, %{state: :done}, _frame} =
             Client.query(control, "SELECT count(*) AS n FROM analytics.events")

    refute_received {:scatter, _measurements, _meta}
  end

  test "a query that does not decompose still answers", %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, "SELECT id, name FROM analytics.events ORDER BY id")

    assert DataFrame.to_columns(frame)["id"] == Enum.to_list(1..15)
    assert job.scatter == nil
    refute_received {:scatter, _measurements, _meta}
  end
end
