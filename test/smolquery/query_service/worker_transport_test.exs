defmodule Smolquery.QueryService.WorkerTransportTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame
  alias Smolquery.QueryService.PartialWorker
  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.WorkerTransport
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.SegmentFixture

  @peer :"query1@elsewhere.invalid"
  @request %{
    table_ref: {"analytics", "events"},
    schema: Schema.new!([{"id", :int64}]),
    files: [],
    partial_sql: "SELECT 1",
    allowed_paths: []
  }

  describe "call/5" do
    test "runs a local shard as a direct call against this node's runtime" do
      assert WorkerTransport.call(node(), :never_started, @request, "job", 5_000) ==
               {:error, :query_service_unavailable}
    end

    test "refuses a peer outside the cluster before any connect" do
      assert WorkerTransport.call(@peer, :never_started, @request, "job", 5_000) ==
               {:error, {:worker_unreachable, @peer, :nodedown}}
    end
  end

  describe "call/5 on a runner" do
    @describetag :tmp_dir

    setup %{tmp_dir: tmp_dir} do
      store = Store.Local.new(dir: tmp_dir)
      schema = Schema.new!([{"id", :int64}])

      {:ok, segment} =
        SegmentFixture.write([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}], schema,
          store: store,
          prefix: "analytics/events"
        )

      runtime =
        Runtime.new(
          name: :"worker_transport_runner_#{System.unique_integer([:positive])}",
          catalog: MapCatalog.new(),
          store: store,
          allowed_directories: [tmp_dir],
          engine_extensions: [:httpfs]
        )

      request = %{
        engine: PartialWorker.engine_spec(runtime),
        table_ref: {"analytics", "events"},
        schema: schema,
        files: [%{"url" => segment.path, "snapshot" => 1, "bytes" => segment.byte_size}],
        partial_sql: "SELECT sum(id) AS s FROM analytics.events",
        allowed_paths: []
      }

      %{request: request}
    end

    test "runs the shard standalone on a runner leased from the pool", %{request: request} do
      pool = :"runner_pool_#{System.unique_integer([:positive])}"

      start_supervised!(
        {FLAME.Pool, name: pool, backend: FLAME.LocalBackend, min: 0, max: 1, max_concurrency: 1}
      )

      target = %{kind: :runner, pool: pool}

      assert {:ok, %{rows: 1, parquet: parquet, timings: %{engine: :standalone}}} =
               WorkerTransport.call(target, :unused, request, "job", 30_000)

      assert DataFrame.to_columns(DataFrame.load_parquet!(parquet))["s"] == [6]
    end

    test "a pool that is not running is an unreachable runner, not an exit", %{request: request} do
      target = %{kind: :runner, pool: :"no_such_pool_#{System.unique_integer([:positive])}"}

      assert {:error, {:worker_unreachable, {:runner, pool}, :noproc}} =
               WorkerTransport.call(target, :unused, request, "job", 5_000)

      assert pool == target.pool
    end

    test "a checkout that waits past the timeout is an unreachable runner", %{request: request} do
      pool = :"runner_pool_busy_#{System.unique_integer([:positive])}"

      start_supervised!(
        {FLAME.Pool, name: pool, backend: FLAME.LocalBackend, min: 0, max: 1, max_concurrency: 1}
      )

      parent = self()

      holder =
        spawn_link(fn ->
          FLAME.call(pool, fn ->
            send(parent, :held)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :held, 5_000

      assert {:error, {:worker_unreachable, {:runner, ^pool}, :timeout}} =
               WorkerTransport.call(%{kind: :runner, pool: pool}, :unused, request, "job", 200)

      Process.unlink(holder)
      Process.exit(holder, :kill)
    end
  end

  describe "destination/2" do
    test "a job always lands on the same scatter slot" do
      assert {@peer, {:scatter, slot}} = WorkerTransport.destination(@peer, "job-1")
      assert slot in 1..WorkerTransport.channels()
      assert WorkerTransport.destination(@peer, "job-1") == {@peer, {:scatter, slot}}
    end

    test "distinct jobs spread over the pool" do
      slots =
        for i <- 1..64, uniq: true do
          {@peer, {:scatter, slot}} = WorkerTransport.destination(@peer, "job-#{i}")
          slot
        end

      assert [_, _ | _] = slots
    end
  end
end
