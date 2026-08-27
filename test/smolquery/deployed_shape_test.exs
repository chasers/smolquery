defmodule Smolquery.DeployedShapeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.BufferService.Runtime, as: BufferRuntime
  alias Smolquery.DeployedShape
  alias Smolquery.IngestService.Runtime, as: IngestRuntime
  alias Smolquery.StorageService.Runtime, as: StorageRuntime

  # The shape lines are info, and the test environment runs Logger at :warning.
  # The point of this module is that the lines exist, so the tests have to be
  # able to see them.
  setup do
    level = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: level) end)

    :ok
  end

  defp buffer_runtime(overrides \\ []) do
    dir = Path.join(System.tmp_dir!(), "deployed-shape-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)

    BufferRuntime.new(
      [name: :"shape_#{System.unique_integer([:positive])}", dir: dir] ++ overrides
    )
  end

  defp storage_runtime(overrides) do
    StorageRuntime.new(
      [name: :"storage_shape_#{System.unique_integer([:positive])}"] ++ overrides
    )
  end

  describe "announce/1 for storage" do
    test "states the seal concurrency and the merge's call budgets (T-335)" do
      log = capture_log(fn -> DeployedShape.announce(storage_runtime([])) end)

      assert log =~ "storage shape:"
      assert log =~ "max_concurrent_seals=2"
      assert log =~ "merge_inputs_per_call=12"
      assert log =~ "merge_copy_timeout_ms=300000"
      assert log =~ "merge_staging_timeout_ms=120000"
      assert log =~ "merge_describe_timeout_ms=120000"
      assert log =~ "seal_backoff_base_ms=30000"
      assert log =~ "seal_backoff_max_ms=600000"
    end

    # The two limits an operator cannot read off their own configuration: each
    # derives from the cgroup limit when nothing configures one (T-250).
    test "states both engine memory limits resolved" do
      log =
        capture_log(fn ->
          DeployedShape.announce(
            storage_runtime(
              engine_memory_limit: "6GiB",
              compact_engine_memory_limit: "1GiB"
            )
          )
        end)

      assert log =~ "merge_engine_memory_limit=6GiB"
      assert log =~ "compact_engine_memory_limit=1GiB"
    end

    test "names the store the sealed tier came up on" do
      log = capture_log(fn -> DeployedShape.announce(storage_runtime(dir: "/tmp/sealed")) end)

      assert log =~ "store=Smolquery.Segments.Store.Local"
    end

    test "renders as a gauge pinned at 1, with the shape in the labels" do
      capture_log(fn -> DeployedShape.announce(storage_runtime([])) end)

      rendered = Smolquery.Telemetry.render()

      assert rendered =~ "# TYPE smolquery_storage_shape_info gauge"
      assert rendered =~ ~s(max_concurrent_seals="2")
      assert rendered =~ "smolquery_storage_shape_info{"
    end
  end

  describe "announce/1 for the buffer" do
    test "states the resolved path rather than what was configured" do
      log = capture_log(fn -> DeployedShape.announce(buffer_runtime()) end)

      assert log =~ "buffer shape:"
      assert log =~ "compression=zstd"
      assert log =~ "flush_idle_interval_ms=5"
      assert log =~ "commit_siblings=0"
      assert log =~ "transport_tls=false"
    end

    # The two values an operator cannot read off their own configuration: the
    # per-member thread count is a division nothing wrote down, and the
    # per-member memory limit is inherited from `Smolquery.Engine` when the
    # buffer config says nothing.
    test "states the write pool's resolved per-member budget" do
      log =
        capture_log(fn ->
          DeployedShape.announce(buffer_runtime(write_pool_size: 2))
        end)

      assert log =~ "write_pool_size=2"
      assert log =~ "write_engine_threads=1"
      assert log =~ "write_engine_memory_limit=512MB"
    end

    # What one seal claim may freeze is a product of two settings, so neither
    # one alone reads off the configuration (T-335).
    test "states the claim valves the seal triggers multiply out to" do
      log =
        capture_log(fn ->
          DeployedShape.announce(
            buffer_runtime(seal_max_files: 64, seal_max_bytes: 1_000, claim_valve_factor: 2)
          )
        end)

      assert log =~ "claim_valve_factor=2"
      assert log =~ "claim_max_files=128"
      assert log =~ "claim_max_bytes=2000"
    end

    test "states an explicit member budget over the derived one" do
      log =
        capture_log(fn ->
          DeployedShape.announce(
            buffer_runtime(
              write_pool_size: 4,
              write_engine_threads: 2,
              write_engine_memory_limit: "256MB"
            )
          )
        end)

      assert log =~ "write_engine_threads=2"
      assert log =~ "write_engine_memory_limit=256MB"
    end
  end

  describe "announce/1 for the ingest edge" do
    test "logs the edge's shape" do
      runtime = %IngestRuntime{name: :t, catalog: nil}

      log = capture_log(fn -> DeployedShape.announce(runtime) end)

      assert log =~ "ingest shape:"
      assert log =~ "write_partitions=1"
    end
  end

  describe "the info gauge" do
    setup do
      table = Smolquery.Telemetry

      if :ets.whereis(table) == :undefined do
        :ets.new(table, [:ordered_set, :public, :named_table])
        on_exit(fn -> :ets.whereis(table) != :undefined && :ets.delete(table) end)
      end

      :ok
    end

    test "renders as a gauge pinned at 1, with the shape in the labels" do
      capture_log(fn -> DeployedShape.announce(buffer_runtime()) end)

      rendered = Smolquery.Telemetry.render()

      assert rendered =~ "# TYPE smolquery_buffer_shape_info gauge"
      assert rendered =~ ~s(compression="zstd")
      assert rendered =~ "smolquery_buffer_shape_info{"
    end

    # Counts the whole family rather than asserting exactly one, because a
    # running application announces its own shape into the same table.
    test "re-registering the same shape does not accumulate series" do
      runtime = buffer_runtime()

      capture_log(fn -> DeployedShape.announce(runtime) end)
      before = shape_series()

      capture_log(fn -> DeployedShape.announce(runtime) end)

      assert shape_series() == before
    end

    defp shape_series do
      Smolquery.Telemetry.render()
      |> String.split("\n")
      |> Enum.count(&String.starts_with?(&1, "smolquery_buffer_shape_info{"))
    end
  end

  describe "put_info/2" do
    # The table is node-wide and this test destroys it, so it has to put the
    # aggregator back. Without the restart, every later test that renders or
    # bumps a counter raises on a table that no longer exists — and the
    # metrics handler detaches itself on the way, silently stopping every
    # count for the rest of the run. ExUnit shuffles within a module, so which
    # tests that hits is a function of the seed.
    test "never raises when no aggregator is running" do
      on_exit(&restart_aggregator/0)

      :ets.whereis(Smolquery.Telemetry) != :undefined && :ets.delete(Smolquery.Telemetry)

      assert Smolquery.Telemetry.put_info("smolquery_buffer_shape_info", a: 1) == :ok
    end

    defp restart_aggregator do
      Supervisor.terminate_child(Smolquery.Supervisor, Smolquery.Telemetry)
      {:ok, _pid} = Supervisor.restart_child(Smolquery.Supervisor, Smolquery.Telemetry)

      :ok
    end
  end
end
