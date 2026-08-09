defmodule Smolquery.DeployedShapeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.BufferService.Runtime, as: BufferRuntime
  alias Smolquery.DeployedShape
  alias Smolquery.IngestService.Runtime, as: IngestRuntime

  # The shape lines are info, and the test environment runs Logger at :warning.
  # The point of this module is that the lines exist, so the tests have to be
  # able to see them.
  setup do
    level = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: level) end)

    :ok
  end

  defp buffer_runtime(overrides) do
    dir = Path.join(System.tmp_dir!(), "deployed-shape-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)

    BufferRuntime.new([name: :"shape_#{System.unique_integer([:positive])}", dir: dir] ++ overrides)
  end

  describe "announce/1 for the buffer" do
    test "states the resolved path rather than what was configured" do
      log = capture_log(fn -> DeployedShape.announce(buffer_runtime(flush_writer: :duckdb)) end)

      assert log =~ "buffer shape:"
      assert log =~ "flush_writer=duckdb"
      assert log =~ "transport_tls=false"
    end

    test "warns when the slower writer is selected, because nothing else will" do
      log = capture_log(fn -> DeployedShape.announce(buffer_runtime(flush_writer: :polars)) end)

      assert log =~ "slow path"
      assert log =~ ":polars"
      assert log =~ "SMOLQUERY_FLUSH_WRITER=duckdb"
    end

    test "stays quiet on the fast path" do
      log = capture_log(fn -> DeployedShape.announce(buffer_runtime(flush_writer: :duckdb)) end)

      refute log =~ "slow path"
    end
  end

  describe "announce/1 for the ingest edge" do
    test "warns when the passthrough is off" do
      runtime = %IngestRuntime{name: :t, catalog: nil, ndjson_passthrough: false}

      log = capture_log(fn -> DeployedShape.announce(runtime) end)

      assert log =~ "slow path"
      assert log =~ "parsed and cast row"
    end

    test "stays quiet when the passthrough is on" do
      runtime = %IngestRuntime{name: :t, catalog: nil, ndjson_passthrough: true}

      log = capture_log(fn -> DeployedShape.announce(runtime) end)

      assert log =~ "ingest shape:"
      refute log =~ "slow path"
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
      capture_log(fn -> DeployedShape.announce(buffer_runtime(flush_writer: :duckdb)) end)

      rendered = Smolquery.Telemetry.render()

      assert rendered =~ "# TYPE smolquery_buffer_shape_info gauge"
      assert rendered =~ ~s(flush_writer="duckdb")
      assert rendered =~ "smolquery_buffer_shape_info{"
    end

    test "re-registering replaces rather than accumulating series" do
      capture_log(fn ->
        DeployedShape.announce(buffer_runtime(flush_writer: :duckdb))
        DeployedShape.announce(buffer_runtime(flush_writer: :duckdb))
      end)

      lines =
        Smolquery.Telemetry.render()
        |> String.split("\n")
        |> Enum.count(&String.starts_with?(&1, "smolquery_buffer_shape_info{"))

      assert lines == 1
    end
  end

  describe "put_info/2" do
    test "never raises when no aggregator is running" do
      :ets.whereis(Smolquery.Telemetry) != :undefined && :ets.delete(Smolquery.Telemetry)

      assert Smolquery.Telemetry.put_info("smolquery_buffer_shape_info", a: 1) == :ok
    end
  end
end
