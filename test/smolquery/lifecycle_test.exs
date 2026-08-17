defmodule Smolquery.LifecycleTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Runs in the serial phase on purpose: these tests execute real
  `[:smolquery, :buffer, :commit]` (and friends) telemetry events, which bump
  the same node-wide counters `Smolquery.TelemetryTest` asserts exact deltas
  on. Concurrent with the async phase, those emissions race its
  snapshot-then-assert windows.
  """

  alias Smolquery.Lifecycle

  @table {"lifecycle_bridge", "events"}

  test "a seal attempt event broadcasts on the parent table's topic" do
    :ok = Lifecycle.subscribe(@table)

    :telemetry.execute(
      [:smolquery, :seal, :attempt],
      %{duration_us: 1_200_000, segments: 16},
      %{result: :ok, table_ref: {"lifecycle_bridge", "events__p1"}}
    )

    assert_receive {:lifecycle, event}
    assert event.kind == :seal
    assert event.result == :ok
    assert event.table_ref == {"lifecycle_bridge", "events__p1"}
    assert event.node == node()
    assert event.measurements.segments == 16
    assert is_integer(event.at)

    assert Smolquery.Telemetry.render() =~ ~s(smolquery_lifecycle_broadcasts_total{kind="seal"})
  end

  test "a commit and a compaction map to their kinds" do
    :ok = Lifecycle.subscribe(@table)

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: 100, bytes: 2_048},
      %{result: :ok, table_ref: @table}
    )

    assert_receive {:lifecycle, %{kind: :commit, measurements: %{rows: 100}}}

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 5, duration_us: 10},
      %{result: :error, table_ref: @table}
    )

    assert_receive {:lifecycle, %{kind: :compaction, result: :error}}
  end

  test "an event without a table_ref broadcasts nothing" do
    :ok = Lifecycle.subscribe(@table)

    :telemetry.execute([:smolquery, :seal, :attempt], %{duration_us: 1}, %{result: :ok})

    refute_receive {:lifecycle, _event}, 100
  end

  test "another table's events do not arrive" do
    :ok = Lifecycle.subscribe(@table)

    :telemetry.execute(
      [:smolquery, :buffer, :commit],
      %{rows: 1, bytes: 1},
      %{result: :ok, table_ref: {"lifecycle_bridge", "other"}}
    )

    refute_receive {:lifecycle, _event}, 100
  end
end
