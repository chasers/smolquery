defmodule Smolquery.StorageService.SealerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.HandoffProbe

  @events {"analytics", "events"}
  @clicks {"analytics", "clicks"}

  defp claim(ids), do: %{ids: ids, keys: ["analytics/events/sealed.parquet"]}

  defp run_attempt(name, table_ref) do
    Sealer.seal_ready(name, table_ref, claim(["a"]))
    assert_receive {:sealing, ^table_ref, _claim, attempt}
    HandoffProbe.release(attempt)

    assert Eventually.until(fn -> Sealer.sealing(name) == [] end)
  end

  defp run_attempt(name, table_ref, result) do
    Sealer.seal_ready(name, table_ref, claim(["a"]))
    assert_receive {:sealing, ^table_ref, _claim, attempt}
    HandoffProbe.release(attempt, result)

    assert Eventually.until(fn -> Sealer.sealing(name) == [] end)
  end

  setup context do
    name = :"sealer_#{:erlang.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        name: name,
        dir: Path.join(context.tmp_dir, "sealed"),
        max_concurrent_seals: Map.get(context, :max_concurrent_seals, 2),
        handoff: {HandoffProbe, {self(), Map.get(context, :result, :ok)}}
      )

    start_supervised!({Task.Supervisor, name: Runtime.seals(name)})
    start_supervised!({Sealer, runtime})

    %{name: name}
  end

  @tag :tmp_dir
  test "hands a signal to the configured handoff", %{name: name} do
    assert Sealer.seal_ready(name, @events, claim(["a", "b"])) == :ok

    assert_receive {:sealing, @events, %{ids: ["a", "b"]}, attempt}
    assert Sealer.sealing(name) == [@events]

    HandoffProbe.release(attempt)
    assert Eventually.until(fn -> Sealer.sealing(name) == [] end)
  end

  @tag :tmp_dir
  test "coalesces a second signal for a table already sealing", %{name: name} do
    Sealer.seal_ready(name, @events, claim(["a"]))
    assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}

    Sealer.seal_ready(name, @events, claim(["a", "b"]))

    refute_receive {:sealing, @events, %{ids: ["a", "b"]}, _attempt}, 50
    assert Sealer.sealing(name) == [@events]

    HandoffProbe.release(attempt)
  end

  @tag :tmp_dir
  test "seals distinct tables concurrently", %{name: name} do
    Sealer.seal_ready(name, @events, claim(["a"]))
    Sealer.seal_ready(name, @clicks, claim(["b"]))

    assert_receive {:sealing, @events, %{ids: ["a"]}, events_attempt}
    assert_receive {:sealing, @clicks, %{ids: ["b"]}, clicks_attempt}
    assert Enum.sort(Sealer.sealing(name)) == Enum.sort([@events, @clicks])

    HandoffProbe.release(events_attempt)
    HandoffProbe.release(clicks_attempt)
  end

  @tag :tmp_dir
  @tag max_concurrent_seals: 1
  test "sheds a signal that arrives at the concurrency bound", %{name: name} do
    Sealer.seal_ready(name, @events, claim(["a"]))
    assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}

    Sealer.seal_ready(name, @clicks, claim(["b"]))

    refute_receive {:sealing, @clicks, _claim, _attempt}, 50
    assert Sealer.sealing(name) == [@events]

    HandoffProbe.release(attempt)
    assert Eventually.until(fn -> Sealer.sealing(name) == [] end)

    Sealer.seal_ready(name, @clicks, claim(["b"]))
    assert_receive {:sealing, @clicks, %{ids: ["b"]}, shed_attempt}

    HandoffProbe.release(shed_attempt)
  end

  @tag :tmp_dir
  test "a table becomes eligible again after a failed attempt", %{name: name} do
    Sealer.seal_ready(name, @events, claim(["a"]))
    assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}
    HandoffProbe.release(attempt)
    assert Eventually.until(fn -> Sealer.sealing(name) == [] end)

    Sealer.seal_ready(name, @events, claim(["a", "b"]))
    assert_receive {:sealing, @events, %{ids: ["a", "b"]}, retry}

    HandoffProbe.release(retry)
  end

  @tag :tmp_dir
  @tag result: {:error, :handoff_refused}
  test "logs a failed attempt and frees the table", %{name: name} do
    log =
      capture_log(fn ->
        Sealer.seal_ready(name, @events, claim(["a"]))
        assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}
        HandoffProbe.release(attempt)
        assert Eventually.until(fn -> Sealer.sealing(name) == [] end)
      end)

    assert log =~ "failed"
    assert log =~ "handoff_refused"

    Sealer.seal_ready(name, @events, claim(["a"]))
    assert_receive {:sealing, @events, %{ids: ["a"]}, retry}

    HandoffProbe.release(retry)
  end

  @tag :tmp_dir
  @tag result: :crash
  test "survives a crashed attempt and frees the table", %{name: name} do
    sealer = Process.whereis(Runtime.sealer(name))

    log =
      capture_log(fn ->
        Sealer.seal_ready(name, @events, claim(["a"]))
        assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}
        HandoffProbe.release(attempt)
        assert Eventually.until(fn -> Sealer.sealing(name) == [] end)
      end)

    assert log =~ "crashed"
    assert Process.alive?(sealer)

    Sealer.seal_ready(name, @events, claim(["a"]))
    assert_receive {:sealing, @events, %{ids: ["a"]}, retry}

    HandoffProbe.release(retry)
  end

  @tag :tmp_dir
  @tag result: {:error, :handoff_refused}
  test "counts consecutive failures per table", %{name: name} do
    capture_log(fn ->
      run_attempt(name, @events)
      run_attempt(name, @events)
      run_attempt(name, @clicks)
    end)

    assert Sealer.failures(name) == %{@events => 2, @clicks => 1}
  end

  @tag :tmp_dir
  @tag result: {:error, :handoff_refused}
  test "clears a table's failures once an attempt succeeds", %{name: name} do
    capture_log(fn -> run_attempt(name, @events) end)
    assert Sealer.failures(name) == %{@events => 1}

    run_attempt(name, @events, :ok)
    assert Sealer.failures(name) == %{}
  end

  @tag :tmp_dir
  @tag result: :crash
  test "counts a crashed attempt as a failure", %{name: name} do
    capture_log(fn -> run_attempt(name, @events) end)

    assert Sealer.failures(name) == %{@events => 1}
  end

  @tag :tmp_dir
  @tag result: {:error, :handoff_refused}
  test "escalates to an error once a table stops looking transient", %{name: name} do
    log = capture_log(fn -> Enum.each(1..4, fn _ -> run_attempt(name, @events) end) end)

    refute log =~ "may never seal"

    log = capture_log(fn -> run_attempt(name, @events) end)

    assert log =~ "[error]"
    assert log =~ "5 consecutive failures"
    assert log =~ "may never seal"
  end

  @tag :tmp_dir
  @tag result: {:error, :handoff_refused}
  test "emits the stuck event at the threshold, not before (T-293)", %{name: name} do
    handler = "stuck-#{:erlang.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler,
        [:smolquery, :seal, :stuck],
        fn _event, measurements, meta, nil -> send(parent, {:stuck, measurements, meta}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn -> Enum.each(1..4, fn _ -> run_attempt(name, @events) end) end)
    refute_received {:stuck, _measurements, _meta}

    capture_log(fn -> run_attempt(name, @events) end)
    assert_receive {:stuck, %{consecutive: 5}, %{table_ref: @events}}

    capture_log(fn -> run_attempt(name, @events) end)
    assert_receive {:stuck, %{consecutive: 6}, %{table_ref: @events}}
  end

  @tag :tmp_dir
  test "ignores an unrelated message", %{name: name} do
    sealer = Process.whereis(Runtime.sealer(name))
    send(sealer, :unrelated)

    assert Sealer.sealing(name) == []
    assert Process.alive?(sealer)
  end

  describe "storage-ring ownership (Milestone 8 L6)" do
    setup context do
      name = :"sealer_owned_#{:erlang.unique_integer([:positive])}"

      runtime =
        Runtime.new(
          name: name,
          dir: Path.join(context.tmp_dir, "sealed"),
          ring: [node(), :"storage1@elsewhere.invalid"],
          handoff: {HandoffProbe, {self(), :ok}}
        )

      start_supervised!({Task.Supervisor, name: Runtime.seals(name)}, id: {:seals, name})
      start_supervised!({Sealer, runtime}, id: {:sealer, name})
      Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(name) end)

      %{name: name}
    end

    @tag :tmp_dir
    test "a signal for a table this node's ring hands to someone else is ignored", %{name: name} do
      foreign_table = find_table_owned_by(name, :"storage1@elsewhere.invalid")

      assert Sealer.seal_ready(name, foreign_table, claim(["a"])) == :ok

      refute_receive {:sealing, ^foreign_table, _claim, _attempt}, 50
      assert Sealer.sealing(name) == []
    end

    @tag :tmp_dir
    test "a signal for a table this node owns still runs", %{name: name} do
      own_table = find_table_owned_by(name, node())

      assert Sealer.seal_ready(name, own_table, claim(["a"])) == :ok

      assert_receive {:sealing, ^own_table, %{ids: ["a"]}, attempt}
      HandoffProbe.release(attempt)
    end
  end

  defp find_table_owned_by(name, target_node) do
    routing = Routing.resolve(name)

    Enum.find(1..1_000, fn i ->
      Routing.owner(routing, {"analytics", "t#{i}"}) == target_node
    end)
    |> then(&{"analytics", "t#{&1}"})
  end
end
