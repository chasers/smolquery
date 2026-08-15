defmodule Smolquery.StorageService.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Result
  alias Smolquery.StorageService
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.HandoffProbe

  @events {"analytics", "events"}

  setup context do
    name = :"storage_sup_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {StorageService.Supervisor,
       name: name,
       dir: Path.join(context.tmp_dir, "sealed"),
       handoff: {HandoffProbe, {self(), :ok}}}
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  @tag :tmp_dir
  test "publishes its runtime for the client to read", %{name: name} do
    assert {:ok, runtime} = Runtime.fetch(name)
    assert runtime.name == name
  end

  @tag :tmp_dir
  test "starts an engine, a task supervisor, and the sealer", %{name: name} do
    assert Process.whereis(Engine.connection_name(Runtime.engine(name)))
    assert Process.whereis(Runtime.seals(name))
    assert Process.whereis(Runtime.sealer(name))
  end

  @tag :tmp_dir
  test "the engine is ready to merge through", %{name: name} do
    assert {:ok, _result} = Engine.query(Runtime.engine(name), "SELECT 1")
  end

  @tag :tmp_dir
  test "the merge engine does not preserve insertion order (T-250)", %{name: name} do
    assert Runtime.engine(name)
           |> Engine.query!("SELECT current_setting('preserve_insertion_order')")
           |> Result.one!() == false
  end

  @tag :tmp_dir
  test "an explicit engine_memory_limit reaches the merge engine (T-250)", context do
    name = :"storage_sup_mem_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {StorageService.Supervisor,
       name: name,
       dir: Path.join(context.tmp_dir, "sealed-mem"),
       engine_memory_limit: "123MiB",
       handoff: {HandoffProbe, {self(), :ok}}},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    assert Runtime.engine(name)
           |> Engine.query!("SELECT current_setting('memory_limit')")
           |> Result.one!() =~ "123"
  end

  @tag :tmp_dir
  test "a seal signal reaches the handoff through the whole subtree", %{name: name} do
    Sealer.seal_ready(name, @events, %{ids: ["a"], keys: ["k"]})

    assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}
    HandoffProbe.release(attempt)
    assert Eventually.until(fn -> Sealer.sealing(name) == [] end)
  end

  @tag :tmp_dir
  test "a crashed sealer is replaced without disturbing the engine", %{name: name} do
    engine = Process.whereis(Engine.connection_name(Runtime.engine(name)))
    sealer = Process.whereis(Runtime.sealer(name))

    Process.exit(sealer, :kill)

    assert Eventually.until(fn ->
             pid = Process.whereis(Runtime.sealer(name))
             is_pid(pid) and pid != sealer
           end)

    assert Process.whereis(Engine.connection_name(Runtime.engine(name))) == engine
  end
end
