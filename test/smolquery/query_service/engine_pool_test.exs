defmodule Smolquery.QueryService.EnginePoolTest do
  @moduledoc """
  The pool's contract (PL-50): it fills to `warm_engines` ahead of demand,
  hands ownership over at checkout and refills, drops an engine that dies
  while warm, and never blocks a job — an empty pool says so, and
  `JobEngine.acquire/1` starts cold. The telemetry event is how a
  deployment sees the warm/cold split, so the tests assert it too.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.EnginePool
  alias Smolquery.QueryService.JobEngine
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

  defp start_service!(opts) do
    name = :"engine_pool_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       [
         name: name,
         catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
         engine_extensions: []
       ] ++ opts},
      id: name
    )

    on_exit(fn -> QueryService.Runtime.delete(name) end)

    name
  end

  defp attach_telemetry do
    parent = self()
    handler = "engine-pool-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:smolquery, :query, :engine],
      fn _event, measurements, meta, _config -> send(parent, {:engine, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  test "fills to warm_engines and refills after a checkout" do
    name = start_service!(warm_engines: 2)

    assert Eventually.until(fn -> EnginePool.size(name) == 2 end)

    assert {:ok, engine} = EnginePool.checkout(name)
    assert Process.alive?(engine.connection)
    assert Process.alive?(engine.database)
    assert {:ok, _result} = Connection.query(engine.connection, "SELECT 1")

    assert Eventually.until(fn -> EnginePool.size(name) == 2 end)

    JobEngine.stop(engine)
  end

  test "a pool with warm_engines: 0 holds nothing, answers :empty, and acquire starts cold" do
    name = start_service!(warm_engines: 0)
    attach_telemetry()
    {:ok, runtime} = Runtime.fetch(name)

    assert EnginePool.size(name) == 0
    assert EnginePool.checkout(name) == :empty
    assert {:ok, engine, :cold} = JobEngine.acquire(runtime)
    assert_received {:engine, %{duration_us: _us}, %{source: :cold}}

    JobEngine.stop(engine)
  end

  test "acquire takes a warm engine, links it to the caller, and reports the source" do
    name = start_service!(warm_engines: 1)
    attach_telemetry()
    {:ok, runtime} = Runtime.fetch(name)

    assert Eventually.until(fn -> EnginePool.size(name) == 1 end)
    assert {:ok, engine, :warm} = JobEngine.acquire(runtime)
    assert_received {:engine, _measurements, %{source: :warm}}

    {:links, links} = Process.info(self(), :links)
    assert engine.connection in links
    assert engine.database in links

    JobEngine.stop(engine)
  end

  test "an engine that dies while warm is dropped and replaced" do
    name = start_service!(warm_engines: 1)

    assert Eventually.until(fn -> EnginePool.size(name) == 1 end)
    assert {:ok, engine} = EnginePool.checkout(name)
    assert Eventually.until(fn -> EnginePool.size(name) == 1 end)

    {:ok, victim} = EnginePool.checkout(name)
    Process.exit(victim.connection, :kill)

    assert Eventually.until(fn -> EnginePool.size(name) == 1 end)
    assert {:ok, replacement} = EnginePool.checkout(name)
    refute replacement.connection == victim.connection

    Enum.each([engine, replacement], &JobEngine.stop/1)
  end

  test "a job served from the pool answers, and the next job finds a warm engine again" do
    name = start_service!(warm_engines: 1)
    attach_telemetry()

    assert Eventually.until(fn -> EnginePool.size(name) == 1 end)

    assert {:ok, %{state: :done}, frame} = Client.query(name, "SELECT 41 + 1 AS answer")
    assert Explorer.DataFrame.to_columns(frame)["answer"] == [42]
    assert_received {:engine, _measurements, %{source: :warm}}

    assert Eventually.until(fn -> EnginePool.size(name) == 1 end)
  end
end
