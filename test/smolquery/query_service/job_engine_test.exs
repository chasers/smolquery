defmodule Smolquery.QueryService.JobEngineTest do
  @moduledoc """
  One engine's lifecycle: `start/1` with `options/1`, the link and unlink
  that move ownership, `stop/1`, and `probe/2` (T-548) — `:ok` when the
  engine answers, `{:stale, _}` when the statement errors or the connection
  is gone — with the `[:smolquery, :query, :engine_probe]` event for each.
  `acquire/1`'s warm and cold paths are covered with the pool, in
  `Smolquery.QueryService.EnginePoolTest`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService.JobEngine
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.FixedCatalog

  setup do
    parent = self()
    handler = "job-engine-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:smolquery, :query, :engine_probe],
      fn _event, measurements, meta, _config -> send(parent, {:probe, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    runtime =
      Runtime.new(
        name: :"job_engine_#{System.unique_integer([:positive])}",
        catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
        engine_extensions: [],
        warm_engines: 0
      )

    {:ok, engine} = JobEngine.start(JobEngine.options(runtime))
    on_exit(fn -> JobEngine.stop(engine) end)

    %{runtime: runtime, engine: engine}
  end

  test "options/1 composes the bootstrap", %{runtime: runtime} do
    options = JobEngine.options(runtime)

    assert Keyword.keys(options) == [:extensions, :settings, :statements, :max_rows]
    assert options[:settings] == [memory_limit: runtime.job_memory_limit]
    assert options[:max_rows] == :infinity
  end

  test "options/1 loads no cache unless the runtime names a file cache directory", %{
    runtime: runtime
  } do
    options = JobEngine.options(runtime)

    refute {:cache_httpfs, :community} in options[:extensions]
    refute Enum.any?(options[:statements], &String.contains?(&1, "cache_httpfs"))
  end

  test "options/1 loads cache_httpfs last and points it at the file cache (T-626)", %{
    runtime: runtime
  } do
    cached = %{runtime | file_cache: %{directory: "/tmp/it's cache", max_bytes: 1_000}}
    options = JobEngine.options(cached)

    assert List.last(options[:extensions]) == {:cache_httpfs, :community}

    assert Enum.take(options[:statements], 7) == [
             "SET cache_httpfs_type = 'on_disk'",
             "SET cache_httpfs_cache_directory = '/tmp/it''s cache'",
             "SET cache_httpfs_disk_cache_reader_enable_memory_cache = false",
             "SET cache_httpfs_max_fanout_subrequest = 8",
             "SET cache_httpfs_min_disk_bytes_for_cache = 1073741824",
             "SET enable_external_file_cache = true",
             "SELECT cache_httpfs_add_exclusion_regex('^https?://')"
           ]
  end

  @tag :integration
  @tag :tmp_dir
  test "an engine with the file cache installs cache_httpfs from community and configures it",
       %{runtime: runtime, tmp_dir: dir} do
    cached = %{
      runtime
      | engine_extensions: [:httpfs],
        file_cache: %{directory: dir, max_bytes: 1_000_000}
    }

    {:ok, engine} = JobEngine.start(JobEngine.options(cached))
    on_exit(fn -> JobEngine.stop(engine) end)

    {:ok, setting} =
      Connection.query(
        engine.connection,
        "SELECT current_setting('cache_httpfs_cache_directory') AS d",
        []
      )

    assert setting.rows == [[dir]]

    {:ok, file_cache} =
      Connection.query(
        engine.connection,
        "SELECT current_setting('enable_external_file_cache') AS on",
        []
      )

    assert file_cache.rows == [[true]]

    {:ok, excluded} =
      Connection.query(
        engine.connection,
        "SELECT * FROM cache_httpfs_list_exclusion_regex()",
        []
      )

    assert excluded.rows == [["^https?://"]]
  end

  test "start/1 links both processes to the caller; unlink/1 and link/1 move them", %{
    engine: engine
  } do
    assert linked?(engine)

    assert JobEngine.unlink(engine) == :ok
    refute linked?(engine)

    assert JobEngine.link(engine) == :ok
    assert linked?(engine)
  end

  test "stop/1 kills both processes without an exit reaching the caller", %{engine: engine} do
    assert JobEngine.stop(engine) == :ok

    refute Process.alive?(engine.connection)
    refute Process.alive?(engine.database)
    assert JobEngine.stop(nil) == :ok
  end

  test "probe/2 answers :ok for an engine that runs the statement", context do
    assert JobEngine.probe(context.runtime, context.engine) == :ok
    assert_received {:probe, %{duration_us: _us}, %{outcome: :ok}}
  end

  test "probe/2 reports a statement the engine refuses as stale", context do
    runtime = %{context.runtime | warm_probe: "SELECT n FROM no_such_table"}

    assert {:stale, %Adbc.Error{}} = JobEngine.probe(runtime, context.engine)
    assert_received {:probe, _measurements, %{outcome: :stale}}
  end

  test "probe/2 reports a dead connection as stale rather than exiting", context do
    JobEngine.unlink(context.engine)
    Process.exit(context.engine.connection, :kill)
    refute Process.alive?(context.engine.connection)

    assert {:stale, {:exit, _reason}} = JobEngine.probe(context.runtime, context.engine)
    assert_received {:probe, _measurements, %{outcome: :stale}}
  end

  defp linked?(engine) do
    {:links, links} = Process.info(self(), :links)

    engine.connection in links and engine.database in links
  end
end
