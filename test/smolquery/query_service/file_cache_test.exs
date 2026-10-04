defmodule Smolquery.QueryService.FileCacheTest do
  use ExUnit.Case, async: false

  alias Smolquery.QueryService.FileCache
  alias Smolquery.Telemetry
  alias Smolquery.Test.FileCacheFixture

  @moduletag :tmp_dir

  defp block!(dir, name, bytes, age_s) do
    path = Path.join(dir, name)
    File.write!(path, :binary.copy("x", bytes))
    mtime = System.os_time(:second) - age_s
    File.touch!(path, mtime)
    path
  end

  test "under the cap, deletes nothing", %{tmp_dir: dir} do
    block!(dir, "a", 400, 100)
    block!(dir, "b", 400, 50)

    assert FileCache.sweep(dir, 1_000) == %{bytes: 800, evicted_bytes: 0, evicted_files: 0}
    assert File.ls!(dir) |> Enum.sort() == ["a", "b"]
  end

  test "over the cap, deletes the oldest blocks until under 90% of it", %{tmp_dir: dir} do
    block!(dir, "oldest", 300, 300)
    block!(dir, "older", 300, 200)
    block!(dir, "newer", 300, 100)
    block!(dir, "newest", 300, 60)

    assert FileCache.sweep(dir, 1_000) == %{bytes: 900, evicted_bytes: 300, evicted_files: 1}
    assert File.ls!(dir) |> Enum.sort() == ["newer", "newest", "older"]

    assert FileCache.sweep(dir, 700) == %{bytes: 600, evicted_bytes: 300, evicted_files: 1}
    assert File.ls!(dir) |> Enum.sort() == ["newer", "newest"]
  end

  test "never deletes a block younger than the minimum age", %{tmp_dir: dir} do
    block!(dir, "old", 300, 300)
    block!(dir, "fresh", 900, 1)

    result = FileCache.sweep(dir, 1_000, System.os_time(:millisecond), 10_000)

    assert result == %{bytes: 900, evicted_bytes: 300, evicted_files: 1}
    assert File.ls!(dir) == ["fresh"]
  end

  test "ignores what is not a regular file, and a missing directory is empty", %{tmp_dir: dir} do
    File.mkdir_p!(Path.join(dir, "sub"))
    block!(dir, "a", 10, 100)

    assert FileCache.sweep(dir, 1_000).bytes == 10
    assert FileCache.sweep(Path.join(dir, "absent"), 1_000).bytes == 0
  end

  test "a directory it cannot create leaves the janitor stopped, not the service", %{
    tmp_dir: dir
  } do
    blocker = Path.join(dir, "a-file")
    File.write!(blocker, "")

    assert {:ok, :undefined} =
             start_supervised(
               {FileCache,
                {:file_cache_unwritable, %{directory: Path.join(blocker, "cache"), max_bytes: 1}}}
             )
  end

  test "the process creates the directory, sweeps it, and reports its size", %{tmp_dir: dir} do
    directory = Path.join(dir, "cache")
    parent = self()
    handler = "file-cache-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:smolquery, :query, :file_cache, :sweep],
      fn _event, measurements, _meta, _config -> send(parent, {:swept, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    start_supervised!(
      {FileCache,
       {:"file_cache_#{System.unique_integer([:positive])}",
        %{directory: directory, max_bytes: 1_000}}}
    )

    assert_receive {:swept, %{evicted_bytes: 0, evicted_files: 0}}
    assert File.dir?(directory)
    assert Telemetry.render() =~ "smolquery_query_file_cache_bytes 0"
  end

  describe "which jobs use the cache (T-629)" do
    @auto %{directory: "/cache", mode: :auto, bypass_bytes: 1_000}

    test "decide/2: no directory is no decision; a forced job does as it says" do
      assert FileCache.decide(%{@auto | directory: nil}, 5_000) == nil
      assert FileCache.decide(%{@auto | mode: true}, 5_000) == :used
      assert FileCache.decide(%{@auto | mode: false}, 0) == :off
    end

    test "decide/2: auto skips only a scan whose never-cached files pass the threshold" do
      assert FileCache.decide(@auto, 1_000) == :used
      assert FileCache.decide(@auto, 1_001) == :bypassed
      assert FileCache.decide(@auto, 0) == :used
    end

    test "decision/2 reads the plan's uncached bytes" do
      plan = %Smolquery.QueryService.Plan{
        sql: "SELECT 1",
        snapshot: 1,
        sealed_uncached_bytes: 5_000
      }

      assert FileCache.decision(@auto, plan) == :bypassed
      assert FileCache.decision(@auto, %{plan | sealed_uncached_bytes: 10}) == :used
    end

    test "statements/1 switches the cache off for a skipped job only" do
      assert FileCache.statements(:bypassed) == ["SET cache_httpfs_type = 'noop'"]
      assert FileCache.statements(:off) == ["SET cache_httpfs_type = 'noop'"]
      assert FileCache.statements(:used) == []
      assert FileCache.statements(nil) == []
    end

    test "the sweep indexes cached bytes by sealed file name", %{tmp_dir: dir} do
      name = :"file_cache_index_#{System.unique_integer([:positive])}"
      hash = String.duplicate("ab", 32)
      block!(dir, "#{hash}-01ABC.parquet-0-524288", 300, 100)
      block!(dir, "#{hash}-01ABC.parquet-524288-524288", 200, 100)
      block!(dir, "#{String.duplicate("cd", 32)}-01DEF.parquet-0-524288", 50, 100)
      block!(dir, "#{hash}-01ABC.parquet-0-524288.0f3a.httpfs_local_cache", 999, 100)

      parent = self()
      handler = "file-cache-index-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:smolquery, :query, :file_cache, :sweep],
        fn _event, _measurements, _meta, _config -> send(parent, :swept) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      start_supervised!({FileCache, {name, %{directory: dir, max_bytes: 1_000_000}}})
      assert_receive :swept

      assert FileCache.cached_bytes(name, ["01ABC.parquet"]) == 500
      assert FileCache.cached_bytes(name, MapSet.new(["01ABC.parquet", "01DEF.parquet"])) == 550
      assert FileCache.cached_bytes(name, ["01XYZ.parquet"]) == 0
      assert FileCache.cached_bytes(:no_such_instance, ["01ABC.parquet"]) == 0
      assert FileCache.cached?(name, "01ABC.parquet")
      refute FileCache.cached?(name, "01XYZ.parquet")
    end

    test "combined/1: a scattered job used the cache when any shard did" do
      assert FileCache.combined([:bypassed, :used, :bypassed]) == :used
      assert FileCache.combined([:bypassed, :bypassed]) == :bypassed
      assert FileCache.combined([:off, :off]) == :off
      assert FileCache.combined([nil, :bypassed]) == :bypassed
      assert FileCache.combined([nil, nil]) == nil
    end

    test "shard_decision/3 weighs the node's sealed files against this node's index",
         %{tmp_dir: dir} do
      name = :"file_cache_shard_#{System.unique_integer([:positive])}"
      block!(dir, FileCacheFixture.block_name("01WARM.parquet"), 10, 100)

      parent = self()
      handler = "file-cache-shard-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:smolquery, :query, :file_cache, :sweep],
        fn _event, _measurements, _meta, _config -> send(parent, :swept) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      start_supervised!({FileCache, {name, %{directory: dir, max_bytes: 1_000_000}}})
      assert_receive :swept

      auto = %{directory: dir, mode: :auto, bypass_bytes: 1_000}
      warm = %{"url" => "s3://lake/t/01WARM.parquet", "bytes" => 5_000}
      cold = %{"url" => "s3://lake/t/01COLD.parquet", "bytes" => 600}
      colder = %{"url" => "s3://lake/t/01COLDER.parquet", "bytes" => 500}

      assert FileCache.shard_decision(name, auto, [warm]) == :used
      assert FileCache.shard_decision(name, auto, [warm, cold]) == :used
      assert FileCache.shard_decision(name, auto, [warm, cold, colder]) == :bypassed
      assert FileCache.shard_decision(name, auto, []) == :used
      assert FileCache.shard_decision(name, %{auto | mode: true}, [cold, colder]) == :used
      assert FileCache.shard_decision(name, %{auto | mode: false}, [warm]) == :off
      assert FileCache.shard_decision(name, %{auto | directory: nil}, [cold, colder]) == nil
    end
  end
end
