defmodule Smolquery.QueryService.FileCacheTest do
  use ExUnit.Case, async: false

  alias Smolquery.QueryService.FileCache
  alias Smolquery.Telemetry

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

    start_supervised!({FileCache, %{directory: directory, max_bytes: 1_000}})

    assert_receive {:swept, %{evicted_bytes: 0, evicted_files: 0}}
    assert File.dir?(directory)
    assert Telemetry.render() =~ "smolquery_query_file_cache_bytes 0"
  end
end
