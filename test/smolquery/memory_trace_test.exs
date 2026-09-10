defmodule Smolquery.MemoryTraceTest do
  use ExUnit.Case, async: false

  alias Smolquery.CgroupMemory
  alias Smolquery.MemoryTrace
  alias Smolquery.Telemetry
  alias Smolquery.Test.Eventually

  @moduletag :tmp_dir

  @mib 1_048_576

  defp fake_cgroup(dir, current_mib, opts \\ []) do
    File.write!(Path.join(dir, "memory.max"), "#{Keyword.get(opts, :limit_mib, 1000) * @mib}\n")
    File.write!(Path.join(dir, "memory.current"), "#{current_mib * @mib}\n")

    File.write!(
      Path.join(dir, "memory.stat"),
      "anon #{Keyword.get(opts, :anon_mib, current_mib - 10) * @mib}\nfile #{10 * @mib}\n" <>
        "kernel 0\nslab #{3 * @mib}\n"
    )

    File.write!(
      Path.join(dir, "memory.events"),
      "low 0\nhigh 0\nmax #{Keyword.get(opts, :max_events, 0)}\noom 0\noom_kill 0\n"
    )

    dir
  end

  defp lines(path), do: path |> File.read!() |> String.split("\n", trim: true)

  describe "CgroupMemory.usage/1" do
    test "reads the v2 charge, its split, and the event counters", %{tmp_dir: dir} do
      fake_cgroup(dir, 500, max_events: 7)

      assert {:ok, usage} = CgroupMemory.usage(dir)
      assert usage.current == 500 * @mib
      assert usage.anon == 490 * @mib
      assert usage.file == 10 * @mib
      assert usage.slab == 3 * @mib
      assert usage.max_events == 7
      assert usage.oom_kill_events == 0
    end

    test "falls back to v1, and to :none without a cgroup filesystem", %{tmp_dir: dir} do
      v1 = Path.join(dir, "memory")
      File.mkdir_p!(v1)
      File.write!(Path.join(v1, "memory.usage_in_bytes"), "#{200 * @mib}\n")
      File.write!(Path.join(v1, "memory.stat"), "cache #{20 * @mib}\nrss #{180 * @mib}\n")
      File.write!(Path.join(v1, "memory.failcnt"), "3\n")

      assert {:ok, usage} = CgroupMemory.usage(dir)
      assert usage.current == 200 * @mib
      assert usage.anon == 180 * @mib
      assert usage.file == 20 * @mib
      assert usage.max_events == 3
      assert usage.slab == nil

      assert CgroupMemory.usage(Path.join(dir, "nowhere")) == :none
    end
  end

  describe "the tracer" do
    test "is :ignore unless enabled" do
      assert MemoryTrace.start_link(enabled: false, name: :t451_off) == :ignore
    end

    test "records the start, new peaks, samples above the threshold, and a heartbeat",
         %{tmp_dir: dir} do
      cgroup = fake_cgroup(Path.join(dir, "cg") |> tap(&File.mkdir_p!/1), 500)
      path = Path.join(dir, "trace/memory-trace.log")

      start_supervised!(
        {MemoryTrace,
         enabled: true,
         name: :t451_trace,
         cgroup_root: cgroup,
         path: path,
         interval_ms: 10,
         heartbeat_ms: 60_000,
         publish: true}
      )

      assert Eventually.until(fn -> File.exists?(path) and Enum.count(lines(path)) >= 2 end)
      [start, first | _rest] = lines(path)
      assert start =~ "start limit=1000.0 above=800.0 interval_ms=10"
      assert first =~ " peak,beat cur=500.0 anon=490.0 file=10.0 slab=3.0 rss="
      assert first =~ " max_events=0 oom_kill=0"

      # A lower reading is neither a peak nor above the threshold: nothing lands.
      File.write!(Path.join(cgroup, "memory.current"), "#{400 * @mib}\n")
      Process.sleep(50)
      assert Enum.count(lines(path)) == 2

      # A new peak above 80% of the limit lands, and warns with the top processes.
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          File.write!(Path.join(cgroup, "memory.current"), "#{900 * @mib}\n")

          assert Eventually.until(fn ->
                   Enum.any?(lines(path), &(&1 =~ "peak,above cur=900.0"))
                 end)
        end)

      assert log =~ "memory trace:"
      assert log =~ "top=["

      # Sitting above the threshold keeps recording, tagged above only.
      assert Eventually.until(fn -> Enum.any?(lines(path), &(&1 =~ " above cur=900.0")) end)

      assert Telemetry.render() =~ ~s(smolquery_memory_cgroup_bytes{kind="current"} #{900 * @mib})
      assert Telemetry.render() =~ ~s(smolquery_memory_beam_bytes{kind="total"} )
      assert Telemetry.render() =~ "# TYPE smolquery_memory_rss_bytes gauge"
    end

    test "without a cgroup filesystem the resident set drives the peaks", %{tmp_dir: dir} do
      path = Path.join(dir, "memory-trace.log")

      start_supervised!(
        {MemoryTrace,
         enabled: true,
         name: :t451_no_cgroup,
         cgroup_root: Path.join(dir, "nowhere"),
         path: path,
         interval_ms: 10,
         publish: false}
      )

      assert Eventually.until(fn -> File.exists?(path) and Enum.count(lines(path)) >= 2 end)
      [start, first | _rest] = lines(path)
      assert start =~ "start limit=- above=-"
      assert first =~ " peak,beat cur=- anon=- file=- slab=- rss="
      refute first =~ "rss=-"
    end

    test "rotates the file once past max_file_bytes", %{tmp_dir: dir} do
      cgroup = fake_cgroup(Path.join(dir, "cg") |> tap(&File.mkdir_p!/1), 100)
      path = Path.join(dir, "memory-trace.log")

      start_supervised!(
        {MemoryTrace,
         enabled: true,
         name: :t451_rotate,
         cgroup_root: cgroup,
         path: path,
         interval_ms: 5,
         heartbeat_ms: 5,
         max_file_bytes: 600,
         publish: false}
      )

      assert Eventually.until(fn -> File.exists?(path <> ".1") end)
      assert File.stat!(path <> ".1").size >= 600
    end
  end
end
