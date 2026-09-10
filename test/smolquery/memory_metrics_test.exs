defmodule Smolquery.MemoryMetricsTest do
  use ExUnit.Case, async: false

  alias Smolquery.CgroupMemory
  alias Smolquery.MemoryMetrics
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

  defp rendered(series),
    do: Telemetry.render() |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, series))

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

  describe "the sampler" do
    test "is :ignore when configured off" do
      assert MemoryMetrics.start_link(enabled: false, name: :t451_off) == :ignore
    end

    test "publishes the cgroup charge, its split, the limit, the kernel counters, and the BEAM",
         %{tmp_dir: dir} do
      cgroup = fake_cgroup(dir, 500, max_events: 7)

      start_supervised!(
        {MemoryMetrics, enabled: true, name: :t451_gauges, cgroup_root: cgroup, interval_ms: 10}
      )

      assert Eventually.until(fn ->
               Telemetry.render() =~
                 ~s(smolquery_memory_cgroup_bytes{kind="current"} #{500 * @mib})
             end)

      metrics = Telemetry.render()
      assert metrics =~ "# TYPE smolquery_memory_cgroup_bytes gauge"
      assert metrics =~ ~s(smolquery_memory_cgroup_bytes{kind="anon"} #{490 * @mib})
      assert metrics =~ ~s(smolquery_memory_cgroup_bytes{kind="file"} #{10 * @mib})
      assert metrics =~ ~s(smolquery_memory_cgroup_bytes{kind="slab"} #{3 * @mib})
      assert metrics =~ "smolquery_memory_cgroup_limit_bytes #{1000 * @mib}"
      assert metrics =~ "# TYPE smolquery_memory_cgroup_events_total counter"
      assert metrics =~ ~s(smolquery_memory_cgroup_events_total{kind="max"} 7)
      assert metrics =~ ~s(smolquery_memory_cgroup_events_total{kind="oom_kill"} 0)
      assert metrics =~ ~s(smolquery_memory_beam_bytes{kind="total"} )
      assert metrics =~ ~s(smolquery_memory_beam_bytes{kind="ets"} )
      assert metrics =~ "# TYPE smolquery_memory_rss_bytes gauge"
      assert [_one] = rendered("smolquery_memory_rss_bytes ")
    end

    test "the peak holds the highest charge of the last window after it drops",
         %{tmp_dir: dir} do
      cgroup = fake_cgroup(dir, 500)

      start_supervised!(
        {MemoryMetrics,
         enabled: true, name: :t451_peak, cgroup_root: cgroup, interval_ms: 10, window_ms: 300}
      )

      File.write!(Path.join(cgroup, "memory.current"), "#{900 * @mib}\n")

      assert Eventually.until(fn ->
               Telemetry.render() =~ "smolquery_memory_cgroup_peak_bytes #{900 * @mib}"
             end)

      File.write!(Path.join(cgroup, "memory.current"), "#{400 * @mib}\n")

      assert Eventually.until(fn ->
               Telemetry.render() =~
                 ~s(smolquery_memory_cgroup_bytes{kind="current"} #{400 * @mib})
             end)

      assert Telemetry.render() =~ "smolquery_memory_cgroup_peak_bytes #{900 * @mib}"

      # Once the spike leaves the window, the peak follows the current reading.
      assert Eventually.until(fn ->
               Telemetry.render() =~ "smolquery_memory_cgroup_peak_bytes #{400 * @mib}"
             end)
    end

    test "without a cgroup filesystem the resident set is what it publishes", %{tmp_dir: dir} do
      start_supervised!(
        {MemoryMetrics,
         enabled: true,
         name: :t451_no_cgroup,
         cgroup_root: Path.join(dir, "nowhere"),
         interval_ms: 10}
      )

      assert Eventually.until(fn -> Telemetry.render() =~ "smolquery_memory_rss_peak_bytes " end)
      assert [_one] = rendered("smolquery_memory_rss_bytes ")
    end
  end
end
