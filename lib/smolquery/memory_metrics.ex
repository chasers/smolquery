defmodule Smolquery.MemoryMetrics do
  @moduledoc """
  The node's memory, as gauges on `GET /metrics` (T-451).

  The sandbox's buffer pods OOMKilled at 4 Gi and again at 6 Gi while reading
  1–2 GiB whenever anyone looked: the cgroup's `memory.events` counted more
  than a thousand `max` hits on a pod at 1.3 GiB, and nothing the node
  exported said so. The BEAM's own accounting stops at its allocators,
  DuckDB's at its buffer manager, and the number that actually kills the pod
  — what the cgroup charges the container — was readable only by hand,
  inside the container, until the kill ended the shell.

  Every `interval_ms` (250) this process reads that charge and its
  anon/file/slab split (`Smolquery.CgroupMemory.usage/1`), the cgroup's
  cumulative `max` and `oom_kill` counters, the OS process's resident set,
  and the BEAM's `total`/`processes`/`binary`/`ets`, and writes them to the
  metrics table. A scrape every 15 or 30 s would miss a spike shorter than
  that, so alongside each current reading it publishes the **peak over the
  last 60 s** of samples: the highest charge the container reached between
  two scrapes is what an OOM investigation needs, and it is exactly what a
  sampled gauge loses. The cgroup limit is published too, so the ratio is one
  query away.

      smolquery_memory_cgroup_bytes{kind="current|anon|file|slab"}   gauge
      smolquery_memory_cgroup_peak_bytes                              gauge, 60 s window
      smolquery_memory_cgroup_limit_bytes                             gauge
      smolquery_memory_cgroup_events_total{kind="max|oom_kill"}       counter (the kernel's)
      smolquery_memory_rss_bytes                                      gauge
      smolquery_memory_rss_peak_bytes                                 gauge, 60 s window
      smolquery_memory_beam_bytes{kind="total|processes|binary|ets"}  gauge

  Without a cgroup filesystem (a laptop, a bare host) the cgroup series are
  absent and the resident set stands in. A sample is three small file reads
  and one `:erlang.memory/0`; the process holds at most 60 s of samples.

  Always on; `config :smolquery, Smolquery.MemoryMetrics, enabled: false`
  turns it off, `interval_ms:` retunes it. It measures and publishes; it
  decides nothing.
  """

  use GenServer

  alias Smolquery.CgroupMemory
  alias Smolquery.Telemetry

  @default_interval_ms 250
  @window_ms 60_000

  @type sample :: %{
          at_ms: integer(),
          cgroup: CgroupMemory.usage() | nil,
          rss: non_neg_integer() | nil,
          beam: %{total: integer(), processes: integer(), binary: integer(), ets: integer()}
        }

  @doc """
  Starts the sampler, or is `:ignore` when configured off.
  """
  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, __MODULE__, []), opts)

    if Keyword.get(config, :enabled, true) do
      GenServer.start_link(__MODULE__, config, name: Keyword.get(config, :name, __MODULE__))
    else
      :ignore
    end
  end

  @doc """
  One reading of the cgroup, the resident set, and the BEAM, at `root`.
  """
  @spec sample(Path.t()) :: sample()
  def sample(root \\ "/sys/fs/cgroup") do
    cgroup =
      case CgroupMemory.usage(root) do
        {:ok, usage} -> usage
        :none -> nil
      end

    beam = :erlang.memory() |> Map.new() |> Map.take([:total, :processes, :binary, :ets])

    %{at_ms: System.monotonic_time(:millisecond), cgroup: cgroup, rss: rss_bytes(), beam: beam}
  end

  @impl GenServer
  def init(config) do
    root = Keyword.get(config, :cgroup_root, "/sys/fs/cgroup")

    case CgroupMemory.limit_bytes(root) do
      {:ok, limit} -> Telemetry.put_gauge("smolquery_memory_cgroup_limit_bytes", [], limit)
      :none -> :ok
    end

    state = %{
      root: root,
      interval_ms: Keyword.get(config, :interval_ms, @default_interval_ms),
      window_ms: Keyword.get(config, :window_ms, @window_ms),
      recent: []
    }

    {:ok, state |> tick() |> schedule()}
  end

  @impl GenServer
  def handle_info(:sample, state), do: {:noreply, state |> tick() |> schedule()}

  defp tick(state) do
    sample = sample(state.root)

    recent = [
      sample | Enum.take_while(state.recent, &(sample.at_ms - &1.at_ms < state.window_ms))
    ]

    publish(sample, recent)

    %{state | recent: recent}
  end

  defp publish(sample, recent) do
    if cgroup = sample.cgroup do
      for kind <- [:current, :anon, :file, :slab], value = cgroup[kind] do
        Telemetry.put_gauge("smolquery_memory_cgroup_bytes", [kind: kind], value)
      end

      for kind <- [:max, :oom_kill], value = cgroup[:"#{kind}_events"] do
        Telemetry.put_gauge("smolquery_memory_cgroup_events_total", [kind: kind], value)
      end

      Telemetry.put_gauge(
        "smolquery_memory_cgroup_peak_bytes",
        [],
        recent |> Enum.map(&(&1.cgroup && &1.cgroup.current)) |> peak()
      )
    end

    if sample.rss do
      Telemetry.put_gauge("smolquery_memory_rss_bytes", [], sample.rss)

      Telemetry.put_gauge(
        "smolquery_memory_rss_peak_bytes",
        [],
        recent |> Enum.map(& &1.rss) |> peak()
      )
    end

    for {kind, value} <- sample.beam do
      Telemetry.put_gauge("smolquery_memory_beam_bytes", [kind: kind], value)
    end

    :ok
  end

  defp peak(values), do: values |> Enum.reject(&is_nil/1) |> Enum.max(fn -> 0 end)

  defp schedule(state) do
    Process.send_after(self(), :sample, state.interval_ms)

    state
  end

  # The resident set from `/proc/self/status`, in bytes. Linux only; `nil`
  # elsewhere.
  defp rss_bytes do
    case File.read("/proc/self/status") do
      {:ok, status} -> status |> String.split("\n") |> Enum.find_value(&vm_rss/1)
      {:error, _reason} -> nil
    end
  end

  defp vm_rss(line) do
    case String.split(line) do
      ["VmRSS:", kib, "kB"] -> String.to_integer(kib) * 1024
      _other -> nil
    end
  end
end
