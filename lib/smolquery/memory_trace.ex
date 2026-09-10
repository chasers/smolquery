defmodule Smolquery.MemoryTrace do
  @moduledoc """
  A memory tracer that survives the kill it is waiting for (T-451).

  The sandbox's buffer pods OOMKilled at 4 Gi and again at 6 Gi while sitting
  at 1–2 GiB whenever anyone looked: the cgroup's `memory.events` counted
  more than a thousand `max` hits on a pod reading 1.3 GiB. Nothing at rest
  predicted the spike, a 2 s sweep caught none, and the one tracer that did
  catch the climb was a shell script started by hand inside a container,
  which the kill that ends the investigation also ends. This is that script
  as part of the node: off by default, one process, a file on the data
  volume, and nothing else.

  ## What it records

  Every `interval_ms` it reads what the cgroup charges the container
  (`Smolquery.CgroupMemory.usage/1`), the process's resident set, and the
  BEAM's own split (`total`, `processes`, `binary`, `ets`). It appends a line
  to the trace file when the charge sets a new peak since start, when it sits
  above `above_bytes` (default: 80% of the cgroup limit), and once per
  `heartbeat_ms` regardless — the heartbeat is what shows a steady climb, the
  peaks are what catch a spike shorter than any sweep. A peak above the
  threshold also logs a warning naming the three largest BEAM processes, at
  most once per ten seconds, so the log line nearest the kill says whether
  the memory was the BEAM's at all.

  The file lives under `:data_dir` (`memory-trace.log`), on the same volume
  as the buffer's manifests, so it outlives the container. It rotates once
  at `max_file_bytes` (to `.1`) rather than growing without bound. The last
  lines before a kill are the samples the investigation needs.

  ## What it publishes

  The same numbers go to `GET /metrics` as gauges
  (`smolquery_memory_cgroup_bytes{kind}`, `smolquery_memory_rss_bytes`,
  `smolquery_memory_beam_bytes{kind}`, `smolquery_memory_cgroup_events{kind}`),
  so a dashboard can see the climb and the ceiling hits without the file —
  T-334 and T-346 both asked for memory a `kubectl exec` is not needed to
  read.

  ## Configuration

      config :smolquery, Smolquery.MemoryTrace,
        enabled: false,        # SMOLQUERY_MEMORY_TRACE
        interval_ms: 200,      # SMOLQUERY_MEMORY_TRACE_INTERVAL_MS
        heartbeat_ms: 60_000,
        above_bytes: nil,      # default 80% of the cgroup limit; nil without one
        path: nil,             # default <data_dir>/memory-trace.log
        max_file_bytes: 16_777_216

  It measures and records; it decides nothing.
  """

  use GenServer

  require Logger

  alias Smolquery.CgroupMemory
  alias Smolquery.Telemetry

  @default_interval_ms 200
  @default_heartbeat_ms 60_000
  @default_max_file_bytes 16 * 1_048_576
  @above_fraction {8, 10}
  @warn_every_ms 10_000
  @top_processes 3

  @type sample :: %{
          at: DateTime.t(),
          cgroup: CgroupMemory.usage() | nil,
          rss: non_neg_integer() | nil,
          beam: %{total: integer(), processes: integer(), binary: integer(), ets: integer()}
        }

  @doc """
  Starts the tracer when `enabled`, and is `:ignore` otherwise — the
  supervisor treats a disabled tracer as a child that never was.
  """
  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, __MODULE__, []), opts)

    if Keyword.get(config, :enabled, false) do
      GenServer.start_link(__MODULE__, config, name: Keyword.get(config, :name, __MODULE__))
    else
      :ignore
    end
  end

  @doc """
  Where the trace file lands: `:path`, or `memory-trace.log` under `:data_dir`.
  """
  @spec path(keyword()) :: Path.t()
  def path(config \\ Application.get_env(:smolquery, __MODULE__, [])) do
    Keyword.get(config, :path) ||
      Path.join(Application.get_env(:smolquery, :data_dir, "priv/data"), "memory-trace.log")
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

    %{at: DateTime.utc_now(), cgroup: cgroup, rss: rss_bytes(), beam: beam}
  end

  @doc """
  The trace line a sample renders as, tagged with why it was recorded.
  """
  @spec line(sample(), [atom()]) :: String.t()
  def line(sample, tags) do
    cgroup = sample.cgroup || %{}

    [
      DateTime.to_iso8601(sample.at),
      Enum.map_join(tags, ",", &Atom.to_string/1),
      "cur=#{mib(cgroup[:current])}",
      "anon=#{mib(cgroup[:anon])}",
      "file=#{mib(cgroup[:file])}",
      "slab=#{mib(cgroup[:slab])}",
      "rss=#{mib(sample.rss)}",
      "beam=#{mib(sample.beam[:total])}",
      "procs=#{mib(sample.beam[:processes])}",
      "bin=#{mib(sample.beam[:binary])}",
      "ets=#{mib(sample.beam[:ets])}",
      "max_events=#{cgroup[:max_events] || "-"}",
      "oom_kill=#{cgroup[:oom_kill_events] || "-"}"
    ]
    |> Enum.join(" ")
  end

  @impl GenServer
  def init(config) do
    root = Keyword.get(config, :cgroup_root, "/sys/fs/cgroup")
    path = path(config)
    File.mkdir_p!(Path.dirname(path))

    limit =
      case CgroupMemory.limit_bytes(root) do
        {:ok, bytes} -> bytes
        :none -> nil
      end

    {numerator, denominator} = @above_fraction
    above = Keyword.get(config, :above_bytes) || (limit && div(limit * numerator, denominator))
    interval_ms = Keyword.get(config, :interval_ms, @default_interval_ms)

    state = %{
      root: root,
      path: path,
      interval_ms: interval_ms,
      heartbeat_ms: Keyword.get(config, :heartbeat_ms, @default_heartbeat_ms),
      max_file_bytes: Keyword.get(config, :max_file_bytes, @default_max_file_bytes),
      publish: Keyword.get(config, :publish, true),
      limit: limit,
      above: above,
      peak: 0,
      last_beat_ms: nil,
      last_warn_ms: nil
    }

    append(
      state,
      "#{DateTime.to_iso8601(DateTime.utc_now())} start limit=#{mib(limit)} " <>
        "above=#{mib(above)} interval_ms=#{interval_ms} os_pid=#{System.pid()}"
    )

    Logger.info(
      "memory trace on: #{path}, every #{interval_ms} ms — new peaks, samples above " <>
        "#{mib(above)} MiB, and a heartbeat every #{state.heartbeat_ms} ms"
    )

    {:ok, schedule(state)}
  end

  @impl GenServer
  def handle_info(:sample, state) do
    sample = sample(state.root)
    if state.publish, do: publish(sample)

    {:noreply, state |> record(sample) |> schedule()}
  end

  defp record(state, sample) do
    now_ms = System.monotonic_time(:millisecond)
    current = current_bytes(sample)

    tags =
      Enum.reject(
        [
          current > state.peak && :peak,
          state.above && current >= state.above && :above,
          (state.last_beat_ms == nil or now_ms - state.last_beat_ms >= state.heartbeat_ms) &&
            :beat
        ],
        &(&1 in [false, nil])
      )

    state = %{state | peak: max(state.peak, current)}

    case tags do
      [] ->
        state

      tags ->
        rendered = line(sample, tags)
        append(state, rendered)

        state
        |> beat(tags, now_ms)
        |> warn(tags, rendered, now_ms)
    end
  end

  defp beat(state, tags, now_ms),
    do: if(:beat in tags, do: %{state | last_beat_ms: now_ms}, else: state)

  defp warn(state, tags, rendered, now_ms) do
    due? = state.last_warn_ms == nil or now_ms - state.last_warn_ms >= @warn_every_ms

    if :above in tags and (:peak in tags or :beat in tags) and due? do
      Logger.warning("memory trace: #{rendered} top=#{inspect(top_processes())}")

      %{state | last_warn_ms: now_ms}
    else
      state
    end
  end

  defp current_bytes(%{cgroup: %{current: current}}), do: current
  defp current_bytes(%{rss: rss}) when is_integer(rss), do: rss
  defp current_bytes(_sample), do: 0

  defp publish(sample) do
    if cgroup = sample.cgroup do
      for kind <- [:current, :anon, :file, :slab], value = cgroup[kind] do
        Telemetry.put_gauge("smolquery_memory_cgroup_bytes", [kind: kind], value)
      end

      for kind <- [:max_events, :oom_kill_events], value = cgroup[kind] do
        Telemetry.put_gauge("smolquery_memory_cgroup_events", [kind: kind], value)
      end
    end

    if sample.rss, do: Telemetry.put_gauge("smolquery_memory_rss_bytes", [], sample.rss)

    for {kind, value} <- sample.beam do
      Telemetry.put_gauge("smolquery_memory_beam_bytes", [kind: kind], value)
    end

    :ok
  end

  defp append(state, line) do
    rotate(state)
    File.write(state.path, line <> "\n", [:append])
  end

  defp rotate(%{path: path, max_file_bytes: max}) do
    case File.stat(path) do
      {:ok, %{size: size}} when size >= max -> File.rename(path, path <> ".1")
      _small_or_absent -> :ok
    end
  end

  defp schedule(state) do
    Process.send_after(self(), :sample, state.interval_ms)

    state
  end

  # The resident set from `/proc/self/status`, in bytes. Linux only; `nil`
  # elsewhere, and the cgroup charge (or nothing) drives the peaks then.
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

  defp top_processes do
    Process.list()
    |> Enum.flat_map(fn pid ->
      case Process.info(pid, [:memory, :registered_name, :initial_call, :dictionary]) do
        nil -> []
        info -> [{info[:memory], name(pid, info)}]
      end
    end)
    |> Enum.sort_by(fn {memory, _name} -> memory end, :desc)
    |> Enum.take(@top_processes)
    |> Enum.map(fn {memory, name} -> {name, "#{mib(memory)} MiB"} end)
  end

  defp name(pid, info) do
    case info[:registered_name] do
      name when is_atom(name) and name != nil -> name
      _unregistered -> info[:dictionary][:"$initial_call"] || info[:initial_call] || pid
    end
  end

  defp mib(nil), do: "-"
  defp mib(bytes), do: :erlang.float_to_binary(bytes / 1_048_576, decimals: 1)
end
