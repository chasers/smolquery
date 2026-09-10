defmodule Smolquery.CgroupMemory do
  @moduledoc """
  The container's memory limit and usage, read from the cgroup filesystem.

  DuckDB sizes its default `memory_limit` from the host's physical memory,
  and a static configured value ignores the pod entirely. Inside a container
  both are wrong in the same direction: the number that actually bounds the
  process is the cgroup memory limit the runtime wrote, not the node's RAM
  and not what a config file guessed (T-250 — a merge OOMing at 954 MiB
  inside a 4 Gi pod, sweep after sweep, forever).

  This module only reads; what fraction of the limit an engine may claim is
  the caller's policy. Cgroup v2 (`memory.max`) is tried first, then v1
  (`memory/memory.limit_in_bytes`). `memory.max` holding `max`, v1's
  everything-fits sentinel (a page-rounded max int), and a host with no
  cgroup filesystem at all resolve to `:none` — on such hosts there is no
  container limit to derive from, and the caller falls back to configuration.

  `usage/1` is the other half (T-451): what the cgroup charges the container
  right now — `memory.current`, its `anon`/`file`/`slab` split from
  `memory.stat`, and the `max` and `oom_kill` counters from `memory.events`,
  which say how many times the container hit its ceiling and how many times
  the kernel killed it for that. Those are the numbers an OOM investigation
  needs and a process cannot see about itself any other way: the BEAM's own
  accounting stops at its allocators, and DuckDB's at its buffer manager.
  """

  @no_limit_floor 1_152_921_504_606_846_976

  @typedoc """
  What the cgroup charges the container. `:anon`, `:file`, `:slab` and the two
  event counters are `nil` where the host does not expose them (cgroup v1
  exposes a coarser `memory.stat`; `usage/1` maps `rss` to `:anon` and
  `cache` to `:file` there, and `failcnt` to `:max_events`).
  """
  @type usage :: %{
          current: non_neg_integer(),
          anon: non_neg_integer() | nil,
          file: non_neg_integer() | nil,
          slab: non_neg_integer() | nil,
          max_events: non_neg_integer() | nil,
          oom_kill_events: non_neg_integer() | nil
        }

  @doc """
  The cgroup memory limit in bytes, or `:none` when the host sets none.
  """
  @spec limit_bytes(Path.t()) :: {:ok, pos_integer()} | :none
  def limit_bytes(root \\ "/sys/fs/cgroup") do
    with :none <- parse(Path.join(root, "memory.max")) do
      parse(Path.join([root, "memory", "memory.limit_in_bytes"]))
    end
  end

  @doc """
  What the cgroup charges the container right now, or `:none` without a
  cgroup filesystem.
  """
  @spec usage(Path.t()) :: {:ok, usage()} | :none
  def usage(root \\ "/sys/fs/cgroup") do
    case read_integer(Path.join(root, "memory.current")) do
      {:ok, current} ->
        stat = pairs(Path.join(root, "memory.stat"))
        events = pairs(Path.join(root, "memory.events"))

        {:ok,
         %{
           current: current,
           anon: stat["anon"],
           file: stat["file"],
           slab: stat["slab"],
           max_events: events["max"],
           oom_kill_events: events["oom_kill"]
         }}

      :none ->
        usage_v1(Path.join(root, "memory"))
    end
  end

  defp usage_v1(root) do
    case read_integer(Path.join(root, "memory.usage_in_bytes")) do
      {:ok, current} ->
        stat = pairs(Path.join(root, "memory.stat"))

        {:ok,
         %{
           current: current,
           anon: stat["rss"],
           file: stat["cache"],
           slab: nil,
           max_events: elem_or_nil(read_integer(Path.join(root, "memory.failcnt"))),
           oom_kill_events: nil
         }}

      :none ->
        :none
    end
  end

  defp elem_or_nil({:ok, value}), do: value
  defp elem_or_nil(:none), do: nil

  defp parse(path) do
    case read_integer(path) do
      {:ok, bytes} when bytes > 0 and bytes < @no_limit_floor -> {:ok, bytes}
      _no_limit -> :none
    end
  end

  defp read_integer(path) do
    with {:ok, contents} <- File.read(path),
         {value, _rest} <- Integer.parse(String.trim(contents)) do
      {:ok, value}
    else
      _unreadable -> :none
    end
  end

  # `key value` per line, as `memory.stat` and `memory.events` are written.
  defp pairs(path) do
    case File.read(path) do
      {:ok, contents} ->
        contents |> String.split("\n", trim: true) |> Enum.flat_map(&pair/1) |> Map.new()

      {:error, _reason} ->
        %{}
    end
  end

  defp pair(line) do
    with [key, value] <- String.split(line),
         {n, ""} <- Integer.parse(value) do
      [{key, n}]
    else
      _malformed -> []
    end
  end
end
