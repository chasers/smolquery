defmodule Smolquery.CgroupMemory do
  @moduledoc """
  The container's memory limit, read from the cgroup filesystem.

  DuckDB sizes its default `memory_limit` from the host's physical memory,
  and a static configured value ignores the pod entirely. Inside a container
  both are wrong in the same direction: the number that actually bounds the
  process is the cgroup memory limit the runtime wrote, not the node's RAM
  and not what a config file guessed (T-250 — a merge OOMing at 954 MiB
  inside a 4 Gi pod, sweep after sweep, forever).

  This module only reads the limit; what fraction of it an engine may claim
  is the caller's policy. Cgroup v2 (`memory.max`) is tried first, then v1
  (`memory/memory.limit_in_bytes`). `memory.max` holding `max`, v1's
  everything-fits sentinel (a page-rounded max int), and a host with no
  cgroup filesystem at all resolve to `:none` — on such hosts there is no
  container limit to derive from, and the caller falls back to configuration.
  """

  @no_limit_floor 1_152_921_504_606_846_976

  @doc """
  The cgroup memory limit in bytes, or `:none` when the host sets none.
  """
  @spec limit_bytes(Path.t()) :: {:ok, pos_integer()} | :none
  def limit_bytes(root \\ "/sys/fs/cgroup") do
    with :none <- parse(Path.join(root, "memory.max")) do
      parse(Path.join([root, "memory", "memory.limit_in_bytes"]))
    end
  end

  defp parse(path) do
    with {:ok, contents} <- File.read(path),
         {bytes, _rest} when bytes > 0 and bytes < @no_limit_floor <-
           Integer.parse(String.trim(contents)) do
      {:ok, bytes}
    else
      _no_limit -> :none
    end
  end
end
