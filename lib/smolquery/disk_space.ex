defmodule Smolquery.DiskSpace do
  @moduledoc """
  Free bytes on the filesystem that holds a path, for the guards that keep a
  spilling merge from filling a node's disk (T-601).

  The BEAM has no `statvfs`, and `:disksup` polls every mounted filesystem on
  a timer from an application smolquery does not start. So this asks `df` for
  the one filesystem in question, when asked: `df -P` output is fixed by
  POSIX, and the fourth column is the space available to an unprivileged
  writer, in 1024-byte blocks under `-k`. A path that does not exist yet
  answers for its nearest existing ancestor, since a spill directory is
  created on first spill.

  `:error` means the answer is unknown (no `df`, or output this cannot read).
  Callers treat unknown as "no guard", not "no space": an engine that never
  spills must not stop working on a host without `df`.
  """

  @doc "The bytes available to an unprivileged writer on `path`'s filesystem."
  @spec free_bytes(Path.t()) :: {:ok, non_neg_integer()} | :error
  def free_bytes(path) do
    with df when is_binary(df) <- System.find_executable("df"),
         {output, 0} <- System.cmd(df, ["-Pk", existing(Path.expand(path))]),
         [_header, line | _more] <- String.split(output, "\n", trim: true),
         [_filesystem, _blocks, _used, available | _rest] <- String.split(line),
         {kib, ""} <- Integer.parse(available) do
      {:ok, kib * 1024}
    else
      _unknown -> :error
    end
  end

  defp existing(path) do
    parent = Path.dirname(path)

    if File.exists?(path) or parent == path, do: path, else: existing(parent)
  end
end
