defmodule Smolquery.Test.FileCacheFixture do
  @moduledoc """
  Names a block the way the `cache_httpfs` extension names one on disk:
  `<sha256 of the path>-<file name>-<offset>-<size>`
  (`Smolquery.QueryService.FileCache`). A test that warms a cache writes a
  file under this name before the janitor's first sweep, so the index
  counts its sealed file as read. The hash is a fixed stand-in: the index
  reads only the file name.
  """

  @doc """
  The name of the first 512 KiB block of the sealed file named `file_name`.
  """
  @spec block_name(String.t()) :: String.t()
  def block_name(file_name), do: "#{String.duplicate("ab", 32)}-#{file_name}-0-524288"
end
