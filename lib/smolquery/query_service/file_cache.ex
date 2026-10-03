defmodule Smolquery.QueryService.FileCache do
  @moduledoc """
  Keeps the node's shared sealed-tier read cache under its byte cap (T-626).

  Job and shard engines write `cache_httpfs` blocks into the runtime's
  `file_cache` directory (`Smolquery.QueryService.JobEngine`). The extension
  bounds that directory only by the free space of its filesystem, and on a
  Kubernetes `emptyDir` that free space is the node's whole disk, so the
  cache could grow until the kubelet evicts the pod. This process is the
  bound instead.

  Every `@sweep_interval_ms` it totals the directory. Over `max_bytes`, it
  deletes the blocks with the oldest modification time until the directory
  is back under 90% of the cap. The extension touches a block every time it
  reads it, so that order is least recently read first. A block read or
  written in the last `@min_age_ms` is never deleted, so a scan touching
  every block at once can hold the directory over the cap until it ends.

  The extension writes each block to a temporary file and renames it into
  place, so a visible block is always complete. Deleting one costs a later
  read one cache miss, never a wrong answer: sealed files are immutable, so
  a block is either present and right or absent, and a reader that finds it
  gone reads the object store. A temporary file left by a killed engine is
  counted and, once old, deleted with the rest.

  A sweep reads one directory listing and one `stat` per entry, about
  `max_bytes / 512 KiB` blocks at the extension's default block size: about
  4,000 at the 2 GiB default.

  A directory it cannot create is logged and left alone: the janitor stops,
  and the query service runs without it rather than failing to boot.

  `smolquery_query_file_cache_bytes` reports the directory's size after each
  sweep, and `smolquery_query_file_cache_evicted_bytes_total` and
  `smolquery_query_file_cache_evicted_files_total` count what it deleted.
  """

  use GenServer

  alias Smolquery.Telemetry

  require Logger

  @sweep_interval_ms 30_000
  @min_age_ms 10_000

  @type sweep :: %{
          bytes: non_neg_integer(),
          evicted_bytes: non_neg_integer(),
          evicted_files: non_neg_integer()
        }

  @doc """
  Starts the janitor for `file_cache` (`%{directory:, max_bytes:}`).
  """
  @spec start_link(%{directory: String.t(), max_bytes: pos_integer()}) :: GenServer.on_start()
  def start_link(file_cache), do: GenServer.start_link(__MODULE__, file_cache)

  @doc """
  Deletes the oldest blocks in `directory` until it holds at most 90% of
  `max_bytes`, when it holds more than `max_bytes`, sparing any block
  modified less than `min_age_ms` before `now_ms` (Unix milliseconds).
  """
  @spec sweep(String.t(), pos_integer(), integer(), non_neg_integer()) :: sweep()
  def sweep(
        directory,
        max_bytes,
        now_ms \\ System.os_time(:millisecond),
        min_age_ms \\ @min_age_ms
      ) do
    blocks = blocks(directory)
    total = Enum.sum_by(blocks, fn {_path, size, _mtime} -> size end)

    if total > max_bytes do
      evict(blocks, total, div(max_bytes * 9, 10), now_ms - min_age_ms)
    else
      %{bytes: total, evicted_bytes: 0, evicted_files: 0}
    end
  end

  @impl GenServer
  def init(%{directory: directory} = file_cache) do
    case File.mkdir_p(directory) do
      :ok ->
        send(self(), :sweep)
        {:ok, file_cache}

      {:error, reason} ->
        Logger.error(
          "file cache directory #{inspect(directory)} cannot be created (#{inspect(reason)}); " <>
            "nothing bounds it"
        )

        :ignore
    end
  end

  @impl GenServer
  def handle_info(:sweep, %{directory: directory, max_bytes: max_bytes} = file_cache) do
    result = sweep(directory, max_bytes)

    Telemetry.put_gauge("smolquery_query_file_cache_bytes", [], result.bytes)

    :telemetry.execute(
      [:smolquery, :query, :file_cache, :sweep],
      %{evicted_bytes: result.evicted_bytes, evicted_files: result.evicted_files},
      %{}
    )

    Process.send_after(self(), :sweep, @sweep_interval_ms)

    {:noreply, file_cache}
  end

  defp blocks(directory) do
    case File.ls(directory) do
      {:ok, names} -> Enum.flat_map(names, &block(Path.join(directory, &1)))
      {:error, _reason} -> []
    end
  end

  defp block(path) do
    case File.lstat(path, time: :posix) do
      {:ok, %File.Stat{type: :regular, size: size, mtime: mtime}} -> [{path, size, mtime * 1_000}]
      _gone_or_not_a_file -> []
    end
  end

  defp evict(blocks, total, target, newest_ms) do
    blocks
    |> Enum.filter(fn {_path, _size, mtime_ms} -> mtime_ms <= newest_ms end)
    |> Enum.sort_by(fn {_path, _size, mtime_ms} -> mtime_ms end)
    |> Enum.reduce_while(%{bytes: total, evicted_bytes: 0, evicted_files: 0}, fn
      _block, %{bytes: bytes} = acc when bytes <= target ->
        {:halt, acc}

      {path, size, _mtime_ms}, acc ->
        {:cont, remove(path, size, acc)}
    end)
  end

  defp remove(path, size, acc) do
    case File.rm(path) do
      :ok ->
        %{
          acc
          | bytes: acc.bytes - size,
            evicted_bytes: acc.evicted_bytes + size,
            evicted_files: acc.evicted_files + 1
        }

      {:error, _reason} ->
        acc
    end
  end
end
