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

  ## Which jobs use it (T-629)

  Filling the cache makes a cold scan wait on writing every block to local
  disk. On the sandbox's disk a 1.26 GB cold scan took 41 s through the
  cache and 15.5 s without it, while its warm rerun took 3 s. So after each
  sweep this process also indexes the directory by sealed file: a block is
  named `<sha256 of the path>-<file name>-<offset>-<size>`, and the index
  keeps each file name's cached bytes in an ETS table
  (`Smolquery.QueryService.Runtime.file_cache_index/1`). The planner sums
  the sizes of the live sealed files with no block in it, files the cache
  has never read, and `decide/2` skips the cache for a job where that sum
  passes `bypass_bytes` (`:bypassed`); every other job uses it (`:used`).
  It counts files, not bytes: the cache holds only the columns a query
  read, so a wide table warmed by a narrow query is never fully cached by
  bytes, and a retired file's blocks must not make its replacement look
  warm. A job can force it with its `file_cache:` option (`true` uses
  it, and so fills it for the next run; `false` is `:off`). A skipped job
  runs `statements/1`, which switches its engine's `cache_httpfs` to
  `noop` before lockdown: the engine then reads like plain `httpfs`. The
  index is up to `@sweep_interval_ms` old, which only delays a just-warmed
  table's first cached run.

  ## Scattered jobs (T-630)

  A scattered job's shards read on other nodes, into those nodes' caches,
  so the coordinator's index cannot say what they hold: judged by it, a
  table warmed through its shards bypassed the cache on every auto run.
  Each shard therefore decides for itself (`shard_decision/3`) against its
  own node's index, from the job's mode, and the job reports what its
  shards did (`combined/1`).

  `smolquery_query_file_cache_bytes` reports the directory's size after each
  sweep, and `smolquery_query_file_cache_evicted_bytes_total` and
  `smolquery_query_file_cache_evicted_files_total` count what it deleted.
  """

  use GenServer

  alias Smolquery.QueryService.Runtime
  alias Smolquery.Telemetry

  require Logger

  @sweep_interval_ms 30_000
  @min_age_ms 10_000

  @typedoc """
  What a job did with the cache: read through it, skipped it because its
  cold scan was too large, or skipped it because the job said so. `nil`
  when the node has no file cache.
  """
  @type decision :: :used | :bypassed | :off | nil

  @type sweep :: %{
          bytes: non_neg_integer(),
          evicted_bytes: non_neg_integer(),
          evicted_files: non_neg_integer()
        }

  @doc """
  Starts the janitor for query service instance `name` and its
  `file_cache` (`%{directory:, max_bytes:}`).
  """
  @spec start_link({atom(), %{directory: String.t(), max_bytes: pos_integer()}}) ::
          GenServer.on_start()
  def start_link({name, file_cache}), do: GenServer.start_link(__MODULE__, {name, file_cache})

  @doc """
  The bytes instance `name` holds cached for the sealed files named
  `file_names` (base names, as the cache's blocks carry them), from the
  last sweep's index. `0` when the instance keeps no index.
  """
  @spec cached_bytes(atom(), Enumerable.t(String.t())) :: non_neg_integer()
  def cached_bytes(name, file_names) do
    index = Runtime.file_cache_index(name)

    Enum.reduce(file_names, 0, fn file_name, sum ->
      case :ets.lookup(index, file_name) do
        [{^file_name, bytes}] -> sum + bytes
        [] -> sum
      end
    end)
  rescue
    ArgumentError -> 0
  end

  @doc """
  Whether instance `name` has cached any block of the sealed file named
  `file_name`: whether the file has been read through the cache.
  """
  @spec cached?(atom(), String.t()) :: boolean()
  def cached?(name, file_name), do: cached_bytes(name, [file_name]) > 0

  @doc """
  What a job with `uncached_bytes` of live sealed files the cache has never
  read does with `file_cache` (the job's runtime field).
  """
  @spec decide(map(), non_neg_integer()) :: decision()
  def decide(%{directory: nil}, _uncached_bytes), do: nil
  def decide(%{mode: true}, _uncached_bytes), do: :used
  def decide(%{mode: false}, _uncached_bytes), do: :off

  def decide(%{bypass_bytes: bypass_bytes}, uncached_bytes),
    do: if(uncached_bytes > bypass_bytes, do: :bypassed, else: :used)

  @doc """
  What a job with `plan` does with `file_cache`: `decide/2` over the plan's
  `sealed_uncached_bytes`.
  """
  @spec decision(map(), Smolquery.QueryService.Plan.t()) :: decision()
  def decision(file_cache, plan), do: decide(file_cache, plan.sealed_uncached_bytes)

  @doc """
  What a shard of a scattered job does with instance `name`'s cache:
  `decide/2` over the bytes of `node_files` that this node's cache has
  never read. `node_files` are the sealed files (`"url"` and `"bytes"`)
  of every shard the job runs on this node, since they all fill the same
  directory, so every shard on a node reaches the same decision, and a
  node whose files add up to no more than `bypass_bytes` always uses the
  cache, as a single-engine job does. `file_cache` carries the job's
  `mode`.
  """
  @spec shard_decision(atom(), map(), [map()]) :: decision()
  def shard_decision(name, %{mode: :auto, directory: directory} = file_cache, node_files)
      when is_binary(directory) do
    uncached =
      node_files
      |> Enum.reject(&cached?(name, Path.basename(&1["url"])))
      |> Enum.sum_by(& &1["bytes"])

    decide(file_cache, uncached)
  end

  def shard_decision(_name, file_cache, _node_files), do: decide(file_cache, 0)

  @doc """
  What a scattered job did with the cache, from its shards' `decisions`:
  `:used` when any shard read through it, otherwise `:bypassed` or `:off`
  when any shard did that, and `nil` when no shard had a cache.
  """
  @spec combined([decision()]) :: decision()
  def combined(decisions), do: Enum.find([:used, :bypassed, :off], &(&1 in decisions))

  @doc """
  The statements a job's engine runs, before lockdown, for `decision`: a
  skipped cache switches `cache_httpfs` to `noop`.
  """
  @spec statements(decision()) :: [String.t()]
  def statements(decision) when decision in [:bypassed, :off],
    do: ["SET cache_httpfs_type = 'noop'"]

  def statements(_used_or_none), do: []

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
  def init({name, %{directory: directory} = file_cache}) do
    case File.mkdir_p(directory) do
      :ok ->
        index =
          :ets.new(Runtime.file_cache_index(name), [
            :named_table,
            :protected,
            read_concurrency: true
          ])

        send(self(), :sweep)
        {:ok, Map.put(file_cache, :index, index)}

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
    reindex(file_cache.index, directory)

    Telemetry.put_gauge("smolquery_query_file_cache_bytes", [], result.bytes)

    :telemetry.execute(
      [:smolquery, :query, :file_cache, :sweep],
      %{evicted_bytes: result.evicted_bytes, evicted_files: result.evicted_files},
      %{}
    )

    Process.send_after(self(), :sweep, @sweep_interval_ms)

    {:noreply, file_cache}
  end

  defp reindex(index, directory) do
    cached =
      directory
      |> blocks()
      |> Enum.reduce(%{}, fn {path, size, _mtime_ms}, acc ->
        case file_name(Path.basename(path)) do
          {:ok, name} -> Map.update(acc, name, size, &(&1 + size))
          :error -> acc
        end
      end)

    :ets.insert(index, Map.to_list(cached))

    for name <- :ets.select(index, [{{:"$1", :_}, [], [:"$1"]}]),
        not Map.has_key?(cached, name),
        do: :ets.delete(index, name)

    :ok
  end

  defp file_name(<<_hash::binary-size(64), "-", rest::binary>>) do
    case rest |> String.split("-") |> Enum.reverse() do
      [size, offset | name] when name != [] ->
        if digits?(size) and digits?(offset),
          do: {:ok, name |> Enum.reverse() |> Enum.join("-")},
          else: :error

      _other ->
        :error
    end
  end

  defp file_name(_other), do: :error

  defp digits?(value), do: value != "" and String.match?(value, ~r/^[0-9]+$/)

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
