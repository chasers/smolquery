defmodule Smolquery.Segments.Store.Local do
  @moduledoc """
  `Smolquery.Segments.Store` on a local filesystem.

  The default store, and the one the hot tier runs on today: a put costs an
  fsync and a rename rather than a network round trip, which is what keeps ack
  latency a fraction of the group-commit cadence.

  ## What durable means here, exactly

  `put/3` writes into a `.tmp` directory under the store root, fsyncs that file,
  and renames it to its key. Both are under the same root, so the rename is
  within one filesystem and therefore atomic: a reader listing a table's prefix,
  or a crash mid-encode, sees either nothing or a complete segment.

  The fsync covers the file's *contents*, not its directory entry — Erlang cannot
  fsync a directory without a NIF. So an acked segment survives a process, BEAM,
  or node crash, and a hard power cut can in principle lose the rename even
  though the bytes reached the platter. That window is strictly smaller than the
  one this store already has by being single-copy — losing the disk loses the
  data outright — so it is accepted and documented rather than worked around. A
  shared store has neither failure mode.

  `fsync: false` trades that guarantee for speed. It is for a store whose
  contents are re-derivable, like a sealer's scratch space, and never for the
  buffer's hot tier.

  ## Usage

      store = Smolquery.Segments.Store.Local.new(dir: "/var/lib/smolquery/buffer")

  """

  @behaviour Smolquery.Segments.Store

  alias Smolquery.Segments.Store

  @enforce_keys [:dir]
  defstruct [:dir, fsync: true]

  @type t :: %__MODULE__{dir: String.t(), fsync: boolean()}

  @type option :: {:dir, String.t()} | {:fsync, boolean()}

  @staging ".tmp"

  @doc """
  A store rooted at `:dir`.

  ## Options

    * `:dir` (required) — directory the store's keys resolve beneath
    * `:fsync` — whether `put/3` fsyncs before the rename. Defaults to `true`;
      see the durability note above before switching it off.

  """
  @spec new([option()]) :: Store.t()
  def new(opts) do
    config = %__MODULE__{
      dir: Keyword.fetch!(opts, :dir),
      fsync: Keyword.get(opts, :fsync, true)
    }

    %Store{impl: __MODULE__, config: config}
  end

  @impl Store
  def put(%__MODULE__{} = config, key, encoder) do
    target = location(config, key)
    staged = staging_path(config, key)

    with :ok <- File.mkdir_p(Path.dirname(target)),
         :ok <- File.mkdir_p(Path.dirname(staged)),
         :ok <- encode(staged, encoder),
         {:ok, %File.Stat{size: size}} <- File.stat(staged),
         :ok <- sync(staged, config.fsync),
         :ok <- File.rename(staged, target) do
      {:ok, %{location: target, byte_size: size}}
    else
      {:error, reason} ->
        File.rm(staged)
        {:error, {:put_failed, key, reason}}
    end
  end

  @impl Store
  def location(%__MODULE__{dir: dir}, key), do: Path.join(dir, key)

  @impl Store
  def list(%__MODULE__{dir: dir} = config, prefix) do
    keys =
      config
      |> location(prefix)
      |> Path.join("**/*.parquet")
      |> Path.wildcard()
      |> Enum.map(&Path.relative_to(&1, dir))
      |> Enum.sort()

    {:ok, keys}
  end

  @impl Store
  def delete(%__MODULE__{} = config, key) do
    case config |> location(key) |> File.rm() do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:delete_failed, key, reason}}
    end
  end

  @impl Store
  def shared?(%__MODULE__{}), do: false

  @impl Store
  def sweep_staging(%__MODULE__{dir: dir}, age_ms) do
    cutoff = System.os_time(:second) - div(age_ms, 1000)

    swept =
      [dir, @staging, "*"]
      |> Path.join()
      |> Path.wildcard()
      |> Enum.filter(&(staged_at(&1) <= cutoff and File.rm(&1) == :ok))

    {:ok, Enum.map(swept, &Path.basename/1)}
  end

  defp staged_at(path) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> mtime
      {:error, _reason} -> :infinity
    end
  end

  defp encode(staged, encoder) do
    encoder.(staged)
  catch
    kind, reason ->
      File.rm(staged)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp staging_path(%__MODULE__{dir: dir}, key) do
    name = Path.basename(key) <> "." <> Integer.to_string(:erlang.unique_integer([:positive]))

    Path.join([dir, @staging, name])
  end

  defp sync(_path, false), do: :ok

  defp sync(path, true) do
    with {:ok, fd} <- :file.open(path, [:read, :write, :raw, :binary]) do
      result = :file.sync(fd)
      :file.close(fd)
      result
    end
  end
end
