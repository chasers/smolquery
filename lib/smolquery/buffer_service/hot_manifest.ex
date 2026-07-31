defmodule Smolquery.BufferService.HotManifest do
  @moduledoc """
  What the hot tier holds, and the record that survives a crash.

  The manifest is the authority on which micro-segments a buffer node has acked.
  Not the store's contents — a segment can exist in the store and never have been
  acknowledged to anyone — and not a process's memory, which a restart loses. The
  authority is a per-table append-only log, fsynced before any caller is told
  their rows are durable.

  ## The commit point

  `add/3` appends the entry to the table's log and fsyncs it *before* the entry
  becomes visible in ETS. That ordering is the whole durability contract:
  everything a caller can observe — a manifest read, a query, an ack — happens
  strictly after the record is on disk. A crash anywhere earlier leaves a segment
  in the store that nobody was promised.

  ## Which is why an unlogged segment is deleted, not adopted

  Recovery replays the log and then reconciles against `Store.list/2`, in both
  directions:

    * a segment in the store with no log record was never acked, so the client
      saw a failure and is entitled to retry — **adopting it would double-count
      the batch**, so it is deleted
    * a log record whose segment is gone from the store is unreadable, so it is
      dropped from the manifest and the log reconciled

  The second case is how a crash between `drop/3`'s store delete and its log
  append heals itself. Neither rule mentions a filesystem, which is why they
  survive the hot tier moving to an object store.

  ## One writer per table, many readers

  Entries live in one ETS table per node, keyed `{table_ref, segment_id}`. Only
  the `TableBuffer` owning a table may call `add/3`, `retire/4`, `drop/3`, or
  `recover/2` for it — that single-writer rule is what makes the log and ETS agree
  without a lock. Reads (`entries/2`, `entry/3`) are free for anyone, which is what
  keeps `HotServer` and the query path off the flush path. ETS does not enforce
  this; the call graph does.

  A torn final log line is expected rather than exceptional: a crash mid-append
  leaves a partial write, and fsync makes everything before it good. Recovery
  therefore tolerates an unparseable *last* line and refuses an unparseable one
  anywhere else, which would mean real corruption.

  ## Usage

      manifest =
        Smolquery.BufferService.HotManifest.new(
          log_dir: "/var/lib/smolquery/buffer",
          store: Smolquery.Segments.Store.Local.new(dir: "/var/lib/smolquery/buffer")
        )

      {:ok, entry} = Smolquery.BufferService.HotManifest.add(manifest, {"analytics", "events"}, segment)
      Smolquery.BufferService.HotManifest.entries(manifest, {"analytics", "events"})

  """

  use GenServer

  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store

  @enforce_keys [:table, :log_dir, :store]
  defstruct [:table, :log_dir, :store]

  @type t :: %__MODULE__{table: atom(), log_dir: String.t(), store: Store.t()}

  @type option :: {:name, atom()} | {:log_dir, String.t()} | {:store, Store.t()}

  @log "manifest.log"
  @staged "manifest.log.staged"

  @doc """
  A child spec identified by the manifest's name rather than the module.

  A node runs one manifest, but the name is what distinguishes it — so a
  supervisor can hold more than one without the ids colliding.
  """
  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: Keyword.get(opts, :name, __MODULE__), start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Starts the process owning the node's manifest table.

  The table outlives any `TableBuffer`, so a buffer crash does not take the index
  with it. Recovery rebuilds a table's entries from its log regardless, which is
  what makes that merely an optimization rather than a correctness argument.
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    GenServer.start_link(__MODULE__, name, name: name)
  end

  @impl GenServer
  def init(name) do
    :ets.new(name, [:ordered_set, :public, :named_table, read_concurrency: true])

    {:ok, name}
  end

  @doc """
  A handle onto a started manifest.

  ## Options

    * `:name` — the name given to `start_link/1`
    * `:log_dir` (required) — directory the per-table logs live in. Local even
      when segments are not: the log is what makes an ack durable, so it stays on
      the node that gave the ack.
    * `:store` (required) — the store holding the segments the entries describe

  """
  @spec new([option()]) :: t()
  def new(opts) do
    %__MODULE__{
      table: Keyword.get(opts, :name, __MODULE__),
      log_dir: Keyword.fetch!(opts, :log_dir),
      store: Keyword.fetch!(opts, :store)
    }
  end

  @doc """
  Records `segment` as part of the table's hot tier.

  Returns once the entry is durable. Until this returns, no caller may be told
  their rows were accepted.
  """
  @spec add(t(), Store.table_ref(), Segment.t()) :: {:ok, Entry.t()} | {:error, term()}
  def add(%__MODULE__{} = manifest, table_ref, %Segment{} = segment) do
    entry = Entry.from_segment(segment, now())

    with :ok <- append(manifest, table_ref, Entry.to_record(entry)) do
      insert(manifest, table_ref, entry)

      {:ok, entry}
    end
  end

  @doc """
  The table's entries, oldest first.

  ULIDs sort lexicographically by creation time, so ordering by id is ordering by
  when the segment was written.
  """
  @spec entries(t(), Store.table_ref()) :: [Entry.t()]
  def entries(%__MODULE__{table: table}, table_ref) do
    table
    |> :ets.match_object({{table_ref, :_}, :_})
    |> Enum.map(&elem(&1, 1))
    |> Enum.sort_by(& &1.id)
  end

  @doc """
  One entry, if the table's hot tier holds it.

  This is how a segment id from an HTTP request becomes a key — resolved through
  the manifest rather than joined into a path.
  """
  @spec entry(t(), Store.table_ref(), String.t()) :: {:ok, Entry.t()} | :error
  def entry(%__MODULE__{table: table}, table_ref, id) do
    case :ets.lookup(table, {table_ref, id}) do
      [{_key, entry}] -> {:ok, entry}
      [] -> :error
    end
  end

  @doc """
  Stamps `ids` as sealed at a catalog snapshot.

  Idempotent in every direction a crashed sealer can retry from: ids already
  sealed, and ids the reaper has since deleted, are both `:ok`. The segments stay
  readable — retirement is a stamp, not a delete.
  """
  @spec retire(t(), Store.table_ref(), [String.t()], non_neg_integer()) ::
          :ok | {:error, term()}
  def retire(%__MODULE__{} = manifest, table_ref, ids, snapshot) do
    case unsealed(manifest, table_ref, ids) do
      [] -> :ok
      pending -> seal_all(manifest, table_ref, pending, snapshot)
    end
  end

  @doc """
  Deletes `ids` from the store and the manifest.

  The store goes first: a crash between the two leaves a log record pointing at a
  segment that no longer exists, which recovery reconciles. The reverse order
  would leak the object with nothing left to name it.
  """
  @spec drop(t(), Store.table_ref(), [String.t()]) :: :ok | {:error, term()}
  def drop(%__MODULE__{} = manifest, table_ref, ids) do
    case Enum.uniq(ids) do
      [] -> :ok
      ids -> delete_and_forget(manifest, table_ref, ids)
    end
  end

  @doc """
  Entries retired before `cutoff`, as unix milliseconds.

  The grace-period reaper's input: an entry is safe to delete once no query that
  could still be reading it remains in flight.
  """
  @spec retired_before(t(), Store.table_ref(), integer()) :: [Entry.t()]
  def retired_before(%__MODULE__{} = manifest, table_ref, cutoff) do
    manifest
    |> entries(table_ref)
    |> Enum.filter(&(not is_nil(&1.retired_at) and &1.retired_at < cutoff))
  end

  @doc """
  Rebuilds a table's entries from its log, reconciling against the store.

  Returns what it found: how many entries are live, which store objects were
  deleted for having no record, and which records were dropped for having no
  object.
  """
  @spec recover(t(), Store.table_ref()) :: {:ok, map()} | {:error, term()}
  def recover(%__MODULE__{} = manifest, table_ref) do
    with {:ok, records} <- read_log(manifest, table_ref),
         {:ok, logged} <- replay(records),
         {:ok, prefix} <- Store.prefix(table_ref),
         {:ok, keys} <- Store.list(manifest.store, prefix) do
      reconcile(manifest, table_ref, logged, keys)
    end
  end

  @doc """
  Every table this node holds a log for.

  Boot recovery's starting point: the logs are what say which tables were owned,
  since nothing else on the node remembers.
  """
  @spec tables(t()) :: [Store.table_ref()]
  def tables(%__MODULE__{log_dir: log_dir}) do
    log_dir
    |> Path.join("*/*/#{@log}")
    |> Path.wildcard()
    |> Enum.map(fn path ->
      table = path |> Path.dirname() |> Path.basename()
      dataset = path |> Path.dirname() |> Path.dirname() |> Path.basename()

      {dataset, table}
    end)
    |> Enum.sort()
  end

  @doc """
  Rewrites a table's log to hold only its live entries.

  A log otherwise grows with history rather than with the tail it describes. The
  rewrite is staged and renamed, so a crash mid-compaction leaves the old log
  intact.
  """
  @spec compact(t(), Store.table_ref()) :: :ok | {:error, term()}
  def compact(%__MODULE__{} = manifest, table_ref) do
    with {:ok, path} <- log_path(manifest, table_ref) do
      staged = Path.join(Path.dirname(path), @staged)
      records = manifest |> entries(table_ref) |> Enum.map(&Entry.to_record/1)

      with :ok <- File.mkdir_p(Path.dirname(path)),
           :ok <- write_records(staged, records),
           :ok <- File.rename(staged, path) do
        :ok
      else
        {:error, reason} ->
          File.rm(staged)
          {:error, {:compaction_failed, reason}}
      end
    end
  end

  @doc """
  The path of a table's manifest log.
  """
  @spec log_path(t(), Store.table_ref()) :: {:ok, String.t()} | {:error, term()}
  def log_path(%__MODULE__{log_dir: log_dir}, table_ref) do
    with {:ok, prefix} <- Store.prefix(table_ref) do
      {:ok, Path.join([log_dir, prefix, @log])}
    end
  end

  defp unsealed(manifest, table_ref, ids) do
    ids
    |> Enum.uniq()
    |> Enum.flat_map(&lookup(manifest, table_ref, &1))
    |> Enum.reject(&Entry.sealed?/1)
  end

  defp lookup(manifest, table_ref, id) do
    case entry(manifest, table_ref, id) do
      {:ok, entry} -> [entry]
      :error -> []
    end
  end

  defp seal_all(manifest, table_ref, pending, snapshot) do
    retired_at = now()

    record = %{
      "op" => "retire",
      "ids" => Enum.map(pending, & &1.id),
      "sealed_at" => snapshot,
      "retired_at" => retired_at
    }

    with :ok <- append(manifest, table_ref, record) do
      Enum.each(pending, &insert(manifest, table_ref, Entry.seal(&1, snapshot, retired_at)))
    end
  end

  defp delete_and_forget(manifest, table_ref, ids) do
    keys =
      ids
      |> Enum.flat_map(&lookup(manifest, table_ref, &1))
      |> Enum.map(& &1.key)

    with :ok <- delete_all(manifest, keys),
         :ok <- append(manifest, table_ref, %{"op" => "drop", "ids" => ids}) do
      Enum.each(ids, &:ets.delete(manifest.table, {table_ref, &1}))
    end
  end

  defp reconcile(manifest, table_ref, logged, keys) do
    held = MapSet.new(keys)
    logged_keys = MapSet.new(logged, & &1.key)

    {live, missing} = Enum.split_with(logged, &MapSet.member?(held, &1.key))
    orphans = Enum.reject(keys, &MapSet.member?(logged_keys, &1))

    with :ok <- delete_all(manifest, orphans),
         :ok <- forget_missing(manifest, table_ref, missing) do
      replace(manifest, table_ref, live)

      {:ok, %{entries: length(live), orphans: orphans, missing: Enum.map(missing, & &1.id)}}
    end
  end

  defp insert(%__MODULE__{table: table}, table_ref, %Entry{} = entry),
    do: :ets.insert(table, {{table_ref, entry.id}, entry})

  defp replace(%__MODULE__{table: table} = manifest, table_ref, entries) do
    :ets.match_delete(table, {{table_ref, :_}, :_})

    Enum.each(entries, &insert(manifest, table_ref, &1))
  end

  defp forget_missing(_manifest, _table_ref, []), do: :ok

  defp forget_missing(manifest, table_ref, missing),
    do: append(manifest, table_ref, %{"op" => "drop", "ids" => Enum.map(missing, & &1.id)})

  defp delete_all(%__MODULE__{store: store}, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case Store.delete(store, key) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp append(%__MODULE__{} = manifest, table_ref, record) do
    with {:ok, path} <- log_path(manifest, table_ref),
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, fd} <- :file.open(path, [:append, :raw, :binary]) do
      result =
        with :ok <- :file.write(fd, [JSON.encode!(record), "\n"]), do: :file.sync(fd)

      :file.close(fd)

      result
    end
  end

  defp write_records(path, records) do
    with {:ok, fd} <- :file.open(path, [:write, :raw, :binary]) do
      lines = Enum.map(records, &[JSON.encode!(&1), "\n"])

      result = with :ok <- :file.write(fd, lines), do: :file.sync(fd)

      :file.close(fd)

      result
    end
  end

  defp read_log(%__MODULE__{} = manifest, table_ref) do
    with {:ok, path} <- log_path(manifest, table_ref) do
      case File.read(path) do
        {:ok, contents} -> parse_lines(contents)
        {:error, :enoent} -> {:ok, []}
        {:error, reason} -> {:error, {:log_unreadable, reason}}
      end
    end
  end

  defp parse_lines(contents) do
    lines = contents |> String.split("\n") |> Enum.reject(&(&1 == ""))
    last = length(lines)

    lines
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {line, index}, {:ok, acc} ->
      case JSON.decode(line) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, _reason} when index == last -> {:halt, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, {:corrupt_log, index, reason}}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp replay(records) do
    Enum.reduce_while(records, {:ok, %{}}, fn record, {:ok, acc} ->
      case apply_record(record, acc) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, entries |> Map.values() |> Enum.sort_by(& &1.id)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_record(%{"op" => "add"} = record, acc) do
    with {:ok, entry} <- Entry.from_record(record) do
      {:ok, Map.put(acc, entry.id, entry)}
    end
  end

  defp apply_record(%{"op" => "retire", "ids" => ids} = record, acc) do
    snapshot = record["sealed_at"]
    retired_at = record["retired_at"]

    {:ok,
     Enum.reduce(ids, acc, fn id, acc ->
       case Map.fetch(acc, id) do
         {:ok, entry} -> Map.put(acc, id, Entry.seal(entry, snapshot, retired_at))
         :error -> acc
       end
     end)}
  end

  defp apply_record(%{"op" => "drop", "ids" => ids}, acc), do: {:ok, Map.drop(acc, ids)}

  defp apply_record(record, _acc), do: {:error, {:unknown_record, record}}

  defp now, do: System.os_time(:millisecond)
end
