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

  ## Claims: the log is also what makes sealing exactly-once

  Before a sealer is told a table is ready, the set it is being told about is
  frozen into the log as a `claim` — the micro-segment ids, and the key(s) of the
  sealed segment they will become. Two properties follow, and the seal handoff
  needs both:

    * **the input set never moves.** A sealer answering a repeated signal, or
      retrying after either side crashed, merges exactly the ids the claim names.
      Rows that landed since wait for the next claim.
    * **the output is named before it is written**, deterministically from the
      inputs (`Smolquery.Segments.Id.derive/2`). So "did this merge already
      commit" is a question about catalog membership of a known key, not about
      timing — which is what lets a query planner exclude a micro-segment at the
      instant its rows appear in the sealed tier, with no window in between.

  A claim is live until every entry in it is sealed, which is what limits a table
  to one outstanding claim and makes recovery free: replaying the log restores it,
  so a restarted buffer re-signals the same set rather than inventing a new one.

  ## One writer per table, many readers

  Entries live in one ETS table per node, keyed `{table_ref, segment_id}`. Only
  the `TableBuffer` owning a table may call `add/4`, `retire/5`, `drop/4`,
  `recover/2`, or `compact/2` for it — that single-writer rule is what makes the
  log and ETS agree without a lock, and what keeps a compaction's rename from
  discarding a concurrent append. Reads (`entries/2`, `entry/3`) are free for
  anyone, which is what keeps `HotServer` and the query path off the flush path.
  ETS does not enforce this; the call graph does.

  The single writer is also why appends can go through a held file descriptor:
  `open_log/2` opens a table's log once, and every mutation takes the handle as
  an optional last argument. Reopening the file around each append costs more
  than the write and the fsync together (measured ~1.0 ms against ~0.3 ms), and
  every group commit pays exactly one append. The handle is bound to the log's
  current file, so a caller that runs `compact/2` — which renames a fresh file
  into place — must close and reopen; appending through the old handle would
  write to the unlinked inode and silently lose records.

  A torn final log line is expected rather than exceptional: a crash mid-append
  leaves a partial write, and fsync makes everything before it good. A record
  missing its newline was never fully synced, so no caller was acked on it.
  Recovery therefore truncates the log back to its last complete line — merely
  tolerating the torn tail would let the next append concatenate onto it and
  garble a line mid-log — and refuses an unparseable line anywhere *else*, which
  would mean real corruption.

  The fsync covers the log file's contents, not its directory entry — Erlang
  cannot fsync a directory without a NIF, the same accepted window
  `Smolquery.Segments.Store.Local` documents for segments. A table's very first
  record can in principle vanish with its file on a hard power cut; from the
  second append on, the directory entry is old metadata.

  Recovery also never empties a table's index on the way to rebuilding it. It
  inserts the recovered entries first and only then removes what is no longer
  live, so a reader mid-recovery sees the right set or briefly a superset of it —
  never nothing. Clearing first would let a query plan against an empty hot tier
  while a buffer restarted and quietly return incomplete results.

  ## What a scanning read costs, and how to see it

  Three reads walk a table's index rather than resolving one key: `entries/2`
  serving the whole manifest over HTTP, `pending/3` deciding what to claim, and
  `retired_before/3` on every reap. Each emits
  `[:smolquery, :hot_manifest, :read]` with its duration and the entries it
  answered with, labelled by `op`, and so does the O(1) `live_claim/2` (T-318).

  That is the seam because it is caller-agnostic. The same index backs the
  sealer, the query planner and the buffer's own write path, and "which read is
  spending the node" is not answerable from any one of them.
  `read_entries_total{op="entries"}` over that op's read count is the mean
  unsealed backlog depth; the same ratio at `op="live_claim"` is the mean claim
  size.

  `entry/3` and `batch_ack/3` are single-key lookups and are not instrumented —
  they run per segment request, and a counter per lookup would cost more than
  the lookup.

  ## The live claim is cached, because the maintenance tick asks every commit

  `live_claim/2` used to derive its answer by scanning the table. It runs on
  every maintenance tick, which runs on every commit, so its cost grew with the
  unsealed backlog — and a deeper backlog is exactly when a buffer can least
  afford it (T-318). The answer now lives in a second ETS table, read by key.

  The cache is **re-derived, never patched**. Every mutation that can change
  which entries are claimed — `freeze/5`, `release_live/5`, `seal_all/5`,
  `delete_and_forget/4`, and recovery's `replace/3` — recomputes it from the
  entries table in full. So the stored value is always the same value a scan
  would produce, and the only thing that changed is *when* the scan runs: once
  per claim, release, retire or drop, rather than once per commit. There is no
  incremental-update rule to get wrong.

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

  @typedoc """
  A frozen set of micro-segment ids, and the sealed segment key(s) they become.
  """
  @type claim :: %{ids: [String.t()], keys: [String.t()]}

  @opaque log :: {:hot_log, :file.fd()}

  @log "manifest.log"
  @staged "manifest.log.staged"

  # `write_concurrency` is not optional here, it is the difference between a
  # node that keeps up and one that does not: every commit inserts, and a node
  # owns many table refs at once. Measured on an `ordered_set` holding 16,384
  # 32-column entries, 8 processes inserting under distinct table refs — 73,705
  # inserts/s without it against 909,556 with it, 12.3x (T-318). A single-writer
  # microbenchmark shows nothing, which is why one is not the evidence here.
  #
  # `:compressed` is deliberately absent. It is a real lever — the same table
  # measures 112.3 MiB plain against 45.3 MiB compressed — but it costs about
  # 1.8x on reads (a 1,024-entry claim read goes 2.9 ms to 5.3 ms), and the read
  # path is what the sealer and the query planner both sit on.
  @index_options [:public, read_concurrency: true, write_concurrency: true]

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
    :ets.new(name, @index_options ++ [:ordered_set, :named_table])
    :ets.new(batches(name), @index_options ++ [:set, :named_table])
    :ets.new(claims(name), @index_options ++ [:set, :named_table])

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
  Opens a table's log for the appends its owner will make.

  For the process making every append — reopening the file around each one
  costs more than the append itself. See the moduledoc for the invalidation
  rule around `compact/2`.
  """
  @spec open_log(t(), Store.table_ref()) :: {:ok, log()} | {:error, term()}
  def open_log(%__MODULE__{} = manifest, table_ref) do
    with {:ok, path} <- log_path(manifest, table_ref) do
      case held_fd(path) do
        {:ok, fd} -> {:ok, {:hot_log, fd}}
        {:error, reason} -> {:error, {:log_open_failed, reason}}
      end
    end
  end

  @doc """
  Closes a held log.
  """
  @spec close_log(log()) :: :ok
  def close_log({:hot_log, fd}) do
    _closed = :file.close(fd)

    :ok
  end

  @doc """
  Records `segment` as part of the table's hot tier.

  Returns once the entry is durable. Until this returns, no caller may be told
  their rows were accepted.

  `batch_ids` are the idempotency keys of the client batches this commit
  covers; they are fsynced in the entry's record, which is what keeps the
  dedup window closed across a restart (`batch_ack/3`).
  """
  @spec add(t(), Store.table_ref(), Segment.t(), log() | nil, [String.t()]) ::
          {:ok, Entry.t()} | {:error, term()}
  def add(%__MODULE__{} = manifest, table_ref, %Segment{} = segment, log \\ nil, batch_ids \\ []) do
    entry = Entry.from_segment(segment, now(), batch_ids)

    with :ok <- append(manifest, table_ref, Entry.to_record(entry), log) do
      insert(manifest, table_ref, entry)
      index_batches(manifest, table_ref, entry)

      {:ok, entry}
    end
  end

  @doc """
  Records an entry another node committed, verbatim (T-96).

  The replication path's half of `add/5`: the owner minted the entry, and the
  follower must hold the *same* record — same `added_at`, same batch ids —
  because a follower's disk has to be indistinguishable from a crashed
  owner's for `Smolquery.BufferService.Adopter` replay to be the promotion
  path. Re-applying an entry already held is harmless: the ETS insert
  overwrites with identical state, exactly as a log replay would.
  """
  @spec put_entry(t(), Store.table_ref(), Entry.t(), log() | nil) ::
          {:ok, Entry.t()} | {:error, term()}
  def put_entry(%__MODULE__{} = manifest, table_ref, %Entry{} = entry, log \\ nil) do
    with :ok <- append(manifest, table_ref, Entry.to_record(entry), log) do
      insert(manifest, table_ref, entry)
      index_batches(manifest, table_ref, entry)

      {:ok, entry}
    end
  end

  @doc """
  The ack a committed batch was (or would have been) answered with.

  The T-41 dedup read: a caller retrying a batch — after a lost ack, a
  transport timeout, or a buffer crash-before-reply — asks with the batch's
  idempotency key and gets the original commit's ack instead of a second
  write. The index is rebuilt from the manifest log on recovery and an id
  lives exactly as long as its entry, so the window stays closed across a
  restart and the index is bounded by the grace-period reaper.
  """
  @spec batch_ack(t(), Store.table_ref(), String.t()) ::
          {:ok, %{segment_id: String.t(), row_count: non_neg_integer()}} | :error
  def batch_ack(%__MODULE__{table: table}, table_ref, batch_id) do
    case :ets.lookup(batches(table), {table_ref, batch_id}) do
      [{_key, ack}] -> {:ok, ack}
      [] -> :error
    end
  end

  @doc """
  The table's entries, oldest first.

  ULIDs sort lexicographically by creation time, so ordering by id is ordering by
  when the segment was written.

  `ids` narrows the read to a known set, and that is the difference between a
  read that costs the same every time and one that grows with the unsealed
  backlog. `:all` scans the table and copies every entry out of ETS; a list of
  ids resolves each one through the `:ordered_set`'s index instead. A sealer
  holds its claim's ids and wants nothing else, so it asks for them by name
  (T-316).

  An id the table no longer holds is absent from the result rather than an
  error. The caller's set is a claim, and an entry can retire between the claim
  and the read — the same answer a full read gives.
  """
  @spec entries(t(), Store.table_ref(), [String.t()] | :all) :: [Entry.t()]
  def entries(manifest, table_ref, ids \\ :all)

  def entries(%__MODULE__{table: table}, table_ref, :all) do
    measured(:entries, fn -> :ets.select(table, entry_spec(table_ref)) end)
  end

  def entries(%__MODULE__{} = manifest, table_ref, ids) when is_list(ids) do
    ids
    |> Enum.uniq()
    |> Enum.flat_map(&lookup(manifest, table_ref, &1))
    |> Enum.sort_by(& &1.id)
  end

  @doc """
  Up to `limit` unsealed entries, oldest first.

  The read the owning `TableBuffer` makes on every commit, so it is bounded
  twice over (T-317).

  It filters *inside* ETS rather than in the caller. An entry carries a
  flush-time stats map — kilobytes on a wide table — and `entries/2` copies
  every one of them out to the calling process so the caller can throw most
  away. That copy is what made commit cost grow with the unsealed backlog: a
  deeper backlog made every commit's maintenance tick more expensive, which
  deepened the backlog.

  And it stops at `limit`. A claim never holds more than the count valve allows,
  so a caller deciding whether to claim, and what to claim, learns nothing from
  the entries past it. `:infinity` reads them all.

  Ordering is the `:ordered_set`'s own. Keys are `{table_ref, id}` and ULIDs
  sort lexicographically by creation time, so key order is age order and nothing
  needs sorting.
  """
  @spec pending(t(), Store.table_ref(), pos_integer() | :infinity) :: [Entry.t()]
  def pending(manifest, table_ref, limit \\ :infinity)

  def pending(%__MODULE__{table: table}, table_ref, :infinity),
    do: measured(:pending, fn -> :ets.select(table, pending_spec(table_ref)) end)

  def pending(%__MODULE__{table: table}, table_ref, limit)
      when is_integer(limit) and limit > 0,
      do: measured(:pending, fn -> select_limited(table, pending_spec(table_ref), limit) end)

  @doc """
  Up to `limit` unsealed entries as the claim path sees them: id, size, and age.

  The three fields `Smolquery.BufferService.TableBuffer` actually reads to decide
  whether and what to claim. Returning whole entries makes the read copy every
  entry's flush-time stats block out of ETS for nothing — measured at 7,151,616
  bytes against 98,304 for the same 1,024 entries, 73x (T-318). The garbage that
  is not created is the point; the traversal costs the same either way.
  """
  @spec claimable(t(), Store.table_ref(), pos_integer()) :: [
          %{id: String.t(), byte_size: non_neg_integer(), added_at: integer()}
        ]
  def claimable(%__MODULE__{table: table}, table_ref, limit)
      when is_integer(limit) and limit > 0 do
    spec = [
      {{{table_ref, :_}, :"$1"}, [{:==, {:map_get, :sealed_at, :"$1"}, nil}],
       [
         {{{:map_get, :id, :"$1"}, {:map_get, :byte_size, :"$1"}, {:map_get, :added_at, :"$1"}}}
       ]}
    ]

    measured(:claimable, fn ->
      table
      |> select_limited(spec, limit)
      |> Enum.map(fn {id, byte_size, added_at} ->
        %{id: id, byte_size: byte_size, added_at: added_at}
      end)
    end)
  end

  @doc """
  Whether the table's hot tier holds nothing at all.

  A bounded existence check: it stops at the first entry rather than copying
  every one out to compare a list against `[]`.
  """
  @spec empty?(t(), Store.table_ref()) :: boolean()
  def empty?(%__MODULE__{table: table}, table_ref) do
    :ets.select(table, [{{{table_ref, :_}, :_}, [], [true]}], 1) == :"$end_of_table"
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
  Freezes `ids` as a claim, to be sealed into the segment(s) `keys` name.

  A claim is what makes the seal handoff exactly-once. The set is frozen *before*
  anyone is told to seal it, and durably: the sealer that answers a signal — or
  retries one after either side crashed — always sees the same input set, so the
  sealed segment it produces is always the same segment. Rows that arrive after
  the claim wait for the next one.

  One live claim per table, enforced here: a claim while a *different* one is
  outstanding is `{:error, :claim_outstanding}`, because two live claims would
  leave `live_claim/2` with no honest answer. Re-claiming exactly the live claim
  (same ids, same keys) is absorbed as `{:ok, claim}` without touching the log —
  a replicated claim whose ack was lost is re-shipped verbatim, and refusing the
  retry would wedge the owner's seal pipeline forever. And the set is frozen whole
  or not at all — an id that cannot be frozen (already sealed, or never held)
  makes the claim `{:error, {:partial_claim, %{missing: ids, sealed: ids}}}`,
  because `keys` were derived from the full requested set and stamping a subset
  with them would break the output's input-derived identity. The error names the
  divergence (T-289): a replica answering an owner's claim is describing a set it
  does not hold, and without the ids the owner can neither repair the replica nor
  say more than "failed" in its log. `missing` ids this manifest never heard of
  are repairable by re-shipping; `sealed` ids it already retired are the owner
  being behind, which re-shipping must not paper over. The split is what this
  manifest can still see: an id that was sealed and then reaped reports as
  `missing`, because the drop erased the record — `missing` means "no entry
  now", not "never held" (T-291). `keys` are recorded rather
  than recomputed at seal time, which is what keeps the output name stable even
  if one of the inputs later goes missing from the store.
  """
  @spec claim(t(), Store.table_ref(), [String.t()], [String.t()], log() | nil) ::
          {:ok, claim()} | {:error, term()}
  def claim(%__MODULE__{} = manifest, table_ref, ids, keys, log \\ nil) do
    case live_claim(manifest, table_ref) do
      {:ok, live} ->
        absorb_identical_claim(live, ids, keys)

      :error ->
        with {:ok, entries} <- freezable(manifest, table_ref, ids) do
          freeze(manifest, table_ref, entries, keys, log)
        end
    end
  end

  defp absorb_identical_claim(live, ids, keys) do
    if live.keys == keys and Enum.sort(live.ids) == ids |> Enum.uniq() |> Enum.sort() do
      {:ok, live}
    else
      {:error, :claim_outstanding}
    end
  end

  @doc """
  Releases the table's live claim, returning its ids to pending.

  The way out of a claim frozen larger than the current valves allow (T-294):
  the input-set-never-moves rule holds for as long as a claim is live, so a
  claim is never shrunk in place — it is released whole, and the next claim
  re-derives from the pending tail under the valves, with a key the new
  subset names. `ids` must match the live claim's, the same whole-or-nothing
  rule `claim/5` freezes under; a different live set is
  `{:error, {:claim_mismatch, %{live_ids: ids}}}`, naming the live claim this
  manifest actually holds — the same reason `:partial_claim` names its diff
  (T-289): a replica refusing an owner's release is describing a claim the
  owner does not hold, and without the ids the owner can neither reconcile
  the divergence nor say more than "failed" in its log (T-297). No live claim
  absorbs as `:ok` — a replica re-applying a release it already holds, or one
  whose claim never froze, has nothing to release and nothing wrong.
  """
  @spec release(t(), Store.table_ref(), [String.t()], log() | nil) :: :ok | {:error, term()}
  def release(%__MODULE__{} = manifest, table_ref, ids, log \\ nil) do
    case live_claim(manifest, table_ref) do
      :error -> :ok
      {:ok, live} -> release_live(manifest, table_ref, live, ids, log)
    end
  end

  defp release_live(manifest, table_ref, live, ids, log) do
    if Enum.sort(live.ids) == ids |> Enum.uniq() |> Enum.sort() do
      record = %{"op" => "release", "ids" => live.ids}

      with :ok <- append(manifest, table_ref, record, log) do
        live.ids
        |> Enum.flat_map(&lookup(manifest, table_ref, &1))
        |> Enum.each(&insert(manifest, table_ref, Entry.claim(&1, [])))

        refresh_claim(manifest, table_ref)
      end
    else
      {:error, {:claim_mismatch, %{live_ids: live.ids}}}
    end
  end

  @doc """
  The table's outstanding claim, if a sealer still owes one.

  A claim is live until every entry in it is sealed — which is what limits a table
  to one at a time, and what makes the signal to re-send after a crash a lookup
  rather than remembered state.
  """
  @spec live_claim(t(), Store.table_ref()) :: {:ok, claim()} | :error
  def live_claim(%__MODULE__{table: table}, table_ref) do
    measured(:live_claim, fn ->
      case :ets.lookup(claims(table), table_ref) do
        [{_ref, claim}] -> {:ok, claim}
        [] -> :error
      end
    end)
  end

  @doc """
  Stamps `ids` as sealed at a catalog snapshot.

  Idempotent in every direction a crashed sealer can retry from: ids already
  sealed, and ids the reaper has since deleted, are both `:ok`. The segments stay
  readable — retirement is a stamp, not a delete.

  Retiring any member of a claim retires all of them. The claim's sealed segment
  holds every input's rows, so once it is in the catalog every input is sealed —
  and stamping only some would leave the rest claimed but unsealed, so the next
  re-signal would ask a sealer to rebuild that claim's segment from a subset and
  overwrite the committed one with fewer rows.

  `keys` fences the retire against a released claim (T-294): a sealer names
  the claim keys its segment was written under, and an unsealed entry whose
  `claim_keys` no longer match refuses the whole retire as
  `{:error, {:stale_claim, ...}}` — the claim was released and re-derived
  while that attempt ran, and stamping its ids sealed would let the
  re-derived claims' segments double-commit the rows. `nil` skips the fence,
  for callers retiring outside any claim — which includes, for exactly one
  rolling deploy, sealers from the release that predates the fence; that
  window keeps the pre-fence exposure and closes when the last old sealer
  drains (`docs/deployment.md` says how to order the rollout). The
  idempotent retry contract is unchanged either way: ids already sealed, and
  ids the reaper has deleted, stay `:ok`.
  """
  @spec retire(
          t(),
          Store.table_ref(),
          [String.t()],
          non_neg_integer(),
          [String.t()] | nil,
          log() | nil
        ) :: :ok | {:error, term()}
  def retire(%__MODULE__{} = manifest, table_ref, ids, snapshot, keys \\ nil, log \\ nil) do
    case unsealed(manifest, table_ref, ids) do
      [] ->
        :ok

      pending ->
        case stale_for(pending, keys) do
          [] ->
            seal_all(manifest, table_ref, with_claim(manifest, table_ref, pending), snapshot, log)

          stale ->
            {:error, {:stale_claim, %{keys: keys, ids: Enum.map(stale, & &1.id)}}}
        end
    end
  end

  defp stale_for(_pending, nil), do: []

  defp stale_for(pending, keys), do: Enum.reject(pending, &(&1.claim_keys == keys))

  @doc """
  Deletes `ids` from the store and the manifest.

  The store goes first: a crash between the two leaves a log record pointing at a
  segment that no longer exists, which recovery reconciles. The reverse order
  would leak the object with nothing left to name it.
  """
  @spec drop(t(), Store.table_ref(), [String.t()], log() | nil) :: :ok | {:error, term()}
  def drop(%__MODULE__{} = manifest, table_ref, ids, log \\ nil) do
    case Enum.uniq(ids) do
      [] -> :ok
      ids -> delete_and_forget(manifest, table_ref, ids, log)
    end
  end

  @doc """
  Entries retired before `cutoff`, as unix milliseconds.

  The grace-period reaper's input: an entry is safe to delete once no query that
  could still be reading it remains in flight.
  """
  @spec retired_before(t(), Store.table_ref(), integer()) :: [Entry.t()]
  def retired_before(%__MODULE__{table: table}, table_ref, cutoff) do
    measured(:retired_before, fn -> reapable(table, table_ref, {table_ref, ""}, cutoff, []) end)
  end

  # Walks the retired prefix rather than scanning the table, because the reaper
  # runs on every maintenance tick and a scan there costs the unsealed backlog
  # (T-318). Entries retire oldest-first — a claim freezes the oldest pending
  # entries and `retire/6` stamps the whole claim at once — so in key order the
  # reapable entries are a prefix, and the first entry that is not reapable ends
  # the walk. Should that ever not hold, the walk under-reaps rather than
  # over-reaps, and it self-corrects: the entry it stopped at is older than
  # whatever it skipped, so it becomes reapable first.
  defp reapable(table, table_ref, key, cutoff, acc) do
    case :ets.next(table, key) do
      {^table_ref, _id} = next -> take_reapable(table, table_ref, next, cutoff, acc)
      _end_or_other_table -> Enum.reverse(acc)
    end
  end

  defp take_reapable(table, table_ref, key, cutoff, acc) do
    case :ets.lookup(table, key) do
      [{_key, %Entry{retired_at: at} = entry}] when is_integer(at) and at < cutoff ->
        reapable(table, table_ref, key, cutoff, [entry | acc])

      _not_reapable ->
        Enum.reverse(acc)
    end
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

  defp with_claim(manifest, table_ref, pending) do
    claimed = pending |> Enum.flat_map(& &1.claim_keys) |> MapSet.new()

    if MapSet.size(claimed) == 0 do
      pending
    else
      siblings =
        manifest
        |> entries(table_ref)
        |> Enum.filter(&sibling?(&1, claimed))

      Enum.uniq_by(pending ++ siblings, & &1.id)
    end
  end

  defp sibling?(%Entry{} = entry, claimed) do
    not Entry.sealed?(entry) and Enum.any?(entry.claim_keys, &MapSet.member?(claimed, &1))
  end

  defp freezable(manifest, table_ref, ids) do
    requested = Enum.uniq(ids)

    case unsealed(manifest, table_ref, requested) do
      [] ->
        {:error, :nothing_to_claim}

      entries when length(entries) == length(requested) ->
        {:ok, entries}

      entries ->
        {:error, {:partial_claim, divergence(manifest, table_ref, requested, entries)}}
    end
  end

  defp divergence(manifest, table_ref, requested, entries) do
    held = MapSet.new(entries, & &1.id)

    {missing, sealed} =
      requested
      |> Enum.reject(&MapSet.member?(held, &1))
      |> Enum.split_with(&(entry(manifest, table_ref, &1) == :error))

    %{missing: missing, sealed: sealed}
  end

  defp freeze(manifest, table_ref, entries, keys, log) do
    ids = Enum.map(entries, & &1.id)
    record = %{"op" => "claim", "ids" => ids, "keys" => keys}

    with :ok <- append(manifest, table_ref, record, log) do
      Enum.each(entries, &insert(manifest, table_ref, Entry.claim(&1, keys)))
      refresh_claim(manifest, table_ref)

      {:ok, %{ids: ids, keys: keys}}
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

  defp seal_all(manifest, table_ref, pending, snapshot, log) do
    retired_at = now()

    record = %{
      "op" => "retire",
      "ids" => Enum.map(pending, & &1.id),
      "sealed_at" => snapshot,
      "retired_at" => retired_at
    }

    with :ok <- append(manifest, table_ref, record, log) do
      Enum.each(pending, &insert(manifest, table_ref, Entry.seal(&1, snapshot, retired_at)))

      refresh_claim(manifest, table_ref)
    end
  end

  defp delete_and_forget(manifest, table_ref, ids, log) do
    case Enum.flat_map(ids, &lookup(manifest, table_ref, &1)) do
      [] ->
        :ok

      entries ->
        record = %{"op" => "drop", "ids" => Enum.map(entries, & &1.id)}

        with :ok <- delete_all(manifest, Enum.map(entries, & &1.key)),
             :ok <- append(manifest, table_ref, record, log) do
          Enum.each(entries, &forget(manifest, table_ref, &1))

          refresh_claim(manifest, table_ref)
        end
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

  defp entry_spec(table_ref), do: [{{{table_ref, :_}, :"$1"}, [], [:"$1"]}]

  defp pending_spec(table_ref) do
    [{{{table_ref, :_}, :"$1"}, [{:==, {:map_get, :sealed_at, :"$1"}, nil}], [:"$1"]}]
  end

  defp select_limited(table, spec, limit),
    do: table |> :ets.select(spec, limit) |> collect(limit, 0, [])

  defp collect(:"$end_of_table", _limit, _taken, chunks),
    do: chunks |> Enum.reverse() |> Enum.concat()

  defp collect({entries, continuation}, limit, taken, chunks) do
    taken = taken + length(entries)
    chunks = [entries | chunks]

    if taken >= limit do
      chunks |> Enum.reverse() |> Enum.concat() |> Enum.take(limit)
    else
      collect(:ets.select(continuation), limit, taken, chunks)
    end
  end

  defp insert(%__MODULE__{table: table}, table_ref, %Entry{} = entry),
    do: :ets.insert(table, {{table_ref, entry.id}, entry})

  defp batches(table), do: Module.concat(table, Batches)

  defp claims(table), do: Module.concat(table, Claims)

  defp refresh_claim(%__MODULE__{table: table} = manifest, table_ref) do
    case derive_claim(manifest, table_ref) do
      {:ok, claim} -> :ets.insert(claims(table), {table_ref, claim})
      :error -> :ets.delete(claims(table), table_ref)
    end

    :ok
  end

  defp derive_claim(%__MODULE__{table: table}, table_ref) do
    spec = [
      {{{table_ref, :_}, :"$1"},
       [
         {:andalso, {:==, {:map_get, :sealed_at, :"$1"}, nil},
          {:"/=", {:map_get, :claim_keys, :"$1"}, {:const, []}}}
       ], [{{{:map_get, :id, :"$1"}, {:map_get, :claim_keys, :"$1"}}}]}
    ]

    case :ets.select(table, spec) do
      [] ->
        :error

      [{_id, keys} | _rest] = claimed ->
        {:ok, %{ids: Enum.map(claimed, &elem(&1, 0)), keys: keys}}
    end
  end

  defp measured(op, fun) do
    started_at = System.monotonic_time()
    result = fun.()

    :telemetry.execute(
      [:smolquery, :hot_manifest, :read],
      %{
        duration_us:
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond),
        entries: read_entries(result)
      },
      %{op: op}
    )

    result
  end

  defp read_entries(entries) when is_list(entries), do: length(entries)
  defp read_entries({:ok, %{ids: ids}}), do: length(ids)
  defp read_entries(_none), do: 0

  defp index_batches(%__MODULE__{table: table}, table_ref, %Entry{} = entry) do
    ack = %{segment_id: entry.id, row_count: entry.row_count}

    Enum.each(entry.batch_ids, &:ets.insert(batches(table), {{table_ref, &1}, ack}))
  end

  defp forget(%__MODULE__{} = manifest, table_ref, %Entry{} = entry) do
    :ets.delete(manifest.table, {table_ref, entry.id})
    forget_batches(manifest, table_ref, entry)
  end

  defp forget_batches(%__MODULE__{table: table}, table_ref, %Entry{} = entry),
    do: Enum.each(entry.batch_ids, &:ets.delete(batches(table), {table_ref, &1}))

  defp replace(%__MODULE__{table: table} = manifest, table_ref, entries) do
    keep = MapSet.new(entries, & &1.id)

    Enum.each(entries, &insert(manifest, table_ref, &1))

    table
    |> :ets.select(entry_spec(table_ref))
    |> Enum.reject(&MapSet.member?(keep, &1.id))
    |> Enum.each(&:ets.delete(table, {table_ref, &1.id}))

    :ets.match_delete(batches(table), {{table_ref, :_}, :_})
    Enum.each(entries, &index_batches(manifest, table_ref, &1))
    refresh_claim(manifest, table_ref)
  end

  defp forget_missing(_manifest, _table_ref, []), do: :ok

  defp forget_missing(manifest, table_ref, missing),
    do: append(manifest, table_ref, %{"op" => "drop", "ids" => Enum.map(missing, & &1.id)}, nil)

  defp delete_all(%__MODULE__{store: store}, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case Store.delete(store, key) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp append(%__MODULE__{} = manifest, table_ref, record, nil) do
    with {:ok, path} <- log_path(manifest, table_ref) do
      path |> write_line(record) |> tag_append()
    end
  end

  defp append(%__MODULE__{}, _table_ref, record, {:hot_log, fd}) do
    fd |> write_sync(record) |> tag_append()
  end

  defp tag_append(:ok), do: :ok
  defp tag_append({:error, reason}), do: {:error, {:log_append_failed, reason}}

  defp write_line(path, record) do
    with {:ok, fd} <- held_fd(path) do
      result = write_sync(fd, record)

      :file.close(fd)

      result
    end
  end

  defp write_sync(fd, record) do
    with :ok <- :file.write(fd, [JSON.encode!(record), "\n"]), do: :file.sync(fd)
  end

  defp held_fd(path) do
    with :ok <- File.mkdir_p(Path.dirname(path)) do
      :file.open(path, [:append, :raw, :binary])
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
        {:ok, contents} -> parse_and_repair(path, contents)
        {:error, :enoent} -> {:ok, []}
        {:error, reason} -> {:error, {:log_unreadable, reason}}
      end
    end
  end

  defp parse_and_repair(path, contents) do
    with {:ok, records, valid_bytes} <- parse_contents(contents),
         :ok <- truncate_torn_tail(path, contents, valid_bytes) do
      {:ok, records}
    end
  end

  defp parse_contents(contents),
    do: parse_lines(String.split(contents, "\n"), 1, 0, [])

  defp parse_lines([""], _index, offset, acc), do: {:ok, Enum.reverse(acc), offset}
  defp parse_lines([_torn], _index, offset, acc), do: {:ok, Enum.reverse(acc), offset}

  defp parse_lines(["" | rest], index, offset, acc),
    do: parse_lines(rest, index + 1, offset + 1, acc)

  defp parse_lines([line | rest], index, offset, acc) do
    case JSON.decode(line) do
      {:ok, record} ->
        parse_lines(rest, index + 1, offset + byte_size(line) + 1, [record | acc])

      {:error, _reason} when rest == [""] ->
        {:ok, Enum.reverse(acc), offset}

      {:error, reason} ->
        {:error, {:corrupt_log, index, reason}}
    end
  end

  defp truncate_torn_tail(_path, contents, valid_bytes)
       when byte_size(contents) == valid_bytes,
       do: :ok

  defp truncate_torn_tail(path, _contents, valid_bytes) do
    case :file.open(path, [:read, :write, :raw, :binary]) do
      {:ok, fd} ->
        result =
          with {:ok, _position} <- :file.position(fd, valid_bytes),
               :ok <- :file.truncate(fd) do
            :file.sync(fd)
          end

        :file.close(fd)

        case result do
          :ok -> :ok
          {:error, reason} -> {:error, {:log_repair_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:log_repair_failed, reason}}
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

  defp apply_record(%{"op" => "claim", "ids" => ids, "keys" => keys}, acc) do
    {:ok,
     Enum.reduce(ids, acc, fn id, acc ->
       case Map.fetch(acc, id) do
         {:ok, entry} -> Map.put(acc, id, Entry.claim(entry, keys))
         :error -> acc
       end
     end)}
  end

  defp apply_record(%{"op" => "release", "ids" => ids}, acc) do
    {:ok,
     Enum.reduce(ids, acc, fn id, acc ->
       case Map.fetch(acc, id) do
         {:ok, entry} -> Map.put(acc, id, Entry.claim(entry, []))
         :error -> acc
       end
     end)}
  end

  defp apply_record(%{"op" => "drop", "ids" => ids}, acc), do: {:ok, Map.drop(acc, ids)}

  defp apply_record(record, _acc), do: {:error, {:unknown_record, record}}

  defp now, do: System.os_time(:millisecond)
end
