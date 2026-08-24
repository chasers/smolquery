defmodule Smolquery.StorageService.Handoff.Seal do
  @moduledoc """
  The seal handoff: merge, commit, retire — exactly once, from any starting point.

  This is the one cross-service dance in smolquery, and the only place where a
  crash could plausibly lose rows or count them twice. It does neither, and the
  reason is not care taken here but two properties it inherits:

    * **the input set is frozen** into a claim before this is ever called, so
      every attempt at a claim sees the same ids and writes the same key
      (`Smolquery.BufferService.HotManifest`)
    * **the output key is derived from the inputs**, so "has this merge already
      committed" is a question about catalog membership rather than about timing

  Given those, every step is idempotent and the handoff needs no rollback — only
  reconciliation.

  ## The order, and why an attempt can start anywhere in it

      merge → put → register → retire

  An attempt begins by asking the catalog whether the claim's sealed segment is
  already registered. If it is, the merge and the commit are done — some earlier
  attempt got that far before dying — and this one skips to retirement. If it is
  not, the whole sequence runs.

  So the interesting crashes cost a `seal_retry_ms` delay and nothing else:

    * *before the commit* — nothing is registered, so the next attempt merges
      again and writes the same key; if an earlier attempt already committed
      the key, the store's conditional write refuses the re-upload and the
      committed segment stands (T-308) — a half-written output never reaches
      a key at all, since both stores stage and commit atomically
    * *after the commit, before retirement* — the rows are in the sealed tier and
      the micro-segments are still unretired, which is exactly the window the
      catalog-membership dedup rule is built for: a query at any snapshot counts
      them once. The next attempt finds the keys registered and retires
    * *after retirement* — there is nothing left to do, and a repeated retire is
      `:ok` by the buffer's contract

  ## A released claim's attempt is refused, not reconciled (T-294)

  A claim frozen larger than the current valves allow is *released* by its
  owner and re-derived as valve-sized claims — see
  `Smolquery.BufferService.TableBuffer`. An attempt for the released claim
  can still be running, and its segment must not register: the re-derived
  claims cover the same rows under other keys, so both landing would commit
  the rows twice. Three gates hold that line. The attempt checks the claim
  is still live against the origin's served manifest before it merges — a
  stale attempt costs one manifest read, not hours of merge. That read names
  the claim's ids and skips the stats, so it costs the buffer node the size
  of the claim rather than the size of its backlog, and the merge reuses the
  entries instead of asking again (T-316). It checks
  again between merge and register, which narrows the remaining window to
  the moments between two calls — the same narrowing standard as the
  stale-owner gate above. And retirement carries the claim's keys, so the
  buffer refuses to stamp ids whose live claim moved on
  (`Smolquery.BufferService.HotManifest.retire/6`), which holds even for an
  attempt that raced past both checks. Entries already sealed, and entries
  gone from the manifest, stay the reconciliation cases they always were —
  the guard skips them.

  A stale refusal also compensates: any of the claim's keys already
  registered are dropped from the catalog before the error returns. An
  attempt refused after its register — or one finding a predecessor's
  registration — would otherwise strand a segment whose rows the re-derived
  claims commit again under their own keys, and nothing else would ever
  remove it: GC deliberately spares committed segments, and no later attempt
  for the released claim runs past the first gate. Between that registration
  and the drop the rows count twice at the current snapshot — the same
  transient window a crash between commit and retire always had, closed the
  same way by the next actor to look.

  ## Retirement goes through the buffer's client, not its HTTP API

  The manifest and the segment bytes come over HTTP because `httpfs` needs them
  to. Retirement is a control-plane call with no bulk data, `HotServer` is
  read-only, and `Smolquery.BufferService.Client` already owns transport
  and the idempotence guarantee. That is the same bulk/control split the buffer
  service draws internally. Both the pull and the retire target the claim's
  `:origin` node rather than the buffer ring's current owner — the entries
  live where the signal came from, and a ring change between signal and seal
  must not point either call at a node that never held them.

  ## The snapshot a retirement is stamped with

  Retirement records the snapshot the commit reported. For a reconciling attempt
  that is the catalog's *current* snapshot rather than the one the original commit
  produced, which is a snapshot at or after it — harmless, because the stamp is
  bookkeeping for the grace-period reaper and no longer what a query dedups on.
  Reading each file's true begin-snapshot out of DuckLake's metadata would be
  precision nothing consumes.

  ## A table the catalog does not know is an error, not something to create

  The ingest edge validates a batch against the catalog before forwarding it, so a
  table with micro-segments is a table the catalog already holds. Creating one here
  would paper over a bug and invent a schema from whatever the merge happened to
  produce.
  """

  @behaviour Smolquery.StorageService.Handoff

  require Logger

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Catalog
  alias Smolquery.Partitions
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime

  @impl Handoff
  def seal(_config, %Runtime{} = runtime, table_ref, claim) do
    result =
      with {:ok, entries} <- claim_manifest(runtime, table_ref, claim),
           :ok <- claim_live(entries, claim),
           {:ok, snapshot} <- commit(runtime, table_ref, claim, entries) do
        retire(runtime, table_ref, claim, snapshot)
      end

    case result do
      {:error, {:stale_claim, _diff}} = error ->
        compensate_stale(runtime, table_ref, claim)

        error

      other ->
        other
    end
  end

  defp compensate_stale(runtime, table_ref, claim) do
    with {:ok, paths} <- sealed_paths(runtime, claim),
         {:ok, registered} <-
           Catalog.segments(runtime.catalog, Partitions.parent(table_ref), :current) do
      held = MapSet.new(registered)

      paths
      |> Enum.filter(&MapSet.member?(held, &1))
      |> drop_orphans(runtime, table_ref)
    end

    :ok
  end

  defp drop_orphans([], _runtime, _table_ref), do: :ok

  defp drop_orphans(orphans, runtime, table_ref) do
    case Catalog.drop_segments(runtime.catalog, Partitions.parent(table_ref), orphans) do
      {:ok, snapshot} ->
        Logger.warning(
          "dropped #{length(orphans)} registered segment(s) of a released claim on " <>
            "#{inspect(table_ref)} at snapshot #{snapshot} — the re-derived claims " <>
            "re-commit these rows under their own keys"
        )

      {:error, reason} ->
        Logger.warning(
          "failed to drop a released claim's registered segment(s) on " <>
            "#{inspect(table_ref)}: #{inspect(reason)} — the rows double-count until dropped"
        )
    end

    :ok
  end

  defp claim_manifest(runtime, table_ref, %{ids: ids} = claim) when is_list(ids) do
    HotTier.manifest(runtime, table_ref, claim[:origin], ids: ids, stats: false)
  end

  defp claim_manifest(_runtime, _table_ref, _claim), do: {:ok, []}

  defp claim_live(entries, %{ids: ids, keys: keys}) when is_list(ids) and is_list(keys) do
    claimed = MapSet.new(ids)

    entries
    |> Enum.filter(&MapSet.member?(claimed, &1["id"]))
    |> Enum.reject(&(&1["sealed_at"] || &1["claim_keys"] == keys))
    |> case do
      [] -> :ok
      stale -> {:error, {:stale_claim, Enum.map(stale, & &1["id"])}}
    end
  end

  defp claim_live(_entries, _claim), do: :ok

  # The hot tier speaks partition refs; the catalog knows only real tables
  # (Smolquery.Partitions). Every catalog operation here maps to the parent,
  # while the retire — buffer-facing — keeps the partition ref it came from.
  defp commit(runtime, table_ref, claim, entries) do
    with {:ok, paths} <- sealed_paths(runtime, claim),
         {:ok, registered} <-
           Catalog.segments(runtime.catalog, Partitions.parent(table_ref), :current) do
      if committed?(paths, registered) do
        {dataset, _table} = Partitions.parent(table_ref)
        Catalog.current_snapshot(runtime.catalog, dataset)
      else
        merge_and_register(runtime, table_ref, claim, entries)
      end
    end
  end

  defp merge_and_register(runtime, table_ref, claim, entries) do
    with {:ok, segment} <- Merge.run(runtime, table_ref, claim, entries),
         {:ok, fresh} <- claim_manifest(runtime, table_ref, claim),
         :ok <- claim_live(fresh, claim) do
      record_segment(table_ref, segment)

      Catalog.register_segments(runtime.catalog, Partitions.parent(table_ref), [segment])
    end
  end

  # Emitted once the merge produced a file, before registration decides whether
  # it lands: the bytes describe what the merge cost to write, which is true
  # whether or not the commit that follows succeeds.
  defp record_segment(table_ref, segment) do
    :telemetry.execute(
      [:smolquery, :seal, :segment],
      %{bytes: segment.byte_size, rows: segment.row_count},
      %{table_ref: table_ref}
    )
  end

  defp retire(runtime, table_ref, claim, snapshot) do
    Client.retire_at(
      claim[:origin],
      runtime.buffer_name,
      table_ref,
      claim.ids,
      snapshot,
      claim[:keys]
    )
  end

  defp sealed_paths(runtime, %{keys: keys}) when is_list(keys) and keys != [],
    do: {:ok, Enum.map(keys, &Store.location(runtime.store, &1))}

  defp sealed_paths(_runtime, claim), do: {:error, {:invalid_claim, claim}}

  defp committed?(paths, registered) do
    held = MapSet.new(registered)

    Enum.all?(paths, &MapSet.member?(held, &1))
  end

  @doc """
  Whether `claim`'s sealed segments are already in the catalog at its current
  snapshot.

  The dedup rule a query planner applies, asked about one claim: this is what
  decides whether a micro-segment still counts. Exposed because the planner needs
  exactly this question answered (Milestone 5) and because it is what makes an
  attempt's reconciliation testable without staging a crash.

  `table_ref` may be a partition ref, the way every caller in the hot tier
  speaks: the catalog lookup maps to `Smolquery.Partitions.parent/1` exactly as
  `commit/3` and `merge_and_register/3` above do. Asking the catalog about a
  partition ref directly does not answer "no": the catalog holds no table by
  that name at all.
  """
  @spec committed?(Runtime.t(), Store.table_ref(), SealConsumer.claim()) ::
          {:ok, boolean()} | {:error, term()}
  def committed?(%Runtime{} = runtime, table_ref, claim) do
    with {:ok, paths} <- sealed_paths(runtime, claim),
         {:ok, registered} <-
           Catalog.segments(runtime.catalog, Partitions.parent(table_ref), :current) do
      {:ok, committed?(paths, registered)}
    end
  end
end
