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
      again, overwriting its own half-written output at the same key
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
  stale attempt costs one manifest read, not hours of merge. It checks
  again between merge and register, which narrows the remaining window to
  the moments between two calls — the same narrowing standard as the
  stale-owner gate above. And retirement carries the claim's keys, so the
  buffer refuses to stamp ids whose live claim moved on
  (`Smolquery.BufferService.HotManifest.retire/6`), which holds even for an
  attempt that raced past both checks. Entries already sealed, and entries
  gone from the manifest, stay the reconciliation cases they always were —
  the guard skips them.

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
    with :ok <- claim_live(runtime, table_ref, claim),
         {:ok, snapshot} <- commit(runtime, table_ref, claim) do
      retire(runtime, table_ref, claim, snapshot)
    end
  end

  defp claim_live(runtime, table_ref, %{ids: ids, keys: keys} = claim)
       when is_list(ids) and is_list(keys) do
    with {:ok, entries} <- HotTier.manifest(runtime, table_ref, claim[:origin]) do
      claimed = MapSet.new(ids)

      entries
      |> Enum.filter(&MapSet.member?(claimed, &1["id"]))
      |> Enum.reject(&(&1["sealed_at"] || &1["claim_keys"] == keys))
      |> case do
        [] -> :ok
        stale -> {:error, {:stale_claim, Enum.map(stale, & &1["id"])}}
      end
    end
  end

  defp claim_live(_runtime, _table_ref, _claim), do: :ok

  # The hot tier speaks partition refs; the catalog knows only real tables
  # (Smolquery.Partitions). Every catalog operation here maps to the parent,
  # while the retire — buffer-facing — keeps the partition ref it came from.
  defp commit(runtime, table_ref, claim) do
    with {:ok, paths} <- sealed_paths(runtime, claim),
         {:ok, registered} <-
           Catalog.segments(runtime.catalog, Partitions.parent(table_ref), :current) do
      if committed?(paths, registered) do
        Catalog.current_snapshot(runtime.catalog)
      else
        merge_and_register(runtime, table_ref, claim)
      end
    end
  end

  defp merge_and_register(runtime, table_ref, claim) do
    with {:ok, segment} <- Merge.run(runtime, table_ref, claim),
         :ok <- claim_live(runtime, table_ref, claim) do
      Catalog.register_segments(runtime.catalog, Partitions.parent(table_ref), [segment])
    end
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
