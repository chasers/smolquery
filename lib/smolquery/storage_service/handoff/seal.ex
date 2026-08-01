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

  ## Retirement goes through the buffer's client, not its HTTP API

  The manifest and the segment bytes come over HTTP because `httpfs` needs them
  to. Retirement is a control-plane call with no bulk data, `HotServer` is
  read-only, and `Smolquery.BufferService.Client` already owns ownership routing
  and the idempotence guarantee. That is the same bulk/control split the buffer
  service draws internally.

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
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime

  @impl Handoff
  def seal(_config, %Runtime{} = runtime, table_ref, claim) do
    with {:ok, snapshot} <- commit(runtime, table_ref, claim) do
      retire(runtime, table_ref, claim, snapshot)
    end
  end

  defp commit(runtime, table_ref, claim) do
    with {:ok, paths} <- sealed_paths(runtime, claim),
         {:ok, registered} <- Catalog.segments(runtime.catalog, table_ref, :current) do
      if committed?(paths, registered) do
        Catalog.current_snapshot(runtime.catalog)
      else
        merge_and_register(runtime, table_ref, claim)
      end
    end
  end

  defp merge_and_register(runtime, table_ref, claim) do
    with {:ok, segment} <- Merge.run(runtime, table_ref, claim) do
      Catalog.register_segments(runtime.catalog, table_ref, [segment])
    end
  end

  defp retire(runtime, table_ref, claim, snapshot) do
    Client.retire(runtime.buffer_name, table_ref, claim.ids, snapshot)
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
  """
  @spec committed?(Runtime.t(), Store.table_ref(), SealConsumer.claim()) ::
          {:ok, boolean()} | {:error, term()}
  def committed?(%Runtime{} = runtime, table_ref, claim) do
    with {:ok, paths} <- sealed_paths(runtime, claim),
         {:ok, registered} <- Catalog.segments(runtime.catalog, table_ref, :current) do
      {:ok, committed?(paths, registered)}
    end
  end
end
