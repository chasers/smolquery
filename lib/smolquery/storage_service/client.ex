defmodule Smolquery.StorageService.Client do
  @moduledoc """
  The only way into the storage service.

  Today it carries one call, the one the buffer service needs: a seal signal. It
  is a `Smolquery.BufferService.SealConsumer`, wired up as configuration rather
  than named in buffer code —

      config :smolquery, Smolquery.BufferService,
        seal_consumer: {Smolquery.StorageService.Client, []}

  — because the buffer service is forbidden from depending on this one. That
  indirection is the only thing crossing back from storage to buffer, and it
  exists so the buffer stays deployable alone.

  ## Naming the owner, not just casting locally

  A node running the `:buffer` role without `:storage` has no local sealer to
  reach — the buffer fleet is separate in Milestone 8, and a signal must reach
  whichever storage node the ring names as `table_ref`'s owner, which is
  routinely a different node than the one that signalled. `seal_ready/3`
  resolves that owner through `Smolquery.StorageService.Routing` (PL-11 D6)
  and casts to it directly (`Smolquery.StorageService.Sealer.seal_ready/4`
  takes the target node), the same way `Routing` is resolvable from a node
  with no local runtime at all so a query-only or buffer-only node still
  answers "who owns this."

  A cluster with no storage node running anywhere still cannot be reached, and
  that case is reported rather than raised: raising here would take down the
  `TableBuffer` that signalled, and it signals from the write path. The honest
  answer is a warning saying the hot tier is accumulating unsealed.

  ## Options

  `config` is a keyword list; `:name` selects the storage service instance,
  defaulting to `Smolquery.StorageService`.
  """

  @behaviour Smolquery.BufferService.SealConsumer

  require Logger

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Cluster
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer

  @impl SealConsumer
  @spec seal_ready(keyword(), Store.table_ref(), SealConsumer.claim()) :: :ok
  def seal_ready(config, table_ref, claim) do
    name = Keyword.get(config, :name, Smolquery.StorageService)

    if reachable?(name) do
      owner = name |> Routing.resolve() |> Routing.owner(table_ref)
      Sealer.seal_ready(name, table_ref, claim, owner)
    else
      report_missing(name, table_ref, claim.ids)
    end
  end

  defp reachable?(name), do: Cluster.enabled?() or match?({:ok, _runtime}, Runtime.fetch(name))

  defp report_missing(name, {dataset, table}, ids) do
    Logger.warning(fn ->
      "seal_ready #{dataset}.#{table}: #{length(ids)} unsealed micro-segments, " <>
        "#{inspect(name)} is not running on this node"
    end)

    :ok
  end
end
