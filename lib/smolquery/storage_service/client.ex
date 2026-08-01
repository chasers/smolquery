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

  ## Signalling a node that is not sealing

  A node running the `:buffer` role without `:storage` has no sealer to reach.
  That is a real deployment (the buffer fleet is separate in Milestone 8) and a
  common misconfiguration, so it is reported rather than raised: raising here
  would take down the `TableBuffer` that signalled, and it signals from the write
  path. Until the ownership ring can name a remote storage node, the honest
  answer is a warning saying the hot tier is accumulating unsealed.

  ## Options

  `config` is a keyword list; `:name` selects the storage service instance,
  defaulting to `Smolquery.StorageService`.
  """

  @behaviour Smolquery.BufferService.SealConsumer

  require Logger

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer

  @impl SealConsumer
  @spec seal_ready(keyword(), Store.table_ref(), SealConsumer.claim()) :: :ok
  def seal_ready(config, table_ref, claim) do
    name = Keyword.get(config, :name, Smolquery.StorageService)

    case Runtime.fetch(name) do
      {:ok, _runtime} -> Sealer.seal_ready(name, table_ref, claim)
      :error -> report_missing(name, table_ref, claim.ids)
    end
  end

  defp report_missing(name, {dataset, table}, ids) do
    Logger.warning(fn ->
      "seal_ready #{dataset}.#{table}: #{length(ids)} unsealed micro-segments, " <>
        "#{inspect(name)} is not running on this node"
    end)

    :ok
  end
end
