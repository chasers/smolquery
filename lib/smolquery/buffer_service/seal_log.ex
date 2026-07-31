defmodule Smolquery.BufferService.SealLog do
  @moduledoc """
  The default `Smolquery.BufferService.SealConsumer` — writes the signal to the log.

  Stands in until `StorageService.Sealer` exists to answer it. A node running the
  buffer role with no storage role behind it keeps accumulating a hot tier that
  nothing seals, and that shows up here rather than silently.
  """

  @behaviour Smolquery.BufferService.SealConsumer

  require Logger

  alias Smolquery.BufferService.SealConsumer

  @impl SealConsumer
  def seal_ready(_config, {dataset, table}, ids) do
    Logger.info(fn ->
      "seal_ready #{dataset}.#{table}: #{length(ids)} unsealed micro-segments, no sealer configured"
    end)

    :ok
  end
end
