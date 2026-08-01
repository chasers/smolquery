defmodule Smolquery.StorageService.Handoff.Log do
  @moduledoc """
  The default `Smolquery.StorageService.Handoff` — writes the attempt to the log.

  Stands in until the merge lands. A node running the storage role can accept seal
  signals, schedule them, and bound them, but it cannot yet seal anything, and
  that shows up here rather than as silent success: the attempt reports
  `{:error, :not_implemented}`, so the table stays unsealed and the buffer's
  level-triggered signal keeps asking.
  """

  @behaviour Smolquery.StorageService.Handoff

  require Logger

  alias Smolquery.StorageService.Handoff

  @impl Handoff
  def seal(_config, _runtime, {dataset, table}, ids) do
    Logger.info(fn ->
      "seal #{dataset}.#{table}: #{length(ids)} micro-segments claimed, merge not implemented"
    end)

    {:error, :not_implemented}
  end
end
