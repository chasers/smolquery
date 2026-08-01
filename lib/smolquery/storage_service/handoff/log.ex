defmodule Smolquery.StorageService.Handoff.Log do
  @moduledoc """
  A `Smolquery.StorageService.Handoff` that writes the attempt to the log and
  refuses it.

  `Handoff.Seal` is what a deployment wants. This is for one narrow case: a storage
  node that should accept, schedule, and bound seal signals without acting on them
  — while a catalog is unavailable, or to watch what sealing *would* be asked to do
  before letting it. The attempt reports `{:error, :not_implemented}` rather than
  success, so nothing is retired and the buffer's level-triggered signal keeps
  asking.
  """

  @behaviour Smolquery.StorageService.Handoff

  require Logger

  alias Smolquery.StorageService.Handoff

  @impl Handoff
  def seal(_config, _runtime, {dataset, table}, %{ids: ids}) do
    Logger.info(fn ->
      "seal #{dataset}.#{table}: #{length(ids)} micro-segments claimed, merge not implemented"
    end)

    {:error, :not_implemented}
  end
end
