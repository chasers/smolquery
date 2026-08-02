defmodule Smolquery.BufferService.Replicator.None do
  @moduledoc """
  The single-copy hot tier, stated as a policy instead of an absence.

  Durability is exactly the owner's disk — the accepted window documented
  since Milestone 3, bounded by sealing. The default until T-96 ships a
  segment-shipping implementation.
  """

  @behaviour Smolquery.BufferService.Replicator

  @impl Smolquery.BufferService.Replicator
  def new(_opts), do: nil

  @impl Smolquery.BufferService.Replicator
  def commit(_config, _commit), do: :ok

  @impl Smolquery.BufferService.Replicator
  def append(_config, _mutation), do: :ok

  @impl Smolquery.BufferService.Replicator
  def redundancy(_config), do: 0
end
