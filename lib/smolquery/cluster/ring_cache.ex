defmodule Smolquery.Cluster.RingCache do
  @moduledoc """
  Resolves a value that is either recomputed live or cached once, forever.

  `Smolquery.BufferService.Routing` (Milestone 8 L4) and
  `Smolquery.StorageService.Routing` (Milestone 8 L6) both build a ring the
  same way: with clustering on, `:pg` membership is already a fast,
  per-node-replicated read, so recomputing fresh on every call is cheap and
  correct as the fleet changes shape. With clustering off, the ring is
  genuinely immutable, and building it every call would mean hashing the same
  node list into a fresh 128-point-per-node ring on every write. `resolve/2`
  is the shared shape: build fresh when clustering is on, otherwise build once
  and cache in `:persistent_term`.
  """

  alias Smolquery.Cluster

  @doc """
  `build.()` every call when clustering is on; otherwise the first call's
  result, cached under `key`.
  """
  @spec resolve(term(), (-> term())) :: term()
  def resolve(key, build) do
    if Cluster.enabled?(), do: build.(), else: cached(key, build)
  end

  @doc """
  Drops a cached value, so the next `resolve/2` for `key` rebuilds it.
  """
  @spec forget(term()) :: boolean()
  def forget(key), do: :persistent_term.erase(key)

  defp cached(key, build) do
    :persistent_term.get(key)
  rescue
    ArgumentError ->
      value = build.()
      :persistent_term.put(key, value)

      value
  end
end
