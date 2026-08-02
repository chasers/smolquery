defmodule Smolquery.Cluster.RingCache do
  @moduledoc """
  Caches a built value against the membership it was built from.

  `Smolquery.BufferService.Routing` (Milestone 8 L4) and
  `Smolquery.StorageService.Routing` (Milestone 8 L6) both build a ring the
  same way. Reading membership is cheap either way — `:pg` is a
  per-node-replicated ETS read, the static list a config read — but what the
  membership *feeds* is not: `Ring.new!/1` hashes 128 points per node and
  sorts them, and `Smolquery.BufferService.Runtime`'s own moduledoc names
  "rebuild a 128-point hash ring on every insert" as exactly the cost the
  runtime exists to avoid. So the cache key is the membership itself:
  `resolve/3` returns the cached value while `fingerprint` (the node list
  just read) matches the one the value was built from, and rebuilds only
  when it changed. Single-node the fingerprint never changes and this
  degenerates to build-once; clustered, a rebuild happens once per actual
  ring change rather than once per write.

  `:persistent_term` fits because updates are that rare: a put triggers a
  global GC, and the fleet changing shape is an operator event, not a data
  path one.
  """

  @doc """
  The value built from `fingerprint`, cached under `key` — `build.()` runs
  only when no value is cached or the cached one was built from a different
  fingerprint.
  """
  @spec resolve(term(), term(), (-> value)) :: value when value: term()
  def resolve(key, fingerprint, build) do
    case :persistent_term.get(key, :miss) do
      {^fingerprint, value} ->
        value

      _miss_or_stale ->
        value = build.()
        :persistent_term.put(key, {fingerprint, value})

        value
    end
  end

  @doc """
  Drops a cached value, so the next `resolve/3` for `key` rebuilds it.
  """
  @spec forget(term()) :: boolean()
  def forget(key), do: :persistent_term.erase(key)
end
