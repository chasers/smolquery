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

  The cache is one named ETS table, created at application boot by this
  process and owned by it for the node's lifetime — deliberately not
  `:persistent_term`, which this module once used: a term is written at
  boot and never again, because every re-put triggers a global heap scan,
  and `:pg` membership changes are not the rare operator events that would
  excuse one — every pod crash, rolling restart, and network flap moves the
  fingerprint on every node. The table is `:public` so callers insert their
  own rebuilds without a serialization hop; two racing rebuilds of the same
  fingerprint insert the same value, and reads are lock-free
  (`read_concurrency: true`). Without the table — the application not
  booted, a bare script — `resolve/3` degrades to building every time,
  which is only the cost the cache exists to amortize, never a wrong
  answer.
  """

  use GenServer

  @table __MODULE__

  @doc """
  Starts the process whose only job is to own the cache table for the
  node's lifetime.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, read_concurrency: true])

    {:ok, %{}}
  end

  @doc """
  The value built from `fingerprint`, cached under `key` — `build.()` runs
  only when no value is cached or the cached one was built from a different
  fingerprint.
  """
  @spec resolve(term(), term(), (-> value)) :: value when value: term()
  def resolve(key, fingerprint, build) do
    case cached(key) do
      {:ok, {^fingerprint, value}} ->
        value

      _miss_or_stale ->
        value = build.()
        cache(key, {fingerprint, value})

        value
    end
  end

  @doc """
  Drops a cached value, so the next `resolve/3` for `key` rebuilds it.
  Answers whether anything was actually cached.
  """
  @spec forget(term()) :: boolean()
  def forget(key) do
    :ets.take(@table, key) != []
  rescue
    ArgumentError -> false
  end

  defp cached(key) do
    case :ets.lookup(@table, key) do
      [{^key, cached}] -> {:ok, cached}
      [] -> :miss
    end
  rescue
    ArgumentError -> :no_table
  end

  defp cache(key, entry) do
    :ets.insert(@table, {key, entry})
  rescue
    ArgumentError -> false
  end
end
