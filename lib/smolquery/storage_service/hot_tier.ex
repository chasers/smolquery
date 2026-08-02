defmodule Smolquery.StorageService.HotTier do
  @moduledoc """
  A buffer node's hot tier, as the sealer sees it.

  A thin adapter: the mechanics live in `Smolquery.BufferService.HotClient`,
  shared with the query planner, so everything that reads the hot tier from
  outside the buffer service speaks the same HTTP. This module supplies the
  storage runtime's answers to where and how long (`buffer_timeout_ms`).

  Where is the claim's `:origin` node when the signal named one and clustering
  is on — the seal signal routes to the storage ring's owner (Milestone 8 L6),
  which is routinely not the node holding the bytes, so `buffer_base_url`
  (an address that is only honest single-node) would point a remote sealer at
  its own loopback. The origin node's `HotServer` URL is derived from the node
  name the same way the query planner derives fan-out URLs
  (`Smolquery.Cluster.node_host/1`, `buffer_hot_port`).
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.Cluster
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime

  @type entry :: HotClient.entry()

  @doc """
  Every micro-segment the buffer node holding `table_ref`'s claim has for it.

  `origin` is the claim's signalling node, or `nil` to fall back to the
  configured `buffer_base_url`.
  """
  @spec manifest(Runtime.t(), Store.table_ref(), node() | nil) ::
          {:ok, [entry()]} | {:error, term()}
  def manifest(%Runtime{} = runtime, table_ref, origin \\ nil) do
    HotClient.manifest(base_url(runtime, origin), table_ref,
      timeout_ms: runtime.buffer_timeout_ms
    )
  end

  defp base_url(%Runtime{buffer_base_url: url} = runtime, origin) do
    if origin != nil and Cluster.enabled?() do
      "http://#{Cluster.node_host(origin)}:#{runtime.buffer_hot_port}"
    else
      url
    end
  end
end
