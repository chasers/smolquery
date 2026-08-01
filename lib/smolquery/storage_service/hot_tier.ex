defmodule Smolquery.StorageService.HotTier do
  @moduledoc """
  A buffer node's hot tier, as the sealer sees it.

  A thin adapter: the mechanics live in `Smolquery.BufferService.HotClient`,
  shared with the query planner, so everything that reads the hot tier from
  outside the buffer service speaks the same HTTP. This module supplies only
  the storage runtime's answers to where (`buffer_base_url`) and how long
  (`buffer_timeout_ms`).
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime

  @type entry :: HotClient.entry()

  @doc """
  Every micro-segment the owning buffer node holds for `table_ref`.
  """
  @spec manifest(Runtime.t(), Store.table_ref()) :: {:ok, [entry()]} | {:error, term()}
  def manifest(%Runtime{} = runtime, table_ref) do
    HotClient.manifest(runtime.buffer_base_url, table_ref, timeout_ms: runtime.buffer_timeout_ms)
  end
end
