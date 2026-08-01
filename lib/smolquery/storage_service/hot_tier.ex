defmodule Smolquery.StorageService.HotTier do
  @moduledoc """
  A buffer node's hot tier, as the sealer sees it: over HTTP.

  The sealer could ask for a manifest through `Smolquery.BufferService.Client`,
  which would be a local function call today and a `gen_rpc` round trip in a
  cluster. It goes over `HotServer`'s HTTP API instead, deliberately: the segment
  *bytes* have to come that way regardless — DuckDB reads them through `httpfs`,
  which speaks HTTP and nothing else — so pulling the manifest over the same
  transport means one connection story instead of two, and means a remote buffer
  node needs nothing new when the cluster arrives.

  Where to reach is `buffer_base_url` in configuration. That is honest for a single
  node and wrong for a fleet, where the owning node comes from the ownership ring;
  replacing it is Milestone 8's work, and it is one function.
  """

  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime

  @typedoc """
  One micro-segment as the manifest reports it — string-keyed, straight off JSON.

  `"url"` is what a reader opens, and the only field a merge strictly needs;
  `"claim_keys"` is what says which claim it belongs to.
  """
  @type entry :: %{optional(String.t()) => term()}

  @doc """
  Every micro-segment the owning buffer node holds for `table_ref`.

  An empty list is a real answer: a table whose tail has already been swept has an
  empty manifest, and so does a table this node never wrote to.
  """
  @spec manifest(Runtime.t(), Store.table_ref()) :: {:ok, [entry()]} | {:error, term()}
  def manifest(%Runtime{} = runtime, {dataset, table} = table_ref) do
    with {:ok, _prefix} <- Store.prefix(table_ref) do
      runtime
      |> url("/v1/datasets/#{dataset}/tables/#{table}/manifest")
      |> get(runtime)
    end
  end

  defp url(%Runtime{buffer_base_url: base}, path), do: String.trim_trailing(base, "/") <> path

  defp get(url, runtime) do
    case Req.get(url, receive_timeout: runtime.buffer_timeout_ms, retry: false) do
      {:ok, %Req.Response{status: 200, body: entries}} when is_list(entries) -> {:ok, entries}
      {:ok, %Req.Response{status: 200, body: body}} -> {:error, {:manifest_malformed, body}}
      {:ok, %Req.Response{status: status}} -> {:error, {:manifest_status, status}}
      {:error, reason} -> {:error, {:manifest_unreachable, reason}}
    end
  end
end
