defmodule Smolquery.BufferService.HotClient do
  @moduledoc """
  The Elixir client of `Smolquery.BufferService.HotServer`'s HTTP API.

  Everything that reads the hot tier from outside the buffer service — the
  sealer pulling a claim's inputs, the query planner unioning micro-segments
  into a plan — reaches it over HTTP, deliberately: the segment *bytes* must
  come that way regardless, because DuckDB reads them through `httpfs`, which
  speaks HTTP and nothing else. Fetching the manifest over the same transport
  means one connection story instead of two, and means a remote buffer node
  needs nothing new when the cluster arrives.

  Callers hold the base URL, not this module. Where the owning node's
  `HotServer` listens is the caller's configuration (`buffer_base_url` on the
  storage and query runtimes) — honest for a single node; a cluster resolves
  it from the ownership ring instead, which arrives with Milestone 8.
  """

  alias Smolquery.Segments.Store

  @default_timeout_ms 30_000

  @typedoc """
  One micro-segment as the manifest reports it — string-keyed, straight off JSON.

  `"url"` is what a reader opens; `"claim_keys"` is what says which claim it
  belongs to; `"stats"` carries the flush-time min-max bounds a planner prunes
  on.
  """
  @type entry :: %{optional(String.t()) => term()}

  @type option :: {:timeout_ms, timeout()}

  @doc """
  Every micro-segment the buffer node at `base_url` holds for `table_ref`.

  An empty list is a real answer: a table whose tail has already been swept has
  an empty manifest, and so does a table that node never wrote to.
  """
  @spec manifest(String.t(), Store.table_ref(), [option()]) ::
          {:ok, [entry()]} | {:error, term()}
  def manifest(base_url, {dataset, table} = table_ref, opts \\ []) do
    with {:ok, _prefix} <- Store.prefix(table_ref) do
      base_url
      |> url("/v1/datasets/#{dataset}/tables/#{table}/manifest")
      |> get(Keyword.get(opts, :timeout_ms, @default_timeout_ms))
    end
  end

  defp url(base, path), do: String.trim_trailing(base, "/") <> path

  defp get(url, timeout_ms) do
    headers = [{Smolquery.InternalSecret.header(), Smolquery.InternalSecret.value()}]

    case Req.get(url, headers: headers, receive_timeout: timeout_ms, retry: false) do
      {:ok, %Req.Response{status: 200, body: entries}} when is_list(entries) -> {:ok, entries}
      {:ok, %Req.Response{status: 200, body: body}} -> {:error, {:manifest_malformed, body}}
      {:ok, %Req.Response{status: status}} -> {:error, {:manifest_status, status}}
      {:error, reason} -> {:error, {:manifest_unreachable, reason}}
    end
  end
end
