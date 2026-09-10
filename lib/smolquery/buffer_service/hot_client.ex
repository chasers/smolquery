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
  on; `"field_ids"` is the column id each of the file's columns was written
  under, by its name in the file, and what every reader projects the file by
  (PL-62) — absent for a file written before ids existed, which is read by
  name.
  """
  @type entry :: %{optional(String.t()) => term()}

  @type option :: {:timeout_ms, timeout()} | {:ids, [String.t()]} | {:stats, boolean()}

  @doc """
  Every micro-segment the buffer node at `base_url` holds for `table_ref`.

  An empty list is a real answer: a table whose tail has already been swept has
  an empty manifest, and so does a table that node never wrote to.

  ## Options

    * `:ids` — read only these micro-segments. The whole manifest costs the
      serving node an amount that grows with its unsealed backlog, so a caller
      that already knows what it wants should say so (T-316). An id the node no
      longer holds is absent from the answer, exactly as it is from a full read.
    * `:stats` — `false` leaves out the flush-time bounds. They are most of an
      entry's bytes and only a pruning caller reads them. A whole-manifest
      read without them goes out as `GET …/manifest?stats=false` (T-449); a
      node from before that parameter ignores it and answers with the stats,
      which costs bytes and nothing else.
    * `:timeout_ms` — how long to wait for the response.

  A scoped read goes out as a `POST`, because 1,024 ULIDs do not fit in a URL.
  It is still a read.
  """
  @spec manifest(String.t(), Store.table_ref(), [option()]) ::
          {:ok, [entry()]} | {:error, term()}
  def manifest(base_url, {dataset, table} = table_ref, opts \\ []) do
    with {:ok, _prefix} <- Store.prefix(table_ref) do
      base_url
      |> url("/v1/datasets/#{dataset}/tables/#{table}/manifest" <> query(opts))
      |> fetch(scope(opts), Keyword.get(opts, :timeout_ms, @default_timeout_ms))
    end
  end

  defp scope(opts) do
    case Keyword.get(opts, :ids) do
      nil -> nil
      ids -> %{"ids" => ids, "stats" => Keyword.get(opts, :stats, true)}
    end
  end

  defp query(opts) do
    if is_nil(Keyword.get(opts, :ids)) and Keyword.get(opts, :stats, true) == false,
      do: "?stats=false",
      else: ""
  end

  defp url(base, path), do: String.trim_trailing(base, "/") <> path

  defp fetch(url, scope, timeout_ms) do
    headers = [{Smolquery.InternalSecret.header(), Smolquery.InternalSecret.value()}]
    common = [url: url, headers: headers, receive_timeout: timeout_ms, retry: false]

    request =
      if scope, do: [method: :post, json: scope] ++ common, else: [method: :get] ++ common

    case Req.request(request) do
      {:ok, %Req.Response{status: 200, body: entries}} when is_list(entries) -> {:ok, entries}
      {:ok, %Req.Response{status: 200, body: body}} -> {:error, {:manifest_malformed, body}}
      {:ok, %Req.Response{status: status}} -> {:error, {:manifest_status, status}}
      {:error, reason} -> {:error, {:manifest_unreachable, reason}}
    end
  end
end
